//! Latest-value broadcast channel with versioned change notifications.
//!
//! # Overview
//!
//! A watch channel broadcasts a single shared value to multiple receivers. When the sender
//! updates the value, all receivers are notified and see the new state. This is useful for
//! configuration updates, status broadcasts, and shared state patterns.
//!
//! # Semantics
//!
//! - `Sender` can call `send()` multiple times; each updates the shared value
//! - `send()` returns `Err(SendError(value))` if all receivers have been dropped
//! - `mark_changed()` advances the version and wakes receivers without updating the stored value
//! - `Receiver` can be cloned; all clones share the same value and version counter
//! - `borrow()` (requires `T: Clone`) returns a clone of the current value
//! - `borrow_arc()` returns `Option<Arc<T>>` for zero-copy access (no clone needed)
//! - `changed()` blocks until a new version arrives (version-based, not value-based)
//! - Only the latest value is stored; intermediate updates are not queued
//!
//! # Lock-free reads
//!
//! - `borrow()` and `borrow_arc()` use lock-free `ArcSwapOption` reads (no locks)
//! - Version tracking uses a lock-free `AtomicUsize`; no Mutex anywhere in this channel
//!
//! # Example
//!
//! ```ignore
//! let (tx, rx) = watch::channel();
//! tx.send("initial");
//!
//! let rx1 = rx.clone();
//! std::thread::spawn(move || {
//!     assert_eq!(*rx.borrow(), Some("initial"));
//!     rx.changed().ok(); // Wait for next update
//!     assert_eq!(*rx.borrow(), Some("updated"));
//! });
//!
//! std::thread::sleep(Duration::from_millis(10));
//! tx.send("updated");
//! ```
//!
//! # Zero-copy reads
//!
//! For expensive-to-clone types, use `borrow_arc()` to avoid cloning:
//!
//! ```ignore
//! let (tx, rx) = watch::channel();
//! tx.send(Arc::new(expensive_data));
//!
//! if let Some(arc_data) = rx.borrow_arc() {
//!     // Use arc_data without cloning; multiple threads can share it
//! }
//! ```
//!
//! # Difference from broadcast
//!
//! Unlike `bounded_broadcast`:
//! - Watch stores *only the latest value* (not a history)
//! - No lag detection (always see current state)
//! - Better for state subscriptions than for message queues

use std::{
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
    thread,
};

use arc_swap::ArcSwapOption;

use crate::{
    error::{RecvError, SendError},
    waiter::{
        RecvWaiter, RecvWaiterGuard, RecvWaiterList, SelectWaiter, UNSELECTED,
        abort_select_waiters, drain_select_waiters, new_recv_waiter_list, push_select_waiter,
        wake_all_recv_waiters, wake_select_all,
    },
};

// ════════════════════════════════════════════════════════════════════════════
// Watch channel constructors
// ════════════════════════════════════════════════════════════════════════════

/// Create a watch channel for broadcasting value changes.
/// Returns a sender for updating the value and a receiver for watching changes.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let version = Arc::new(AtomicUsize::new(0));
    let value = Arc::new(ArcSwapOption::empty());
    let recv_waiters = new_recv_waiter_list();
    let select_waiters = Arc::new(AtomicPtr::new(std::ptr::null_mut()));
    let receiver_count = Arc::new(AtomicUsize::new(1));
    log_debug!("watch::channel: created chan={:p}", Arc::as_ptr(&version));
    (
        Sender {
            version: Arc::clone(&version),
            value: Arc::clone(&value),
            recv_waiters: Arc::clone(&recv_waiters),
            select_waiters: Arc::clone(&select_waiters),
            receiver_count: Arc::clone(&receiver_count),
        },
        Receiver {
            version,
            value,
            recv_waiters,
            select_waiters,
            wait_version: Arc::new(AtomicUsize::new(0)),
            wait_armed: Arc::new(AtomicBool::new(false)),
            receiver_count,
        },
    )
}

// ════════════════════════════════════════════════════════════════════════════
// Watch borrow guard
// ════════════════════════════════════════════════════════════════════════════

pub struct Ref<T> {
    snapshot: Option<T>,
}

impl<T> std::ops::Deref for Ref<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Watch channel internals
// ════════════════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════════════════
// WatchSender<T>
// ════════════════════════════════════════════════════════════════════════════

pub struct Sender<T> {
    version: Arc<AtomicUsize>,
    value: Arc<ArcSwapOption<T>>,
    recv_waiters: RecvWaiterList,
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    receiver_count: Arc<AtomicUsize>,
}

impl<T> Sender<T> {
    /// Send a new value, waking all waiting receivers.
    ///
    /// Returns `Err(SendError(value))` if all receivers have been dropped,
    /// giving ownership of the value back to the caller.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.receiver_count.load(Ordering::Acquire) == 0 {
            return Err(SendError(value));
        }

        #[cfg(feature = "debug-logs")]
        let chan_id = Arc::as_ptr(&self.version);
        #[cfg(feature = "debug-logs")]
        let old_version = self.version.load(Ordering::SeqCst);

        // Publish value then advance version atomically (SeqCst ensures ordering).
        self.value.store(Some(Arc::new(value)));
        #[cfg_attr(not(feature = "debug-logs"), allow(unused_variables))]
        let new_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        #[cfg(feature = "debug-logs")]
        log_debug!(
            "watch::send: chan={:p}, version={} -> {}",
            chan_id,
            old_version,
            new_version,
        );

        // Wake all lock-free recv waiters
        wake_all_recv_waiters(&self.recv_waiters, UNSELECTED);

        // Wake all select waiters (lock-free, frees nodes)
        wake_select_all(&self.select_waiters);

        Ok(())
    }

    /// Signal all receivers that the value has changed, without updating it.
    ///
    /// Advances the version counter and wakes all waiting receivers, exactly as
    /// `send()` would, but leaves the stored value untouched. Receivers will
    /// unblock from `changed()` or select arms and can re-read the same value.
    ///
    /// Returns `Err(SendError(()))` if all receivers have been dropped.
    pub fn mark_changed(&self) -> Result<(), SendError<()>> {
        if self.receiver_count.load(Ordering::Acquire) == 0 {
            return Err(SendError(()));
        }

        #[cfg(feature = "debug-logs")]
        let chan_id = Arc::as_ptr(&self.version);
        #[cfg(feature = "debug-logs")]
        let old_version = self.version.load(Ordering::SeqCst);

        // Advance version without touching the stored value.
        #[cfg_attr(not(feature = "debug-logs"), allow(unused_variables))]
        let new_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        #[cfg(feature = "debug-logs")]
        log_debug!(
            "watch::mark_changed: chan={:p}, version={} -> {}",
            chan_id,
            old_version,
            new_version,
        );

        // Wake all lock-free recv waiters
        wake_all_recv_waiters(&self.recv_waiters, UNSELECTED);

        // Wake all select waiters (lock-free, frees nodes)
        wake_select_all(&self.select_waiters);

        Ok(())
    }

    /// Check if all receivers have been dropped.
    pub fn is_closed(&self) -> bool {
        self.receiver_count.load(Ordering::Acquire) == 0
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            version: Arc::clone(&self.version),
            value: Arc::clone(&self.value),
            recv_waiters: Arc::clone(&self.recv_waiters),
            select_waiters: Arc::clone(&self.select_waiters),
            receiver_count: Arc::clone(&self.receiver_count),
        }
    }
}

impl<T: Send + 'static> crate::SelectableSender for Sender<T> {
    type Input = T;

    /// Watch send is always immediately ready (overwrite semantics, no capacity limit).
    fn is_ready(&self) -> bool {
        true
    }

    /// No registration needed — watch senders are always ready.
    fn register_select(&self, _case_id: usize, _selected: Arc<AtomicUsize>) {}

    fn abort_select(&self, _selected: &Arc<AtomicUsize>) {}

    fn complete_send(&self, value: T) -> Result<(), crate::SendError<T>> {
        self.send(value)
    }
}

// ════════════════════════════════════════════════════════════════════════════
// WatchReceiver<T>
// ════════════════════════════════════════════════════════════════════════════

pub struct Receiver<T> {
    version: Arc<AtomicUsize>,
    value: Arc<ArcSwapOption<T>>,
    recv_waiters: RecvWaiterList,
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    wait_version: Arc<AtomicUsize>,
    wait_armed: Arc<AtomicBool>,
    receiver_count: Arc<AtomicUsize>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.receiver_count.fetch_add(1, Ordering::Relaxed);
        Receiver {
            version: Arc::clone(&self.version),
            value: Arc::clone(&self.value),
            recv_waiters: Arc::clone(&self.recv_waiters),
            select_waiters: Arc::clone(&self.select_waiters),
            // Each clone gets its own independent cursor so that one receiver
            // consuming a notification does not suppress it for sibling clones.
            wait_version: Arc::new(AtomicUsize::new(
                self.wait_version.load(Ordering::SeqCst),
            )),
            wait_armed: Arc::new(AtomicBool::new(
                self.wait_armed.load(Ordering::SeqCst),
            )),
            receiver_count: Arc::clone(&self.receiver_count),
        }
    }
}

impl<T> Receiver<T> {
    fn snapshot_wait_version(&self) -> Result<usize, RecvError> {
        if Arc::strong_count(&self.version) == 1 {
            self.wait_armed.store(false, Ordering::SeqCst);
            return Err(RecvError::Disconnected);
        }
        let v = self.version.load(Ordering::SeqCst);
        self.wait_version.store(v, Ordering::SeqCst);
        self.wait_armed.store(true, Ordering::SeqCst);
        Ok(v)
    }

    fn await_change_from(&self, baseline: usize) -> Result<usize, RecvError> {
        #[cfg(feature = "debug-logs")]
        let state_id = Arc::as_ptr(&self.version);
        let sel = Arc::new(AtomicUsize::new(UNSELECTED));

        loop {
            let cur = self.version.load(Ordering::SeqCst);
            if cur != baseline {
                self.wait_version.store(cur, Ordering::SeqCst);
                self.wait_armed.store(false, Ordering::SeqCst);
                #[cfg(feature = "debug-logs")]
                log_debug!(
                    "watch::await_change_from: chan={:p}, observed version={} -> {}",
                    state_id,
                    baseline,
                    cur
                );
                return Ok(cur);
            }
            if Arc::strong_count(&self.version) == 1 {
                self.wait_armed.store(false, Ordering::SeqCst);
                #[cfg(feature = "debug-logs")]
                log_debug!(
                    "watch::await_change_from: chan={:p}, disconnected while waiting",
                    state_id
                );
                return Err(RecvError::Disconnected);
            }

            // Register on lock-free stack for simple recv blocking
            let waiter = RecvWaiter::new(0, Arc::clone(&sel));
            let _guard = RecvWaiterGuard::register(waiter, &self.recv_waiters);
            #[cfg(feature = "debug-logs")]
            log_debug!(
                "watch::await_change_from: chan={:p}, waiting on version={}",
                state_id,
                baseline
            );
            thread::park_timeout(std::time::Duration::from_secs(1));
        }
    }

    /// Snapshot the current value as `Arc<T>` without taking any lock.
    pub fn borrow_arc(&self) -> Option<Arc<T>> {
        self.value.load_full()
    }

    /// Borrow the current value, if any.
    ///
    /// This remains ergonomic for `*rx.borrow()` style call sites by cloning
    /// from the lockless `ArcSwapOption` snapshot.
    pub fn borrow(&self) -> Ref<T>
    where
        T: Clone,
    {
        #[cfg(feature = "debug-logs")]
        let state_id = Arc::as_ptr(&self.version);
        #[cfg(feature = "debug-logs")]
        let version = self.version.load(Ordering::SeqCst);
        let snapshot = self.borrow_arc().as_deref().cloned();
        #[cfg(feature = "debug-logs")]
        log_debug!("watch::borrow: chan={:p}, version={}", state_id, version);
        Ref { snapshot }
    }

    /// Wait for a change in the value.
    /// Returns the new version number.
    pub fn changed(&self) -> Result<usize, RecvError> {
        #[cfg(feature = "debug-logs")]
        let state_id = Arc::as_ptr(&self.version);
        let current_version = self.snapshot_wait_version()?;
        #[cfg(feature = "debug-logs")]
        log_debug!(
            "watch::changed: chan={:p}, current_version={}",
            state_id,
            current_version
        );

        self.await_change_from(current_version)
    }

    // ── Hooks for select! integration ─────────────────────────────

    /// True if the value has changed since last check.
    pub(crate) fn is_ready(&self) -> bool {
        if Arc::strong_count(&self.version) == 1 {
            return true;
        }
        let cur = self.version.load(Ordering::SeqCst);
        if !self.wait_armed.load(Ordering::SeqCst) {
            self.wait_version.store(cur, Ordering::SeqCst);
            self.wait_armed.store(true, Ordering::SeqCst);
            return false;
        }
        cur != self.wait_version.load(Ordering::SeqCst)
    }

    /// Register a select waiter.
    pub(crate) fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        log_trace!(
            "watch::register_select: chan={:p}, case_id={}",
            Arc::as_ptr(&self.version),
            case_id
        );
        let cur = self.version.load(Ordering::SeqCst);
        if !self.wait_armed.load(Ordering::SeqCst) {
            self.wait_version.store(cur, Ordering::SeqCst);
            self.wait_armed.store(true, Ordering::SeqCst);
        }
        if Arc::strong_count(&self.version) == 1 || cur != self.wait_version.load(Ordering::SeqCst)
        {
            return;
        }
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.select_waiters);
    }

    /// Abort select waiter.
    pub(crate) fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        log_trace!("watch::abort_select: chan={:p}", Arc::as_ptr(&self.version));
        abort_select_waiters(&self.select_waiters, selected);
    }

    /// Complete the select operation.
    pub fn complete_changed(&self) -> Result<usize, RecvError> {
        // Use the stored wait_version as the baseline directly.
        // The select! macro calls complete() on the *original* receiver (not the
        // armed clone stored in Select), so we cannot rely on wait_armed being
        // set. Using wait_version (which defaults to 0) as the baseline means we
        // return immediately if any version > baseline already exists.
        let baseline = self.wait_version.load(Ordering::SeqCst);
        self.await_change_from(baseline)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.receiver_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            drain_select_waiters(&self.select_waiters);
        }
    }
}

impl<T> crate::SelectableReceiver for Receiver<T> {
    type Output = usize;

    fn is_ready(&self) -> bool {
        self.is_ready()
    }

    fn register_select(
        &self,
        case_id: usize,
        selected: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        self.register_select(case_id, selected)
    }

    fn abort_select(&self, selected: &std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        self.abort_select(selected)
    }

    fn complete(&self) -> Result<Self::Output, crate::RecvError> {
        self.complete_changed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn watch_channel_broadcasts_changes() {
        let (tx, rx) = channel::<i32>();
        assert!(rx.borrow().is_none());

        tx.send(42).unwrap();
        assert_eq!(*rx.borrow(), Some(42));

        let rx2 = rx.clone();
        tx.send(100).unwrap();
        assert_eq!(*rx.borrow(), Some(100));
        assert_eq!(*rx2.borrow(), Some(100));
    }

    #[test]
    fn watch_changed_waits_for_update() {
        let (tx, rx) = channel::<i32>();
        tx.send(1).unwrap();

        let handle = thread::spawn(move || {
            assert_eq!(rx.changed().unwrap(), 2);
            rx.borrow().unwrap()
        });

        thread::sleep(Duration::from_millis(10));
        tx.send(2).unwrap();

        assert_eq!(handle.join().unwrap(), 2);
    }

    #[test]
    fn clones_have_independent_cursors() {
        let (tx, rx) = channel::<i32>();
        tx.send(1).unwrap(); // version = 1

        let clone = rx.clone();

        // is_ready() arms each receiver at the current version (establishes
        // baseline); returns false because no new change has occurred since
        // arming. The two cursors are independent.
        assert!(!rx.is_ready());    // arms rx at version 1
        assert!(!clone.is_ready()); // arms clone at version 1, independently

        tx.send(2).unwrap(); // version = 2

        // Both see version 2 > their cursor of 1.
        assert!(rx.is_ready());
        assert!(clone.is_ready());

        // Completing via rx advances only rx's cursor, not clone's.
        assert_eq!(rx.complete_changed(), Ok(2));
        assert!(!rx.is_ready());   // rx cursor now at 2
        assert!(clone.is_ready()); // clone cursor still at 1 < 2

        tx.send(3).unwrap();
        assert!(rx.is_ready());
        assert_eq!(rx.complete_changed(), Ok(3));
    }

    #[test]
    fn send_returns_err_when_no_receivers() {
        let (tx, rx) = channel::<i32>();
        assert!(!tx.is_closed());
        drop(rx);
        assert!(tx.is_closed());
        assert_eq!(tx.send(42), Err(SendError(42)));
    }

    #[test]
    fn mark_changed_wakes_without_updating_value() {
        let (tx, rx) = channel::<&str>();
        tx.send("initial").unwrap();

        let rx2 = rx.clone();
        let handle = thread::spawn(move || {
            rx2.changed().unwrap();
            rx2.borrow_arc().map(|a| *a)
        });

        thread::sleep(Duration::from_millis(10));
        tx.mark_changed().unwrap();

        // Thread wakes and the value is still "initial" — not updated
        assert_eq!(handle.join().unwrap(), Some("initial"));
        assert_eq!(*rx.borrow(), Some("initial"));

        // Returns Err when all receivers dropped
        drop(rx);
        assert_eq!(tx.mark_changed(), Err(SendError(())));
    }

    #[test]
    fn multiple_receivers_all_wake_on_change() {
        let (tx, rx1) = channel::<i32>();
        tx.send(0).unwrap(); // establish baseline

        let rx2 = rx1.clone();
        let rx3 = rx1.clone();

        let h1 = thread::spawn(move || rx1.changed().unwrap());
        let h2 = thread::spawn(move || rx2.changed().unwrap());
        let h3 = thread::spawn(move || rx3.changed().unwrap());

        thread::sleep(Duration::from_millis(20));
        tx.send(1).unwrap();

        // All three should wake and return version 2 (second send).
        assert_eq!(h1.join().unwrap(), 2);
        assert_eq!(h2.join().unwrap(), 2);
        assert_eq!(h3.join().unwrap(), 2);
    }

    #[test]
    fn changed_returns_err_when_sender_drops() {
        let (tx, rx) = channel::<i32>();
        tx.send(1).unwrap();

        let handle = thread::spawn(move || rx.changed());
        thread::sleep(Duration::from_millis(20));
        drop(tx);
        assert_eq!(
            handle.join().unwrap(),
            Err(crate::error::RecvError::Disconnected)
        );
    }

    #[test]
    fn borrow_reflects_latest_send_immediately() {
        let (tx, rx) = channel::<i32>();
        assert!(rx.borrow_arc().is_none());

        tx.send(7).unwrap();
        assert_eq!(*rx.borrow_arc().unwrap(), 7);

        tx.send(8).unwrap();
        assert_eq!(*rx.borrow_arc().unwrap(), 8);
    }

    #[test]
    fn rapid_sends_borrow_shows_latest() {
        let (tx, rx) = channel::<u32>();
        for i in 0..100u32 {
            tx.send(i).unwrap();
        }
        // borrow() returns the most recently sent value.
        assert_eq!(*rx.borrow_arc().unwrap(), 99);
    }

    #[test]
    fn select_recv_arm_fires_on_change() {
        use crate::select;

        let (tx, rx) = channel::<i32>();
        tx.send(1).unwrap(); // prime the baseline

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(15));
            tx.send(2).unwrap();
        });

        // Blocking select: fires when watch version changes.
        select! {
            recv(rx) -> ver => assert_eq!(ver, Ok(2)),
            default(Duration::from_millis(200)) => panic!("timeout"),
        }
    }
}
