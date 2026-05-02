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
//! - `changed()` blocks until a new version arrives; returns `Ok(())` on change, `Err` on disconnect
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
//! tx.send("initial").unwrap();
//!
//! let rx_thread = rx.clone();
//! std::thread::spawn(move || {
//!     assert_eq!(rx_thread.borrow(), Some("initial"));
//!     rx_thread.changed().unwrap(); // Wait for next update
//!     assert_eq!(rx_thread.borrow(), Some("updated"));
//! });
//!
//! std::thread::sleep(Duration::from_millis(10));
//! tx.send("updated").unwrap();
//! ```
//!
//! # Zero-copy reads
//!
//! For expensive-to-clone types, use `borrow_arc()` to avoid cloning:
//!
//! ```ignore
//! let (tx, rx) = watch::channel();
//! tx.send(Arc::new(expensive_data)).unwrap();
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
    sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering::*},
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
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let version = Arc::new(AtomicUsize::new(0));
    let value = Arc::new(ArcSwapOption::empty());
    let recv_waiters = new_recv_waiter_list();
    let select_waiters = Arc::new(AtomicPtr::new(std::ptr::null_mut()));
    let receiver_count = Arc::new(AtomicUsize::new(1));
    let sender_count = Arc::new(AtomicUsize::new(1));
    log_debug!("watch::channel: created chan={:p}", Arc::as_ptr(&version));
    (
        Sender {
            version: Arc::clone(&version),
            value: Arc::clone(&value),
            recv_waiters: Arc::clone(&recv_waiters),
            select_waiters: Arc::clone(&select_waiters),
            receiver_count: Arc::clone(&receiver_count),
            sender_count: Arc::clone(&sender_count),
        },
        Receiver {
            version,
            value,
            recv_waiters,
            select_waiters,
            cursor_version: AtomicUsize::new(0),
            cursor_armed: AtomicBool::new(false),
            receiver_count,
            sender_count,
        },
    )
}

// ════════════════════════════════════════════════════════════════════════════
// Sender<T>
// ════════════════════════════════════════════════════════════════════════════

pub struct Sender<T> {
    version: Arc<AtomicUsize>,
    value: Arc<ArcSwapOption<T>>,
    recv_waiters: RecvWaiterList,
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    receiver_count: Arc<AtomicUsize>,
    sender_count: Arc<AtomicUsize>,
}

impl<T> Sender<T> {
    /// Send a new value, waking all waiting receivers.
    ///
    /// Returns `Err(SendError(value))` if all receivers have been dropped,
    /// giving ownership of the value back to the caller.
    ///
    /// Note: a successful `Ok(())` does not guarantee any receiver is still
    /// alive to observe the value — the last receiver may drop concurrently
    /// after the disconnected check but before the value is stored.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.receiver_count.load(Acquire) == 0 {
            return Err(SendError(value));
        }
        self.value.store(Some(Arc::new(value)));
        self.bump_version_and_notify();
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
        if self.receiver_count.load(Acquire) == 0 {
            return Err(SendError(()));
        }
        self.bump_version_and_notify();
        Ok(())
    }

    /// Check if all receivers have been dropped.
    pub fn is_closed(&self) -> bool {
        self.receiver_count.load(Acquire) == 0
    }

    /// Advance the version counter and wake all blocked receivers and select arms.
    ///
    /// `Release` ordering pairs with `Acquire` loads in receivers so the new
    /// value (if any) is visible before the version bump is observed.
    fn bump_version_and_notify(&self) {
        #[cfg(feature = "debug-logs")]
        let chan_id = Arc::as_ptr(&self.version);
        #[cfg(feature = "debug-logs")]
        let old_version = self.version.load(Relaxed);
        #[cfg_attr(not(feature = "debug-logs"), allow(unused_variables))]
        let new_version = self.version.fetch_add(1, Release) + 1;
        #[cfg(feature = "debug-logs")]
        log_debug!(
            "watch::bump_version: chan={:p}, version={} -> {}",
            chan_id,
            old_version,
            new_version,
        );
        self.wake_all();
    }

    /// Wake all waiting receivers and select arms without bumping the version.
    ///
    /// Used by both `bump_version_and_notify` (after a version change) and
    /// `Drop` (to unblock receivers waiting on a now-gone sender).
    fn wake_all(&self) {
        wake_all_recv_waiters(&self.recv_waiters);
        wake_select_all(&self.select_waiters);
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let prev = self.sender_count.fetch_sub(1, AcqRel);
        if prev == 1 {
            // Last sender dropped — wake all waiting receivers so they can
            // observe the disconnected state.
            self.wake_all();
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.sender_count.fetch_add(1, Relaxed);
        Sender {
            version: Arc::clone(&self.version),
            value: Arc::clone(&self.value),
            recv_waiters: Arc::clone(&self.recv_waiters),
            select_waiters: Arc::clone(&self.select_waiters),
            receiver_count: Arc::clone(&self.receiver_count),
            sender_count: Arc::clone(&self.sender_count),
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
// Receiver<T>
// ════════════════════════════════════════════════════════════════════════════

pub struct Receiver<T> {
    version: Arc<AtomicUsize>,
    value: Arc<ArcSwapOption<T>>,
    recv_waiters: RecvWaiterList,
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    // Per-receiver cursor: not shared across clones, so no Arc needed.
    cursor_version: AtomicUsize,
    cursor_armed: AtomicBool,
    receiver_count: Arc<AtomicUsize>,
    sender_count: Arc<AtomicUsize>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.receiver_count.fetch_add(1, Relaxed);
        Receiver {
            version: Arc::clone(&self.version),
            value: Arc::clone(&self.value),
            recv_waiters: Arc::clone(&self.recv_waiters),
            select_waiters: Arc::clone(&self.select_waiters),
            // Each clone gets its own independent cursor so that one receiver
            // consuming a notification does not suppress it for sibling clones.
            cursor_version: AtomicUsize::new(self.cursor_version.load(Relaxed)),
            cursor_armed: AtomicBool::new(self.cursor_armed.load(Relaxed)),
            receiver_count: Arc::clone(&self.receiver_count),
            sender_count: Arc::clone(&self.sender_count),
        }
    }
}

impl<T> Receiver<T> {
    /// Arm the per-receiver cursor at the current channel version if not already armed.
    ///
    /// Returns the current (live) channel version in all cases.
    ///
    /// - **First call / unarmed**: stores the live version into `cursor_version` and sets
    ///   `cursor_armed`, establishing a change baseline.
    /// - **Already armed**: leaves `cursor_version` untouched and returns the fresh live
    ///   version. Callers can compare the return value against `cursor_version` to detect
    ///   whether a change has occurred since the cursor was last armed.
    ///
    /// The cursor is disarmed (reset to unarmed) when a change is consumed via
    /// `await_change_from` returning `Ok`.
    fn arm_cursor(&self) -> usize {
        let cur = self.version.load(Acquire);
        if !self.cursor_armed.load(Relaxed) {
            self.cursor_version.store(cur, Relaxed);
            self.cursor_armed.store(true, Relaxed);
        }
        cur
    }

    fn await_change_from(&self, baseline: usize) -> Result<usize, RecvError> {
        #[cfg(feature = "debug-logs")]
        let state_id = Arc::as_ptr(&self.version);
        let sel = Arc::new(AtomicUsize::new(UNSELECTED));

        loop {
            let cur = self.version.load(Acquire);
            if cur != baseline {
                self.cursor_version.store(cur, Relaxed);
                self.cursor_armed.store(false, Relaxed);
                #[cfg(feature = "debug-logs")]
                log_debug!(
                    "watch::await_change_from: chan={:p}, observed version={} -> {}",
                    state_id,
                    baseline,
                    cur
                );
                return Ok(cur);
            }
            if self.sender_count.load(Acquire) == 0 {
                self.cursor_armed.store(false, Relaxed);
                #[cfg(feature = "debug-logs")]
                log_debug!(
                    "watch::await_change_from: chan={:p}, disconnected while waiting",
                    state_id
                );
                return Err(RecvError::Disconnected);
            }

            // `sel` is declared above the loop so the Arc allocation happens
            // once; we clone it per iteration because RecvWaiter takes
            // ownership and the guard drops at loop-bottom, deregistering the
            // waiter before we park. The clone is a cheap refcount bump.
            let waiter = RecvWaiter::new(0, Arc::clone(&sel));
            let _guard = RecvWaiterGuard::register(waiter, &self.recv_waiters);
            #[cfg(feature = "debug-logs")]
            log_debug!(
                "watch::await_change_from: chan={:p}, waiting on version={}",
                state_id,
                baseline
            );
            // Re-check after registering to close the lost-wakeup window.
            if self.version.load(Acquire) != baseline || self.sender_count.load(Acquire) == 0 {
                continue;
            }
            thread::park();
        }
    }

    /// Snapshot the current value as `Arc<T>` without taking any lock.
    pub fn borrow_arc(&self) -> Option<Arc<T>> {
        self.value.load_full()
    }

    /// Borrow the current value, if any.
    ///
    /// Returns a clone of the currently stored value via the lock-free `ArcSwapOption`.
    pub fn borrow(&self) -> Option<T>
    where
        T: Clone,
    {
        #[cfg(feature = "debug-logs")]
        let state_id = Arc::as_ptr(&self.version);
        #[cfg(feature = "debug-logs")]
        let version = self.version.load(Relaxed);
        let snapshot = self.borrow_arc().as_deref().cloned();
        #[cfg(feature = "debug-logs")]
        log_debug!("watch::borrow: chan={:p}, version={}", state_id, version);
        snapshot
    }

    /// Returns `true` if all [`Sender`] handles have been dropped.
    pub fn is_closed(&self) -> bool {
        self.sender_count.load(Acquire) == 0
    }

    /// Wait until the value changes.
    ///
    /// Blocks until a new version is published or all senders are dropped.
    /// Returns `Ok(())` on a successful change, `Err(RecvError::Disconnected)`
    /// if all senders have been dropped.
    ///
    /// **Baseline semantics**: `changed()` always uses the *live* channel version as
    /// its change baseline (via `arm_cursor`). If the cursor was previously armed by
    /// an `is_ready()` call, any version change that occurred between that arming and
    /// this call will be skipped — use `complete_recv()` to consume a change that
    /// was already observed by `is_ready()`.
    pub fn changed(&self) -> Result<(), RecvError> {
        #[cfg(feature = "debug-logs")]
        let state_id = Arc::as_ptr(&self.version);
        if self.sender_count.load(Acquire) == 0 {
            self.cursor_armed.store(false, Relaxed);
            return Err(RecvError::Disconnected);
        }
        // arm_cursor() returns the live channel version. Whether or not the
        // cursor was previously armed, we pass this live version as the
        // baseline: await_change_from blocks until the version exceeds it.
        // Any changes that occurred between a prior is_ready() arming and
        // this call are therefore skipped; use complete_recv() instead
        // to consume a change already observed by is_ready().
        let current_version = self.arm_cursor();
        #[cfg(feature = "debug-logs")]
        log_debug!(
            "watch::changed: chan={:p}, current_version={}",
            state_id,
            current_version
        );
        self.await_change_from(current_version).map(|_| ())
    }

    // ── Hooks for select! integration ─────────────────────────────

    /// True if the value has changed since last check.
    pub(crate) fn is_ready(&self) -> bool {
        if self.sender_count.load(Acquire) == 0 {
            return true;
        }
        let cur = self.arm_cursor();
        cur != self.cursor_version.load(Relaxed)
    }

    /// Register a select waiter.
    pub(crate) fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        log_debug!(
            "watch::register_select: chan={:p}, case_id={}",
            Arc::as_ptr(&self.version),
            case_id
        );
        let cur = self.arm_cursor();
        if self.sender_count.load(Acquire) == 0 || cur != self.cursor_version.load(Relaxed) {
            return;
        }
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.select_waiters);
    }

    /// Abort select waiter.
    pub(crate) fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        log_debug!("watch::abort_select: chan={:p}", Arc::as_ptr(&self.version));
        abort_select_waiters(&self.select_waiters, selected);
    }

    /// Complete the select operation: block until the version seen by `is_ready()`
    /// is consumed, then reset the cursor.
    pub(crate) fn complete_recv(&self) -> Result<(), RecvError> {
        let baseline = self.cursor_version.load(Relaxed);
        self.await_change_from(baseline).map(|_| ())
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.receiver_count.fetch_sub(1, AcqRel) == 1 {
            drain_select_waiters(&self.select_waiters);
        }
    }
}

impl_selectable_receiver!([T] Receiver<T>, ());

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn watch_channel_broadcasts_changes() {
        let (tx, rx) = channel::<i32>();
        assert!(rx.borrow().is_none());

        tx.send(42).unwrap();
        assert_eq!(rx.borrow(), Some(42));

        let rx2 = rx.clone();
        tx.send(100).unwrap();
        assert_eq!(rx.borrow(), Some(100));
        assert_eq!(rx2.borrow(), Some(100));
    }

    #[test]
    fn watch_changed_waits_for_update() {
        let (tx, rx) = channel::<i32>();
        tx.send(1).unwrap();

        let handle = thread::spawn(move || {
            rx.changed().unwrap();
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
        assert!(!rx.is_ready()); // arms rx at version 1
        assert!(!clone.is_ready()); // arms clone at version 1, independently

        tx.send(2).unwrap(); // version = 2

        // Both see version 2 > their cursor of 1.
        assert!(rx.is_ready());
        assert!(clone.is_ready());

        // Completing via rx advances only rx's cursor, not clone's.
        assert_eq!(rx.complete_recv(), Ok(()));
        assert!(!rx.is_ready()); // rx cursor now at 2
        assert!(clone.is_ready()); // clone cursor still at 1 < 2

        tx.send(3).unwrap();
        assert!(rx.is_ready());
        assert_eq!(rx.complete_recv(), Ok(()));
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
    fn mark_changed_returns_err_when_no_receivers_no_value() {
        // Verify mark_changed() returns Err even when no value has ever been
        // stored — guards against any future code assuming a value exists.
        let (tx, rx) = channel::<i32>();
        drop(rx);
        assert_eq!(tx.mark_changed(), Err(SendError(())));
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
        assert_eq!(rx.borrow(), Some("initial"));

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

        let h1 = thread::spawn(move || rx1.changed());
        let h2 = thread::spawn(move || rx2.changed());
        let h3 = thread::spawn(move || rx3.changed());

        thread::sleep(Duration::from_millis(20));
        tx.send(1).unwrap();

        // All three should wake with Ok(()).
        assert_eq!(h1.join().unwrap(), Ok(()));
        assert_eq!(h2.join().unwrap(), Ok(()));
        assert_eq!(h3.join().unwrap(), Ok(()));
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
    fn multi_receiver_all_detect_sender_disconnect() {
        // Regression test: with Arc::strong_count-based detection, cloned
        // receivers never observed Disconnected when the sender dropped.
        const N: usize = 4;
        let (tx, rx) = channel::<i32>();
        tx.send(0).unwrap(); // establish a baseline version

        let mut handles = Vec::new();
        for _ in 0..N {
            let rx_clone = rx.clone();
            handles.push(thread::spawn(move || rx_clone.changed()));
        }

        thread::sleep(Duration::from_millis(20)); // let all threads reach park
        drop(tx);

        for h in handles {
            assert_eq!(
                h.join().unwrap(),
                Err(crate::error::RecvError::Disconnected)
            );
        }
    }

    #[test]
    fn borrow_reflects_latest_send_immediately() {
        let (tx, rx) = channel::<i32>();
        assert!(rx.borrow().is_none());

        tx.send(7).unwrap();
        assert_eq!(rx.borrow(), Some(7));

        tx.send(8).unwrap();
        assert_eq!(rx.borrow(), Some(8));
    }

    #[test]
    fn borrow_arc_reflects_latest_send_immediately() {
        let (tx, rx) = channel::<i32>();
        assert!(rx.borrow_arc().is_none());

        tx.send(7).unwrap();
        assert_eq!(*rx.borrow_arc().unwrap(), 7);

        tx.send(8).unwrap();
        assert_eq!(*rx.borrow_arc().unwrap(), 8);
    }

    #[test]
    fn is_closed_reflects_sender_liveness() {
        let (tx, rx) = channel::<i32>();
        assert!(!rx.is_closed());
        let tx2 = tx.clone();
        drop(tx);
        assert!(!rx.is_closed()); // tx2 still alive
        drop(tx2);
        assert!(rx.is_closed());
    }

    #[test]
    fn rapid_sends_borrow_shows_latest() {
        let (tx, rx) = channel::<u32>();
        for i in 0..100u32 {
            tx.send(i).unwrap();
        }
        // borrow() returns the most recently sent value.
        assert_eq!(rx.borrow(), Some(99));
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
            recv(rx) -> res => assert_eq!(res, Ok(())),
            default(Duration::from_millis(200)) => panic!("timeout"),
        }
    }

    #[test]
    fn multiple_senders_last_write_wins() {
        // All senders share the same ArcSwapOption; only the latest send is visible.
        let (tx1, rx) = channel::<i32>();
        let tx2 = tx1.clone();

        tx1.send(10).unwrap();
        tx2.send(20).unwrap();

        // Last write wins; only 20 is visible.
        assert_eq!(rx.borrow(), Some(20));
    }

    #[test]
    fn sender_clone_extends_liveness() {
        let (tx1, rx) = channel::<i32>();
        let tx2 = tx1.clone();

        drop(tx1);
        // rx should not observe disconnect yet — tx2 is still alive.
        assert!(!rx.is_closed());
        assert!(!tx2.is_closed());

        tx2.send(42).unwrap();
        assert_eq!(rx.borrow(), Some(42));

        drop(tx2);
        assert!(rx.is_closed());
        assert_eq!(rx.changed(), Err(crate::error::RecvError::Disconnected));
    }
}
