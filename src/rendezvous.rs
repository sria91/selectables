//! Rendezvous channel: synchronous, zero-buffer handoff between sender and receiver.
//!
//! # Overview
//!
//! A rendezvous channel has no internal buffer. A call to `send()` blocks until a receiver
//! is simultaneously calling `recv()`, at which point the value is transferred directly.
//! Likewise, `recv()` blocks until a sender parks itself. This achieves a synchronisation
//! point — both threads must be ready before the exchange occurs.
//!
//! This is distinct from `bounded_mpmc::channel(0)` or `bounded_mpsc::channel(0)`, which
//! make `send()` fail immediately when the buffer is full (capacity 0 means always full).
//! Use `rendezvous::channel()` when you want true synchronous handoff semantics.
//!
//! # Semantics
//!
//! - `send()` parks the calling thread until a receiver takes the value; returns `Err` if
//!   all receivers are dropped before the value is taken.
//! - `try_recv()` returns the value from a parked sender if one is waiting, or `Err` otherwise.
//! - `recv()` blocks until a sender arrives or all senders disconnect.
//! - Neither `Sender` nor `Receiver` is `Clone`: this is a strictly SPSC channel.
//!
//! # Example
//!
//! ```ignore
//! use std::thread;
//! use selectables::rendezvous;
//!
//! let (tx, rx) = rendezvous::channel::<i32>();
//!
//! let handle = thread::spawn(move || {
//!     rx.recv().unwrap()
//! });
//!
//! tx.send(42).unwrap(); // blocks until handle's recv() takes the value
//! assert_eq!(handle.join().unwrap(), 42);
//! ```
//!
//! # Integration with select!
//!
//! `Receiver` implements `SelectableReceiver`, so it participates in select arms:
//!
//! ```ignore
//! select! {
//!     recv(rx) -> msg => println!("Got: {:?}", msg),
//!     default(Duration::from_millis(10)) => println!("timeout"),
//! }
//! ```

use std::{
    cell::UnsafeCell,
    mem::ManuallyDrop,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering::*},
    },
    thread,
};

use crate::{
    error::{RecvError, SendError, TryRecvError},
    internals::UNSELECTED,
    waiter::{
        RecvWaiter, RecvWaiterGuard, RecvWaiterList, SelectWaiter, abort_select_waiters,
        drain_select_waiters, new_recv_waiter_list, push_select_waiter, wake_all_recv_waiters,
        wake_select_all, wake_select_one,
    },
};

// ════════════════════════════════════════════════════════════════════════════
// SenderWaiter — intrusive stack node for a blocked sender
// ════════════════════════════════════════════════════════════════════════════

/// A stack-allocated node representing a sender blocked in `send()`.
///
/// The receiver reads `value` via `ptr::read`, then stores `taken = true` with
/// Release ordering before unparking the sender. The sender checks `taken` with
/// Acquire ordering before returning from `send()`, guaranteeing it doesn't
/// access the `UnsafeCell` again after the receiver has read it.
///
/// # Safety invariant
/// The node lives on the sender's stack frame. The sender must not return from
/// `send()` until either:
/// - `taken` is `true` (receiver has completed the read), or
/// - the sender has successfully removed itself from the stack (disconnect path).
struct SenderWaiter<T> {
    /// Value to be taken by a receiver. Written once by sender before push;
    /// read once by receiver via `ptr::read` while `taken == false`.
    value: UnsafeCell<ManuallyDrop<T>>,
    /// Set to `true` by the receiver after `ptr::read` completes (Release).
    /// Sender checks with Acquire before returning from `send()`.
    taken: AtomicBool,
    /// Thread to unpark after `taken` is set.
    thread: thread::Thread,
    /// Intrusive link to the next node in the stack.
    next: AtomicPtr<SenderWaiter<T>>,
}

// SAFETY: `T: Send` ensures the value can move across threads.
// Access is serialized by the `taken` protocol: sender writes before push,
// receiver reads only while `taken == false`, sender destroys only after
// `taken == true` or successful self-removal.
unsafe impl<T: Send> Send for SenderWaiter<T> {}
unsafe impl<T: Send> Sync for SenderWaiter<T> {}

impl<T> SenderWaiter<T> {
    fn new(value: T) -> Self {
        SenderWaiter {
            value: UnsafeCell::new(ManuallyDrop::new(value)),
            taken: AtomicBool::new(false),
            thread: thread::current(),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Chan — shared channel state
// ════════════════════════════════════════════════════════════════════════════

struct Chan<T> {
    /// Lock-free LIFO stack of parked senders. Head = most recently parked.
    sender_waiters: AtomicPtr<SenderWaiter<T>>,
    /// Lock-free stack of parked plain-recv() threads.
    recv_waiters: RecvWaiterList,
    /// Lock-free stack of parked select recv-arms.
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    /// Lock-free stack of parked select send-arms.
    send_select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    /// Number of live `Sender<T>` handles.
    sender_count: AtomicUsize,
    /// Number of live `Receiver<T>` handles.
    receiver_count: AtomicUsize,
}

// ════════════════════════════════════════════════════════════════════════════
// Sender
// ════════════════════════════════════════════════════════════════════════════

pub struct Sender<T>(Arc<Chan<T>>);

impl<T: Send> Sender<T> {
    /// Send a value, blocking until a receiver takes it.
    ///
    /// Returns `Err(SendError(value))` if all receivers have been dropped before
    /// the value was taken.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        // Fast disconnect check before allocating the waiter.
        if self.0.receiver_count.load(Acquire) == 0 {
            return Err(SendError(value));
        }

        // Place the waiter on this thread's stack frame.
        let waiter = SenderWaiter::new(value);
        let waiter_ptr = &waiter as *const SenderWaiter<T> as *mut SenderWaiter<T>;

        // CAS-push onto the sender_waiters stack.
        loop {
            let head = self.0.sender_waiters.load(Acquire);
            waiter.next.store(head, Relaxed);
            if self
                .0
                .sender_waiters
                .compare_exchange(head, waiter_ptr, AcqRel, Acquire)
                .is_ok()
            {
                break;
            }
        }

        // Notify one waiting receiver that a sender has parked.
        // This covers both plain recv() and select arms.
        wake_all_recv_waiters(&self.0.recv_waiters, UNSELECTED);
        wake_select_one(&self.0.select_waiters);

        // Park loop: wait until receiver sets `taken` or all receivers drop.
        loop {
            // Check first (covers the case where receiver acted before we parked).
            if waiter.taken.load(Acquire) {
                // Receiver completed ptr::read and released the node.
                return Ok(());
            }
            if self.0.receiver_count.load(Acquire) == 0 {
                // No receivers left. Remove ourselves from the stack.
                self.remove_sender_waiter(waiter_ptr);
                // SAFETY: `taken` is still false, so no receiver touched the value.
                // We recover it by reading from the UnsafeCell.
                let val = unsafe { ManuallyDrop::into_inner(ptr::read(waiter.value.get())) };
                return Err(SendError(val));
            }
            thread::park();
        }
    }

    /// Remove `ptr` from the sender_waiters stack via CAS traversal.
    /// Called only on the disconnect path when `taken` is still `false`.
    fn remove_sender_waiter(&self, ptr: *mut SenderWaiter<T>) {
        loop {
            let head = self.0.sender_waiters.load(Acquire);
            if head.is_null() {
                return;
            }
            // If we're the head, swing the head to next.
            if head == ptr {
                let next = unsafe { (*ptr).next.load(Acquire) };
                if self
                    .0
                    .sender_waiters
                    .compare_exchange(head, next, AcqRel, Acquire)
                    .is_ok()
                {
                    return;
                }
                continue; // CAS failed, retry
            }
            // Otherwise traverse to find ourselves.
            let mut current = head;
            loop {
                let next_ptr = unsafe { (*current).next.load(Acquire) };
                if next_ptr == ptr {
                    let my_next = unsafe { (*ptr).next.load(Acquire) };
                    if unsafe {
                        (*current)
                            .next
                            .compare_exchange(next_ptr, my_next, AcqRel, Acquire)
                            .is_ok()
                    } {
                        return;
                    }
                    break; // CAS failed, retry outer loop
                }
                if next_ptr.is_null() {
                    return; // already removed by someone else
                }
                current = next_ptr;
            }
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let prev = self.0.sender_count.fetch_sub(1, AcqRel);
        if prev == 1 {
            // Last sender: wake all waiting receivers so they observe disconnect.
            wake_all_recv_waiters(&self.0.recv_waiters, UNSELECTED);
            wake_select_all(&self.0.select_waiters);
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Receiver
// ════════════════════════════════════════════════════════════════════════════

pub struct Receiver<T>(Arc<Chan<T>>);

impl<T: Send> Receiver<T> {
    /// Non-blocking: take a value from a parked sender if one is waiting.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(val) = self.pop_sender() {
            return Ok(val);
        }
        if self.0.sender_count.load(Acquire) == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Blocking: wait until a sender parks or all senders disconnect.
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            // Fast path: grab a parked sender.
            if let Some(val) = self.pop_sender() {
                return Ok(val);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            // Slow path: register as a waiter, re-check, then park.
            let marker = Arc::new(AtomicUsize::new(UNSELECTED));
            let waiter = RecvWaiter::new(usize::MAX, Arc::clone(&marker));
            let _guard = RecvWaiterGuard::register(waiter, &self.0.recv_waiters);

            // Notify any send-select arms that a receiver is now parked.
            wake_select_one(&self.0.send_select_waiters);

            // TOCTOU: check again after registration to close the window.
            if let Some(val) = self.pop_sender() {
                return Ok(val);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            thread::park();
        }
    }

    /// Try to pop one parked sender from the stack and take its value.
    ///
    /// # Safety
    /// We read `value` via `ptr::read` (taking ownership), then set `taken`
    /// with Release ordering. The sender holds its stack frame alive until it
    /// observes `taken == true` (Acquire), so the read is safe.
    fn pop_sender(&self) -> Option<T> {
        loop {
            let head = self.0.sender_waiters.load(Acquire);
            if head.is_null() {
                return None;
            }
            let next = unsafe { (*head).next.load(Acquire) };
            if self
                .0
                .sender_waiters
                .compare_exchange(head, next, AcqRel, Acquire)
                .is_ok()
            {
                // We own this slot now; no other receiver can claim it.
                // SAFETY: sender wrote `value` before pushing and will not
                // touch it again until `taken` is set to true.
                let val = unsafe { ManuallyDrop::into_inner(ptr::read((*head).value.get())) };
                // Release: happens-before the sender's Acquire load of `taken`.
                unsafe { (*head).taken.store(true, Release) };
                unsafe { (*head).thread.unpark() };
                return Some(val);
            }
            // CAS lost to another receiver; retry.
        }
    }

    // ── select! integration ──────────────────────────────────────────────

    /// Ready when a sender is parked or all senders have disconnected.
    pub(crate) fn is_ready(&self) -> bool {
        !self.0.sender_waiters.load(Acquire).is_null() || self.0.sender_count.load(Acquire) == 0
    }

    pub(crate) fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.0.select_waiters);
    }

    pub(crate) fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        abort_select_waiters(&self.0.select_waiters, selected);
    }

    pub fn complete_recv(&self) -> Result<T, RecvError> {
        self.recv()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let prev = self.0.receiver_count.fetch_sub(1, AcqRel);
        if prev == 1 {
            // Last receiver: unpark all blocked senders so they observe disconnect.
            // Do NOT set `taken`; senders will check `receiver_count == 0` instead.
            let mut current = self.0.sender_waiters.load(Acquire);
            while !current.is_null() {
                let next = unsafe { (*current).next.load(Acquire) };
                unsafe { (*current).thread.unpark() };
                current = next;
            }
            // Drain any pending select waiters to avoid memory leaks.
            drain_select_waiters(&self.0.select_waiters);
            // Wake send-select arms so they observe receiver_count == 0.
            wake_select_all(&self.0.send_select_waiters);
        }
    }
}

impl<T: Send + 'static> crate::SelectableSender for Sender<T> {
    type Input = T;

    /// Ready when a receiver is parked (in `recv_waiters`) or all receivers disconnected.
    fn is_ready(&self) -> bool {
        !self.0.recv_waiters.lock().unwrap().is_empty() || self.0.receiver_count.load(Acquire) == 0
    }

    fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.0.send_select_waiters);
    }

    fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        abort_select_waiters(&self.0.send_select_waiters, selected);
    }

    fn complete_send(&self, value: T) -> Result<(), crate::SendError<T>> {
        self.send(value)
    }
}

impl<T: Send> crate::SelectableReceiver for Receiver<T> {
    type Output = T;

    fn is_ready(&self) -> bool {
        self.is_ready()
    }

    fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        self.register_select(case_id, selected)
    }

    fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        self.abort_select(selected)
    }

    fn complete(&self) -> Result<Self::Output, RecvError> {
        self.complete_recv()
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Constructor
// ════════════════════════════════════════════════════════════════════════════

/// Create a rendezvous channel.
///
/// `send()` blocks until a receiver is simultaneously calling `recv()`.
/// There is no internal buffer: every sent value requires a paired receive.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Chan {
        sender_waiters: AtomicPtr::new(ptr::null_mut()),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(ptr::null_mut())),
        send_select_waiters: Arc::new(AtomicPtr::new(ptr::null_mut())),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
    });
    (Sender(Arc::clone(&chan)), Receiver(chan))
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[test]
    fn test_basic_rendezvous() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.recv().unwrap());
        // send() blocks until the spawned thread's recv() claims the value.
        tx.send(42).unwrap();
        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn test_try_recv_empty() {
        let (tx, rx) = channel::<i32>();
        // No sender parked yet.
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn test_try_recv_wins() {
        let (tx, rx) = channel::<i32>();
        // Park a sender in a background thread, then try_recv from main.
        let handle = thread::spawn(move || tx.send(99));
        thread::sleep(Duration::from_millis(20)); // let sender park
        assert_eq!(rx.try_recv(), Ok(99));
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn test_sender_disconnect_wakes_recv() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.recv());
        thread::sleep(Duration::from_millis(10));
        drop(tx);
        assert_eq!(handle.join().unwrap(), Err(RecvError::Disconnected));
    }

    #[test]
    fn test_receiver_disconnect_wakes_sender() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || tx.send(7));
        thread::sleep(Duration::from_millis(10));
        drop(rx);
        assert_eq!(handle.join().unwrap(), Err(SendError(7)));
    }

    #[test]
    fn test_multiple_senders_one_receiver() {
        let (tx, rx) = channel::<i32>();
        let tx2 = tx.clone();
        let tx3 = tx.clone();

        let h1 = thread::spawn(move || tx.send(1).unwrap());
        let h2 = thread::spawn(move || tx2.send(2).unwrap());
        let h3 = thread::spawn(move || tx3.send(3).unwrap());

        let mut results = vec![rx.recv().unwrap(), rx.recv().unwrap(), rx.recv().unwrap()];
        results.sort();
        assert_eq!(results, vec![1, 2, 3]);

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    }

    #[test]
    fn test_select_arm_rendezvous() {
        use crate::{Select, bounded_mpmc};

        let (tx_rdv, rx_rdv) = channel::<i32>();
        // never() stays permanently alive and empty — never fires.
        let rx_never = bounded_mpmc::never::<i32>();

        // Park a sender so the rendezvous arm becomes ready.
        let h = thread::spawn(move || tx_rdv.send(55).unwrap());
        thread::sleep(Duration::from_millis(20));

        let mut sel = Select::new();
        let i_rdv = sel.recv(rx_rdv.clone());
        let _i_never = sel.recv(rx_never);

        let op = sel.select();
        assert_eq!(op.index, i_rdv);
        assert_eq!(rx_rdv.complete_recv(), Ok(55));

        h.join().unwrap();
    }

    #[test]
    fn test_one_sender_multiple_receivers_each_gets_one() {
        // 3 receivers, 3 sends: each receiver should win exactly one handoff.
        let (tx, rx) = channel::<i32>();
        let rx2 = rx.clone();
        let rx3 = rx.clone();

        let h1 = thread::spawn(move || rx.recv().unwrap());
        let h2 = thread::spawn(move || rx2.recv().unwrap());
        let h3 = thread::spawn(move || rx3.recv().unwrap());

        // Give receivers time to park.
        thread::sleep(Duration::from_millis(20));
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();

        let mut results = vec![h1.join().unwrap(), h2.join().unwrap(), h3.join().unwrap()];
        results.sort();
        assert_eq!(results, vec![1, 2, 3]);
    }

    #[test]
    fn test_mpmc_stress() {
        const SENDERS: usize = 4;
        const PER_SENDER: usize = 64;
        const TOTAL: usize = SENDERS * PER_SENDER;

        let (tx, rx) = channel::<usize>();
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // Spawn senders (they block until a receiver is ready).
        for s in 0..SENDERS {
            let txc = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER_SENDER {
                    // Ignore errors from early receiver disconnect
                    let _ = txc.send(s * PER_SENDER + i);
                }
            }));
        }
        drop(tx); // drop original sender

        // Single receiver drains all messages until all senders disconnect.
        let ctr = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            while rx.recv().is_ok() {
                ctr.fetch_add(1, Relaxed);
            }
        }));

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Relaxed), TOTAL);
    }

    #[test]
    fn test_try_recv_disconnected_immediately() {
        let (tx, rx) = channel::<i32>();
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }
}
