//! Bounded multi-producer, multi-consumer channel with lock-free send and recv.
//!
//! # Overview
//!
//! A bounded MPMC channel allows multiple senders and multiple receivers to share a fixed-size
//! ring buffer. Each message is consumed by exactly one receiver (no broadcast). This is ideal
//! for work-stealing patterns, thread pools, and load-balanced message distribution.
//!
//! # Capacity and send behavior
//!
//! When the buffer is full:
//! - `send()` returns `Err(SendError(msg))` immediately (fail-fast, no blocking)
//! - As any receiver pops a message, space opens up for new sends
//! - Each send wakes at most one waiting receiver (efficient, prevents thundering herd)
//!
//! # Lock-free design
//!
//! - `send()` is lock-free: atomic ring buffer push; wakes one receiver via Mutex briefly
//! - `try_recv()` is lock-free: atomic ring buffer pop
//! - `recv()` is lock-free on hot path; uses Mutex only for waiter registration
//! - MPMC wake semantics: one send wakes one receiver to reduce contention
//!
//! # Cloning
//!
//! - Both `Sender` and `Receiver` are fully cloneable
//! - Each clone gets an independent identity tracked via atomic counters
//! - Disconnect is only observed when the last sender or receiver is dropped
//!
//! # Example
//!
//! ```no_run
//! use selectables::bounded_mpmc;
//!
//! let (tx, rx) = bounded_mpmc::channel(10);
//!
//! // Multiple senders
//! let tx1 = tx.clone();
//! std::thread::spawn(move || { tx.send("from tx").ok(); });
//! std::thread::spawn(move || { tx1.send("from tx1").ok(); });
//!
//! // Multiple receivers (each message goes to one receiver)
//! let rx1 = rx.clone();
//! std::thread::spawn(move || { let msg = rx.recv(); });
//! std::thread::spawn(move || { let msg = rx1.recv(); });
//! ```

use std::{
    sync::Arc,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering::*},
    thread,
};

use crate::{
    error::{RecvError, SendError, TryRecvError},
    internals::LockFreeBoundedRing,
    waiter::{
        RecvWaiterList, SelectWaiter, abort_select_waiters, drain_select_waiters,
        new_recv_waiter_list, push_select_waiter, register_plain_recv_waiter,
        wake_all_recv_waiters, wake_one_recv_waiter, wake_select_all, wake_select_one,
    },
};

// ════════════════════════════════════════════════════════════════════════════
// Channel shared state
// ════════════════════════════════════════════════════════════════════════════

pub(crate) struct Chan<T> {
    ring: LockFreeBoundedRing<T>,
    recv_waiters: RecvWaiterList,
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    sender_count: AtomicUsize,
    /// Number of live `Receiver<T>` handles; tracked so senders can detect disconnect.
    receiver_count: AtomicUsize,
    /// Select waiters registered by `SelectableSender::register_select`; woken when
    /// buffer space becomes available (receiver pops) or all receivers disconnect.
    send_select_waiters: Arc<AtomicPtr<SelectWaiter>>,
}

// ════════════════════════════════════════════════════════════════════════════
// Sender
// ════════════════════════════════════════════════════════════════════════════

pub struct Sender<T>(pub(crate) Arc<Chan<T>>);

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.sender_count.fetch_add(1, Relaxed);
        Sender(Arc::clone(&self.0))
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let prev = self.0.sender_count.fetch_sub(1, AcqRel);
        if prev == 1 {
            // Last sender dropped — wake all waiting receivers so they can
            // observe the disconnected state.
            wake_all_recv_waiters(&self.0.recv_waiters);
            wake_select_all(&self.0.select_waiters);
        }
    }
}

impl<T> Sender<T> {
    /// Send a value. Returns `Err(SendError(val))` if the buffer is full or all
    /// receivers have been dropped.
    ///
    /// This is entirely lock-free for the enqueue step.  The wakeup of a
    /// waiting receiver acquires the `waiters` mutex briefly.
    ///
    /// **Note:** `Ok(())` confirms the value was accepted into the ring buffer
    /// but does not guarantee a receiver is still live — the last receiver may
    /// drop concurrently after the disconnected check and before the push.
    /// The value will be freed when the channel tears down.
    pub fn send(&self, val: T) -> Result<(), SendError<T>> {
        // Fail-fast: bail if all receivers are gone.
        if self.0.receiver_count.load(Acquire) == 0 {
            return Err(SendError(val));
        }
        // Lock-free enqueue.
        self.0.ring.try_push(val).map_err(SendError)?;

        // Wake at most one waiting receiver — we pushed exactly one item.
        // (wake_one_recv_waiter returns bool and handles unpark internally)
        wake_one_recv_waiter(&self.0.recv_waiters);
        wake_select_one(&self.0.select_waiters);
        Ok(())
    }

    /// Returns `true` if all [`Receiver`] handles have been dropped.
    pub fn is_closed(&self) -> bool {
        self.0.receiver_count.load(Acquire) == 0
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Receiver
// ════════════════════════════════════════════════════════════════════════════

pub struct Receiver<T>(pub(crate) Arc<Chan<T>>);

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.0.receiver_count.fetch_add(1, Relaxed);
        Receiver(Arc::clone(&self.0))
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let prev = self.0.receiver_count.fetch_sub(1, AcqRel);
        if prev == 1 {
            // Last receiver dropped — free any Box<SelectWaiter> nodes left on the
            // recv select_waiters stack, then wake send-side select waiters so
            // blocked send arms observe the disconnect.
            drain_select_waiters(&self.0.select_waiters);
            wake_select_all(&self.0.send_select_waiters);
        }
    }
}

impl<T> Receiver<T> {
    /// Non-blocking receive — entirely lock-free.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(msg) = self.0.ring.try_pop() {
            // Space freed — wake one send-side select waiter.
            wake_select_one(&self.0.send_select_waiters);
            return Ok(msg);
        }
        if self.0.sender_count.load(Acquire) == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Blocking receive.
    ///
    /// Fast path is lock-free.  When the queue is empty, the thread registers
    /// itself in the waiter list (under the waiters lock), re-checks the queue
    /// to close the lost-wakeup window, then parks until a sender unparks it.
    ///
    /// In a multi-consumer scenario `try_pop()` is a CAS, so concurrent
    /// receivers cannot both dequeue the same item.  A woken receiver that
    /// loses the CAS simply loops and parks again.
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            // --- fast path (lock-free) ---
            if let Some(msg) = self.0.ring.try_pop() {
                // Space freed — wake one send-side select waiter.
                wake_select_one(&self.0.send_select_waiters);
                return Ok(msg);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            // --- slow path: register waiter, re-check, park ---
            let _guard = register_plain_recv_waiter(&self.0.recv_waiters);

            // TOCTOU check inside critical section (after push, before park)
            if let Some(msg) = self.0.ring.try_pop() {
                wake_select_one(&self.0.send_select_waiters);
                return Ok(msg);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            thread::park();
            // After waking, retry the fast path
        }
    }

    // ── Hooks used by Select (crate-internal) ────────────────────────────

    /// Lock-free readiness check for the select try-phase.
    pub(crate) fn is_ready(&self) -> bool {
        !self.0.ring.is_empty() || self.0.sender_count.load(Acquire) == 0
    }

    pub(crate) fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        log_debug!(
            "bounded_mpmc::register_select: chan={:p}, case_id={}",
            Arc::as_ptr(&self.0),
            case_id
        );
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.0.select_waiters);
    }

    pub(crate) fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        log_debug!(
            "bounded_mpmc::abort_select: chan={:p}",
            Arc::as_ptr(&self.0)
        );
        abort_select_waiters(&self.0.select_waiters, selected);
    }

    /// Called by `select!` after this arm wins.  Uses `recv()` so that in a
    /// multi-consumer scenario where a concurrent receiver stole the item
    /// between selection and completion, we block until the next message
    /// arrives rather than returning a spurious error.
    pub(crate) fn complete_recv(&self) -> Result<T, RecvError> {
        self.recv()
    }
}

impl_selectable_receiver!([T] Receiver<T>, T);

// ════════════════════════════════════════════════════════════════════════════
// SelectableSender impl for Sender<T>
// ════════════════════════════════════════════════════════════════════════════

impl<T: Send + 'static> crate::SelectableSender for Sender<T> {
    type Input = T;

    /// Ready when the buffer is not full OR all receivers are gone (disconnect).
    fn is_ready(&self) -> bool {
        !self.0.ring.is_full() || self.0.receiver_count.load(Acquire) == 0
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

// ════════════════════════════════════════════════════════════════════════════
// Constructor
// ════════════════════════════════════════════════════════════════════════════

/// Create a lock-free bounded MPMC channel with the given capacity.
///
/// `send` returns `Err` immediately if the buffer is full — it never blocks.
/// `recv` blocks until a message is available or all senders are dropped.
///
/// **Note**: `channel(0)` creates a permanently-full channel where every
/// `send` fails immediately. This is **not** a rendezvous channel.
/// For synchronous sender-receiver handoff, use [`crate::rendezvous::channel`].
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Chan {
        ring: LockFreeBoundedRing::new(capacity),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
        send_select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
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
    use std::thread;

    use crate::select;

    #[test]
    fn basic_send_recv() {
        let (tx, rx) = channel(4);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx.recv().unwrap(), 2);
    }

    #[test]
    fn try_recv_empty_and_full() {
        let (tx, rx) = channel(2);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert!(tx.send(3).is_err(), "buffer full");
        assert_eq!(rx.try_recv(), Ok(1));
        tx.send(3).unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(3));
    }

    #[test]
    fn zero_capacity_always_full() {
        let (tx, _rx) = channel::<i32>(0);
        assert!(tx.send(42).is_err());
    }

    #[test]
    fn sender_drop_disconnects() {
        let (tx, rx) = channel::<i32>(4);
        tx.send(1).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert!(rx.recv().is_err());
    }

    #[test]
    fn clone_sender_keeps_alive() {
        let (tx1, rx) = channel(4);
        let tx2 = tx1.clone();
        drop(tx1);
        tx2.send(42).unwrap();
        drop(tx2);
        assert_eq!(rx.recv().unwrap(), 42);
        assert!(rx.recv().is_err()); // now disconnected
    }

    #[test]
    fn multiple_senders() {
        let (tx1, rx) = channel(16);
        let tx2 = tx1.clone();
        let tx3 = tx1.clone();
        tx1.send(1).unwrap();
        tx2.send(2).unwrap();
        tx3.send(3).unwrap();
        let mut vals: Vec<i32> = (0..3).map(|_| rx.recv().unwrap()).collect();
        vals.sort();
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn multiple_receivers() {
        let (tx, rx1) = channel(16);
        let rx2 = rx1.clone();
        tx.send(10).unwrap();
        tx.send(20).unwrap();
        let a = rx1.try_recv().unwrap_or_else(|_| rx2.try_recv().unwrap());
        let b = rx1.try_recv().unwrap_or_else(|_| rx2.try_recv().unwrap());
        let mut vals = vec![a, b];
        vals.sort();
        assert_eq!(vals, vec![10, 20]);
    }

    #[test]
    fn blocking_recv_woken_by_send() {
        let (tx, rx) = channel(4);
        let handle = thread::spawn(move || rx.recv().unwrap());
        thread::sleep(std::time::Duration::from_millis(20));
        tx.send(42).unwrap();
        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn blocking_recv_woken_by_disconnect() {
        let (tx, rx) = channel::<i32>(4);
        let handle = thread::spawn(move || rx.recv());
        thread::sleep(std::time::Duration::from_millis(20));
        drop(tx);
        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn is_ready() {
        let (tx, rx) = channel(4);
        assert!(!rx.is_ready());
        tx.send(1).unwrap();
        assert!(rx.is_ready());
        rx.try_recv().unwrap();
        assert!(!rx.is_ready());
        drop(tx);
        assert!(rx.is_ready()); // disconnected counts as ready
    }

    #[test]
    fn concurrent_producers_consumers() {
        const PRODUCERS: usize = 4;
        const PER_PRODUCER: usize = 256;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;

        // Small buffer relative to total items; producers retry on full.
        let (tx, rx) = channel(32);
        let received = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for _ in 0..PRODUCERS {
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    // Retry until the buffer has space.
                    loop {
                        match tx.send(i) {
                            Ok(()) => break,
                            Err(_) => thread::yield_now(),
                        }
                    }
                }
            }));
        }

        // Consumers drain until the channel is fully disconnected and empty.
        for _ in 0..PRODUCERS {
            let rx = rx.clone();
            let received = Arc::clone(&received);
            handles.push(thread::spawn(move || {
                loop {
                    match rx.try_recv() {
                        Ok(_) => {
                            received.fetch_add(1, Relaxed);
                        }
                        Err(TryRecvError::Empty) => thread::yield_now(),
                        Err(TryRecvError::Disconnected) => return,
                        Err(TryRecvError::Lagged { .. }) => unreachable!("bounded_mpmc cannot lag"),
                    }
                }
            }));
        }

        drop(tx); // signals disconnection once all producer clones drop too
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.load(Relaxed), TOTAL);
    }

    #[test]
    fn drop_values_in_buffer() {
        use std::sync::atomic::AtomicBool;
        static DROPPED: AtomicBool = AtomicBool::new(false);

        #[derive(Debug)]
        struct Guard;
        impl Drop for Guard {
            fn drop(&mut self) {
                DROPPED.store(true, Relaxed);
            }
        }

        let (tx, rx) = channel(4);
        tx.send(Guard).unwrap();
        drop(tx);
        drop(rx); // Guard should be dropped here
        assert!(DROPPED.load(Relaxed));
    }

    #[test]
    fn fifo_ordering() {
        let (tx, rx) = channel(16);
        for i in 0..8u32 {
            tx.send(i).unwrap();
        }
        for i in 0..8u32 {
            assert_eq!(rx.recv().unwrap(), i);
        }
    }

    #[test]
    fn recv_drains_before_disconnect() {
        let (tx, rx) = channel(4);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        drop(tx);
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx.recv().unwrap(), 2);
        assert!(rx.recv().is_err());
    }

    #[test]
    fn send_select_wakes_when_receiver_pops() {
        let (tx, rx) = channel::<i32>(1);
        tx.send(1).unwrap(); // fill the buffer

        let tx2 = tx.clone();
        let rx2 = rx.clone();
        let handle = std::thread::spawn(move || {
            select! {
                send(tx2, 99) -> res => assert!(res.is_ok()),
            }
        });

        thread::sleep(std::time::Duration::from_millis(20));
        // Pop to create space → wakes the send-select arm.
        assert_eq!(rx.recv().unwrap(), 1);
        handle.join().unwrap();
        assert_eq!(rx2.recv().unwrap(), 99);
    }

    #[test]
    fn select_ready_when_disconnected_and_empty() {
        let (tx, rx) = channel::<i32>(4);
        drop(tx);
        // Disconnected state counts as ready for recv arms.
        assert!(rx.is_ready());
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn sender_is_closed() {
        let (tx, rx) = channel::<i32>(4);
        assert!(!tx.is_closed());
        let rx2 = rx.clone();
        drop(rx);
        assert!(!tx.is_closed()); // rx2 still alive
        drop(rx2);
        assert!(tx.is_closed());
    }
}
