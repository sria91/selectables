//! Bounded multi-producer, single-consumer channel with lock-free send and recv.
//!
//! # Overview
//!
//! A bounded MPSC channel allows multiple senders to push messages into a fixed-size ring buffer,
//! while a single receiver consumes them. This is ideal for scenarios with many producers
//! and one consumer (e.g., worker threads reporting to a main thread).
//!
//! # Capacity and backpressure
//!
//! When the buffer is full:
//! - `send()` returns `Err(SendError(msg))` immediately (non-blocking, fail-fast)
//! - No blocking, no waiting for space — the message is returned to the sender
//! - As the receiver pops messages, space opens up and subsequent sends succeed
//!
//! # Lock-free design
//!
//! - `send()` is entirely lock-free: uses atomics and ring buffer operations only
//! - `try_recv()` is entirely lock-free: fast-path pop without contention
//! - `recv()` is lock-free on the hot path; only locks during slow-path waiter registration
//! - Wake semantics: one send wakes one waiting receiver (efficient for MPSC model)
//!
//! # Cloning
//!
//! - `Sender` is cloneable, creating additional independent send handles
//! - `Receiver` is NOT cloneable (single consumer)
//! - Use separate channel instances if you need multiple consumers
//!
//! # Example
//!
//! ```ignore
//! let (tx, rx) = bounded_mpsc::channel(16);
//!
//! // Multiple senders
//! let tx1 = tx.clone();
//! std::thread::spawn(move || { tx1.send(1).ok(); });
//! std::thread::spawn(move || { tx.send(2).ok(); });
//!
//! // Single consumer receives all messages
//! assert!(rx.recv().is_ok());
//! assert!(rx.recv().is_ok());
//! ```

use std::{
    sync::Arc,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering::*},
    thread,
    time::{Duration, Instant},
};

use crate::{
    error::{RecvError, SendError, TryRecvError},
    internals::LockFreeBoundedRing,
    waiter::{
        RecvWaiter, RecvWaiterGuard, RecvWaiterList, SelectWaiter, UNSELECTED,
        abort_select_waiters, drain_select_waiters, new_recv_waiter_list, push_select_waiter,
        wake_all_recv_waiters, wake_all_unselected_recv_waiters, wake_select_all, wake_select_one,
    },
};

// ════════════════════════════════════════════════════════════════════════════
// MPSC Channel Internals
// ════════════════════════════════════════════════════════════════════════════

pub(crate) struct Chan<T> {
    ring: LockFreeBoundedRing<T>,
    recv_waiters: RecvWaiterList,
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    pub(crate) sender_count: AtomicUsize,
    pub(crate) receiver_count: AtomicUsize,
    /// Select waiters for send arms; woken when buffer space is freed or receivers disconnect.
    send_select_waiters: Arc<AtomicPtr<SelectWaiter>>,
}

// ════════════════════════════════════════════════════════════════════════════
// MPSC Sender
// ════════════════════════════════════════════════════════════════════════════

pub struct Sender<T>(pub(crate) Arc<Chan<T>>);

impl<T> Sender<T> {
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        // Lock-free early-out: bail if all receivers are gone.
        if self.0.receiver_count.load(Acquire) == 0 {
            return Err(SendError(msg));
        }
        // Lock-free enqueue.
        match self.0.ring.try_push(msg) {
            Err(msg) => return Err(SendError(msg)), // full or zero-capacity
            Ok(()) => {}
        }
        // Wake up all threads waiting for a message (select or plain recv).
        wake_all_unselected_recv_waiters(&self.0.recv_waiters);
        wake_select_one(&self.0.select_waiters);
        Ok(())
    }
}

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
            wake_all_recv_waiters(&self.0.recv_waiters, UNSELECTED);
            wake_select_all(&self.0.select_waiters);
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// MPSC Receiver
// ════════════════════════════════════════════════════════════════════════════

pub struct Receiver<T>(pub(crate) Arc<Chan<T>>);

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.0.receiver_count.fetch_add(1, Relaxed);
        Receiver(Arc::clone(&self.0))
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
    pub fn recv(&self) -> Result<T, RecvError> {
        // Allocate the wakeup marker once; reused across spurious-wakeup iterations.
        let marker = Arc::new(AtomicUsize::new(UNSELECTED));
        loop {
            // --- fast path (lock-free) ---
            if let Some(msg) = self.0.ring.try_pop() {
                wake_select_one(&self.0.send_select_waiters);
                return Ok(msg);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            // --- slow path: lock-free registration ---
            let waiter = RecvWaiter::new(usize::MAX, Arc::clone(&marker));
            let _guard = RecvWaiterGuard::register(waiter, &self.0.recv_waiters);

            // Re-check after push
            if let Some(msg) = self.0.ring.try_pop() {
                wake_select_one(&self.0.send_select_waiters);
                return Ok(msg);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }
            if marker.load(Acquire) != UNSELECTED {
                if let Some(msg) = self.0.ring.try_pop() {
                    wake_select_one(&self.0.send_select_waiters);
                    return Ok(msg);
                }
                return Err(RecvError::Disconnected);
            }

            thread::park();
        }
    }

    /// Lock-free readiness check used by the select protocol.
    pub(crate) fn is_ready(&self) -> bool {
        !self.0.ring.is_empty() || self.0.sender_count.load(Acquire) == 0
    }

    pub(crate) fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.0.select_waiters);
    }

    pub(crate) fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        abort_select_waiters(&self.0.select_waiters, selected);
    }

    pub fn complete_recv(&self) -> Result<T, RecvError> {
        match self.try_recv() {
            Ok(msg) => Ok(msg),
            Err(TryRecvError::Empty) => Err(RecvError::Disconnected),
            Err(TryRecvError::Disconnected) => Err(RecvError::Disconnected),
            Err(TryRecvError::Lagged { .. }) => unreachable!("bounded_mpmc cannot lag"),
        }
    }
}

impl<T> crate::SelectableReceiver for Receiver<T> {
    type Output = T;

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
        self.complete_recv()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let prev = self.0.receiver_count.fetch_sub(1, AcqRel);
        if prev == 1 {
            drain_select_waiters(&self.0.select_waiters);
            // Wake all send-side select waiters so blocked send arms observe disconnect.
            wake_select_all(&self.0.send_select_waiters);
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// SelectableSender impl for Sender<T>
// ════════════════════════════════════════════════════════════════════════════

impl<T: Send + 'static> crate::SelectableSender for Sender<T> {
    type Input = T;

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
// Constructors
// ════════════════════════════════════════════════════════════════════════════

/// Create a bounded MPSC channel with the given capacity.
/// `send` fails immediately if the buffer is full.
///
/// **Note**: `channel(0)` creates a permanently-full channel where every
/// `send` fails immediately. This is **not** a rendezvous channel.
/// For synchronous sender-receiver handoff, use [`crate::rendezvous::channel`].
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let chan: Arc<Chan<T>> = Arc::new(Chan {
        ring: LockFreeBoundedRing::new(capacity),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
        send_select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
    });
    (Sender(Arc::clone(&chan)), Receiver(chan))
}

/// One-shot `MpscReceiver<Instant>` that fires once after `duration`.
pub fn after(duration: Duration) -> Receiver<Instant> {
    let chan: Arc<Chan<Instant>> = Arc::new(Chan {
        ring: LockFreeBoundedRing::new(1),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
        send_select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
    });
    let rx = Receiver(Arc::clone(&chan));
    if let Err(error) = thread::Builder::new()
        .name("selectables::mpsc_after".to_owned())
        .spawn(move || {
            thread::sleep(duration);
            let _ = chan.ring.try_push(Instant::now());

            wake_all_unselected_recv_waiters(&chan.recv_waiters);
            wake_select_one(&chan.select_waiters);
        })
    {
        eprintln!("failed to spawn 'mpsc_after' thread: {}", error);
    }
    rx
}

/// A `MpscReceiver<T>` that never yields values and never disconnects.
///
/// Uses receiver-only channel state with `sender_count = 1` and no `MpscSender`
/// handle, so disconnect is never observed by the receiver.
pub fn never<T>() -> Receiver<T> {
    Receiver(Arc::new(Chan {
        ring: LockFreeBoundedRing::new(0),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
        send_select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    use crate::{Select, select};

    #[test]
    fn test_basic_send_recv() {
        let (tx, rx) = channel(10);
        tx.send(42).unwrap();
        assert_eq!(rx.recv().unwrap(), 42);
    }

    #[test]
    fn test_try_recv() {
        let (tx, rx) = channel(10);
        assert!(rx.try_recv().is_err()); // Empty
        tx.send(123).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 123);
    }

    #[test]
    fn test_bounded_capacity() {
        let (tx, rx) = channel(2);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert!(tx.send(3).is_err()); // Full
        assert_eq!(rx.recv().unwrap(), 1);
        tx.send(3).unwrap(); // Now space
        assert_eq!(rx.recv().unwrap(), 2);
        assert_eq!(rx.recv().unwrap(), 3);
    }

    #[test]
    fn test_multiple_senders() {
        let (tx1, rx) = channel(10);
        let tx2 = tx1.clone();
        let tx3 = tx1.clone();

        tx1.send("a").unwrap();
        tx2.send("b").unwrap();
        tx3.send("c").unwrap();

        // Order not guaranteed, but all should be received
        let mut received = vec![];
        for _ in 0..3 {
            received.push(rx.recv().unwrap());
        }
        received.sort();
        assert_eq!(received, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_sender_cloneable() {
        let (tx1, _rx) = channel::<i32>(10);
        let tx2 = tx1.clone();
        let tx3 = tx2.clone();
        // All should be able to send
        tx1.send(1).unwrap();
        tx2.send(2).unwrap();
        tx3.send(3).unwrap();
    }

    #[test]
    fn test_receiver_drop_causes_send_error() {
        let (tx, rx) = channel(10);
        drop(rx);
        assert!(tx.send(42).is_err());
    }

    #[test]
    fn test_sender_drop_causes_recv_disconnect() {
        let (tx, rx) = channel::<i32>(10);
        drop(tx);
        assert!(rx.recv().is_err()); // Disconnected
    }

    #[test]
    fn test_select_hooks() {
        let (tx, rx) = channel(10);

        // Initially not ready
        assert!(!rx.is_ready());

        // Send something
        tx.send(42).unwrap();
        assert!(rx.is_ready());

        // Complete recv
        assert_eq!(rx.complete_recv().unwrap(), 42);
        assert!(!rx.is_ready());
    }

    #[test]
    fn test_zero_capacity() {
        let (tx, rx) = channel::<i32>(0);
        assert!(tx.send(42).is_err()); // Can't send to zero capacity
        // Receiver not ready until sender drops
        assert!(!rx.is_ready());
        drop(tx);
        assert!(rx.is_ready());
        assert!(rx.recv().is_err()); // Disconnected
    }

    #[test]
    fn test_never_channel_is_alive_but_empty() {
        let rx: Receiver<i32> = never();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        let mut sel = Select::new();
        sel.recv(rx.clone());
        assert!(sel.try_select().is_none());
    }

    #[test]
    fn test_after_fires_once() {
        let rx = after(Duration::from_millis(30));
        let t = rx.recv().unwrap();
        assert!(t.elapsed() < Duration::from_secs(5));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn test_concurrent_send_recv() {
        let (tx, rx) = channel(100);

        let handle = thread::spawn(move || {
            for i in 0..50 {
                tx.send(i).unwrap();
            }
        });

        handle.join().unwrap();

        for i in 0..50 {
            assert_eq!(rx.recv().unwrap(), i);
        }
    }

    #[test]
    fn test_fifo_ordering() {
        let (tx, rx) = channel(16);
        for i in 0..8u32 {
            tx.send(i).unwrap();
        }
        for i in 0..8u32 {
            assert_eq!(rx.recv().unwrap(), i);
        }
    }

    #[test]
    fn test_blocking_recv_woken_by_disconnect() {
        let (tx, rx) = channel::<i32>(4);
        let handle = thread::spawn(move || rx.recv());
        thread::sleep(Duration::from_millis(20));
        drop(tx);
        assert_eq!(
            handle.join().unwrap(),
            Err(crate::error::RecvError::Disconnected)
        );
    }

    #[test]
    fn test_blocking_recv_drains_before_disconnect() {
        let (tx, rx) = channel(4);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        drop(tx);
        // Items buffered before sender drop are still returned.
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx.recv().unwrap(), 2);
        assert!(rx.recv().is_err());
    }

    #[test]
    fn test_drop_values_in_buffer() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let counter = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct Guard(Arc<AtomicUsize>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let (tx, rx) = channel(4);
        tx.send(Guard(Arc::clone(&counter))).unwrap();
        tx.send(Guard(Arc::clone(&counter))).unwrap();
        drop(tx);
        drop(rx); // Both Guards should be dropped via the channel's Drop impl.
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_send_select_wakes_when_receiver_pops() {
        // Fill the buffer so is_ready() returns false for the sender.
        let (tx, rx) = channel::<i32>(1);
        tx.send(1).unwrap(); // full

        let tx2 = tx.clone();
        // Spawn a thread that will wait on the send select arm.
        let sent = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sent2 = std::sync::Arc::clone(&sent);
        let handle = thread::spawn(move || {
            select! {
                send(tx2, 99) -> res => {
                    assert!(res.is_ok());
                    sent2.store(true, std::sync::atomic::Ordering::Relaxed);
                },
            }
        });

        thread::sleep(Duration::from_millis(20));
        // Pop from buffer to create space → wakes the send-select arm.
        assert_eq!(rx.recv().unwrap(), 1);
        handle.join().unwrap();
        assert!(sent.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(rx.recv().unwrap(), 99);
    }
}
