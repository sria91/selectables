//! Unbounded lock-free multi-producer, single-consumer channel.
//!
//! # Overview
//!
//! An unbounded MPSC channel has unlimited capacity (backed by Crossbeam's `SegQueue`).
//! Multiple senders push messages into a lock-free queue, and a single consumer pops from it.
//! This is the simplest model: no backpressure, no lag, predictable semantics. Ideal for
//! worker threads reporting results to a main collector.
//!
//! # Send behavior
//!
//! - `send()` never fails — capacity is unlimited
//! - Always succeeds unless the receiver is disconnected
//! - Wakes all waiting receiver threads to ensure prompt delivery (MPSC wake-all semantics)
//!
//! # Lock-free throughout
//!
//! - `send()` is lock-free: atomic queue push; Mutex only to wake waiting threads
//! - `try_recv()` is lock-free: atomic queue pop
//! - `recv()` is lock-free on hot path; uses Mutex only for waiter registration during blocking
//!
//! # Single receiver
//!
//! - `Receiver` is NOT cloneable (single consumer ownership)
//! - `Sender` is fully cloneable
//! - If you need multiple consumers, use `unbounded_mpmc` instead
//!
//! # Example
//!
//! ```ignore
//! let (tx, rx) = unbounded_mpsc::channel();
//!
//! // Many senders
//! let tx1 = tx.clone();
//! let tx2 = tx.clone();
//! std::thread::spawn(move || { tx.send(1).ok(); });
//! std::thread::spawn(move || { tx1.send(2).ok(); });
//! std::thread::spawn(move || { tx2.send(3).ok(); });
//!
//! // Single receiver gets all messages
//! assert!(rx.recv().is_ok());
//! assert!(rx.recv().is_ok());
//! assert!(rx.recv().is_ok());
//! ```

use std::{
    sync::Arc,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering::*},
    thread,
};

use crossbeam_queue::SegQueue;

use crate::{
    error::{RecvError, SendError, TryRecvError},
    waiter::{
        RecvWaiter, RecvWaiterGuard, RecvWaiterList, SelectWaiter, UNSELECTED,
        abort_select_waiters, drain_select_waiters, new_recv_waiter_list, push_select_waiter,
        wake_all_recv_waiters, wake_all_unselected_recv_waiters, wake_select_all, wake_select_one,
    },
};

// ════════════════════════════════════════════════════════════════════════════
// Channel shared state
// ════════════════════════════════════════════════════════════════════════════

pub(crate) struct Chan<T> {
    /// Lock-free unbounded queue. Push is infallible.
    queue: SegQueue<T>,
    recv_waiters: RecvWaiterList,
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    /// Number of live senders.
    sender_count: AtomicUsize,
    /// Number of live receivers.
    receiver_count: AtomicUsize,
}

// ════════════════════════════════════════════════════════════════════════════
// Constructors
// ════════════════════════════════════════════════════════════════════════════

/// Create a lock-free unbounded MPSC channel.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Chan {
        queue: SegQueue::new(),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
    });
    log_debug!("unbounded_mpsc::new: chan={:p}", Arc::as_ptr(&chan));
    (Sender(Arc::clone(&chan)), Receiver(chan))
}

// ════════════════════════════════════════════════════════════════════════════
// Sender
// ════════════════════════════════════════════════════════════════════════════

pub struct Sender<T>(pub(crate) Arc<Chan<T>>);

impl<T> Sender<T> {
    /// Send never blocks on unbounded channel.
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        if self.0.receiver_count.load(Acquire) == 0 {
            return Err(SendError(msg));
        }
        self.0.queue.push(msg);
        // MPSC send path wakes all waiters to ensure any blocked recv/select
        // observes new data promptly.
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
            wake_all_recv_waiters(&self.0.recv_waiters, UNSELECTED);
            wake_select_all(&self.0.select_waiters);
        }
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
            drain_select_waiters(&self.0.select_waiters);
        }
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(v) = self.0.queue.pop() {
            return Ok(v);
        }
        if self.0.sender_count.load(Acquire) == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        let marker = Arc::new(AtomicUsize::new(UNSELECTED));
        loop {
            // Fast path.
            if let Some(v) = self.0.queue.pop() {
                return Ok(v);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            // Slow path: lock-free registration.
            let waiter = RecvWaiter::new(usize::MAX, Arc::clone(&marker));
            let _guard = RecvWaiterGuard::register(waiter, &self.0.recv_waiters);

            // Re-check after push
            if let Some(v) = self.0.queue.pop() {
                return Ok(v);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }
            if marker.load(Acquire) != UNSELECTED {
                if let Some(v) = self.0.queue.pop() {
                    return Ok(v);
                }
                return Err(RecvError::Disconnected);
            }

            thread::park();
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        !self.0.queue.is_empty() || self.0.sender_count.load(Acquire) == 0
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
            Err(TryRecvError::Lagged { .. }) => unreachable!("unbounded_mpsc cannot lag"),
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

// ════════════════════════════════════════════════════════════════════════════
// SelectableSender impl for Sender<T>
// ════════════════════════════════════════════════════════════════════════════

impl<T: Send + 'static> crate::SelectableSender for Sender<T> {
    type Input = T;

    /// Unbounded send is always immediately ready.
    fn is_ready(&self) -> bool {
        true
    }

    fn register_select(&self, _case_id: usize, _selected: Arc<AtomicUsize>) {}

    fn abort_select(&self, _selected: &Arc<AtomicUsize>) {}

    fn complete_send(&self, value: T) -> Result<(), crate::SendError<T>> {
        self.send(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Select;
    use std::thread;

    #[test]
    fn basic_send_recv() {
        let (tx, rx) = channel();
        tx.send(10).unwrap();
        tx.send(20).unwrap();
        assert_eq!(rx.recv().unwrap(), 10);
        assert_eq!(rx.recv().unwrap(), 20);
    }

    #[test]
    fn receiver_drop_causes_send_error() {
        let (tx, rx) = channel::<i32>();
        drop(rx);
        assert!(tx.send(1).is_err());
    }

    #[test]
    fn disconnect_after_drain() {
        let (tx, rx) = channel::<i32>();
        tx.send(1).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn multiple_senders_stress_count() {
        const SENDERS: usize = 4;
        const MSGS: usize = 500;
        let (tx, rx) = channel::<usize>();

        let mut hs = Vec::new();
        for s in 0..SENDERS {
            let txc = tx.clone();
            hs.push(thread::spawn(move || {
                for i in 0..MSGS {
                    txc.send(s * MSGS + i).unwrap();
                }
            }));
        }
        drop(tx);
        for h in hs {
            h.join().unwrap();
        }

        let mut seen = 0usize;
        loop {
            match rx.try_recv() {
                Ok(_) => seen += 1,
                Err(TryRecvError::Empty) => thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Lagged { .. }) => unreachable!("unbounded_mpsc cannot lag"),
            }
        }
        assert_eq!(seen, SENDERS * MSGS);
    }

    #[test]
    fn select_ready_and_complete() {
        let (tx, rx) = channel::<i32>();
        tx.send(77).unwrap();

        let mut sel = Select::new();
        sel.recv(rx.clone());
        assert!(sel.try_select().is_some());
        assert_eq!(rx.complete_recv().unwrap(), 77);
    }

    #[test]
    fn fifo_ordering() {
        let (tx, rx) = channel::<u32>();
        for i in 0..8u32 {
            tx.send(i).unwrap();
        }
        for i in 0..8u32 {
            assert_eq!(rx.recv().unwrap(), i);
        }
    }

    #[test]
    fn blocking_recv_wakes_on_send() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.recv().unwrap());
        thread::sleep(std::time::Duration::from_millis(20));
        tx.send(42).unwrap();
        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn blocking_recv_wakes_on_disconnect() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.recv());
        thread::sleep(std::time::Duration::from_millis(20));
        drop(tx);
        assert_eq!(
            handle.join().unwrap(),
            Err(crate::error::RecvError::Disconnected)
        );
    }

    #[test]
    fn try_recv_empty_then_disconnected() {
        let (tx, rx) = channel::<i32>();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn sender_count_tracks_clones() {
        let (tx1, rx) = channel::<i32>();
        let tx2 = tx1.clone();
        let tx3 = tx2.clone();
        drop(tx1);
        // Still alive because tx2, tx3 exist.
        tx2.send(1).unwrap();
        drop(tx2);
        tx3.send(2).unwrap();
        drop(tx3);
        // Now disconnected.
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }
}
