//! Single-send, single-delivery channel for one-time messages.
//!
//! # Overview
//!
//! A oneshot channel allows exactly one sender to deliver exactly one message to one receiver.
//! Perfect for one-time responses, futures resolution, and join semantics. Once a value is
//! sent or either end is dropped, the channel is "consumed".
//!
//! # Semantics
//!
//! - `Sender` can only call `send()` once; consumes the sender
//! - `Receiver::recv()` blocks until the sender delivers or drops
//! - If sender drops without sending, receiver gets `RecvError::Disconnected`
//! - If receiver drops without receiving, sender's `send()` returns `SendError(value)`
//!
//! # Clone semantics
//!
//! `Receiver` is `Clone` to support `select!` integration (the macro clones receiver
//! handles for arm registration).  Cloning a `Receiver` creates a second handle that
//! races for the same single value: whichever clone takes the value first wins and the
//! other gets `Err(RecvError::Disconnected)`.  Avoid cloning outside of `select!`.
//!
//! # Lock-free storage
//!
//! - Value stored in `ArcSwapOption<ValueCell<T>>` for atomic single-take semantics
//! - `recv()` uses CAS to atomically take the value exactly once
//! - No spurious wakeups: atomic check-before-park pattern
//!
//! # Example
//!
//! ```ignore
//! let (tx, rx) = oneshot::channel();
//!
//! std::thread::spawn(move || {
//!     std::thread::sleep(Duration::from_millis(10));
//!     tx.send("done").ok(); // Consumes tx
//! });
//!
//! // Blocks until sender sends or drops
//! match rx.recv() {
//!     Ok(msg) => println!("Got: {}", msg),
//!     Err(RecvError::Disconnected) => println!("Sender dropped without sending"),
//! }
//! ```
//!
//! # Integration with select!
//!
//! Oneshot receivers work in select arms just like other channels:
//!
//! ```ignore
//! let (tx_data, rx_data) = unbounded_mpmc::channel();
//! let (tx_once, rx_once) = oneshot::channel();
//!
//! select! {
//!     recv(rx_data) -> msg => println!("Data: {:?}", msg),
//!     recv(rx_once) -> msg => println!("Once: {:?}", msg),
//! }
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicPtr, AtomicUsize, Ordering::*},
};
use std::thread;

use arc_swap::ArcSwapOption;

use crate::{
    error::{RecvError, SendError, TryRecvError},
    waiter::{
        RecvWaiterList, SelectWaiter, abort_select_waiters, drain_select_waiters,
        new_recv_waiter_list, push_select_waiter, register_plain_recv_waiter,
        wake_all_recv_waiters, wake_one_recv_waiter, wake_select_all, wake_select_one,
    },
};

/// Tracks the lifecycle of the oneshot sender.
///
/// Stored in an `AtomicUsize` so it can be updated from `Sender::send` (which
/// consumes `self`) and observed from `Receiver` concurrently.
#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SenderState {
    /// Sender is alive and has not yet called `send()`.
    Open = 0,
    /// `send()` was called; the value is in the `ArcSwapOption`.
    Sent = 1,
    /// Sender was dropped without calling `send()`.
    Dropped = 2,
}

impl From<usize> for SenderState {
    fn from(v: usize) -> Self {
        match v {
            0 => Self::Open,
            1 => Self::Sent,
            _ => Self::Dropped,
        }
    }
}

struct ValueCell<T> {
    taken: std::sync::atomic::AtomicBool,
    value: std::cell::UnsafeCell<std::mem::MaybeUninit<T>>,
}

// SAFETY: ValueCell only exposes mutation through atomic single-take CAS.
unsafe impl<T: Send> Send for ValueCell<T> {}
// SAFETY: Shared access is synchronized by `taken` and ownership protocol.
unsafe impl<T: Send> Sync for ValueCell<T> {}

impl<T> ValueCell<T> {
    fn new(value: T) -> Self {
        ValueCell {
            taken: std::sync::atomic::AtomicBool::new(false),
            value: std::cell::UnsafeCell::new(std::mem::MaybeUninit::new(value)),
        }
    }

    fn take(&self) -> Option<T> {
        if self
            .taken
            .compare_exchange(false, true, AcqRel, Acquire)
            .is_err()
        {
            return None;
        }
        // SAFETY: successful CAS grants unique ownership of initialized value.
        Some(unsafe { (*self.value.get()).assume_init_read() })
    }
}

impl<T> Drop for ValueCell<T> {
    fn drop(&mut self) {
        if !*self.taken.get_mut() {
            // SAFETY: value is initialized iff it was never taken.
            unsafe { (*self.value.get()).assume_init_drop() };
        }
    }
}

pub(crate) struct Chan<T> {
    value: ArcSwapOption<ValueCell<T>>,
    // Lock-free stack for simple recv() waiters
    recv_waiters: RecvWaiterList,
    // Lock-free stack for select() waiters (heap-allocated SelectWaiter nodes)
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    sender_state: AtomicUsize,
    receiver_count: AtomicUsize,
}

pub struct Sender<T>(pub(crate) Arc<Chan<T>>);

impl<T> Sender<T> {
    /// Returns `true` if all [`Receiver`] handles have been dropped.
    pub fn is_closed(&self) -> bool {
        self.0.receiver_count.load(Acquire) == 0
    }

    pub fn send(self, val: T) -> Result<(), SendError<T>> {
        if self.0.receiver_count.load(Acquire) == 0 {
            return Err(SendError(val));
        }

        self.0.value.store(Some(Arc::new(ValueCell::new(val))));

        self.0
            .sender_state
            .store(SenderState::Sent as usize, Release);
        // Wake one from recv lock-free stack
        wake_one_recv_waiter(&self.0.recv_waiters);
        // Wake one select waiter (lock-free)
        wake_select_one(&self.0.select_waiters);
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self
            .0
            .sender_state
            .compare_exchange(
                SenderState::Open as usize,
                SenderState::Dropped as usize,
                AcqRel,
                Acquire,
            )
            .is_ok()
        {
            // Wake all recv waiters (lock-free)
            wake_all_recv_waiters(&self.0.recv_waiters);
            // Wake all select waiters (lock-free, frees nodes)
            wake_select_all(&self.0.select_waiters);
        }
    }
}

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
            // Last receiver dropped — drain any heap-allocated select waiters
            // to avoid memory leaks.
            drain_select_waiters(&self.0.select_waiters);
        }
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if SenderState::from(self.0.sender_state.load(Acquire)) == SenderState::Sent {
            if let Some(cell) = self.0.value.swap(None) {
                if let Some(val) = cell.take() {
                    return Ok(val);
                }
            }
            return Err(TryRecvError::Disconnected);
        }

        if SenderState::from(self.0.sender_state.load(Acquire)) == SenderState::Open {
            Err(TryRecvError::Empty)
        } else {
            Err(TryRecvError::Disconnected)
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Disconnected) => return Err(RecvError::Disconnected),
                Err(TryRecvError::Empty) => {}
                // oneshot channels cannot produce Lagged; this arm is unreachable.
                Err(TryRecvError::Lagged { .. }) => unreachable!("oneshot cannot lag"),
            }

            if self.0.sender_state.load(Acquire) != SenderState::Open as usize {
                return Err(RecvError::Disconnected);
            }

            // --- slow path: register waiter, re-check, park ---
            let _guard = register_plain_recv_waiter(&self.0.recv_waiters);

            // Re-check after registration to close the lost-wakeup window.
            if let Some(v) = self.try_recv().ok() {
                return Ok(v);
            }
            if self.0.sender_state.load(Acquire) != SenderState::Open as usize {
                return Err(RecvError::Disconnected);
            }

            thread::park();
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.0.sender_state.load(Acquire) != SenderState::Open as usize
    }

    pub(crate) fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        // Fast check: if channel already delivered/dropped, no need to register.
        if self.0.sender_state.load(Acquire) != SenderState::Open as usize {
            return;
        }
        // Allocate and push onto lock-free stack. Node is freed by sender or drain.
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.0.select_waiters);
    }

    pub(crate) fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        // Mark matching nodes as aborted (O(n) traversal, O(1) per node).
        // Nodes are freed when the sender next iterates the stack.
        abort_select_waiters(&self.0.select_waiters, selected);
    }

    pub(crate) fn complete_recv(&self) -> Result<T, RecvError> {
        self.recv()
    }
}

impl_selectable_receiver!([T] Receiver<T>, T);

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Chan {
        value: ArcSwapOption::empty(),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
        sender_state: AtomicUsize::new(SenderState::Open as usize),
        receiver_count: AtomicUsize::new(1),
    });
    (Sender(Arc::clone(&chan)), Receiver(chan))
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use crate::{select, unbounded_mpmc};

    use super::*;

    #[test]
    fn basic_send_recv() {
        let (tx, rx) = channel();
        tx.send(42).unwrap();
        assert_eq!(rx.recv(), Ok(42));
    }

    #[test]
    fn try_recv_empty_then_value() {
        let (tx, rx) = channel();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        tx.send(7).unwrap();
        assert_eq!(rx.try_recv(), Ok(7));
    }

    #[test]
    fn sender_drop_disconnects_receiver() {
        let (tx, rx) = channel::<i32>();
        drop(tx);
        assert_eq!(rx.recv(), Err(RecvError::Disconnected));
    }

    #[test]
    fn receiver_drop_causes_send_error() {
        let (tx, rx) = channel::<i32>();
        drop(rx);
        assert_eq!(tx.send(10), Err(SendError(10)));
    }

    #[test]
    fn single_delivery_across_clones() {
        let (tx, rx) = channel();
        let rx2 = rx.clone();
        tx.send(11).unwrap();

        let a = rx.try_recv();
        let b = rx2.try_recv();

        assert!(matches!(a, Ok(11)) || matches!(b, Ok(11)));
        assert!(
            matches!(a, Err(TryRecvError::Disconnected))
                || matches!(b, Err(TryRecvError::Disconnected))
        );
    }

    #[test]
    fn select_with_default_timeout_and_mixed_arms() {
        let (otx, orx) = channel::<&str>();
        let (tx, rx) = unbounded_mpmc::channel::<i32>();

        select! {
            recv(orx) -> _msg => panic!("oneshot must not be ready yet"),
            recv(rx) -> _msg => panic!("mpmc must not be ready yet"),
            default => {}
        }

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            otx.send("done").unwrap();
            let _ = tx.send(1);
        });

        select! {
            recv(orx) -> msg => assert_eq!(msg, Ok("done")),
            recv(rx) -> _msg => panic!("oneshot should win this race"),
        }
    }

    #[test]
    fn blocking_recv_waits_for_send() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.recv().unwrap());
        thread::sleep(Duration::from_millis(20));
        tx.send(77).unwrap();
        assert_eq!(handle.join().unwrap(), 77);
    }

    #[test]
    fn try_recv_disconnected_after_delivery() {
        let (tx, rx) = channel::<i32>();
        tx.send(42).unwrap();
        assert_eq!(rx.try_recv(), Ok(42));
        // After the value was taken the sender end is gone too; Disconnected.
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn is_ready_reflects_state() {
        let (tx, rx) = channel::<i32>();
        assert!(!rx.is_ready());
        tx.send(1).unwrap();
        assert!(rx.is_ready());
        rx.complete_recv().unwrap();
        // After delivery the value is gone and sender is consumed; Disconnected → ready.
        assert!(rx.is_ready());
    }

    #[test]
    fn complete_recv_blocks_if_not_yet_sent() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.complete_recv().unwrap());
        thread::sleep(Duration::from_millis(15));
        tx.send(5).unwrap();
        assert_eq!(handle.join().unwrap(), 5);
    }
}
