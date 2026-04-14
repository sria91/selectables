//! Bounded multi-producer, multi-receiver broadcast channel with per-receiver lag detection.
//!
//! # Overview
//!
//! A bounded broadcast channel allows multiple senders to transmit messages to multiple receivers.
//! Each receiver maintains an independent position (cursor) in the message history, enabling
//! receivers to operate at their own pace. When a receiver falls behind the senders, it receives
//! a `Lagged` error indicating how many messages were skipped.
//!
//! # Lag semantics
//!
//! Lag occurs when a sender writes messages faster than a receiver can consume them, and the
//! bounded ring buffer fills and wraps around before the receiver catches up. The channel
//! detects this and reports it via `TryRecvError::Lagged { skipped }` or `RecvError::Lagged { skipped }`.
//!
//! When a receiver detects lag:
//! - It automatically advances its cursor to the oldest message still in the ring buffer.
//! - Subsequent recv/try_recv calls continue from this point (no additional lag errors).
//! - The `skipped` count tells you how many messages were lost.
//!
//! # Recommended lag handling patterns
//!
//! ## Pattern 1: Log and continue (common for telemetry)
//! ```ignore
//! loop {
//!     match rx.recv() {
//!         Ok(msg) => metrics.record(msg),
//!         Err(RecvError::Lagged { skipped }) => {
//!             eprintln!("Dropped {} metric events", skipped);
//!             // Automatically recovered; receiver cursor advanced
//!         }
//!         Err(RecvError::Disconnected) => break,
//!     }
//! }
//! ```
//!
//! ## Pattern 2: Backoff retry (for critical messages)
//! ```ignore
//! let mut backoff = Duration::from_millis(1);
//! loop {
//!     match select! {
//!         recv(rx) -> msg => msg,
//!         default(backoff) => continue,
//!     } {
//!         Ok(msg) => process(msg),
//!         Err(RecvError::Lagged { skipped }) => {
//!             eprintln!("Recovered from {} skipped messages", skipped);
//!             backoff = Duration::from_millis(1); // Reset backoff
//!         }
//!         Err(RecvError::Disconnected) => break,
//!     }
//! }
//! ```
//!
//! ## Pattern 3: High-performance polling with try_recv
//! ```ignore
//! while let Some(msg) = spin_poll_recv(&rx, Duration::from_micros(100)) {
//!     process(msg);
//! }
//!
//! fn spin_poll_recv<T: Clone>(rx: &Receiver<T>, spin_duration: Duration) -> Option<T> {
//!     let deadline = Instant::now() + spin_duration;
//!     loop {
//!         match rx.try_recv() {
//!             Ok(msg) => return Some(msg),
//!             Err(TryRecvError::Lagged { skipped }) => {
//!                 eprintln!("Spin loop lagged by {}", skipped);
//!                 // Cursor already recovered; continue polling
//!             }
//!             Err(TryRecvError::Empty) if Instant::now() < deadline => {
//!                 std::hint::spin_loop();
//!             }
//!             _ => return None,
//!         }
//!     }
//! }
//! ```
//!
//! # Cloning receivers
//!
//! Each cloned receiver starts with an independent cursor at the current write position,
//! meaning it will not receive messages that were already sent before the clone. This allows
//! new subscribers to join a broadcast without seeing historical messages.
//!
//! # Performance notes
//!
//! - `send()` is lock-free: only updates atomics and wakes waiting receivers.
//! - `try_recv()` is lock-free: uses atomic loads and CAS to detect lag.
//! - `recv()` uses a Mutex only to manage the waiter list during blocking.
//! - Per-receiver cursors are stored in `Arc<AtomicUsize>`, enabling independent lag tracking.

use std::{
    sync::Arc,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering::*},
    thread,
};

use arc_swap::ArcSwapOption;

use crate::{
    error::{RecvError, SendError, TryRecvError},
    waiter::{
        RecvWaiter, RecvWaiterGuard, RecvWaiterList, SelectWaiter, UNSELECTED,
        abort_select_waiters, drain_select_waiters, new_recv_waiter_list, push_select_waiter,
        wake_all_recv_waiters, wake_all_unselected_recv_waiters, wake_select_all,
    },
};

struct Slot<T> {
    seq: AtomicUsize,
    value: ArcSwapOption<T>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Slot {
            seq: AtomicUsize::new(0),
            value: ArcSwapOption::empty(),
        }
    }
}

pub(crate) struct Chan<T> {
    slots: Box<[Slot<T>]>,
    cap: usize,
    write_seq: AtomicUsize,
    recv_waiters: RecvWaiterList,
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
}

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
            wake_all_recv_waiters(&self.0.recv_waiters, UNSELECTED);
            wake_select_all(&self.0.select_waiters);
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.0.receiver_count.load(Acquire) == 0 {
            return Err(SendError(value));
        }

        let seq = self.0.write_seq.fetch_add(1, AcqRel);
        let slot = &self.0.slots[seq % self.0.cap];

        slot.value.store(Some(Arc::new(value)));
        slot.seq.store(seq + 1, Release);

        // Broadcast channel should wake all potentially waiting receivers.
        wake_all_unselected_recv_waiters(&self.0.recv_waiters);
        wake_select_all(&self.0.select_waiters);
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.0.receiver_count.load(Acquire) == 0
    }
}

pub struct Receiver<T> {
    chan: Arc<Chan<T>>,
    next_seq: Arc<AtomicUsize>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.chan.receiver_count.fetch_add(1, Relaxed);
        Receiver {
            chan: Arc::clone(&self.chan),
            // New subscribers start at current tail and observe future sends.
            next_seq: Arc::new(AtomicUsize::new(self.chan.write_seq.load(Acquire))),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let prev = self.chan.receiver_count.fetch_sub(1, AcqRel);
        if prev == 1 {
            drain_select_waiters(&self.chan.select_waiters);
        }
    }
}

impl<T: Clone> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        loop {
            let next = self.next_seq.load(Acquire);
            let write = self.chan.write_seq.load(Acquire);

            if write == next {
                if self.chan.sender_count.load(Acquire) == 0 {
                    return Err(TryRecvError::Disconnected);
                }
                return Err(TryRecvError::Empty);
            }

            let oldest = write.saturating_sub(self.chan.cap);
            if next < oldest {
                let skipped = oldest - next;
                self.next_seq.store(oldest, Release);
                return Err(TryRecvError::Lagged { skipped });
            }

            let expected = next + 1;
            let slot = &self.chan.slots[next % self.chan.cap];
            let seq_before = slot.seq.load(Acquire);

            if seq_before < expected {
                // Writer has advanced tail but this slot is not yet published.
                if self.chan.sender_count.load(Acquire) == 0
                    && self.chan.write_seq.load(Acquire) == next
                {
                    return Err(TryRecvError::Disconnected);
                }
                return Err(TryRecvError::Empty);
            }

            if seq_before > expected {
                let write_now = self.chan.write_seq.load(Acquire);
                let oldest_now = write_now.saturating_sub(self.chan.cap);
                let skipped = oldest_now.saturating_sub(next).max(1);
                self.next_seq.store(oldest_now, Release);
                return Err(TryRecvError::Lagged { skipped });
            }

            let snapshot = slot.value.load_full();
            let seq_after = slot.seq.load(Acquire);
            if seq_after != expected {
                continue;
            }

            if let Some(v) = snapshot {
                self.next_seq.store(next + 1, Release);
                return Ok((*v).clone());
            }
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        let marker = Arc::new(AtomicUsize::new(UNSELECTED));
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Lagged { skipped }) => {
                    return Err(RecvError::Lagged { skipped });
                }
                Err(TryRecvError::Disconnected) => return Err(RecvError::Disconnected),
                Err(TryRecvError::Empty) => {}
            }

            let waiter = RecvWaiter::new(usize::MAX, Arc::clone(&marker));
            let _guard = RecvWaiterGuard::register(waiter, &self.chan.recv_waiters);

            // Re-check after push
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Lagged { skipped }) => return Err(RecvError::Lagged { skipped }),
                Err(TryRecvError::Disconnected) => return Err(RecvError::Disconnected),
                Err(TryRecvError::Empty) => {}
            }
            if marker.load(Acquire) != UNSELECTED {
                match self.try_recv() {
                    Ok(v) => return Ok(v),
                    _ => return Err(RecvError::Disconnected),
                }
            }

            thread::park();
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        let next = self.next_seq.load(Acquire);
        let write = self.chan.write_seq.load(Acquire);
        write > next || self.chan.sender_count.load(Acquire) == 0
    }

    pub(crate) fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        let next = self.next_seq.load(Acquire);
        let write = self.chan.write_seq.load(Acquire);
        if write > next || self.chan.sender_count.load(Acquire) == 0 {
            return;
        }
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.chan.select_waiters);
    }

    pub(crate) fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        abort_select_waiters(&self.chan.select_waiters, selected);
    }

    pub fn complete_recv(&self) -> Result<T, RecvError> {
        self.recv()
    }
}

impl<T: Clone> crate::SelectableReceiver for Receiver<T> {
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

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "broadcast capacity must be > 0");

    let slots: Box<[Slot<T>]> = (0..capacity).map(|_| Slot::new()).collect();
    let chan = Arc::new(Chan {
        slots,
        cap: capacity,
        write_seq: AtomicUsize::new(0),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
    });

    (
        Sender(Arc::clone(&chan)),
        Receiver {
            chan,
            next_seq: Arc::new(AtomicUsize::new(0)),
        },
    )
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use crate::{select, unbounded_mpmc};

    use super::*;

    #[test]
    fn broadcast_reaches_multiple_receivers() {
        let (tx, rx1) = channel::<i32>(8);
        let rx2 = rx1.clone();

        tx.send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(rx1.recv(), Ok(1));
        assert_eq!(rx1.recv(), Ok(2));
        assert_eq!(rx2.recv(), Ok(1));
        assert_eq!(rx2.recv(), Ok(2));
    }

    #[test]
    fn lagged_receiver_reports_skipped() {
        let (tx, rx) = channel::<i32>(2);

        tx.send(10).unwrap();
        tx.send(20).unwrap();
        tx.send(30).unwrap();

        assert_eq!(rx.try_recv(), Err(TryRecvError::Lagged { skipped: 1 }));
        assert_eq!(rx.recv(), Ok(20));
        assert_eq!(rx.recv(), Ok(30));
    }

    #[test]
    fn disconnect_after_drain() {
        let (tx, rx) = channel::<i32>(4);
        tx.send(7).unwrap();
        drop(tx);

        assert_eq!(rx.recv(), Ok(7));
        assert_eq!(rx.recv(), Err(RecvError::Disconnected));
    }

    #[test]
    fn select_integration_mixed_arms() {
        let (btx, brx) = channel::<&str>(4);
        let (_tx, rx) = unbounded_mpmc::channel::<i32>();

        select! {
            recv(brx) -> _ => panic!("broadcast should not be ready yet"),
            recv(rx) -> _ => panic!("mpmc should not be ready yet"),
            default => {}
        }

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            btx.send("hi").unwrap();
        });

        select! {
            recv(brx) -> msg => assert_eq!(msg, Ok("hi")),
            recv(rx) -> _ => panic!("mpmc arm should remain empty"),
            default(Duration::from_millis(100)) => panic!("unexpected timeout"),
        }
    }

    #[test]
    fn try_recv_empty_when_nothing_sent() {
        let (_tx, rx) = channel::<i32>(4);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn try_recv_disconnected_when_sender_dropped() {
        let (tx, rx) = channel::<i32>(4);
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn multiple_senders_all_broadcast_to_each_receiver() {
        let (tx1, rx1) = channel::<i32>(16);
        let tx2 = tx1.clone();
        let rx2 = rx1.clone();

        tx1.send(10).unwrap();
        tx2.send(20).unwrap();

        // Every receiver sees every message.
        let mut r1: Vec<i32> = vec![rx1.recv().unwrap(), rx1.recv().unwrap()];
        let mut r2: Vec<i32> = vec![rx2.recv().unwrap(), rx2.recv().unwrap()];
        r1.sort();
        r2.sort();
        assert_eq!(r1, vec![10, 20]);
        assert_eq!(r2, vec![10, 20]);
    }

    #[test]
    fn blocking_recv_woken_by_send() {
        let (tx, rx) = channel::<i32>(4);
        let handle = thread::spawn(move || rx.recv().unwrap());
        thread::sleep(Duration::from_millis(20));
        tx.send(55).unwrap();
        assert_eq!(handle.join().unwrap(), 55);
    }

    #[test]
    fn blocking_recv_woken_by_sender_disconnect() {
        let (tx, rx) = channel::<i32>(4);
        let handle = thread::spawn(move || rx.recv());
        thread::sleep(Duration::from_millis(20));
        drop(tx);
        assert_eq!(handle.join().unwrap(), Err(RecvError::Disconnected));
    }

    #[test]
    fn capacity_one_causes_lag() {
        let (tx, rx) = channel::<i32>(1);
        tx.send(1).unwrap();
        tx.send(2).unwrap(); // overwrites; receiver missed seq 0
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Lagged { .. })));
        assert_eq!(rx.recv(), Ok(2));
    }

    #[test]
    fn receiver_clone_starts_at_write_position() {
        // Cloning a receiver creates a NEW cursor at the current write position.
        // The clone does NOT see messages that were sent before the clone was made.
        let (tx, rx1) = channel::<i32>(8);
        tx.send(1).unwrap();
        tx.send(2).unwrap();

        // Clone made AFTER two sends; clone starts at write_seq (position 2).
        let rx2 = rx1.clone();

        // Original still sees messages from its cursor (position 0).
        assert_eq!(rx1.recv().unwrap(), 1);
        assert_eq!(rx1.recv().unwrap(), 2);

        // Both see future messages sent after the clone was created.
        tx.send(3).unwrap();
        assert_eq!(rx1.recv().unwrap(), 3);
        assert_eq!(rx2.recv().unwrap(), 3);
    }

    #[test]
    fn sender_is_closed_only_after_all_receivers_drop() {
        let (tx, rx1) = channel::<i32>(4);
        let rx2 = rx1.clone();
        assert!(!tx.is_closed());
        drop(rx1);
        assert!(!tx.is_closed()); // rx2 still alive
        drop(rx2);
        assert!(tx.is_closed());
    }

    #[test]
    fn stress_broadcast_many_messages() {
        const MSGS: usize = 1_000;
        let (tx, rx1) = channel::<usize>(MSGS + 8);
        let rx2 = rx1.clone();

        let sender = thread::spawn(move || {
            for i in 0..MSGS {
                tx.send(i).unwrap();
            }
        });
        sender.join().unwrap();

        for i in 0..MSGS {
            assert_eq!(rx1.recv().unwrap(), i);
            assert_eq!(rx2.recv().unwrap(), i);
        }
    }
}
