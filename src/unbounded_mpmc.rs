//! Unbounded lock-free multi-producer, multi-consumer channel.
//!
//! # Overview
//!
//! An unbounded MPMC channel has unlimited capacity (backed by Crossbeam's `SegQueue`).
//! Multiple senders push messages into a lock-free queue, and multiple receivers pop them.
//! Each message is consumed by exactly one receiver. Ideal for workloads where producers
//! can briefly outpace consumers but need guaranteed delivery without backpressure.
//!
//! # Send behavior
//!
//! - `send()` never fails — capacity is unlimited
//! - Always succeeds unless all receivers are disconnected
//! - Wakes one waiting receiver per send (efficient MPMC wake-up)
//!
//! # Lock-free throughout
//!
//! - `send()` is lock-free: uses atomic queue push; Mutex only to wake one receiver
//! - `try_recv()` is lock-free: atomic queue pop
//! - `recv()` is lock-free on hot path; uses Mutex only for waiter registration during slow-path blocks
//!
//! # No lag or backpressure
//!
//! Unlike `bounded_broadcast`:
//! - No lag detection (unlimited capacity)
//! - No message loss (all sends succeed)
//! - No buffer constraints (memory-limited only by the heap)
//!
//! # Cloning
//!
//! - Both `Sender` and `Receiver` are fully cloneable
//! - All senders/receivers share the same underlying queue
//! - Disconnect observed when last sender OR last receiver drops
//!
//! # Example
//!
//! ```ignore
//! let (tx, rx) = unbounded_mpmc::channel();
//!
//! // Multiple senders — never fails
//! for i in 0..1000 {
//!     tx.send(i).unwrap(); // Always succeeds
//! }
//!
//! // Multiple receivers
//! let rx1 = rx.clone();
//! std::thread::spawn(move || { while let Ok(v) = rx.recv() { process(v); } });
//! std::thread::spawn(move || { while let Ok(v) = rx1.recv() { process(v); } });
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
        abort_select_waiters, new_recv_waiter_list, push_select_waiter, wake_one_recv_waiter,
        wake_select_all, wake_select_one,
    },
};

// ════════════════════════════════════════════════════════════════════════════
// Channel shared state
// ════════════════════════════════════════════════════════════════════════════

pub(crate) struct Chan<T> {
    /// Lock-free unbounded queue.  Push never fails; pop is O(1) amortised.
    queue: SegQueue<T>,
    /// Lock-free waiter stack for simple recv path
    recv_waiters: RecvWaiterList,
    /// Mutex-protected waiter list for select registration
    select_waiters: Arc<AtomicPtr<SelectWaiter>>,
    /// Number of live `Sender<T>` handles.
    sender_count: AtomicUsize,
    /// Number of live `Receiver<T>` handles; tracked so senders can detect disconnect.
    receiver_count: AtomicUsize,
}

// ════════════════════════════════════════════════════════════════════════════
// Constructors
// ════════════════════════════════════════════════════════════════════════════

/// Create an unbounded MPMC channel.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Chan {
        queue: SegQueue::new(),
        recv_waiters: new_recv_waiter_list(),
        select_waiters: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
    });
    log_debug!("unbounded_mpmc::unbounded: chan={:p}", Arc::as_ptr(&chan));
    (Sender(Arc::clone(&chan)), Receiver(chan))
}

// ════════════════════════════════════════════════════════════════════════════
// Sender<T>
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
        log_debug!(
            "unbounded_mpmc::sender_drop: chan={:p}, remaining_senders={}",
            Arc::as_ptr(&self.0),
            prev - 1
        );
        if prev == 1 {
            // Last sender dropped — wake all waiting receivers so they can
            // observe the disconnected state.
            log_debug!(
                "unbounded_mpmc::sender_drop: chan={:p}, disconnecting",
                Arc::as_ptr(&self.0)
            );
            wake_one_recv_waiter(&self.0.recv_waiters, UNSELECTED);
            wake_select_all(&self.0.select_waiters);
        }
    }
}

impl<T> Sender<T> {
    /// Send a value.  An unbounded channel never blocks; this always succeeds.
    pub fn send(&self, val: T) -> Result<(), SendError<T>> {
        // ① Push to the lock-free queue.
        // ② Acquire the waiters lock and scan for a receiver to wake.
        //
        // Correctness: the push happens BEFORE the waiters lock is acquired,
        // so any receiver that re-checks the queue under the waiters lock (in
        // recv()'s slow path) will observe the new item and avoid parking.
        self.0.queue.push(val);
        log_debug!(
            "unbounded_mpmc::send: chan={:p}, queue_len={}",
            Arc::as_ptr(&self.0),
            self.0.queue.len()
        );

        wake_one_recv_waiter(&self.0.recv_waiters, UNSELECTED);
        wake_select_one(&self.0.select_waiters);
        Ok(())
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Receiver<T>
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
        self.0.receiver_count.fetch_sub(1, AcqRel);
    }
}

impl<T> Receiver<T> {
    // ── Public API ────────────────────────────────────────────────────────

    /// Non-blocking receive.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(v) = self.0.queue.pop() {
            log_debug!(
                "unbounded_mpmc::try_recv: chan={:p}, got value",
                Arc::as_ptr(&self.0)
            );
            return Ok(v);
        }
        if self.0.sender_count.load(Acquire) == 0 {
            log_debug!(
                "unbounded_mpmc::try_recv: chan={:p}, Disconnected",
                Arc::as_ptr(&self.0)
            );
            Err(TryRecvError::Disconnected)
        } else {
            log_debug!(
                "unbounded_mpmc::try_recv: chan={:p}, Empty",
                Arc::as_ptr(&self.0)
            );
            Err(TryRecvError::Empty)
        }
    }

    /// Blocking receive.  Parks the calling thread until a message arrives
    /// or all senders are dropped.
    pub fn recv(&self) -> Result<T, RecvError> {
        // A per-call Arc lets us identify and remove stale waiters after
        // spurious wakeups without holding any lock during the park.
        let marker = Arc::new(AtomicUsize::new(UNSELECTED));
        loop {
            // --- fast path (lock-free) ---
            if let Some(v) = self.0.queue.pop() {
                return Ok(v);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            // --- slow path: register waiter on lock-free stack, re-check, park ---
            // Re-checking closes the lost-wakeup window: if a sender pushed between
            // the fast-path check above and us pushing onto the stack, we'll find
            // the item here. Otherwise, the sender will see our entry and wake us.
            if let Some(v) = self.0.queue.pop() {
                return Ok(v);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            // Create waiter and push onto lock-free stack
            let waiter = RecvWaiter::new(usize::MAX, Arc::clone(&marker));
            let _guard = RecvWaiterGuard::register(waiter, &self.0.recv_waiters);

            // Another check after pushing
            if let Some(v) = self.0.queue.pop() {
                return Ok(v);
            }
            if self.0.sender_count.load(Acquire) == 0 {
                return Err(RecvError::Disconnected);
            }

            // Check if we were already selected (sender beat us)
            if marker.load(Acquire) != UNSELECTED {
                if let Some(v) = self.0.queue.pop() {
                    return Ok(v);
                }
                return Err(RecvError::Disconnected);
            }

            thread::park();
        }
    }

    // ── Hooks used by Select (crate-internal) ─────────────────────────────

    /// `true` if a message is available *or* the channel is disconnected.
    /// Lock-free — called during the try phase of select.
    pub(crate) fn is_ready(&self) -> bool {
        !self.0.queue.is_empty() || self.0.sender_count.load(Acquire) == 0
    }

    /// Register a select waiter (**registration phase**).
    pub(crate) fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        log_trace!(
            "unbounded_mpmc::register_select: chan={:p}, case_id={}",
            Arc::as_ptr(&self.0),
            case_id
        );
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.0.select_waiters);
    }

    /// Remove our select waiter (**abort phase**), identified by pointer
    /// equality on the shared `selected` arc.
    pub(crate) fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        log_trace!(
            "unbounded_mpmc::abort_select: chan={:p}",
            Arc::as_ptr(&self.0)
        );
        abort_select_waiters(&self.0.select_waiters, selected);
    }

    /// Called by `select!` after this arm wins, to consume the item.
    ///
    /// If another receiver raced ahead and took the item between `select()`
    /// returning and this call, we simply block until the next message — this
    /// is safe for any number of concurrent consumers.
    pub fn complete_recv(&self) -> Result<T, RecvError> {
        self.recv()
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

    /// Unbounded send is always immediately ready (unlimited capacity).
    /// Returns `true` even when disconnected so the arm completes fast with `Err`.
    fn is_ready(&self) -> bool {
        true
    }

    /// No registration needed — unbounded senders are always ready.
    fn register_select(&self, _case_id: usize, _selected: Arc<AtomicUsize>) {}

    fn abort_select(&self, _selected: &Arc<AtomicUsize>) {}

    fn complete_send(&self, value: T) -> Result<(), crate::SendError<T>> {
        self.send(value)
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::Select;

    use super::*;

    // ── Basic send / receive ──────────────────────────────────────────────

    #[test]
    fn basic_send_recv() {
        let (tx, rx) = channel();
        tx.send(42u32).unwrap();
        assert_eq!(rx.try_recv(), Ok(42));
    }

    #[test]
    fn fifo_ordering() {
        let (tx, rx) = channel();
        for i in 0..5u32 {
            tx.send(i).unwrap();
        }
        for i in 0..5u32 {
            assert_eq!(rx.recv().unwrap(), i);
        }
    }

    #[test]
    fn empty_then_disconnected() {
        let (tx, rx) = channel::<i32>();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    // ── Blocking recv ─────────────────────────────────────────────────────

    #[test]
    fn blocking_recv_wakes_on_send() {
        let (tx, rx) = channel::<u32>();
        let handle = thread::spawn(move || rx.recv().unwrap());
        thread::sleep(Duration::from_millis(20));
        tx.send(7).unwrap();
        assert_eq!(handle.join().unwrap(), 7);
    }

    #[test]
    fn blocking_recv_wakes_on_disconnect() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.recv());
        thread::sleep(Duration::from_millis(20));
        drop(tx);
        assert_eq!(handle.join().unwrap(), Err(RecvError::Disconnected));
    }

    // ── Multi-sender / multi-receiver ─────────────────────────────────────

    #[test]
    fn multiple_senders() {
        let (tx, rx) = channel::<u32>();
        let mut handles = Vec::new();
        for i in 0..4u32 {
            let tx_clone = tx.clone();
            handles.push(thread::spawn(move || tx_clone.send(i).unwrap()));
        }
        drop(tx);
        for h in handles {
            h.join().unwrap();
        }
        let mut collected: Vec<u32> = (0..4).map(|_| rx.recv().unwrap()).collect();
        collected.sort();
        assert_eq!(collected, vec![0, 1, 2, 3]);
    }

    #[test]
    fn multiple_receivers_share_messages() {
        let (tx, rx) = channel::<u32>();
        let rx2 = rx.clone();

        let h1 = thread::spawn(move || rx.recv().unwrap());
        let h2 = thread::spawn(move || rx2.recv().unwrap());

        tx.send(1).unwrap();
        tx.send(2).unwrap();

        let mut results = vec![h1.join().unwrap(), h2.join().unwrap()];
        results.sort();
        assert_eq!(results, vec![1, 2]);
    }

    // ── select integration ────────────────────────────────────────────────

    #[test]
    fn select_recv_ready() {
        let (tx, rx) = channel::<i32>();
        tx.send(99).unwrap();
        let mut sel = Select::new();
        let idx = sel.recv(rx.clone());
        let op = sel.try_select().expect("should be ready");
        assert_eq!(op.index, idx);
        let val = rx.complete_recv().unwrap();
        assert_eq!(val, 99);
    }

    #[test]
    fn select_not_ready_when_empty() {
        let (_tx, rx) = channel::<i32>();
        let mut sel = Select::new();
        sel.recv(rx.clone());
        assert!(sel.try_select().is_none());
    }

    // ── complete_recv race safety ─────────────────────────────────────────

    #[test]
    fn complete_recv_waits_when_race_steals_message() {
        let (tx, rx) = channel();
        let rx_other = rx.clone();
        tx.send(1).unwrap();

        let started = Arc::new(AtomicBool::new(false));
        let consumed = Arc::new(AtomicBool::new(false));
        let start_flag = started.clone();
        let consumed_flag = consumed.clone();
        let tx_clone = tx.clone();

        let handle = thread::spawn(move || {
            let mut sel = Select::new();
            sel.recv(rx_other.clone());
            let _ = sel.select();
            start_flag.store(true, Ordering::SeqCst);
            while !consumed_flag.load(Ordering::SeqCst) {
                thread::yield_now();
            }
            tx_clone.send(2).unwrap();
            rx_other.complete_recv().unwrap()
        });

        while !started.load(Ordering::SeqCst) {
            thread::yield_now();
        }

        let value = rx.recv().unwrap();
        consumed.store(true, Ordering::SeqCst);
        assert_eq!(value, 1);

        let result = handle.join().unwrap();
        assert_eq!(result, 2);
    }

    // ── stress test ───────────────────────────────────────────────────────

    #[test]
    fn stress_concurrent_senders_receivers() {
        const SENDERS: usize = 4;
        const RECEIVERS: usize = 4;
        const MSGS_PER_SENDER: u32 = 500;
        const TOTAL: u32 = SENDERS as u32 * MSGS_PER_SENDER;

        let (tx, rx) = channel::<u32>();

        let mut handles = Vec::new();

        for s in 0..SENDERS {
            let tx_clone = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..MSGS_PER_SENDER {
                    tx_clone.send(s as u32 * MSGS_PER_SENDER + i).unwrap();
                }
            }));
        }

        let received = Arc::new(AtomicUsize::new(0));
        for _ in 0..RECEIVERS {
            let rx_clone = rx.clone();
            let counter = Arc::clone(&received);
            handles.push(thread::spawn(move || {
                loop {
                    match rx_clone.try_recv() {
                        Ok(_) => {
                            counter.fetch_add(1, Relaxed);
                        }
                        Err(TryRecvError::Empty) => thread::yield_now(),
                        Err(TryRecvError::Disconnected) => break,
                        Err(TryRecvError::Lagged { .. }) => {
                            unreachable!("unbounded_mpmc cannot lag")
                        }
                    }
                }
            }));
        }

        drop(tx); // signal receivers to stop once queue drains
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.load(Relaxed) as u32, TOTAL);
    }
}
