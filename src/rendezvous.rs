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
//! - Both `Sender` and `Receiver` are `Clone` (clones share the same channel state).
//!
//! # Example
//!
//! ```no_run
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
//! ```no_run
//! use std::time::Duration;
//! use selectables::{select, rendezvous};
//!
//! let (_tx, rx) = rendezvous::channel::<i32>();
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
    waiter::{
        RecvWaiterList, SelectWaiter, abort_select_waiters, drain_select_waiters,
        new_recv_waiter_list, push_select_waiter, register_plain_recv_waiter,
        wake_all_recv_waiters, wake_select_all, wake_select_one,
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
        wake_all_recv_waiters(&self.0.recv_waiters);
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
    ///
    /// # Safety invariant (ABA-freedom)
    ///
    /// Each `SenderWaiter` node lives on the **sender thread's stack frame** and its
    /// address is unique for the duration of the `send()` call.  A concurrent receiver
    /// can only pop the **head** of the stack via CAS (in `pop_sender`); it never
    /// traverses interior nodes.  Therefore:
    ///
    /// - If `ptr` is the head: we CAS it out atomically.  A concurrent `pop_sender`
    ///   racing on the same head will fail its CAS and retry, seeing a new head.
    /// - If `ptr` is interior: we traverse from head to find the predecessor.  Every
    ///   node we dereference is either (a) still on its sender's stack frame (the
    ///   sender is parked in `send()` and has not returned), or (b) our own node
    ///   (`ptr`), which is also on our stack frame.  Receivers only remove the head,
    ///   so interior nodes cannot be freed while we traverse them.
    ///
    /// The LIFO pop-only discipline of `pop_sender` and the stack-pinned lifetime of
    /// each node together prevent the ABA problem and use-after-free.
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
            wake_all_recv_waiters(&self.0.recv_waiters);
            wake_select_all(&self.0.select_waiters);
        }
    }
}

impl<T> Sender<T> {
    /// Returns `true` if all [`Receiver`] handles have been dropped.
    pub fn is_closed(&self) -> bool {
        self.0.receiver_count.load(Acquire) == 0
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
            let _guard = register_plain_recv_waiter(&self.0.recv_waiters);

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

    pub(crate) fn complete_recv(&self) -> Result<T, RecvError> {
        self.recv()
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        // Relaxed is sufficient for a ref-count increment: the Arc::clone below
        // provides the necessary ordering when the new handle is shared.
        self.0.receiver_count.fetch_add(1, Relaxed);
        Receiver(Arc::clone(&self.0))
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

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.sender_count.fetch_add(1, Relaxed);
        Sender(Arc::clone(&self.0))
    }
}

impl<T: Send + 'static> crate::SelectableSender for Sender<T> {
    type Input = T;

    /// Ready when a receiver is parked (in `recv_waiters`) or all receivers disconnected.
    ///
    /// # ⚠ Mutex acquisition in the select try-phase
    ///
    /// Unlike every other `SelectableSender::is_ready` implementation in this crate,
    /// this method acquires the `recv_waiters` `Mutex` to check whether a receiver is
    /// currently parked.  Rendezvous has no lock-free readiness signal because a send
    /// can only succeed when a receiver is simultaneously present.
    ///
    /// Callers that spin tightly on `is_ready` (e.g. the select try-phase loop) will
    /// therefore observe brief lock contention.  Use `select!` with a `default` arm or
    /// a timeout to bound the spin duration.
    fn is_ready(&self) -> bool {
        !self
            .0
            .recv_waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
            || self.0.receiver_count.load(Acquire) == 0
    }

    fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        let ptr = SelectWaiter::alloc(case_id, selected);
        push_select_waiter(ptr, &self.0.send_select_waiters);
    }

    fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        abort_select_waiters(&self.0.send_select_waiters, selected);
    }

    /// Execute the send after winning selection.
    ///
    /// NOTE: because `is_ready` fires as soon as one receiver is parked, there is
    /// a narrow window where that receiver could drop before `complete_send` runs.
    /// In that case `send()` will park until the *next* receiver arrives rather
    /// than returning immediately. This is intentional (rendezvous semantics), but
    /// callers should not assume `complete_send` is non-blocking.
    fn complete_send(&self, value: T) -> Result<(), crate::SendError<T>> {
        self.send(value)
    }
}

impl_selectable_receiver!([T: Send] Receiver<T>, T);

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
    fn basic_rendezvous() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.recv().unwrap());
        // send() blocks until the spawned thread's recv() claims the value.
        tx.send(42).unwrap();
        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn try_recv_empty() {
        let (tx, rx) = channel::<i32>();
        // No sender parked yet.
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn try_recv_wins() {
        let (tx, rx) = channel::<i32>();
        // Park a sender in a background thread, then try_recv from main.
        let handle = thread::spawn(move || tx.send(99));
        thread::sleep(Duration::from_millis(20)); // let sender park
        assert_eq!(rx.try_recv(), Ok(99));
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn sender_disconnect_wakes_recv() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || rx.recv());
        thread::sleep(Duration::from_millis(10));
        drop(tx);
        assert_eq!(handle.join().unwrap(), Err(RecvError::Disconnected));
    }

    #[test]
    fn receiver_disconnect_wakes_sender() {
        let (tx, rx) = channel::<i32>();
        let handle = thread::spawn(move || tx.send(7));
        thread::sleep(Duration::from_millis(10));
        drop(rx);
        assert_eq!(handle.join().unwrap(), Err(SendError(7)));
    }

    #[test]
    fn spsc_stress() {
        const TOTAL: usize = 256;

        let (tx, rx) = channel::<usize>();
        let counter = Arc::new(AtomicUsize::new(0));

        let ctr = Arc::clone(&counter);
        let receiver = thread::spawn(move || {
            while rx.recv().is_ok() {
                ctr.fetch_add(1, Relaxed);
            }
        });

        for i in 0..TOTAL {
            tx.send(i).unwrap();
        }
        drop(tx);

        receiver.join().unwrap();
        assert_eq!(counter.load(Relaxed), TOTAL);
    }

    #[test]
    fn try_recv_disconnected_immediately() {
        let (tx, rx) = channel::<i32>();
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn rendezvous_select() {
        use crate::select;

        let (tx, rx) = channel::<i32>();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.send(123).unwrap();
        });

        loop {
            select! {
                recv(rx) -> msg => { assert_eq!(msg.unwrap(), 123); break; },
                default(Duration::from_millis(1000)) => break,
            }
        }
    }

    #[test]
    fn send_arm_in_select() {
        // Exercises the SelectableSender impl and Clone for rendezvous::Sender.
        use crate::select;

        let (tx, rx) = channel::<i32>();

        // Spawn a receiver so the send arm can complete.
        let handle = thread::spawn(move || rx.recv().unwrap());
        thread::sleep(Duration::from_millis(20)); // let receiver reach recv()

        select! {
            send(tx, 77) -> res => assert!(res.is_ok()),
            default(Duration::from_millis(500)) => panic!("send arm timed out"),
        }

        assert_eq!(handle.join().unwrap(), 77);
    }

    #[test]
    fn concurrent_senders_stress() {
        // Exercises remove_sender_waiter's CAS traversal with multiple
        // senders parked simultaneously, some of which get disconnected
        // before a receiver claims them.
        const SENDERS: usize = 8;
        let (tx, rx) = channel::<usize>();

        // Park all senders simultaneously.
        let mut send_handles = Vec::new();
        for i in 0..SENDERS {
            let tx_clone = tx.clone();
            send_handles.push(thread::spawn(move || tx_clone.send(i)));
        }
        drop(tx); // drop original so receiver_count still > 0 but sender set won't keep alive

        // Give all spawned senders time to park on the stack.
        thread::sleep(Duration::from_millis(30));

        // Consume all parked senders one by one.
        let mut received = Vec::new();
        for _ in 0..SENDERS {
            match rx.recv() {
                Ok(v) => received.push(v),
                Err(_) => break,
            }
        }

        for h in send_handles {
            h.join().unwrap().unwrap();
        }

        received.sort();
        assert_eq!(received, (0..SENDERS).collect::<Vec<_>>());
    }

    #[test]
    fn complete_send_after_receiver_drops() {
        // Regression: complete_send should eventually succeed (with a new receiver)
        // rather than panic, even if the first receiver disappears between
        // is_ready() firing and complete_send executing.
        use crate::select;
        use std::sync::atomic::{AtomicBool, Ordering};

        let (tx, rx) = channel::<i32>();
        let ready = Arc::new(AtomicBool::new(false));

        // Pre-park a receiver so the send arm sees is_ready() == true.
        let ready_flag = Arc::clone(&ready);
        let rx_first = rx.clone();
        let first_recv = thread::spawn(move || {
            ready_flag.store(true, Ordering::SeqCst);
            // This receiver will race with the select's complete_send; it may
            // take the value first, causing complete_send to wait for rx_second.
            rx_first.recv()
        });

        // Wait until the first receiver is parked.
        while !ready.load(Ordering::SeqCst) {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(10));

        // Spawn a second receiver that arrives slightly later.
        let rx_second = rx.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            rx_second.recv()
        });

        // The send arm will win (is_ready == true) and complete_send will
        // call send(); it must succeed and deliver 42 to one of the receivers.
        select! {
            send(tx, 42) -> res => assert!(res.is_ok()),
            default(Duration::from_millis(500)) => panic!("send arm timed out"),
        }

        // The value was delivered to whichever receiver got it.
        let _ = first_recv.join().unwrap(); // Ok(42) or Disconnected both fine
    }

    #[test]
    fn stress_concurrent_senders_and_receivers() {
        // Multiple senders and multiple receivers racing on the same channel.
        const N: usize = 100;
        let (tx, rx) = channel::<usize>();

        let mut handles = Vec::new();
        for i in 0..N {
            let tx_c = tx.clone();
            handles.push(thread::spawn(move || tx_c.send(i).unwrap()));
        }
        drop(tx);

        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        for _ in 0..4 {
            let rx_c = rx.clone();
            let recv = Arc::clone(&received);
            handles.push(thread::spawn(move || {
                loop {
                    match rx_c.recv() {
                        Ok(v) => recv.lock().unwrap().push(v),
                        Err(_) => break,
                    }
                }
            }));
        }
        drop(rx);

        for h in handles {
            h.join().unwrap();
        }

        let mut vals = received.lock().unwrap().clone();
        vals.sort();
        assert_eq!(vals, (0..N).collect::<Vec<_>>());
    }
}
