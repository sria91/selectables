//! Waiter infrastructure for channel blocking and `select!` operations.
//!
//! Two subsystems serve distinct needs:
//!
//! ## Recv waiters — [`RecvWaiter`] / [`RecvWaiterList`] / [`RecvWaiterGuard`]
//!
//! Used by blocking `recv()` calls to park the calling thread until a message
//! arrives or the channel disconnects.  Backed by `Mutex<Vec<Arc<RecvWaiter>>>`
//! so concurrent wake calls can iterate the list without use-after-free hazards.
//!
//! ```text
//! let waiter = RecvWaiter::new(case_id, Arc::clone(&selected));
//! let _guard = RecvWaiterGuard::register(Arc::clone(&waiter), &chan.recv_waiters);
//! thread::park();  // woken by sender or on disconnect
//! // _guard removes the waiter from the list on drop
//! ```
//!
//! ## Select waiters — [`SelectWaiter`] / [`push_select_waiter`] / [`wake_select_one`]
//!
//! Used by `select!` arm registration.  Because `register_select()` returns
//! *before* the thread parks, the node must outlive its registration call site;
//! nodes are therefore heap-allocated via [`Box`] and owned by the stack.
//! Losing arms mark their nodes `aborted`; the next sender to drain the stack
//! frees aborted nodes inline, avoiding a separate GC pass.
//!
//! ## `selected` atomic protocol
//!
//! Both subsystems share the same arm-selection protocol.  A `selected`
//! `Arc<AtomicUsize>` starts at [`UNSELECTED`] (`usize::MAX`).  Whichever
//! sender wins the compare-exchange writes the winning `case_id` into it.
//! The woken thread reads `selected` after unparking to discover which arm fired.

use std::{
    sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering::*},
    sync::{Arc, Mutex},
    thread,
};

// ════════════════════════════════════════════════════════════════════════════
// Recv Waiter List
// ════════════════════════════════════════════════════════════════════════════

/// A heap-allocated waiter node for blocking recv operations.
///
/// Each parked receiver thread creates one `Arc<RecvWaiter>` and registers it
/// in its channel's [`RecvWaiterList`].  The `Arc` reference count ensures the
/// node outlives any concurrent wake call, eliminating the use-after-free that
/// would otherwise affect intrusive stack-allocated waiters.
pub(crate) struct RecvWaiter {
    /// Select-arm index this waiter belongs to.  Plain (non-`select!`) recv
    /// calls use `usize::MAX` ([`UNSELECTED`]).
    pub(crate) case_id: usize,
    /// Shared atomic written by the winning sender.  Starts at [`UNSELECTED`];
    /// the sender stores `case_id` here via compare-exchange before unparking.
    pub(crate) selected: Arc<AtomicUsize>,
    /// Handle to the parked thread, used to call [`thread::Thread::unpark`].
    pub(crate) thread: thread::Thread,
}

impl RecvWaiter {
    /// Create a new waiter for the calling thread and return it wrapped in an `Arc`.
    ///
    /// `case_id` is the select-arm index (use [`UNSELECTED`] for plain recv).
    /// `selected` is the shared atomic that coordinates which arm wins.
    pub(crate) fn new(case_id: usize, selected: Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(RecvWaiter {
            case_id,
            selected,
            thread: thread::current(),
        })
    }
}

/// Shared list of pending recv waiters for a single channel direction.
///
/// An `Arc` lets the list be shared across cloned sender/receiver handles;
/// the inner `Mutex` serialises insertion, removal, and snapshot operations.
pub(crate) type RecvWaiterList = Arc<Mutex<Vec<Arc<RecvWaiter>>>>;

/// Allocate a new, empty [`RecvWaiterList`].
pub(crate) fn new_recv_waiter_list() -> RecvWaiterList {
    Arc::new(Mutex::new(Vec::new()))
}

/// Create and register a [`RecvWaiterGuard`] for a plain (non-`select!`) blocking recv call.
///
/// This is a convenience wrapper for the common slow-path pattern in blocking `recv()`
/// implementations.  The waiter uses `case_id = UNSELECTED` as an identity-only token;
/// the internal `selected` atomic is never observed by the caller — it exists solely to
/// satisfy the [`RecvWaiter`] constructor signature.
pub(crate) fn register_plain_recv_waiter(list: &RecvWaiterList) -> RecvWaiterGuard {
    let marker = Arc::new(AtomicUsize::new(UNSELECTED));
    let waiter = RecvWaiter::new(UNSELECTED, marker);
    RecvWaiterGuard::register(waiter, list)
}

/// RAII guard that registers a [`RecvWaiter`] in a [`RecvWaiterList`] on
/// construction and removes it on drop.
///
/// Create one immediately before parking a thread; the guard ensures the
/// waiter is removed from the list when the thread wakes and the enclosing
/// scope exits, so stale waiters never accumulate.
pub(crate) struct RecvWaiterGuard {
    waiter: Arc<RecvWaiter>,
    list: RecvWaiterList,
}

impl RecvWaiterGuard {
    /// Insert `waiter` into `list` and return a guard that removes it on drop.
    pub(crate) fn register(waiter: Arc<RecvWaiter>, list: &RecvWaiterList) -> Self {
        // Poisoning is harmless: the waiter list is append/remove-only data and
        // contains no invariant that a panicking thread could violate. Recovering
        // the inner Vec via `into_inner` lets the channel keep operating.
        list.lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Arc::clone(&waiter));
        RecvWaiterGuard {
            waiter,
            list: Arc::clone(list),
        }
    }
}

impl Drop for RecvWaiterGuard {
    fn drop(&mut self) {
        let ptr = Arc::as_ptr(&self.waiter);
        let mut guard = self.list.lock().unwrap_or_else(|e| e.into_inner());
        guard.retain(|w| Arc::as_ptr(w) != ptr);
    }
}

/// Wake at most one parked receiver.
///
/// Takes a snapshot of the list under the lock, then iterates outside the lock
/// attempting a compare-exchange on each waiter's `selected` field.  The first
/// waiter whose CAS succeeds (`unselected` → `case_id`) is unparked and the
/// function returns `true`.  Returns `false` if the list was empty or every
/// waiter had already been claimed by another event.
///
/// Called after a single message is pushed into a channel buffer.
pub(crate) fn wake_one_recv_waiter(list: &RecvWaiterList) -> bool {
    let waiters: Vec<Arc<RecvWaiter>> = list.lock().unwrap_or_else(|e| e.into_inner()).clone();
    for waiter in waiters {
        if waiter
            .selected
            .compare_exchange(UNSELECTED, waiter.case_id, SeqCst, SeqCst)
            .is_ok()
        {
            waiter.thread.unpark();
            return true;
        }
    }
    false
}

/// Wake every parked receiver.
///
/// Takes a snapshot of the list under the lock, then for each waiter attempts
/// a CAS from `unselected` to `case_id`.  The CAS may fail if the waiter was
/// already claimed by a concurrent event; the thread is unparked regardless so
/// it can re-check channel state.
///
/// Called on sender disconnect and in broadcast channels.
pub(crate) fn wake_all_recv_waiters(list: &RecvWaiterList) {
    let waiters: Vec<Arc<RecvWaiter>> = list.lock().unwrap_or_else(|e| e.into_inner()).clone();
    for waiter in waiters {
        waiter
            .selected
            .compare_exchange(UNSELECTED, waiter.case_id, SeqCst, SeqCst)
            .ok();
        waiter.thread.unpark();
    }
}

/// Wake every receiver that has not yet been selected.
///
/// Unlike [`wake_all_recv_waiters`], this skips waiters whose `selected` field
/// is no longer [`UNSELECTED`].  Used by MPSC sends that need to nudge blocked
/// receivers without clobbering a waiter that is mid-`select!` on another arm.
pub(crate) fn wake_all_unselected_recv_waiters(list: &RecvWaiterList) {
    let waiters: Vec<Arc<RecvWaiter>> = list.lock().unwrap_or_else(|e| e.into_inner()).clone();
    for waiter in waiters {
        if waiter.selected.load(Acquire) == UNSELECTED {
            waiter.thread.unpark();
        }
    }
}

/// Sentinel stored in the shared `selected` atomic when no arm has won yet.
pub(crate) const UNSELECTED: usize = usize::MAX;

// ════════════════════════════════════════════════════════════════════════════
// Lock-free Select Waiters
// ════════════════════════════════════════════════════════════════════════════
//
// The select protocol calls `register_select()` on channels, then parks, then
// calls `abort_select()` on all losing channels after waking. The critical
// constraint is that the waiter must outlive the `register_select()` call
// (persisting until the sender walks the stack), but the caller's stack frame
// is gone by then.
//
// Solution: heap-allocate each `SelectWaiter` via `Box`. The lock-free stack
// holds raw pointers to these. On `abort_select`, we mark the node `aborted`
// atomically (O(1)). On sender iteration, aborted nodes are skipped *and
// freed* (the sender assumes ownership and drops them). This cleanly reclaims
// memory without a separate GC pass.
//
// Safety invariant: a `SelectWaiter` pointer in the stack is valid as long as
// either (a) it has not yet been popped by a sender, or (b) `aborted` was set
// to `true` and the sender is responsible for dropping it. The channel field
// `select_waiters: Arc<AtomicPtr<SelectWaiter>>` acts as the shared stack head.

/// A heap-allocated waiter node for `select!` arm registration.
///
/// Unlike [`RecvWaiter`] (whose lifetime is bounded by `Arc` reference
/// counting), `SelectWaiter` is allocated with [`Box::into_raw`] and freed
/// by whichever party drains the stack — either the sender that calls
/// [`wake_select_one`] / [`wake_select_all`], or the channel cleanup path
/// that calls [`drain_select_waiters`].
pub(crate) struct SelectWaiter {
    /// Which select arm index this waiter belongs to.
    pub(crate) case_id: usize,
    /// Shared atomic marking which arm won the select call.
    pub(crate) selected: Arc<AtomicUsize>,
    /// Thread to unpark when this arm is chosen.
    pub(crate) thread: thread::Thread,
    /// Intrusive link — next node in the stack.
    pub(crate) next: AtomicPtr<SelectWaiter>,
    /// Mark-and-skip: `true` means this arm lost (abort was called).
    /// Sender skips aborted nodes and frees them.
    pub(crate) aborted: AtomicBool,
}

impl SelectWaiter {
    /// Allocate a new select waiter on the heap and return a raw pointer to it.
    ///
    /// Ownership is transferred to the lock-free stack. The pointer is freed
    /// either by the sender (after waking or skipping) or during stack cleanup.
    pub(crate) fn alloc(case_id: usize, selected: Arc<AtomicUsize>) -> *mut SelectWaiter {
        Box::into_raw(Box::new(SelectWaiter {
            case_id,
            selected,
            thread: thread::current(),
            next: AtomicPtr::new(std::ptr::null_mut()),
            aborted: AtomicBool::new(false),
        }))
    }
}

/// Push a `SelectWaiter` (already heap-allocated) onto a lock-free stack.
///
/// After this call, the stack owns the pointer. The caller must not free the
/// pointer — it will be freed by the sender or by [`drain_select_waiters`].
pub(crate) fn push_select_waiter(ptr: *mut SelectWaiter, stack: &Arc<AtomicPtr<SelectWaiter>>) {
    loop {
        let head = stack.load(Acquire);
        unsafe { (*ptr).next.store(head, Relaxed) };
        if stack.compare_exchange(head, ptr, AcqRel, Acquire).is_ok() {
            return;
        }
    }
}

/// Mark all `SelectWaiter` nodes that share the given `selected` Arc as aborted.
///
/// This is O(n) in stack length but involves only atomic stores — no lock.
/// Aborted nodes will be freed by the next sender that iterates the stack.
pub(crate) fn abort_select_waiters(
    stack: &Arc<AtomicPtr<SelectWaiter>>,
    selected: &Arc<AtomicUsize>,
) {
    let mut current = stack.load(Acquire);
    while !current.is_null() {
        let node = unsafe { &*current };
        if Arc::ptr_eq(&node.selected, selected) {
            node.aborted.store(true, Release);
        }
        current = node.next.load(Acquire);
    }
}

/// Drain the select-waiter stack and wake the first unaborted arm that wins
/// the `selected` CAS.
///
/// The stack is atomically swapped to null before any node is touched, so
/// concurrent callers cannot observe the same node.  Aborted nodes are freed
/// inline; the single winner is unparked.  All remaining nodes (their select
/// call already has a winner) are freed without unparking.
///
/// Returns `true` if a waiter was woken, `false` if the stack was empty or
/// every node was aborted.
pub(crate) fn wake_select_one(stack: &Arc<AtomicPtr<SelectWaiter>>) -> bool {
    // Atomically drain the entire stack into a local snapshot, then process.
    // This avoids walking a live stack that other threads may concurrently modify.
    let head = stack.swap(std::ptr::null_mut(), AcqRel);
    let mut current = head;
    let mut winner_found = false;

    while !current.is_null() {
        let node_box = unsafe { Box::from_raw(current) };
        current = node_box.next.load(Acquire);

        if node_box.aborted.load(Acquire) {
            // Aborted: free and skip.
            continue;
        }

        if !winner_found {
            // Try to win the CAS
            if node_box
                .selected
                .compare_exchange(UNSELECTED, node_box.case_id, SeqCst, SeqCst)
                .is_ok()
            {
                node_box.thread.unpark();
                winner_found = true;
                continue;
            }
        }

        // CAS failed: another sender already won this select call.
        // Free the node; the woken thread will handle everything else.
    }

    winner_found
}

/// Wake all unaborted select waiters (used for disconnect and broadcast sends).
///
/// All unaborted waiters attempt to win the CAS; conflicts are harmless.
/// Aborted nodes are freed inline.
pub(crate) fn wake_select_all(stack: &Arc<AtomicPtr<SelectWaiter>>) {
    // Drain the stack atomically.
    let head = stack.swap(std::ptr::null_mut(), AcqRel);
    let mut current = head;

    while !current.is_null() {
        let node_box = unsafe { Box::from_raw(current) };
        current = node_box.next.load(Acquire);

        if node_box.aborted.load(Acquire) {
            // Aborted — free inline.
            continue;
        }

        // Attempt to win the CAS; conflicts are harmless — the thread is
        // unparked regardless so it can re-check channel state.
        node_box
            .selected
            .compare_exchange(UNSELECTED, node_box.case_id, SeqCst, SeqCst)
            .ok();
        node_box.thread.unpark();
    }
}

/// Drain and free all select waiters without waking them.
///
/// Called during channel cleanup (e.g. final Drop) to avoid memory leaks.
/// Waiters that haven't been woken are simply freed.
pub(crate) fn drain_select_waiters(stack: &Arc<AtomicPtr<SelectWaiter>>) {
    let head = stack.swap(std::ptr::null_mut(), AcqRel);
    let mut current = head;
    while !current.is_null() {
        let node_box = unsafe { Box::from_raw(current) };
        current = node_box.next.load(Acquire);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const UNSELECTED: usize = usize::MAX;

    // ── SelectWaiter tests ───────────────────────────────────────────────

    #[test]
    fn select_waiter_push_and_wake_one() {
        let stack = Arc::new(AtomicPtr::new(std::ptr::null_mut()));
        let selected = Arc::new(AtomicUsize::new(UNSELECTED));

        let ptr = SelectWaiter::alloc(3, Arc::clone(&selected));
        push_select_waiter(ptr, &stack);

        assert!(wake_select_one(&stack));
        // Stack should now be empty (drained by wake_select_one)
        assert!(stack.load(Acquire).is_null());
        // CAS should have set selected to case_id=3
        assert_eq!(selected.load(Acquire), 3);
    }

    #[test]
    fn select_waiter_abort_skips() {
        let stack = Arc::new(AtomicPtr::new(std::ptr::null_mut()));
        let selected = Arc::new(AtomicUsize::new(UNSELECTED));

        let ptr = SelectWaiter::alloc(7, Arc::clone(&selected));
        push_select_waiter(ptr, &stack);

        // Abort before wake
        abort_select_waiters(&stack, &selected);

        // wake_select_one should skip the aborted node and return false
        assert!(!wake_select_one(&stack));
        assert_eq!(selected.load(Acquire), UNSELECTED);
    }

    #[test]
    fn select_wake_all_frees_nodes() {
        let stack = Arc::new(AtomicPtr::new(std::ptr::null_mut()));
        let sel1 = Arc::new(AtomicUsize::new(UNSELECTED));
        let sel2 = Arc::new(AtomicUsize::new(UNSELECTED));

        push_select_waiter(SelectWaiter::alloc(1, Arc::clone(&sel1)), &stack);
        push_select_waiter(SelectWaiter::alloc(2, Arc::clone(&sel2)), &stack);

        wake_select_all(&stack);

        // Stack drained
        assert!(stack.load(Acquire).is_null());
        // Each waiter has its own `selected` arc, so both CASes succeed independently.
    }

    #[test]
    fn drain_select_waiters_no_leak() {
        let stack = Arc::new(AtomicPtr::new(std::ptr::null_mut()));
        let selected = Arc::new(AtomicUsize::new(UNSELECTED));

        push_select_waiter(SelectWaiter::alloc(0, Arc::clone(&selected)), &stack);
        push_select_waiter(SelectWaiter::alloc(1, Arc::clone(&selected)), &stack);

        drain_select_waiters(&stack);
        assert!(stack.load(Acquire).is_null());
    }

    #[test]
    fn abort_only_matching_selected() {
        let stack = Arc::new(AtomicPtr::new(std::ptr::null_mut()));
        let sel_a = Arc::new(AtomicUsize::new(UNSELECTED));
        let sel_b = Arc::new(AtomicUsize::new(UNSELECTED));

        push_select_waiter(SelectWaiter::alloc(0, Arc::clone(&sel_a)), &stack);
        push_select_waiter(SelectWaiter::alloc(1, Arc::clone(&sel_b)), &stack);

        // Only abort sel_a's waiters
        abort_select_waiters(&stack, &sel_a);

        // Wake one: should wake the sel_b waiter (sel_a's is aborted)
        assert!(wake_select_one(&stack));
        assert_eq!(sel_b.load(Acquire), 1);
        assert_eq!(sel_a.load(Acquire), UNSELECTED); // untouched
    }
}
