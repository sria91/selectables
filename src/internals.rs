use std::{
    cell::UnsafeCell,
    mem::{ManuallyDrop, MaybeUninit},
    ptr,
    sync::atomic::{AtomicUsize, Ordering::*},
};

// ════════════════════════════════════════════════════════════════════════════
// Channel internals
// ════════════════════════════════════════════════════════════════════════════
//
// Note on structural duplication: bounded_mpmc, bounded_mpsc, unbounded_mpmc,
// and unbounded_mpsc share ~80% identical `Chan` / `Sender` / `Receiver`
// scaffolding.  They differ in:
//   1. Backing queue: LockFreeBoundedRing (bounded) vs SegQueue (unbounded)
//   2. Wake-on-send: wake_one (MPMC) vs wake_all_unselected (MPSC)
//   3. Bounded channels carry send_select_waiters; unbounded do not
//   4. Bounded SelectableSender::is_ready checks !ring.is_full(); unbounded always true
//
// These differences are interspersed throughout method bodies, making a generic
// or macro-based consolidation complex with diminishing readability returns.
// The duplication is kept explicit so each channel flavour can be understood
// in isolation.  When modifying shared logic, grep across all four modules.

/// Shared lock-free bounded ring buffer used by bounded MPMC/MPSC channels.
///
/// Sequence protocol per slot:
/// - `sequence == pos`       => empty, producer for `tail == pos` may write
/// - `sequence == pos + 1`   => full, consumer for `head == pos` may read
/// - `sequence == pos + cap` => consumed, next producer cycle may reuse slot
struct RingSlot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: access is serialized by the sequence protocol/CAS ownership.
unsafe impl<T: Send> Send for RingSlot<T> {}
unsafe impl<T: Send> Sync for RingSlot<T> {}

pub(crate) struct LockFreeBoundedRing<T> {
    slots: Box<[RingSlot<T>]>,
    cap: usize,
    /// Monotonic write cursor, claimed by producers via CAS.
    tail: AtomicUsize,
    /// Monotonic read cursor, claimed by consumers via CAS.
    head: AtomicUsize,
}

impl<T> LockFreeBoundedRing<T> {
    pub(crate) fn new(cap: usize) -> Self {
        let slots: Box<[RingSlot<T>]> = (0..cap)
            .map(|i| RingSlot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        LockFreeBoundedRing {
            slots,
            cap,
            tail: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
        }
    }

    /// Returns `Err(value)` when the ring is full or capacity is 0.
    ///
    /// **Note**: the `Err` path conflates two distinct conditions — "buffer
    /// full" and "zero-capacity channel" — into the same return value.
    /// Callers that need to distinguish them must check `self.cap == 0` or
    /// `self.is_full()` before calling.
    pub(crate) fn try_push(&self, value: T) -> Result<(), T> {
        if self.cap == 0 {
            return Err(value);
        }
        let value = ManuallyDrop::new(value);
        loop {
            let pos = self.tail.load(Relaxed);
            let slot = &self.slots[pos % self.cap];
            let seq = slot.sequence.load(Acquire);
            let diff = seq as isize - pos as isize;

            if diff == 0 {
                if self
                    .tail
                    .compare_exchange_weak(pos, pos + 1, Relaxed, Relaxed)
                    .is_ok()
                {
                    // SAFETY: this producer owns the slot until sequence advance.
                    unsafe {
                        ptr::write(
                            (*slot.value.get()).as_mut_ptr(),
                            ptr::read(&*value),
                        );
                    }
                    slot.sequence.store(pos + 1, Release);
                    return Ok(());
                }
            } else if diff < 0 {
                return Err(ManuallyDrop::into_inner(value));
            }
            // diff > 0 means stale tail snapshot; retry.
        }
    }

    /// Lock-free MPMC pop.
    pub(crate) fn try_pop(&self) -> Option<T> {
        if self.cap == 0 {
            return None;
        }
        loop {
            let pos = self.head.load(Relaxed);
            let slot = &self.slots[pos % self.cap];
            let seq = slot.sequence.load(Acquire);
            let diff = seq as isize - (pos + 1) as isize;

            if diff == 0 {
                if self
                    .head
                    .compare_exchange_weak(pos, pos + 1, Relaxed, Relaxed)
                    .is_ok()
                {
                    // SAFETY: this consumer owns the claimed slot.
                    let value = unsafe { (*slot.value.get()).assume_init_read() };
                    slot.sequence.store(pos + self.cap, Release);
                    return Some(value);
                }
            } else if diff < 0 {
                return None;
            }
            // diff > 0 means stale head snapshot; retry.
        }
    }

    /// Snapshot check used during select try phase.
    pub(crate) fn is_empty(&self) -> bool {
        if self.cap == 0 {
            return true;
        }
        let pos = self.head.load(Acquire);
        self.slots[pos % self.cap].sequence.load(Acquire) != pos + 1
    }

    /// Snapshot check: returns `true` when no further push can succeed immediately.
    /// Capacity-0 rings are always full.
    pub(crate) fn is_full(&self) -> bool {
        if self.cap == 0 {
            return true;
        }
        // tail and head are monotonically increasing. The number of items
        // currently in the ring equals (tail - head). The ring is full when
        // that equals the capacity. Using wrapping arithmetic is safe because
        // overflow takes billions of operations.
        let tail = self.tail.load(Acquire);
        let head = self.head.load(Acquire);
        tail.wrapping_sub(head) >= self.cap
    }
}

impl<T> Drop for LockFreeBoundedRing<T> {
    fn drop(&mut self) {
        if self.cap == 0 {
            return;
        }
        let head = *self.head.get_mut();
        let tail = *self.tail.get_mut();
        for i in head..tail {
            let slot = &mut self.slots[i % self.cap];
            if *slot.sequence.get_mut() == i + 1 {
                // SAFETY: sequence indicates initialized but unconsumed value.
                unsafe { (*slot.value.get()).assume_init_drop() };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LockFreeBoundedRing;

    #[test]
    fn ring_basic_push_pop() {
        let ring = LockFreeBoundedRing::new(4);
        assert!(ring.is_empty());
        assert!(!ring.is_full());

        ring.try_push(1u32).unwrap();
        ring.try_push(2).unwrap();
        assert!(!ring.is_empty());
        assert!(!ring.is_full());

        assert_eq!(ring.try_pop(), Some(1));
        assert_eq!(ring.try_pop(), Some(2));
        assert_eq!(ring.try_pop(), None);
        assert!(ring.is_empty());
    }

    #[test]
    fn ring_zero_capacity() {
        let ring = LockFreeBoundedRing::<i32>::new(0);
        assert!(ring.is_empty());
        assert!(ring.is_full());
        assert!(ring.try_push(1).is_err());
        assert_eq!(ring.try_pop(), None);
    }

    #[test]
    fn ring_full_rejects_push() {
        let ring = LockFreeBoundedRing::new(2);
        ring.try_push(10u32).unwrap();
        ring.try_push(20).unwrap();
        assert!(ring.is_full());
        assert!(ring.try_push(30).is_err());
        // After pop there is space again.
        assert_eq!(ring.try_pop(), Some(10));
        assert!(!ring.is_full());
        ring.try_push(30).unwrap();
    }

    #[test]
    fn ring_fifo_ordering() {
        let ring = LockFreeBoundedRing::new(8);
        for i in 0u32..8 {
            ring.try_push(i).unwrap();
        }
        for i in 0u32..8 {
            assert_eq!(ring.try_pop(), Some(i));
        }
    }

    #[test]
    fn ring_wrap_around() {
        // Fill, drain, fill again to exercise index wrapping.
        let ring = LockFreeBoundedRing::new(4);
        for i in 0u32..4 {
            ring.try_push(i).unwrap();
        }
        for i in 0u32..4 {
            assert_eq!(ring.try_pop(), Some(i));
        }
        // Second fill/drain cycle.
        for i in 4u32..8 {
            ring.try_push(i).unwrap();
        }
        for i in 4u32..8 {
            assert_eq!(ring.try_pop(), Some(i));
        }
        assert!(ring.is_empty());
    }

    #[test]
    fn ring_is_full_capacity_one() {
        let ring = LockFreeBoundedRing::new(1);
        assert!(!ring.is_full());
        ring.try_push(42u32).unwrap();
        assert!(ring.is_full());
        assert_eq!(ring.try_pop(), Some(42));
        assert!(!ring.is_full());
        // Can push again after drain.
        ring.try_push(99).unwrap();
        assert!(ring.is_full());
    }

    #[test]
    fn ring_drop_runs_for_buffered_items() {
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

        {
            let ring = LockFreeBoundedRing::new(4);
            ring.try_push(Guard(Arc::clone(&counter))).unwrap();
            ring.try_push(Guard(Arc::clone(&counter))).unwrap();
            // Drop ring without consuming items.
        }
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }
}

#[cfg(feature = "debug-logs")]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        log::debug!($($arg)*)
    };
}

#[cfg(not(feature = "debug-logs"))]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        ()
    };
}

/// Generate a boilerplate [`crate::SelectableReceiver`] impl for a channel `Receiver`.
///
/// # Usage
///
/// ```rust,ignore
/// // Plain receiver with output type T:
/// impl_selectable_receiver!([T] MyReceiver<T>, T);
///
/// // Receiver requiring a bound, e.g. T: Clone:
/// impl_selectable_receiver!([T: Clone] MyReceiver<T>, T);
///
/// // Watch-style receiver whose output type is ():
/// impl_selectable_receiver!([T] WatchReceiver<T>, ());
/// ```
///
/// The generated impl delegates every method to the receiver's own inherent
/// methods (`is_ready`, `register_select`, `abort_select`, `complete_recv`),
/// which must already exist on the type.
macro_rules! impl_selectable_receiver {
    ([$($generics:tt)*] $Receiver:ty, $Output:ty) => {
        impl<$($generics)*> $crate::SelectableReceiver for $Receiver {
            type Output = $Output;

            fn is_ready(&self) -> bool {
                self.is_ready()
            }

            fn register_select(
                &self,
                case_id: usize,
                selected: ::std::sync::Arc<::std::sync::atomic::AtomicUsize>,
            ) {
                self.register_select(case_id, selected);
            }

            fn abort_select(&self, selected: &::std::sync::Arc<::std::sync::atomic::AtomicUsize>) {
                self.abort_select(selected);
            }

            fn complete(&self) -> ::std::result::Result<Self::Output, $crate::RecvError> {
                self.complete_recv()
            }
        }
    };
}
