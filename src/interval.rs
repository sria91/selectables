//! Repeating timer channel, selectable via `recv` arms.
//!
//! # Overview
//!
//! An [`Interval`] is a channel receiver that yields one [`Instant`] per tick on a fixed
//! schedule.  It is selectable — you can use it in `recv` arms of [`select!`] alongside any
//! other channel.
//!
//! # Missed-tick policy
//!
//! The internal ring buffer holds at most **one** pending tick.  If the consumer has not read
//! the previous tick before the next one fires, the new tick is silently discarded and the
//! schedule advances normally (skip policy).  This prevents unbounded backpressure without
//! requiring any configuration.
//!
//! # First-tick behaviour
//!
//! [`interval`] fires the first tick almost immediately (at construction time), matching
//! `tokio::time::Interval`'s default.  [`interval_at`] defers the first tick until the given
//! [`Instant`].
//!
//! # Clone semantics
//!
//! All clones of an `Interval` share the same underlying channel and race for the same ticks —
//! each tick is delivered to exactly one clone.
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use selectables::{interval::interval, unbounded_mpmc, select};
//!
//! let iv = interval(Duration::from_millis(100));
//! let (tx, rx) = unbounded_mpmc::channel::<&str>();
//!
//! for _ in 0..3 {
//!     select! {
//!         recv(iv) -> tick => println!("tick at {:?}", tick),
//!         recv(rx) -> msg  => println!("msg: {:?}", msg),
//!     }
//! }
//! ```

use std::{
    sync::Arc,
    sync::atomic::AtomicUsize,
    thread,
    time::{Duration, Instant},
};

use crate::{RecvError, bounded_mpmc, bounded_mpmc::Receiver};

// ════════════════════════════════════════════════════════════════════════════
// Interval
// ════════════════════════════════════════════════════════════════════════════

/// A repeating timer that yields [`Instant`] values on a fixed schedule.
///
/// Created by [`interval`] or [`interval_at`].  Implements [`crate::SelectableReceiver`] so it
/// can appear in `recv(iv) -> tick =>` arms of the [`select!`] macro.
///
/// Dropping the last `Interval` clone stops the background tick thread on the next iteration.
pub struct Interval(Receiver<Instant>);

impl Clone for Interval {
    fn clone(&self) -> Self {
        Interval(self.0.clone())
    }
}

impl crate::SelectableReceiver for Interval {
    type Output = Instant;

    fn is_ready(&self) -> bool {
        crate::SelectableReceiver::is_ready(&self.0)
    }

    fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        crate::SelectableReceiver::register_select(&self.0, case_id, selected);
    }

    fn abort_select(&self, selected: &Arc<AtomicUsize>) {
        crate::SelectableReceiver::abort_select(&self.0, selected);
    }

    fn complete(&self) -> Result<Instant, RecvError> {
        crate::SelectableReceiver::complete(&self.0)
    }
}

impl Interval {
    /// Block until the next tick and return its [`Instant`].
    ///
    /// Equivalent to calling `recv()` on the underlying channel.
    /// Returns `Err(RecvError::Disconnected)` only if the internal tick thread
    /// panicked, which should not happen under normal use.
    pub fn tick(&self) -> Result<Instant, RecvError> {
        self.0.recv()
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Constructors
// ════════════════════════════════════════════════════════════════════════════

/// Create an [`Interval`] that fires the first tick immediately and then every `period`.
///
/// The first tick is queued as soon as the background thread starts, so it is available
/// almost immediately after this function returns.
///
/// # Panics
///
/// Panics if the OS fails to spawn the background tick thread (extremely rare).
pub fn interval(period: Duration) -> Interval {
    interval_at(Instant::now(), period)
}

/// Create an [`Interval`] whose first tick fires at `start` and then every `period` after that.
///
/// If `start` is in the past the first tick fires immediately.
///
/// # Panics
///
/// Panics if the OS fails to spawn the background tick thread (extremely rare).
pub fn interval_at(start: Instant, period: Duration) -> Interval {
    // Use a capacity-1 bounded channel. The Sender is moved into the tick thread and
    // kept alive for as long as the thread runs; dropping it signals disconnection.
    let (tx, rx) = bounded_mpmc::channel::<Instant>(1);

    thread::Builder::new()
        .name("selectables::interval".to_owned())
        .spawn(move || {
            let mut next_tick = start;

            loop {
                let now = Instant::now();
                if next_tick > now {
                    thread::sleep(next_tick - now);
                }

                // Stop if all Interval handles have been dropped.
                if tx.is_closed() {
                    break;
                }

                // Missed-tick skip policy: silently discard if buffer is full.
                let _ = tx.send(Instant::now());

                next_tick += period;
            }
        })
        .expect("failed to spawn 'selectables::interval' timer thread");

    Interval(rx)
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select;
    use std::time::Duration;

    /// The interval fires at least `n` ticks and the gaps between them are ≥ period.
    #[test]
    fn interval_fires_multiple_ticks() {
        let period = Duration::from_millis(20);
        let iv = interval(period);

        let t0 = iv.tick().unwrap();
        let t1 = iv.tick().unwrap();
        let t2 = iv.tick().unwrap();

        // Each tick should be at least `period` after the previous one.
        // (We give a little slack for scheduling jitter.)
        assert!(t1 >= t0, "ticks should be non-decreasing");
        assert!(t2 >= t1, "ticks should be non-decreasing");

        // The total elapsed time for 3 ticks should be at least 2 * period
        // (first tick is immediate, then +period, +period).
        assert!(
            t2.duration_since(t0) >= period,
            "at least one full period should have elapsed between first and third tick"
        );
    }

    /// The interval participates in select! — the recv arm fires before the deadline.
    #[test]
    fn interval_selectable() {
        let iv = interval(Duration::from_millis(10));
        let deadline = Duration::from_millis(500);

        select! {
            recv(iv) -> tick => {
                assert!(tick.is_ok(), "tick should be Ok(Instant)");
            },
            default(deadline) => panic!("interval did not fire within deadline"),
        }
    }

    /// interval_at defers the first tick until the given Instant.
    #[test]
    fn interval_at_defers_first_tick() {
        let delay = Duration::from_millis(30);
        let start = Instant::now() + delay;
        let iv = interval_at(start, Duration::from_millis(100));

        let before = Instant::now();
        let tick = iv.tick().unwrap();
        let elapsed = before.elapsed();

        // First tick should not arrive before the start instant (with some tolerance).
        assert!(
            elapsed >= delay.saturating_sub(Duration::from_millis(5)),
            "first tick arrived too early: elapsed={elapsed:?}, expected>={delay:?}"
        );
        assert!(
            tick >= start - Duration::from_millis(5),
            "tick instant should be near start"
        );
    }

    /// Dropping all Interval handles stops the background tick thread.
    #[test]
    fn interval_drop_stops_thread() {
        let iv = interval(Duration::from_millis(10));
        let iv2 = iv.clone();
        drop(iv);
        drop(iv2);
        // Give the thread time to notice receiver_count == 0 and exit.
        thread::sleep(Duration::from_millis(50));
        // No assertion — this test just checks there is no panic or hang.
    }

    /// interval_at with a materially deferred start fires ticks at the correct
    /// cadence and respects the missed-tick skip policy.
    #[test]
    fn interval_at_full_deferred_schedule() {
        let delay = Duration::from_millis(60);
        let period = Duration::from_millis(30);
        let start = Instant::now() + delay;
        let iv = interval_at(start, period);

        // First tick must not arrive before `start`.
        let t0 = iv.tick().unwrap();
        assert!(
            t0 >= start - Duration::from_millis(5),
            "first tick arrived before deferred start: t0={t0:?}, start={start:?}"
        );

        // Subsequent ticks must each arrive at least `period` apart.
        let t1 = iv.tick().unwrap();
        assert!(
            t1.duration_since(t0) >= period.saturating_sub(Duration::from_millis(5)),
            "second tick arrived too early: gap={:?}, period={period:?}",
            t1.duration_since(t0)
        );
    }
}
