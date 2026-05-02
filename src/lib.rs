//! # selectables
//!
//! Lock-free channels with a unified recv-arm selection model.
//!
//! The crate provides multiple channel flavors that can be selected with the same
//! `recv`-based protocol, including non-blocking and timed fallback behavior.
//!
//! ## Channel modules
//! - [unbounded_mpmc] lock-free unbounded multi-producer, multi-consumer channel.
//! - [bounded_mpmc] lock-free bounded multi-producer, multi-consumer channel.
//! - [bounded_mpsc] lock-free bounded multi-producer, single-consumer channel.
//! - [unbounded_mpsc] lock-free unbounded multi-producer, single-consumer channel.
//! - [bounded_broadcast] bounded multi-producer, multi-receiver broadcast channel with per-receiver
//!   lag detection and independent cursors.
//! - [oneshot] single-send/single-delivery channel compatible with recv select arms.
//! - [watch] latest-value broadcast channel with versioned change notifications.
//! - [rendezvous] zero-buffer synchronous handoff channel.
//!
//! ## Selection model
//! - [select!] supports `recv(rx) -> msg => { ... }` and `send(tx, val) -> res => { ... }` arms.
//! - Non-blocking fallback: `default => { ... }`.
//! - Timed fallback: `default(duration) => { ... }`.
//! - Low-level builder API is provided by [Select] and [SelectedOperation].
//!
//! ## Timers
//! - [interval::interval] and [interval::interval_at] create repeating timer receivers.
//!
//! ## Error model
//! - [RecvError] for blocking receive/completion failures. Includes `Lagged { skipped }` for
//!   bounded broadcast channels when a receiver falls behind senders.
//! - [error::TryRecvError] for non-blocking receive attempts.
//! - [SendError] for failed sends that return ownership of the message.
//!
//! ## Lag handling (broadcast channels only)
//! When using [bounded_broadcast], receivers may encounter `Lagged { skipped }` if they fall
//! behind the senders. This is normal and indicates the ring buffer wrapped around before the
//! receiver caught up. Recommended handling:
//!
//! ```rust,ignore
//! match rx.recv() {
//!     Ok(msg) => process(msg),
//!     Err(RecvError::Lagged { skipped }) => {
//!         log::warn!("Receiver lagged by {} messages; recovered", skipped);
//!         // Receiver cursor was automatically advanced; next recv will get oldest available
//!     }
//!     Err(RecvError::Disconnected) => {
//!         log::info!("All senders disconnected");
//!         break;
//!     }
//! }
//! ```
//!
//! See [bounded_broadcast] module docs for detailed lag recovery patterns.
//!
//! ## Quick example
//! ```
//! use std::time::Duration;
//! use selectables::{bounded_mpmc, select};
//!
//! let (tx, rx) = bounded_mpmc::channel::<i32>(1);
//! tx.send(7).unwrap();
//!
//! select! {
//!     recv(rx) -> msg => assert_eq!(msg, Ok(7)),
//!     default(Duration::from_millis(10)) => panic!("unexpected timeout"),
//! }
//! ```

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[macro_use]
mod internals;
mod error;
mod select;
mod waiter;

pub mod bounded_broadcast;
pub mod bounded_mpmc;
pub mod bounded_mpsc;
pub mod interval;
pub mod oneshot;
pub mod rendezvous;
pub mod unbounded_mpmc;
pub mod unbounded_mpsc;
pub mod watch;

pub use error::{RecvError, SendError};
pub use select::{Select, SelectedOperation};

/// Trait for receivers that can participate in `recv` arms of [`select!`].
///
/// The select protocol is a four-phase algorithm: **try → register → park → complete**.
/// Each phase is represented by one or more methods on this trait.
pub trait SelectableReceiver {
    /// The value type produced by a successful receive.
    type Output;
    /// `true` when a value is immediately available (try phase).
    ///
    /// Returns `true` if the channel is disconnected as well, so that
    /// [`complete`](Self::complete) can return the appropriate error right away.
    fn is_ready(&self) -> bool;
    /// Register a waiter for the select park phase.
    ///
    /// `case_id` identifies this arm among all arms in the `select!` call.
    /// `selected` is the shared atomic that the first arm to fire sets to its `case_id`.
    fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>);
    /// Remove the previously registered waiter (losing-arm cleanup).
    ///
    /// Called on every arm that did *not* win the selection so that no dangling
    /// waker references remain in the channel's waiter list.
    fn abort_select(&self, selected: &Arc<AtomicUsize>);
    /// Consume one value after winning the selection (complete phase).
    ///
    /// Returns `Err(RecvError::Disconnected)` if the channel is empty and all
    /// senders have been dropped.
    fn complete(&self) -> Result<Self::Output, RecvError>;
}

/// Trait for senders that can participate in `send` arms of `select!`.
///
/// `is_ready` returns `true` when a send can complete without blocking (buffer
/// has space or the channel is disconnected). The select protocol uses the same
/// four-phase algorithm as for receivers: try → register → park → complete.
///
/// Implementors must also implement [`Clone`] because the `select!` macro clones
/// the sender handle before handing it to the low-level [`Select`] builder.
pub trait SelectableSender: Clone {
    /// The value type this sender accepts.
    type Input: Send;
    /// `true` when a send would complete immediately (buffer not full, or
    /// all receivers dropped — in which case `complete_send` returns `Err`).
    fn is_ready(&self) -> bool;
    /// Register a send-side waiter for the select park phase.
    fn register_select(&self, case_id: usize, selected: Arc<AtomicUsize>);
    /// Remove the previously registered waiter (losing arm cleanup).
    fn abort_select(&self, selected: &Arc<AtomicUsize>);
    /// Execute the send after winning the selection.
    /// Returns `Err(SendError(val))` if all receivers have been dropped.
    fn complete_send(&self, value: Self::Input) -> Result<(), SendError<Self::Input>>;
}
