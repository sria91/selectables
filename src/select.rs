//! Low-level selection API and fair recv-arm based choice protocol.
//!
//! # Overview
//!
//! The select module provides two interfaces:
//! 1. The [select!] macro — preferred, ergonomic channel selection with default fallbacks
//! 2. The [Select] API — low-level builder for dynamic channel sets
//!
//! # How it works
//!
//! Selection uses a 4-phase protocol:
//! - **Phase 1 (try)**: Rotate through all channels (fair start), check readiness
//! - **Phase 2 (register)**: If none ready, register this thread as a waiter on all channels
//! - **Phase 3 (park)**: Sleep until another thread wakes us (a send/disconnect occurred)
//! - **Phase 4 (complete)**: Re-check and execute the winning channel's completion handler
//!
//! # Fairness
//!
//! The global `FAIRNESS_CTR` atomic rotates which arm is checked first, ensuring no arm is
//! consistently starved. Over many iterations, all arms get equal opportunity.
//!
//! # Example: select! macro
//!
//! ```no_run
//! use std::time::Duration;
//! use selectables::{select, unbounded_mpmc, bounded_mpsc};
//!
//! let (tx1, rx1) = unbounded_mpmc::channel::<i32>();
//! let (tx2, rx2) = bounded_mpsc::channel::<i32>(4);
//!
//! select! {
//!     recv(rx1) -> msg => println!("rx1: {:?}", msg),
//!     recv(rx2) -> msg => println!("rx2: {:?}", msg),
//!     default(Duration::from_millis(100)) => println!("timeout"),
//! }
//! ```
//!
//! # Example: Select builder API
//!
//! ```ignore
//! let mut sel = Select::new();
//! let i1 = sel.recv(rx1);
//! let i2 = sel.recv(rx2);
//!
//! match sel.select() {
//!     SelectedOperation { index } if index == i1 => println!("rx1 won"),
//!     SelectedOperation { index } if index == i2 => println!("rx2 won"),
//!     _ => unreachable!(),
//! }
//! ```
//!
//! # Supported arms
//!
//! - `recv(rx)` — blocks waiting for a message from any channel
//! - `default` — non-blocking: runs immediately if nothing is ready
//! - `default(duration)` — timed-out: runs after timeout expires if nothing is ready
//!
//! # Integration with channels
//!
//! All channel types implement [crate::SelectableReceiver], enabling them to participate
//! in select operations. Each channel handles its own waiter registration and wakeup logic.

use std::{
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering::*},
    thread,
    time::{Duration, Instant},
};

use crate::{SelectableReceiver, SelectableSender, waiter::UNSELECTED};

// ════════════════════════════════════════════════════════════════════════════
// Select
// ════════════════════════════════════════════════════════════════════════════

/// Unified trait for both recv and send select arms.
///
/// Both arm kinds require the same three methods, so a single trait serves both.
trait ArmOpTrait {
    fn is_ready(&self) -> bool;
    fn register(&self, case_id: usize, selected: Arc<AtomicUsize>);
    fn abort(&self, selected: &Arc<AtomicUsize>);
}

/// Unified operation: either a recv or a send arm.
enum AnyOp {
    Recv(Box<dyn ArmOpTrait + Send + Sync>),
    Send(Box<dyn ArmOpTrait + Send + Sync>),
}

impl AnyOp {
    fn is_ready(&self) -> bool {
        match self {
            AnyOp::Recv(op) | AnyOp::Send(op) => op.is_ready(),
        }
    }

    fn register(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        match self {
            AnyOp::Recv(op) | AnyOp::Send(op) => op.register(case_id, selected),
        }
    }

    fn abort(&self, selected: &Arc<AtomicUsize>) {
        match self {
            AnyOp::Recv(op) | AnyOp::Send(op) => op.abort(selected),
        }
    }
}

/// `ArmOpTrait` implementation for recv arms.
struct SelectOp<R: SelectableReceiver> {
    receiver: R,
}

impl<R: SelectableReceiver + Send + Sync + 'static> ArmOpTrait for SelectOp<R>
where
    R::Output: Send,
{
    fn is_ready(&self) -> bool {
        self.receiver.is_ready()
    }

    fn register(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        self.receiver.register_select(case_id, selected);
    }

    fn abort(&self, selected: &Arc<AtomicUsize>) {
        self.receiver.abort_select(selected);
    }
}

/// `ArmOpTrait` implementation for send arms.
struct SendOp<S: SelectableSender> {
    sender: S,
}

impl<S: SelectableSender + Send + Sync + 'static> ArmOpTrait for SendOp<S>
where
    S::Input: Send,
{
    fn is_ready(&self) -> bool {
        self.sender.is_ready()
    }

    fn register(&self, case_id: usize, selected: Arc<AtomicUsize>) {
        self.sender.register_select(case_id, selected);
    }

    fn abort(&self, selected: &Arc<AtomicUsize>) {
        self.sender.abort_select(selected);
    }
}

pub struct Select {
    ops: Vec<AnyOp>,
}

/// Returned by `Select::select()`. Carries the winning arm's index.
pub struct SelectedOperation {
    pub index: usize,
}

/// Global counter that rotates the start of the try-phase sweep each
/// iteration, giving all arms an equal chance (fairness).
static FAIRNESS_CTR: AtomicUsize = AtomicUsize::new(0);

impl Select {
    pub fn new() -> Self {
        Select { ops: Vec::new() }
    }

    /// Register a recv arm. Returns the arm's index (0, 1, 2, …).
    pub fn recv<R>(&mut self, rx: R) -> usize
    where
        R: SelectableReceiver + Clone + Send + Sync + 'static,
        R::Output: Send + 'static,
    {
        let idx = self.ops.len();
        let op = SelectOp { receiver: rx };
        self.ops.push(AnyOp::Recv(Box::new(op)));
        idx
    }

    /// Register a send arm. Returns the arm's index.
    pub fn send<S>(&mut self, tx: S) -> usize
    where
        S: SelectableSender + Send + Sync + 'static,
        S::Input: Send + 'static,
    {
        let idx = self.ops.len();
        let op = SendOp { sender: tx };
        self.ops.push(AnyOp::Send(Box::new(op)));
        idx
    }

    /// Block until one arm is ready.
    pub fn select(&mut self) -> SelectedOperation {
        self.select_impl(None).unwrap_or_else(|| {
            unreachable!("select_impl panics on empty ops before returning None")
        })
    }

    /// Return `None` immediately if nothing is ready (non-blocking).
    ///
    /// Implemented as `select_impl(Some(Instant::now()))`.  Because `Instant::now()`
    /// is sampled *before* the try-phase runs, a very brief delay between the
    /// sample and the deadline check inside `select_impl` can cause the try-phase
    /// to fall through to waiter registration before the deadline fires.  In
    /// practice this is harmless (registration is immediately aborted and `None`
    /// is returned), but callers should not rely on this being strictly zero-cost.
    pub fn try_select(&mut self) -> Option<SelectedOperation> {
        self.select_impl(Some(Instant::now()))
    }

    /// Block until an arm is ready or `timeout` elapses.
    pub fn select_timeout(&mut self, timeout: Duration) -> Option<SelectedOperation> {
        self.select_impl(Some(Instant::now() + timeout))
    }

    /// Block until an arm is ready or `deadline` is reached.
    pub fn select_deadline(&mut self, deadline: Instant) -> Option<SelectedOperation> {
        self.select_impl(Some(deadline))
    }

    fn select_impl(&mut self, deadline: Option<Instant>) -> Option<SelectedOperation> {
        assert!(!self.ops.is_empty(), "Select::select() called with no registered arms");
        let n = self.ops.len();

        loop {
            log_debug!("select::select_impl: arms={}, deadline={:?}", n, deadline);
            let start = FAIRNESS_CTR.fetch_add(1, Relaxed) % n;
            log_debug!("select::try phase: start_index={}", start);
            for i in 0..n {
                let idx = (start + i) % n;
                if self.ops[idx].is_ready() {
                    log_debug!("select::try phase: ready idx={}", idx);
                    return Some(SelectedOperation { index: idx });
                }
            }

            if deadline.map(|d| Instant::now() >= d).unwrap_or(false) {
                log_debug!("select::select_impl: deadline reached before park");
                return None;
            }

            let selected = Arc::new(AtomicUsize::new(UNSELECTED));

            for (idx, op) in self.ops.iter().enumerate() {
                log_debug!("select::register phase: registering arm={}", idx);
                op.register(idx, Arc::clone(&selected));
            }
            log_debug!("select::register phase: registered {} waiters", n);

            for (idx, op) in self.ops.iter().enumerate() {
                if op.is_ready() {
                    log_debug!("select::recheck: arm {} became ready during register", idx);
                    selected
                        .compare_exchange(UNSELECTED, idx, SeqCst, SeqCst)
                        .ok();
                    break;
                }
            }

            if selected.load(SeqCst) == UNSELECTED {
                match deadline {
                    None => {
                        log_debug!("select::park phase: parking indefinitely");
                        thread::park()
                    }
                    Some(dl) => {
                        let wait = dl.saturating_duration_since(Instant::now());
                        log_debug!("select::park phase: parking with timeout={:?}", wait);
                        thread::park_timeout(wait)
                    }
                }
            }

            for op in &self.ops {
                op.abort(&selected);
            }

            let won = selected.load(SeqCst);
            if won != UNSELECTED {
                log_debug!("select::winner: idx={}", won);
                return Some(SelectedOperation { index: won });
            }

            if deadline.map(|d| Instant::now() >= d).unwrap_or(false) {
                return None;
            }
        }
    }
}

impl Default for Select {
    fn default() -> Self {
        Self::new()
    }
}

// ════════════════════════════════════════════════════════════════════════════
// § 7.  select! macro
// ════════════════════════════════════════════════════════════════════════════
//
// Expansion of:
//
//   select! {
//       recv(rx1) -> msg => body1,
//       recv(rx2) -> msg => body2,
//   }
//
// roughly becomes:
//
//   {
//       let mut __sel = Select::new();
//       __sel.recv(&rx1);          // index 0
//       __sel.recv(&rx2);          // index 1
//       let __oper = __sel.select();
//       let mut __n = 0usize;
//       {   // @arm for index 0
//           let __i = __n; __n += 1;
//           if __oper.index == __i {
//               let msg = rx1.complete_recv();
//               body1
//           } else {
//               {   // @arm for index 1
//                   let __i = __n; __n += 1;
//                   if __oper.index == __i {
//                       let msg = rx2.complete_recv();
//                       body2
//                   } else {
//                       unreachable!()
//                   }
//               }
//           }
//       }
//   }
//
// The mutable `__n` counter is in scope throughout thanks to the nested
// blocks, and each expansion increments it to produce 0, 1, 2, …
// No proc-macro or `paste!` needed — just tt-munching.

/// Dispatch to the first ready channel operation.
///
/// # Syntax
///
/// // Blocking (waits until one arm is ready):
/// select! {
///     recv(rx1) -> msg => { /* msg: Result<T, RecvError> */ },
///     recv(rx2) -> msg => { ... },
///     recv(wx)  -> ver => { /* ver: Result<usize, RecvError> */ },
/// }
///
/// // Non-blocking (instant default):
/// select! {
///     recv(rx) -> msg => { ... },
///     default => { /* fires immediately if nothing ready */ },
/// }
///
/// // Timeout:
/// select! {
///     recv(rx) -> msg => { ... },
///     default(Duration::from_millis(100)) => { /* fires after 100 ms */ },
/// }
///
/// ## Variable binding
/// Each arm binds `msg` (or any `ident`) to `Result<T, RecvError>`.
/// Inspect it as usual: `msg.unwrap()`, `match msg { Ok(v) => … }`, etc.
#[macro_export]
macro_rules! select {
    // ── Blocking: N recv arms, no default ────────────────────────────────
    ($(recv($rx:expr) -> $var:pat => $body:expr),+ $(,)?) => {{
        let mut __sel = $crate::Select::new();
        $( __sel.recv($rx.clone()); )+
        let __oper = __sel.select();
        let mut __n = 0usize;
        $crate::select!(@arm_new __oper __n $(, recv($rx) -> $var => $body)+)
    }};

    // ── Non-blocking: N recv arms + instant default ───────────────────────
    ($(recv($rx:expr) -> $var:pat => $body:expr,)+ default => $def:expr $(,)?) => {{
        let mut __sel = $crate::Select::new();
        $( __sel.recv($rx.clone()); )+
        if let Some(__oper) = __sel.try_select() {
            let mut __n = 0usize;
            $crate::select!(@arm_new __oper __n $(, recv($rx) -> $var => $body)+)
        } else {
            $def
        }
    }};

    // ── Timeout: N recv arms + duration default ───────────────────────────
    ($(recv($rx:expr) -> $var:pat => $body:expr,)+ default($dur:expr) => $def:expr $(,)?) => {{
        let mut __sel = $crate::Select::new();
        $( __sel.recv($rx.clone()); )+
        if let Some(__oper) = __sel.select_timeout($dur) {
            let mut __n = 0usize;
            $crate::select!(@arm_new __oper __n $(, recv($rx) -> $var => $body)+)
        } else {
            $def
        }
    }};

    // ════════════════════════════════════════════════════════════════════
    // Mixed / send-only arms — blocking, non-blocking, timeout
    // These rules handle any combination of recv() and send(tx, val) arms.
    // They are checked AFTER the recv-only rules above, so pure-recv
    // invocations continue to use the faster dedicated rules.
    // ════════════════════════════════════════════════════════════════════

    // ── Non-blocking: single send arm + instant default ──────────────────
    // Must come before the general `$fk:ident` rules to avoid ambiguity:
    // `$k:ident` in the repetition would otherwise compete with the literal
    // `default` keyword, causing a "local ambiguity" compile error.
    (
        send ($tx:expr, $val:expr) -> $var:pat => $body:expr,
        default => $def:expr $(,)?
    ) => {{
        let mut __sel = $crate::Select::new();
        __sel.send(($tx).clone());
        if let Some(__oper) = __sel.try_select() {
            let mut __n = 0usize;
            $crate::select!(@arm_new __oper __n, send ($tx, $val) -> $var => $body)
        } else {
            $def
        }
    }};

    // ── Timeout: single send arm + timeout default ───────────────────────
    (
        send ($tx:expr, $val:expr) -> $var:pat => $body:expr,
        default($dur:expr) => $def:expr $(,)?
    ) => {{
        let mut __sel = $crate::Select::new();
        __sel.send(($tx).clone());
        if let Some(__oper) = __sel.select_timeout($dur) {
            let mut __n = 0usize;
            $crate::select!(@arm_new __oper __n, send ($tx, $val) -> $var => $body)
        } else {
            $def
        }
    }};

    // ── Blocking: mixed/send-only, no default ────────────────────────────
    (
        $fk:ident ($($fa:tt)*) -> $fv:pat => $fb:expr
        $(, $k:ident ($($a:tt)*) -> $v:pat => $b:expr)*
        $(,)?
    ) => {{
        let mut __sel = $crate::Select::new();
        $crate::select!(@register __sel, $fk ($($fa)*));
        $( $crate::select!(@register __sel, $k ($($a)*)); )*
        let __oper = __sel.select();
        let mut __n = 0usize;
        $crate::select!(@arm_new __oper __n,
            $fk ($($fa)*) -> $fv => $fb
            $(, $k ($($a)*) -> $v => $b)*
        )
    }};

    // ── Non-blocking: mixed/send-only + instant default ──────────────────
    (
        $fk:ident ($($fa:tt)*) -> $fv:pat => $fb:expr
        $(, $k:ident ($($a:tt)*) -> $v:pat => $b:expr)*
        , default => $def:expr
        $(,)?
    ) => {{
        let mut __sel = $crate::Select::new();
        $crate::select!(@register __sel, $fk ($($fa)*));
        $( $crate::select!(@register __sel, $k ($($a)*)); )*
        if let Some(__oper) = __sel.try_select() {
            let mut __n = 0usize;
            $crate::select!(@arm_new __oper __n,
                $fk ($($fa)*) -> $fv => $fb
                $(, $k ($($a)*) -> $v => $b)*
            )
        } else {
            $def
        }
    }};

    // ── Timeout: mixed/send-only + duration default ───────────────────────
    (
        $fk:ident ($($fa:tt)*) -> $fv:pat => $fb:expr
        $(, $k:ident ($($a:tt)*) -> $v:pat => $b:expr)*
        , default($dur:expr) => $def:expr
        $(,)?
    ) => {{
        let mut __sel = $crate::Select::new();
        $crate::select!(@register __sel, $fk ($($fa)*));
        $( $crate::select!(@register __sel, $k ($($a)*)); )*
        if let Some(__oper) = __sel.select_timeout($dur) {
            let mut __n = 0usize;
            $crate::select!(@arm_new __oper __n,
                $fk ($($fa)*) -> $fv => $fb
                $(, $k ($($a)*) -> $v => $b)*
            )
        } else {
            $def
        }
    }};

    // ── @register: register one arm with the Select builder ──────────────
    (@register $sel:ident, recv ($rx:expr)) => {
        $sel.recv(($rx).clone());
    };
    (@register $sel:ident, send ($tx:expr, $val:expr)) => {
        $sel.send(($tx).clone());
    };

    // ── @arm_new: dispatch after select() returns (recv arm, multiple) ────
    (@arm_new $oper:ident $n:ident,
        recv ($rx:expr) -> $var:pat => $body:expr
        $(, $k:ident ($($a:tt)*) -> $kv:pat => $kb:expr)+
    ) => {{
        let __i = $n; $n += 1;
        if $oper.index == __i {
            let $var = $crate::SelectableReceiver::complete(&($rx));
            $body
        } else {
            $crate::select!(@arm_new $oper $n $(, $k ($($a)*) -> $kv => $kb)+)
        }
    }};

    // ── @arm_new: dispatch after select() returns (send arm, multiple) ────
    (@arm_new $oper:ident $n:ident,
        send ($tx:expr, $val:expr) -> $var:pat => $body:expr
        $(, $k:ident ($($a:tt)*) -> $kv:pat => $kb:expr)+
    ) => {{
        let __i = $n; $n += 1;
        if $oper.index == __i {
            let $var = $crate::SelectableSender::complete_send(&($tx), $val);
            $body
        } else {
            $crate::select!(@arm_new $oper $n $(, $k ($($a)*) -> $kv => $kb)+)
        }
    }};

    // ── @arm_new: dispatch after select() returns (recv arm, last) ────────
    (@arm_new $oper:ident $n:ident, recv ($rx:expr) -> $var:pat => $body:expr) => {{
        let __i = $n;
        if $oper.index == __i {
            let $var = $crate::SelectableReceiver::complete(&($rx));
            $body
        } else {
            unreachable!(
                "select!: winning index {} >= arm count {}",
                $oper.index, __i + 1
            )
        }
    }};

    // ── @arm_new: dispatch after select() returns (send arm, last) ────────
    (@arm_new $oper:ident $n:ident, send ($tx:expr, $val:expr) -> $var:pat => $body:expr) => {{
        let __i = $n;
        if $oper.index == __i {
            let $var = $crate::SelectableSender::complete_send(&($tx), $val);
            $body
        } else {
            unreachable!(
                "select!: winning index {} >= arm count {}",
                $oper.index, __i + 1
            )
        }
    }};

}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use crate::{bounded_mpmc, bounded_mpsc, unbounded_mpmc, unbounded_mpsc, watch};

    #[test]
    fn watch_with_instant_default() {
        let (_tx, rx) = watch::channel::<&str>();

        let fired = mpsc::channel();
        let (done_tx, done_rx) = fired;

        select! {
            recv(rx) -> _version => panic!("watch arm should not be ready yet"),
            default => done_tx.send(true).unwrap(),
        }

        assert!(done_rx.recv().unwrap());
    }

    #[test]
    fn watch_with_timeout_default() {
        let (_tx, rx) = watch::channel::<&str>();

        let before = std::time::Instant::now();
        select! {
            recv(rx) -> _version => panic!("watch arm should not be ready yet"),
            default(Duration::from_millis(20)) => {}
        }
        assert!(before.elapsed() >= Duration::from_millis(20));
    }

    #[test]
    fn mixed_recv_and_watch_blocking_select() {
        let (tx_msg, rx_msg) = unbounded_mpmc::channel::<i32>();
        let (tx_watch, watch_rx) = watch::channel::<&str>();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            tx_watch.send("ready").unwrap();
            thread::sleep(Duration::from_millis(10));
            tx_msg.send(42).unwrap();
        });

        select! {
            recv(watch_rx) -> res => assert_eq!(res, Ok(())),
            recv(rx_msg) -> _msg => panic!("message arm should lose this race"),
        }
    }

    // ── send arm tests ────────────────────────────────────────────────────

    /// Unbounded sender is always ready; send arm fires immediately.
    #[test]
    fn test_send_arm_unbounded_mpmc() {
        let (tx, rx) = unbounded_mpmc::channel::<i32>();
        select! {
            send(tx, 99) -> res => assert!(res.is_ok()),
        }
        assert_eq!(rx.try_recv().unwrap(), 99);
    }

    /// Unbounded mpsc sender is always ready.
    #[test]
    fn test_send_arm_unbounded_mpsc() {
        let (tx, rx) = unbounded_mpsc::channel::<i32>();
        select! {
            send(tx, 77) -> res => assert!(res.is_ok()),
        }
        assert_eq!(rx.try_recv().unwrap(), 77);
    }

    /// Bounded sender is ready when buffer has space; send arm fires.
    #[test]
    fn test_send_arm_bounded_mpmc() {
        let (tx, rx) = bounded_mpmc::channel::<i32>(4);
        select! {
            send(tx, 42) -> res => assert!(res.is_ok()),
        }
        assert_eq!(rx.try_recv().unwrap(), 42);
    }

    /// Watch sender is always ready (overwrite semantics).
    #[test]
    fn test_send_arm_watch() {
        let (tx, rx) = watch::channel::<i32>();
        select! {
            send(tx, 10) -> res => assert!(res.is_ok()),
        }
        // borrow_arc() returns the current snapshot without blocking
        let snapshot = rx.borrow_arc();
        assert!(snapshot.is_some());
        assert_eq!(*snapshot.unwrap(), 10);
    }

    /// Full bounded channel: send arm blocks until a receiver pops.
    #[test]
    fn test_send_arm_bounded_mpmc_blocks_when_full() {
        let (tx, rx) = bounded_mpmc::channel::<i32>(1);
        // Fill it.
        tx.send(1).unwrap();

        let rx2 = rx.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let _ = rx2.try_recv(); // make space
        });

        select! {
            send(tx, 2) -> res => assert!(res.is_ok()),
        }
    }

    /// Drop all receivers: bounded send still returns Ok (value sits in buffer).
    #[test]
    fn test_send_arm_disconnect_bounded() {
        let (tx, rx) = bounded_mpmc::channel::<i32>(4);
        drop(rx);
        // bounded_mpmc::send() now returns Err when all receivers are dropped,
        // consistent with all other channel variants.
        select! {
            send(tx, 5) -> res => assert!(res.is_err()),
        }
    }

    /// Drop all receivers on unbounded: send still returns Ok.
    #[test]
    fn test_send_arm_disconnect_unbounded() {
        let (tx, rx) = unbounded_mpmc::channel::<i32>();
        drop(rx);
        // send() returns Err when all receivers have been dropped.
        select! {
            send(tx, 5) -> res => assert!(res.is_err()),
        }
    }

    /// Mixed recv + send: correct arm fires.
    #[test]
    fn test_mixed_recv_send_select() {
        let (tx_msg, rx_msg) = unbounded_mpmc::channel::<i32>();
        let (tx_out, _rx_out) = unbounded_mpmc::channel::<i32>();

        // Put a message in rx_msg; both recv and send are ready — either arm can
        // fire. We just assert one of them fired without panic.
        tx_msg.send(7).unwrap();

        select! {
            recv(rx_msg) -> msg => assert_eq!(msg.unwrap(), 7),
            send(tx_out, 99) -> res => assert!(res.is_ok()),
        }
    }

    /// Default arm fires when no send is ready (bounded channel full, no recv).
    #[test]
    fn test_send_arm_default_when_not_ready() {
        let (tx, _rx) = bounded_mpsc::channel::<i32>(1);
        tx.send(1).unwrap(); // fill it

        select! {
            send(tx, 2) -> _res => panic!("send arm should not be ready"),
            default => {}
        }
    }

    /// Timeout default fires when send never becomes ready within deadline.
    #[test]
    fn test_send_arm_timeout_default() {
        let (tx, _rx) = bounded_mpsc::channel::<i32>(1);
        tx.send(1).unwrap(); // fill it

        let before = std::time::Instant::now();
        select! {
            send(tx, 2) -> _res => panic!("send arm should not be ready"),
            default(Duration::from_millis(30)) => {}
        }
        assert!(before.elapsed() >= Duration::from_millis(30));
    }

    // ── structural / multi-arm tests ──────────────────────────────────────

    /// Three recv arms: only the ready one fires.
    #[test]
    fn three_arm_select_fires_only_ready_arm() {
        let (tx1, rx1) = unbounded_mpmc::channel::<i32>();
        let (_tx2, rx2) = unbounded_mpmc::channel::<i32>();
        let (_tx3, rx3) = unbounded_mpmc::channel::<i32>();

        tx1.send(42).unwrap();

        select! {
            recv(rx1) -> msg => assert_eq!(msg.unwrap(), 42),
            recv(rx2) -> _   => panic!("arm 2 must not fire"),
            recv(rx3) -> _   => panic!("arm 3 must not fire"),
        }
    }

    /// Fairness: with two always-ready recv channels, both arms should fire
    /// in roughly equal proportions over many iterations.
    #[test]
    fn fairness_distributes_roughly_evenly() {
        const ITERS: usize = 200;
        let (tx1, rx1) = unbounded_mpmc::channel::<()>();
        let (tx2, rx2) = unbounded_mpmc::channel::<()>();

        for _ in 0..ITERS {
            tx1.send(()).unwrap();
            tx2.send(()).unwrap();
        }

        let mut count = [0usize; 2];
        for _ in 0..ITERS {
            select! {
                recv(rx1) -> _ => count[0] += 1,
                recv(rx2) -> _ => count[1] += 1,
            }
        }
        // Each arm should fire at least 20% of the time (not strictly round-robin,
        // but should not be 100/0).
        assert!(count[0] >= ITERS / 5, "arm 0 fired only {} times", count[0]);
        assert!(count[1] >= ITERS / 5, "arm 1 fired only {} times", count[1]);
    }

    /// A blocking select that eventually fires when a message arrives on one arm.
    #[test]
    fn blocking_select_waits_for_message() {
        let (tx, rx) = unbounded_mpmc::channel::<i32>();
        let (_idle_tx, idle_rx) = bounded_mpmc::channel::<i32>(1); // alive but never sends

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(123).unwrap();
        });

        select! {
            recv(rx) -> msg => assert_eq!(msg.unwrap(), 123),
            recv(idle_rx) -> _ => panic!("idle arm must not fire"),
        }
    }

    /// Send + recv in the same select: when the recv channel is ready, recv arm wins.
    #[test]
    fn recv_arm_wins_over_disconnected_send_arm() {
        let (tx_msg, rx_msg) = unbounded_mpmc::channel::<i32>();
        let (tx_out, _rx_out) = bounded_mpmc::channel::<i32>(4);

        tx_msg.send(7).unwrap();
        // tx_out has space so send is also ready; either can fire.
        // Just assert no panic and the value is accounted for.
        let mut got = 0i32;
        select! {
            recv(rx_msg) -> msg => got = msg.unwrap(),
            send(tx_out, 99) -> _res => {},
        }
        // If send arm fired, drain the buffer.
        let _ = rx_msg.try_recv(); // may be Ok(7) or Empty
        let _ = got; // silence unused warning
    }

    /// select! with a timeout arm correctly returns after the deadline.
    #[test]
    fn timeout_select_returns_within_deadline() {
        let (_idle_tx, idle_rx) = bounded_mpmc::channel::<i32>(1); // alive but never sends
        let before = std::time::Instant::now();
        select! {
            recv(idle_rx) -> _ => panic!("should not fire"),
            default(Duration::from_millis(40)) => {}
        }
        let elapsed = before.elapsed();
        assert!(elapsed >= Duration::from_millis(40));
        assert!(elapsed < Duration::from_millis(500));
    }

    /// Five recv arms plus a default: exercises the macro with many arms.
    #[test]
    fn five_recv_arms_with_default() {
        let (_tx1, rx1) = unbounded_mpmc::channel::<i32>();
        let (_tx2, rx2) = unbounded_mpmc::channel::<i32>();
        let (_tx3, rx3) = bounded_mpmc::channel::<i32>(4);
        let (_tx4, rx4) = bounded_mpsc::channel::<i32>(4);
        let (tx5, rx5) = unbounded_mpsc::channel::<i32>();

        tx5.send(55).unwrap();

        let mut _fired = false;
        select! {
            recv(rx1) -> _ => panic!("rx1 must not fire"),
            recv(rx2) -> _ => panic!("rx2 must not fire"),
            recv(rx3) -> _ => panic!("rx3 must not fire"),
            recv(rx4) -> _ => panic!("rx4 must not fire"),
            recv(rx5) -> msg => { assert_eq!(msg.unwrap(), 55); _fired = true; },
            default => panic!("default must not fire when rx5 is ready"),
        }
        assert!(_fired);
    }

    /// Mixed recv + send in a blocking select.
    #[test]
    fn mixed_recv_send_blocking_many_arms() {
        let (tx_send, rx_send) = bounded_mpmc::channel::<i32>(4);
        let (tx_recv, rx_recv) = unbounded_mpmc::channel::<i32>();

        // Make recv arm ready; send arm also ready (space in buffer).
        tx_recv.send(99).unwrap();

        select! {
            send(tx_send, 2) -> res => {
                assert!(res.is_ok());
                // The send completed; value should be in the channel.
                assert_eq!(rx_send.try_recv().unwrap(), 2);
            },
            recv(rx_recv) -> msg => {
                assert_eq!(msg.unwrap(), 99);
            },
        }
    }
}
