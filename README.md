# selectables

Lock-free Rust channels with a unified recv-arm `select!` model.

`selectables` provides a consistent selection interface across channel flavours,
with Crossbeam-style ergonomics and a small, focused API.

## Highlights

- Lock-free fast paths for send and receive operations
- Unified `select!` macro for recv arms (`recv`, `default`, `default(duration)`)
- Bounded and unbounded MPMC/MPSC channels
- Rendezvous channel for synchronous handoff between sender and receiver
- Bounded broadcast with lag detection (`Lagged { skipped }`)
- Watch channel for latest-value subscriptions
- Oneshot channel for single-send/single-delivery
- Repeating timer receiver via `interval::interval()` and `interval::interval_at()`

## When to use

Use `selectables` when you need:

- one selection model across different channel types
- predictable lock-free send/receive with optional blocking recv
- bounded broadcast with explicit lag reporting and recovery
- synchronous rendezvous semantics (zero-buffer handoff)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
selectables = "0.1"
```

## Quick Start

```rust
use std::time::Duration;
use selectables::{bounded_mpmc, select};

let (tx, rx) = bounded_mpmc::channel::<i32>(1);
tx.send(7).unwrap();

// recv-arm selection with timeout fallback
select! {
    recv(rx) -> msg => assert_eq!(msg, Ok(7)),
    default(Duration::from_millis(10)) => panic!("unexpected timeout"),
}
```

## Channel types

| Module | Description |
|---|---|
| `unbounded_mpmc` | Lock-free unbounded multi-producer, multi-consumer queue |
| `bounded_mpmc` | Lock-free bounded multi-producer, multi-consumer ring buffer |
| `unbounded_mpsc` | Lock-free unbounded multi-producer, single-consumer queue |
| `bounded_mpsc` | Lock-free bounded multi-producer, single-consumer ring buffer |
| `rendezvous` | Zero-buffer synchronous handoff — sender blocks until a receiver is ready |
| `bounded_broadcast` | Bounded multi-producer, multi-receiver broadcast with per-receiver cursors |
| `watch` | Latest-value broadcast channel with versioned change notifications |
| `oneshot` | Single-send, single-delivery channel |
| `interval` | Repeating timer that yields `Instant` ticks on a fixed schedule |

## Selection model

`select!` supports recv arms, send arms, and fallback arms:

```rust
use std::time::Duration;
use selectables::{bounded_mpmc, unbounded_mpmc, select};

let (tx_bounded, rx_bounded) = bounded_mpmc::channel::<&str>(4);
let (tx_unbounded, rx_unbounded) = unbounded_mpmc::channel::<i32>();

tx_bounded.send("hello").unwrap();

select! {
    recv(rx_bounded) -> msg => println!("recv: {:?}", msg),
    send(tx_unbounded, 42) -> res => println!("sent: {:?}", res),
    default(Duration::from_millis(10)) => println!("timed out"),
}
```

Arm syntax at a glance:

| Arm | Fires when |
|-----|------------|
| `recv(rx) -> msg => { ... }` | a value is available on `rx` |
| `send(tx, val) -> res => { ... }` | `tx` has buffer space (or is disconnected) |
| `default => { ... }` | no arm is ready (non-blocking) |
| `default(duration) => { ... }` | no arm is ready within the timeout |

A low-level builder API is also available via `Select` and `SelectedOperation`.

## Lag handling (`bounded_broadcast`)

Broadcast receivers return `Lagged { skipped }` when they fall behind a
bounded ring buffer.  The cursor is automatically advanced to the oldest
surviving message, so the next call succeeds without any manual recovery.

```rust
use selectables::{bounded_broadcast, RecvError};

let (tx, rx) = bounded_broadcast::channel::<i32>(8);

tx.send(1).unwrap();

match rx.recv() {
    Ok(msg) => println!("got {}", msg),
    Err(RecvError::Lagged { skipped }) => {
        eprintln!("missed {} messages; resuming from oldest available", skipped);
    }
    Err(RecvError::Disconnected) => println!("sender gone"),
}
```

See the [`bounded_broadcast`](https://docs.rs/selectables/latest/selectables/bounded_broadcast/index.html) module for detailed lag recovery patterns.

## Examples

Run the bundled demo:

```bash
cargo run --example demo
```

The demo covers:

- Blocking and timed select usage
- `default` and `default(duration)` branches
- Bounded/unbounded MPMC and MPSC behaviour
- Watch updates and select integration
- Broadcast lag and recovery
- Oneshot usage and mixed-arm selection

For full API docs and module-level examples, see [docs.rs/selectables](https://docs.rs/selectables).

## Feature flags

- `debug-logs`: enables internal trace logging via the `log` crate

## License

Licensed under either of:

- MIT license
- Apache License, Version 2.0

at your option.

## Repository and docs

- Repository: <https://github.com/sria91/selectables>
- Documentation: <https://docs.rs/selectables>
