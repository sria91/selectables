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
- Timer and disabled-arm helpers via `after()` and `never()`

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

## Selection model

`select!` supports both recv and send arms:

- `recv(rx) -> msg => { ... }`
- `send(tx, value) -> res => { ... }`
- `default => { ... }` for a non-blocking fallback
- `default(duration) => { ... }` for a timed fallback

A low-level builder API is also available via `Select` and `SelectedOperation`.

## Lag handling (`bounded_broadcast`)

Broadcast receivers may return `Lagged { skipped }` when they fall behind a
bounded ring buffer.

Recovery is straightforward:

```rust
use selectables::RecvError;

fn handle_recv<T>(result: Result<T, RecvError>) -> bool {
    match result {
        Ok(_msg) => true,
        Err(RecvError::Lagged { skipped }) => {
            eprintln!("receiver lagged by {} messages; recovered", skipped);
            true
        }
        Err(RecvError::Disconnected) => false,
    }
}
```

After `Lagged`, the receiver cursor is automatically advanced to the oldest
available message and subsequent recvs continue from there.

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
