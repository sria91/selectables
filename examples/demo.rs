use selectables::*;
use std::{
    env, thread,
    time::{Duration, Instant},
};

// ════════════════════════════════════════════════════════════════════════════
// § 8.  Demonstrations
// ════════════════════════════════════════════════════════════════════════════

fn main() {
    unsafe { env::set_var("RUST_LOG", "trace") };
    minimal_logger::init(minimal_logger::config_from_env()).unwrap();

    demo_basic_select();
    demo_instant_default();
    demo_timeout_default();
    demo_disconnection();
    demo_bounded_channel();
    demo_mpsc_bounded();
    demo_watch_channel();
    demo_bounded_broadcast();
    demo_oneshot_basic();
    demo_oneshot_select();
    demo_priority_without_macro();
    demo_interval();

    minimal_logger::shutdown();
}

// ── Demo 1: Blocking select across two channels ───────────────────────────
fn demo_basic_select() {
    println!("\n╔══ Demo 1: blocking select across two channels ══╗");

    let (tx1, rx1) = unbounded_mpmc::channel::<&str>();
    let (tx2, rx2) = unbounded_mpmc::channel::<&str>();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        let _ = tx1.send("hello from ch1");
    });
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        tx2.send("hello from ch2").unwrap();
    });

    // Select twice — should receive from rx2 first (shorter delay), then rx1.
    for _ in 0..2 {
        select! {
            recv(rx1) -> msg => println!("  rx1 → {:?}", msg),
            recv(rx2) -> msg => println!("  rx2 → {:?}", msg),
        }
    }
}

// ── Demo 2: Non-blocking select with `default` ────────────────────────────
fn demo_instant_default() {
    println!("\n╔══ Demo 2: non-blocking select (instant default) ══╗");

    let (_tx, rx) = unbounded_mpmc::channel::<i32>(); // nothing will ever be sent

    select! {
        recv(rx) -> msg => println!("  got: {:?}", msg),
        default => println!("  nothing ready → took default branch"),
    }
}

// ── Demo 3: Timeout via `default(duration)` ───────────────────────────────
fn demo_timeout_default() {
    println!("\n╔══ Demo 3: select with timeout ══╗");

    let (_tx, rx) = unbounded_mpmc::channel::<i32>();

    let t0 = Instant::now();
    select! {
        recv(rx) -> msg => println!("  got: {:?}", msg),
        default(Duration::from_millis(50)) => {
            println!("  timed out after {:?}", t0.elapsed());
        },
    }
}

// ── Demo 4: Disconnection arrives through select ──────────────────
fn demo_disconnection() {
    println!("\n╔══ Demo 4: detecting sender disconnect ══╗");

    let (tx, rx) = unbounded_mpmc::channel::<i32>();
    drop(tx); // drop the only sender immediately

    select! {
        recv(rx) -> msg => println!("  result: {:?}", msg),  // Err(Disconnected)
        default(Duration::from_millis(100)) => println!("  timed out (unexpected)"),
    }
}

// ── Demo 5: Bounded channel with capacity limit ──────────────────
fn demo_bounded_channel() {
    println!("\n╔══ Demo 5: bounded channel capacity ══╗");

    let (tx, rx) = bounded_mpmc::channel::<i32>(2);

    tx.send(10).unwrap();
    tx.send(20).unwrap();
    let full = tx.send(30).err();

    println!("  sent 10, 20, send 30 full: {:?}", full.is_some());
    println!("  recv -> {:?}", rx.recv().unwrap());
    println!("  recv -> {:?}", rx.recv().unwrap());
}

// ── Demo 6: bounded MPSC channel with select! ══╗

fn demo_mpsc_bounded() {
    println!("\n╔══ Demo 6: bounded MPSC channel with select! ══╗");

    let (tx1, rx1) = bounded_mpsc::channel::<&str>(2);
    let (tx2, rx2) = bounded_mpsc::channel::<&str>(2);
    let (tx3, rx3) = bounded_mpsc::channel::<&usize>(2);

    let tx1_clone = tx1.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        let _ = tx1_clone.send("from mpsc1"); // receiver may have dropped; that's ok
    });

    let tx2_clone = tx2.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let _ = tx2_clone.send("from mpsc2");
    });

    let tx3_clone = tx3.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(1));
        let _ = tx3_clone.send(&0);
    });

    loop {
        select! {
            recv(rx1) -> msg => println!("  rx1 → {:?}", msg),
            recv(rx2) -> msg => println!("  rx2 → {:?}", msg),
            recv(rx3) -> msg => println!("  rx3 → {:?}", msg),
            default(Duration::from_millis(50)) => {println!("  timed out"); break;},
        }
    }

    // Fill rx1 to test bounded behavior
    tx1.send("immediate").unwrap();
    tx1.send("second").unwrap();
    tx3.send(&1).unwrap();
    // This should fail since capacity is 2
    let full = tx1.send("blocked").err();
    println!("  mpsc1 full after send: {:?}", full.is_some());

    select! {
        recv(rx1) -> msg => println!("  rx1 → {:?}", msg),
        recv(rx2) -> msg => println!("  rx2 → {:?}", msg),
        recv(rx3) -> msg => println!("  rx3 → {:?}", msg),
    }

    // Now rx1 should be empty, try again
    select! {
        recv(rx1) -> msg => println!("  rx1 → {:?}", msg),
        recv(rx2) -> msg => println!("  rx2 → {:?}", msg),
        recv(rx3) -> msg => println!("  rx3 → {:?}", msg),
    }

    loop {
        select! {
            recv(rx1) -> msg => println!("  rx1 → {:?}", msg),
            recv(rx2) -> msg => println!("  rx2 → {:?}", msg),
            recv(rx3) -> msg => println!("  rx3 → {:?}", msg),
            default(Duration::from_millis(50)) => {println!("  timed out"); break;},
        }
    }

    let (tx1, rx1) = bounded_mpsc::channel::<i32>(1);
    let (tx2, rx2) = bounded_mpsc::channel::<String>(1);
    let (tx3, rx3) = bounded_mpsc::channel::<&i32>(1);

    // Demonstrate mixed-type recv arms: send to one, select picks it up.
    tx3.send(&42).unwrap();
    tx2.send("Hello".to_owned()).unwrap();
    tx1.send(42).unwrap();
    println!("\n  [mixed types]");
    loop {
        select! {
            recv(rx1) -> msg => println!("  i32  arm: {:?}", msg),
            recv(rx2) -> msg => {println!("  String arm: {:?}", msg);},
            recv(rx3) -> msg => {println!("  &i32 arm: {:?}", msg); break;},
        }
    }
}

// ── Demo 7: watch channel updates through recv arms ───────────────────────
fn demo_watch_channel() {
    println!("\n╔══ Demo 7: watch channel updates ══╗");

    let (tx, rx) = watch::channel::<&str>();
    let tx_boot = tx.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        tx_boot.send("booting").unwrap();
    });

    println!("  changed() -> {:?}, value {:?}", rx.changed(), rx.borrow());

    let rx_for_select = rx.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send("ready").unwrap();
    });

    select! {
        recv(rx_for_select) -> res => println!("  select recv -> {:?}", res),
        default(Duration::from_millis(100)) => println!("  watch timed out"),
    }

    println!("  final value -> {:?}", rx.borrow());
}

// ── Demo 8: bounded broadcast with lag detection ─────────────────────────
fn demo_bounded_broadcast() {
    println!("\n╔══ Demo 8: bounded broadcast lag behavior + recovery ══╗");

    let (tx, rx1) = bounded_broadcast::channel::<i32>(2);
    let rx2 = rx1.clone();

    // Send 3 messages into a capacity-2 buffer: rx1 will lag
    tx.send(10).unwrap();
    tx.send(20).unwrap();
    tx.send(30).unwrap();

    // ─ Demonstrate lag detection
    println!("  rx1 first try_recv -> {:?}", rx1.try_recv());
    println!("    ^ Shows Lagged {{ skipped: 1 }} because buffer wrapped");

    // ─ Demonstrate automatic cursor recovery
    println!("  rx1 next recv -> {:?}", rx1.recv());
    println!("    ^ Cursor was auto-advanced; now receives oldest available (20)");

    // ─ New subscriber (rx2) starts at write position, sees future messages only
    println!("  rx2 first recv -> {:?}", rx2.recv());
    println!("    ^ rx2 subscribed after 30 was sent, so it won't see old messages");

    // ─ Demonstrate select! integration with lag handling
    let rx_sel = rx1.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let _ = tx.send(40);
    });

    select! {
        recv(rx_sel) -> msg => match msg {
            Ok(v) => println!("  select broadcast -> Ok({})", v),
            Err(e) => println!("  select broadcast -> Err({:?})", e),
        },
        default(Duration::from_millis(100)) => println!("  broadcast timed out"),
    }
}

// ── Demo 9: oneshot basics ─────────────────────────────────────────────────
fn demo_oneshot_basic() {
    println!("\n╔══ Demo 9: oneshot basic send/recv ══╗");

    let (tx, rx) = oneshot::channel::<&str>();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        tx.send("delivered once").unwrap();
    });

    println!("  recv -> {:?}", rx.recv());
}

// ── Demo 10: oneshot in select! recv arms ───────────────────────────────────────
fn demo_oneshot_select() {
    println!("\n╔══ Demo 10: oneshot in select! recv arms ══╗");

    let (tx_data, rx_data) = unbounded_mpmc::channel::<i32>();
    let (tx_once, rx_once) = oneshot::channel::<&str>();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(15));
        tx_once.send("oneshot won").unwrap();
    });

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        let _ = tx_data.send(99);
    });

    select! {
        recv(rx_once) -> msg => println!("  oneshot -> {:?}", msg),
        recv(rx_data) -> msg => println!("  mpmc -> {:?}", msg),
        default(Duration::from_millis(100)) => println!("  timed out"),
    }
}

// ── Demo 11: Priority select without the macro ──────────────────────────────
//
// `select!` rotates arms for fairness.  When you *always* want arm 0 to
// take precedence, call try_recv directly first, then fall back to select.
fn demo_priority_without_macro() {
    println!("\n╔══ Demo 11: manual priority select (no macro) ══╗");

    let (tx_hi, rx_hi) = unbounded_mpmc::channel::<&str>();
    let (tx_lo, rx_lo) = unbounded_mpmc::channel::<&str>();

    tx_hi.send("HIGH priority").unwrap();
    tx_lo.send("low priority").unwrap();

    // Drain hi-priority channel first.
    if let Ok(msg) = rx_hi.try_recv() {
        println!("  [hi] {}", msg);
    }

    // Fall back to the Select API (no macro) for the rest.
    let mut sel = Select::new();
    let i_hi = sel.recv(rx_hi.clone());
    let i_lo = sel.recv(rx_lo.clone());

    if let Some(oper) = sel.try_select() {
        match oper.index {
            i if i == i_hi => println!("  [hi] {:?}", SelectableReceiver::complete(&rx_hi)),
            i if i == i_lo => println!("  [lo] {:?}", SelectableReceiver::complete(&rx_lo)),
            _ => unreachable!(),
        }
    } else {
        println!("  nothing ready");
    }
}

// ── Demo 12: selectable interval ───────────────────────────────────────────────────────
fn demo_interval() {
    println!("\n╔══ Demo 12: selectable interval ══╗");

    let iv = interval::interval(Duration::from_millis(50));
    let (tx, rx) = unbounded_mpmc::channel::<&str>();

    // Send a message after 125 ms — it should arrive between tick 2 and tick 4.
    thread::spawn({
        let tx = tx.clone();
        move || {
            thread::sleep(Duration::from_millis(125));
            tx.send("message between ticks").unwrap();
        }
    });

    for i in 0..10 {
        select! {
            recv(iv) -> tick => println!("  tick {} at {:?}", i, tick.unwrap()),
            recv(rx) -> msg  => println!("  msg: {:?}", msg.unwrap()),
        }
    }
}
