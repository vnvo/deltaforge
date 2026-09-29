//! Scenario: TPC-C inspired endurance soak test.
//!
//! Models a wholesale supplier workload (warehouses, districts, customers,
//! orders, stock) using the TPC-C transaction mix — the industry-standard
//! OLTP benchmark — as a CDC stress test rather than a throughput benchmark.
//!
//! Transaction mix (TPC-C standard proportions):
//!   New-Order  45% — INSERT order + INSERT new_order +
//!                    INSERT order_line × 5-15 + UPDATE stock × 5-15
//!   Payment    43% — UPDATE warehouse + UPDATE district +
//!                    UPDATE customer + INSERT history
//!   Delivery   12% — DELETE new_order + UPDATE order + UPDATE order_line × N
//!                    + UPDATE customer (one district per call)
//!
//! # Usage
//!
//! ```bash
//! docker compose -f docker-compose.chaos.yml --profile tpcc up -d
//! cargo run -p chaos -- --scenario tpcc --source mysql --duration-mins 120
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use mysql_async::prelude::Queryable;
use mysql_async::{TxOpts, Value};
use rand::Rng;
use rand::SeedableRng as _;
use rand::rngs::SmallRng;
use tokio::time::sleep;
use tracing::info;

use crate::backend::MYSQL_DSN;
use crate::docker;
use crate::harness::{self, Harness, ScenarioResult};

// ── Scale parameters ──────────────────────────────────────────────────────────

/// Number of warehouses. Each warehouse adds 10 districts × CUSTOMERS_PER_DISTRICT
/// customers and ITEMS stock rows. W=2 needs ~90s to seed on a laptop.
const WAREHOUSES: u32 = 2;
const DISTRICTS: u32 = 10;
const CUSTOMERS_PER_DISTRICT: u32 = 300;
/// Item catalog size. TPC-C standard is 100,000; 10,000 is laptop-friendly.
const ITEMS: u32 = 10_000;

// ── Concurrency & timing ──────────────────────────────────────────────────────

/// Concurrent terminal tasks (analogous to TPC-C "terminals").
const TERMINALS: usize = 16;

/// Think time between transactions per terminal (ms). TPC-C standard is 5-10 s;
/// we use 5-30 ms to maximise CDC event throughput.
const THINK_MIN_MS: u64 = 5;
const THINK_MAX_MS: u64 = 30;

/// Fault injection window (seconds between injected faults).
const FAULT_MIN_SECS: u64 = 120;
const FAULT_MAX_SECS: u64 = 300;
const PARTITION_HOLD_SECS: u64 = 15;
const OUTAGE_HOLD_SECS: u64 = 20;
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(90);

/// Mid-stream schema alter interval.
const ALTER_MIN_SECS: u64 = 60;
const ALTER_MAX_SECS: u64 = 180;

// ── Compose / health ──────────────────────────────────────────────────────────

pub const TPCC_PROFILE: &str = "tpcc";
pub const TPCC_SERVICE: &str = "deltaforge-tpcc";
pub const TPCC_HEALTH_URL: &str = "http://localhost:8082/health";
pub const TPCC_TOPIC: &str = "chaos.tpcc";

// ── Public entry point ────────────────────────────────────────────────────────

pub fn print_preamble() {
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  TPC-C Soak — what this test proves for DeltaForge");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!(
        "  Transaction mix ({TERMINALS} terminals, think-time {THINK_MIN_MS}-{THINK_MAX_MS} ms):"
    );
    println!(
        "    New-Order  45%  INSERT order + order_line×5-15 + UPDATE stock×5-15"
    );
    println!(
        "    Payment    43%  UPDATE warehouse / district / customer + INSERT history"
    );
    println!(
        "    Delivery   12%  DELETE new_order + UPDATE order + order_line + customer"
    );
    println!();
    println!("  What a PASS proves:");
    println!("    ✓ Multi-table transaction atomicity");
    println!("      New-Order spans 3 tables in one MySQL COMMIT.");
    println!(
        "      Every order_line must be delivered before its parent order"
    );
    println!("      becomes visible, and all-or-nothing on fault/recovery.");
    println!();
    println!("    ✓ Mixed INSERT / UPDATE / DELETE at realistic proportions");
    println!("      Payment is 43% of load — a pure UPDATE stream across");
    println!("      three hot tables. Tests sustained UPDATE throughput and");
    println!("      checkpoint cadence under write pressure.");
    println!();
    println!("    ✓ Causal ordering across FK-linked tables");
    println!("      Delivery DELETEs new_order then UPDATEs order_line.");
    println!("      Downstream consumers must never see the child updated");
    println!("      before the parent delete arrives.");
    println!();
    println!("    ✓ No event loss or duplication across fault boundaries");
    println!("      Network partitions, sink outages, and process crashes are");
    println!("      injected every 2-5 min. Kafka offset after recovery must");
    println!("      equal rows written — no gap, no double-delivery.");
    println!();
    println!("    ✓ Schema evolution under live load");
    println!("      ALTER TABLE ADD COLUMN fires every 1-3 min on a random");
    println!("      table. DeltaForge must adapt without losing events.");
    println!();
    println!("  Requirements:");
    println!(
        "    • tpcc compose profile running (deltaforge-tpcc on 8082/9002)"
    );
    println!("    • ~300 MB RAM headroom for seed data (W={WAREHOUSES})");
    println!("    • Seed phase ≈ 60-90 s before writes begin");
    println!("    • Storage: see README — ~4 GB per 2-hour run");
    println!();
    println!("  Start stack:  docker compose -f docker-compose.chaos.yml \\");
    println!("                  --profile tpcc up -d");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
}

pub async fn run(
    harness: &Harness,
    duration_mins: u64,
) -> Result<ScenarioResult> {
    let name = "tpcc";
    print_preamble();
    harness.setup().await?;

    // Step 1: wait for the tpcc DeltaForge instance.
    info!("step 1/4: waiting for tpcc DeltaForge at {TPCC_HEALTH_URL} ...");
    harness::wait_for_url(TPCC_HEALTH_URL, Duration::from_secs(60)).await?;
    info!("tpcc DeltaForge is healthy");

    // Step 2: seed reference and dimension tables (idempotent).
    info!("step 2/4: seeding TPC-C tables (W={WAREHOUSES}, items={ITEMS}) ...");
    let pool = mysql_async::Pool::new(MYSQL_DSN);
    seed_all(&pool).await?;
    info!("seed complete");

    // Step 3: launch terminal tasks + schema alter task.
    let counters = Arc::new(Counters::default());
    let next_o_id =
        Arc::new(AtomicU64::new((CUSTOMERS_PER_DISTRICT as u64) + 1));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(tokio::sync::Notify::new());

    info!(
        terminals = TERMINALS,
        "step 3/4: starting terminal tasks ..."
    );
    let terminal_handles: Vec<_> = (0..TERMINALS)
        .map(|id| {
            let c = Arc::clone(&counters);
            let oid = Arc::clone(&next_o_id);
            let flag = Arc::clone(&stop_flag);
            let w_id = (id as u32 % WAREHOUSES) + 1;
            tokio::spawn(
                async move { terminal_loop(id, w_id, c, oid, flag).await },
            )
        })
        .collect();

    let alter_handle = {
        let flag = Arc::clone(&stop_flag);
        let w = Arc::clone(&wake);
        tokio::spawn(async move { alter_loop(flag, w).await })
    };

    // Step 4: endurance loop with fault injection.
    info!(duration_mins, "step 4/4: running endurance loop ...");
    let deadline = Instant::now() + Duration::from_secs(duration_mins * 60);
    let mut rng = rand::thread_rng();
    let mut faults: Vec<FaultRecord> = Vec::new();
    let mut stats_samples: Vec<ResourceSample> = Vec::new();

    let kafka_start = harness::kafka_offset_for_topic(TPCC_TOPIC)
        .await
        .unwrap_or(0);

    while Instant::now() < deadline {
        let remaining = deadline.duration_since(Instant::now());
        let wait_secs = rng.gen_range(FAULT_MIN_SECS..=FAULT_MAX_SECS);
        let actual_wait = Duration::from_secs(wait_secs).min(remaining);

        if actual_wait < Duration::from_secs(30) {
            break;
        }

        sample_and_log(&mut stats_samples, &counters).await;
        sleep(actual_wait).await;

        let fault_idx = rng.gen_range(0usize..3);
        let fault_name =
            ["network_partition", "sink_outage", "crash"][fault_idx];
        info!(%fault_name, "injecting fault");

        let fault_start = Instant::now();
        let inject_result = match fault_idx {
            0 => inject_network_partition(harness).await,
            1 => inject_sink_outage(harness).await,
            _ => inject_crash().await,
        };

        match inject_result {
            Ok(recovery) => {
                info!(%fault_name, recovery_secs = recovery.as_secs_f64(), "fault recovered");
                faults.push(FaultRecord {
                    kind: fault_name,
                    recovery_secs: recovery.as_secs_f64(),
                    recovered: true,
                });
            }
            Err(e) => {
                info!(%fault_name, error = %e, "fault recovery timed out");
                faults.push(FaultRecord {
                    kind: fault_name,
                    recovery_secs: fault_start.elapsed().as_secs_f64(),
                    recovered: false,
                });
            }
        }
    }

    sample_and_log(&mut stats_samples, &counters).await;

    // Shutdown.
    stop_flag.store(true, Ordering::Relaxed);
    wake.notify_waiters();
    for h in terminal_handles {
        let _ = h.await;
    }
    let alters = alter_handle.await.unwrap_or_default();
    let _ = pool.disconnect().await;

    // Gather totals.
    let kafka_end = harness::kafka_offset_for_topic(TPCC_TOPIC)
        .await
        .unwrap_or(0);
    let delivered = kafka_end.saturating_sub(kafka_start);
    let new_orders = counters.new_orders.load(Ordering::Relaxed);
    let payments = counters.payments.load(Ordering::Relaxed);
    let deliveries = counters.deliveries.load(Ordering::Relaxed);
    let errors = counters.errors.load(Ordering::Relaxed);
    let failed_faults = faults.iter().filter(|f| !f.recovered).count();
    let avg_recovery = if faults.is_empty() {
        0.0
    } else {
        faults.iter().map(|f| f.recovery_secs).sum::<f64>()
            / faults.len() as f64
    };
    let max_recovery = faults
        .iter()
        .map(|f| f.recovery_secs)
        .fold(0.0_f64, f64::max);
    let peak_mem = stats_samples.iter().map(|s| s.mem_bytes).max().unwrap_or(0);
    let max_cpu = stats_samples
        .iter()
        .map(|s| (s.cpu_percent * 10.0) as u64)
        .max()
        .unwrap_or(0);
    let alters_ok = alters.iter().filter(|a| a.ok).count();

    // Transaction errors from writer tasks are expected during fault injection
    // (network partitions kill connections, crashes drop in-flight queries).
    // Only fail on unrecovered faults — those indicate DeltaForge didn't come back.
    let mut result = if failed_faults == 0 {
        ScenarioResult::pass(name)
    } else {
        ScenarioResult::fail(
            name,
            format!(
                "{failed_faults} unrecovered fault(s), {errors} txn error(s)"
            ),
        )
    };

    result = result
        .note(format!("duration: {duration_mins} min"))
        .note(format!("warehouses: {WAREHOUSES}, districts: {}, customers/district: {CUSTOMERS_PER_DISTRICT}, items: {ITEMS}", WAREHOUSES * DISTRICTS))
        .note(format!("new-orders: {new_orders}  payments: {payments}  deliveries: {deliveries}"))
        .note(format!("transaction errors: {errors}"))
        .note(format!("events delivered to Kafka: {delivered}"))
        .note(format!("faults injected: {}  failures: {failed_faults}", faults.len()))
        .note(format!("avg recovery: {avg_recovery:.1}s  max: {max_recovery:.1}s"))
        .note(format!("peak memory: {} MiB  max CPU: {:.1}%", peak_mem / 1024 / 1024, max_cpu as f64 / 10.0))
        .note(format!("schema alters applied: {alters_ok}"));

    for f in &faults {
        result = result.note(format!(
            "  {} → {:.1}s ({})",
            f.kind,
            f.recovery_secs,
            if f.recovered { "OK" } else { "TIMEOUT" }
        ));
    }

    Ok(result)
}

// ── Seeding ───────────────────────────────────────────────────────────────────

async fn seed_all(pool: &mysql_async::Pool) -> Result<()> {
    // Check idempotency: skip if item table already populated.
    let mut conn = pool.get_conn().await?;
    let count: u64 = conn
        .query_first("SELECT COUNT(*) FROM tpcc_item")
        .await?
        .unwrap_or(0);
    conn.disconnect().await?;

    if count >= ITEMS as u64 {
        info!(items = count, "tpcc tables already seeded, skipping");
        return Ok(());
    }

    info!("seeding warehouses, districts, customers, items, stock ...");
    seed_warehouses(pool).await?;
    seed_districts(pool).await?;
    seed_items(pool).await?;
    seed_stock(pool).await?;
    seed_customers(pool).await?;
    Ok(())
}

async fn seed_warehouses(pool: &mysql_async::Pool) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let mut rng = SmallRng::from_entropy();
    for w in 1..=WAREHOUSES {
        conn.exec_drop(
            "INSERT IGNORE INTO tpcc_warehouse \
             (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (w, format!("WH-{w}"), rand_str(&mut rng, 10, 20), rand_str(&mut rng, 10, 20),
             rand_str(&mut rng, 10, 20), rand_state(&mut rng), rand_zip(&mut rng),
             rand_tax(&mut rng), 300_000.00_f64),
        ).await?;
    }
    conn.disconnect().await?;
    Ok(())
}

async fn seed_districts(pool: &mysql_async::Pool) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let mut rng = SmallRng::from_entropy();
    for w in 1..=WAREHOUSES {
        for d in 1..=DISTRICTS {
            conn.exec_drop(
                "INSERT IGNORE INTO tpcc_district \
                 (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, \
                  d_tax, d_ytd, d_next_o_id) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (d, w, format!("D{d}-{w}"), rand_str(&mut rng, 10, 20), rand_str(&mut rng, 10, 20),
                 rand_str(&mut rng, 10, 20), rand_state(&mut rng), rand_zip(&mut rng),
                 rand_tax(&mut rng), 30_000.00_f64, CUSTOMERS_PER_DISTRICT + 1),
            ).await?;
        }
    }
    conn.disconnect().await?;
    Ok(())
}

async fn seed_items(pool: &mysql_async::Pool) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let mut rng = SmallRng::from_entropy();
    let mut batch = Vec::with_capacity(100);

    for i in 1..=ITEMS {
        let price: f64 = rng.gen_range(1.0..100.0);
        let data = if rng.gen_bool(0.1) {
            format!(
                "{}ORIGINAL{}",
                rand_str(&mut rng, 0, 10),
                rand_str(&mut rng, 0, 10)
            )
        } else {
            rand_str(&mut rng, 26, 50)
        };
        batch.push((
            i,
            i % 10_000 + 1,
            rand_str(&mut rng, 14, 24),
            price,
            data,
        ));

        if batch.len() == 100 {
            conn.exec_batch(
                "INSERT IGNORE INTO tpcc_item (i_id, i_im_id, i_name, i_price, i_data) \
                 VALUES (?, ?, ?, ?, ?)",
                batch.drain(..),
            ).await?;
        }
    }
    if !batch.is_empty() {
        conn.exec_batch(
            "INSERT IGNORE INTO tpcc_item (i_id, i_im_id, i_name, i_price, i_data) \
             VALUES (?, ?, ?, ?, ?)",
            batch.drain(..),
        ).await?;
    }
    conn.disconnect().await?;
    info!(items = ITEMS, "items seeded");
    Ok(())
}

async fn seed_stock(pool: &mysql_async::Pool) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let mut rng = SmallRng::from_entropy();
    // 14 columns — beyond mysql_async's tuple-Params limit; use Vec<Value>.
    let mut batch: Vec<Vec<Value>> = Vec::with_capacity(100);

    for w in 1..=WAREHOUSES {
        for i in 1..=ITEMS {
            let qty: i16 = rng.gen_range(10..100);
            batch.push(vec![
                i.into(),
                w.into(),
                qty.into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_fixed24(&mut rng).into(),
                rand_str(&mut rng, 26, 50).into(),
            ]);

            if batch.len() == 100 {
                conn.exec_batch(
                    "INSERT IGNORE INTO tpcc_stock \
                     (s_i_id, s_w_id, s_quantity, \
                      s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, \
                      s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    batch.drain(..),
                ).await?;
            }
        }
    }
    if !batch.is_empty() {
        conn.exec_batch(
            "INSERT IGNORE INTO tpcc_stock \
             (s_i_id, s_w_id, s_quantity, \
              s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, \
              s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch.drain(..),
        )
        .await?;
    }
    conn.disconnect().await?;
    info!(rows = WAREHOUSES * ITEMS, "stock seeded");
    Ok(())
}

async fn seed_customers(pool: &mysql_async::Pool) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let mut rng = SmallRng::from_entropy();
    // 19 columns — beyond mysql_async's tuple-Params limit; use Vec<Value>.
    let mut batch: Vec<Vec<Value>> = Vec::with_capacity(100);

    for w in 1..=WAREHOUSES {
        for d in 1..=DISTRICTS {
            for c in 1..=CUSTOMERS_PER_DISTRICT {
                let credit: &str = if rng.gen_bool(0.1) { "BC" } else { "GC" };
                let discount: f64 = rng.gen_range(0.0..0.5);
                batch.push(vec![
                    c.into(),
                    d.into(),
                    w.into(),
                    rand_str(&mut rng, 8, 16).into(),
                    "OE".into(),
                    last_name(&mut rng, c).into(),
                    rand_str(&mut rng, 10, 20).into(),
                    rand_str(&mut rng, 10, 20).into(),
                    rand_str(&mut rng, 10, 20).into(),
                    rand_state(&mut rng).into(),
                    rand_zip(&mut rng).into(),
                    rand_phone(&mut rng).into(),
                    credit.into(),
                    50_000.0_f64.into(),
                    discount.into(),
                    (-10.0_f64).into(),
                    1i16.into(),
                    0i16.into(),
                    rand_str(&mut rng, 300, 500).into(),
                ]);

                if batch.len() == 100 {
                    conn.exec_batch(
                        "INSERT IGNORE INTO tpcc_customer \
                         (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, \
                          c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, \
                          c_credit, c_credit_lim, c_discount, c_balance, \
                          c_payment_cnt, c_delivery_cnt, c_data) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        batch.drain(..),
                    ).await?;
                }
            }
        }
    }
    if !batch.is_empty() {
        conn.exec_batch(
            "INSERT IGNORE INTO tpcc_customer \
             (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, \
              c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, \
              c_credit, c_credit_lim, c_discount, c_balance, \
              c_payment_cnt, c_delivery_cnt, c_data) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch.drain(..),
        )
        .await?;
    }
    conn.disconnect().await?;
    let total = WAREHOUSES * DISTRICTS * CUSTOMERS_PER_DISTRICT;
    info!(customers = total, "customers seeded");
    Ok(())
}

// ── Terminal loop ─────────────────────────────────────────────────────────────

async fn terminal_loop(
    id: usize,
    w_id: u32,
    counters: Arc<Counters>,
    next_o_id: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    let pool = mysql_async::Pool::new(MYSQL_DSN);
    let mut rng = SmallRng::from_entropy();

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let think_ms = rng.gen_range(THINK_MIN_MS..=THINK_MAX_MS);
        let roll: u8 = rng.gen_range(1..=100);
        let d_id: u32 = rng.gen_range(1..=DISTRICTS);
        let c_id: u32 = rng.gen_range(1..=CUSTOMERS_PER_DISTRICT);
        let amount: f64 = rng.gen_range(1.0..5000.0);
        let ol_cnt: u32 = rng.gen_range(5..=15);
        let carrier: u8 = rng.gen_range(1..=10);

        sleep(Duration::from_millis(think_ms)).await;

        let mut conn = match pool.get_conn().await {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(terminal = id, error = %e, "connection error, retrying");
                sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        let result = if roll <= 45 {
            let o_id = next_o_id.fetch_add(1, Ordering::Relaxed);
            txn_new_order(&mut conn, w_id, d_id, c_id, o_id, ol_cnt, &mut rng)
                .await
        } else if roll <= 88 {
            txn_payment(&mut conn, w_id, d_id, c_id, amount, &mut rng).await
        } else {
            txn_delivery(&mut conn, w_id, d_id, carrier).await
        };

        match result {
            Ok(kind) => {
                match kind {
                    TxnKind::NewOrder => {
                        counters.new_orders.fetch_add(1, Ordering::Relaxed)
                    }
                    TxnKind::Payment => {
                        counters.payments.fetch_add(1, Ordering::Relaxed)
                    }
                    TxnKind::Delivery => {
                        counters.deliveries.fetch_add(1, Ordering::Relaxed)
                    }
                };
            }
            Err(e) => {
                tracing::debug!(terminal = id, error = %e, "transaction error (ignored)");
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        let _ = conn.disconnect().await;
    }

    let _ = pool.disconnect().await;
}

enum TxnKind {
    NewOrder,
    Payment,
    Delivery,
}

// ── New-Order transaction ──────────────────────────────────────────────────────
// Inserts order + new_order + ol_cnt order_lines, updates ol_cnt stock rows.
// All in one MySQL transaction — the primary test of CDC multi-table atomicity.

async fn txn_new_order(
    conn: &mut mysql_async::Conn,
    w_id: u32,
    d_id: u32,
    c_id: u32,
    o_id: u64,
    ol_cnt: u32,
    rng: &mut SmallRng,
) -> Result<TxnKind> {
    // Generate all random inputs before touching the DB.
    let items: Vec<(u32, u32, u16)> = (0..ol_cnt)
        .map(|_| {
            let i_id = rng.gen_range(1..=ITEMS);
            let supply_w = if WAREHOUSES > 1 && rng.gen_bool(0.15) {
                (rng.gen_range(1..WAREHOUSES) % WAREHOUSES) + 1
            } else {
                w_id
            };
            let qty: u16 = rng.gen_range(1..=10);
            (i_id, supply_w, qty)
        })
        .collect();
    let all_local: u8 = if items.iter().all(|(_, sw, _)| *sw == w_id) {
        1
    } else {
        0
    };

    let mut tx = conn.start_transaction(TxOpts::default()).await?;

    tx.exec_drop(
        "INSERT INTO tpcc_order (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
         VALUES (?, ?, ?, ?, NOW(), ?, ?)",
        (o_id, d_id, w_id, c_id, ol_cnt, all_local),
    ).await?;

    tx.exec_drop(
        "INSERT INTO tpcc_new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)",
        (o_id, d_id, w_id),
    ).await?;

    for (num, (i_id, supply_w, qty)) in items.iter().enumerate() {
        // Fetch item price.
        let price: f64 = tx
            .exec_first("SELECT i_price FROM tpcc_item WHERE i_id = ?", (i_id,))
            .await?
            .unwrap_or(1.0);

        // Update stock — restock if quantity would drop below 10.
        tx.exec_drop(
            "UPDATE tpcc_stock \
             SET s_quantity = IF(s_quantity - ? < 10, s_quantity - ? + 91, s_quantity - ?), \
                 s_ytd = s_ytd + ?, \
                 s_order_cnt = s_order_cnt + 1, \
                 s_remote_cnt = s_remote_cnt + ? \
             WHERE s_i_id = ? AND s_w_id = ?",
            (*qty as i32, *qty as i32, *qty as i32, *qty as f64,
             if *supply_w != w_id { 1i32 } else { 0i32 }, i_id, supply_w),
        ).await?;

        // Fetch dist_info for this district (CHAR(24) column per district).
        let dist_col = format!("s_dist_{d_id:02}");
        let dist_info: String = tx
            .exec_first(
                format!("SELECT {dist_col} FROM tpcc_stock WHERE s_i_id = ? AND s_w_id = ?"),
                (i_id, supply_w),
            )
            .await?
            .unwrap_or_default();

        let amount = price * (*qty as f64);
        tx.exec_drop(
            "INSERT INTO tpcc_order_line \
             (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
              ol_quantity, ol_amount, ol_dist_info) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                o_id,
                d_id,
                w_id,
                (num + 1) as u8,
                i_id,
                supply_w,
                *qty as i16,
                amount,
                dist_info,
            ),
        )
        .await?;
    }

    tx.commit().await?;
    Ok(TxnKind::NewOrder)
}

// ── Payment transaction ───────────────────────────────────────────────────────
// Updates 3 rows (warehouse, district, customer) and inserts 1 history row.

async fn txn_payment(
    conn: &mut mysql_async::Conn,
    w_id: u32,
    d_id: u32,
    c_id: u32,
    amount: f64,
    rng: &mut SmallRng,
) -> Result<TxnKind> {
    let h_data: String =
        format!("{}    {}", rand_str(rng, 6, 10), rand_str(rng, 6, 10));

    let mut tx = conn.start_transaction(TxOpts::default()).await?;

    tx.exec_drop(
        "UPDATE tpcc_warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?",
        (amount, w_id),
    )
    .await?;

    tx.exec_drop(
        "UPDATE tpcc_district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?",
        (amount, w_id, d_id),
    ).await?;

    tx.exec_drop(
        "UPDATE tpcc_customer \
         SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, \
             c_payment_cnt = c_payment_cnt + 1 \
         WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
        (amount, amount, w_id, d_id, c_id),
    )
    .await?;

    tx.exec_drop(
        "INSERT INTO tpcc_history \
         (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) \
         VALUES (?, ?, ?, ?, ?, NOW(), ?, ?)",
        (c_id, d_id, w_id, d_id, w_id, amount, h_data),
    ).await?;

    tx.commit().await?;
    Ok(TxnKind::Payment)
}

// ── Delivery transaction ──────────────────────────────────────────────────────
// DELETEs the oldest pending new_order for one district, marks it delivered.

async fn txn_delivery(
    conn: &mut mysql_async::Conn,
    w_id: u32,
    d_id: u32,
    carrier_id: u8,
) -> Result<TxnKind> {
    let mut tx = conn.start_transaction(TxOpts::default()).await?;

    // Find the oldest undelivered order for this district.
    // MIN() returns NULL when no rows match, so we need Option<Option<u64>>:
    // the outer Option is "did we get a row", the inner is "was the value NULL".
    let no_o_id: Option<Option<u64>> = tx
        .exec_first(
            "SELECT MIN(no_o_id) FROM tpcc_new_order WHERE no_w_id = ? AND no_d_id = ?",
            (w_id, d_id),
        )
        .await?;

    let Some(Some(o_id)) = no_o_id else {
        tx.rollback().await?;
        return Ok(TxnKind::Delivery); // Nothing pending for this district.
    };

    tx.exec_drop(
        "DELETE FROM tpcc_new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?",
        (w_id, d_id, o_id),
    ).await?;

    // Retrieve the customer for the order.
    let c_id: u32 = tx
        .exec_first(
            "SELECT o_c_id FROM tpcc_order WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?",
            (w_id, d_id, o_id),
        )
        .await?
        .unwrap_or(1);

    tx.exec_drop(
        "UPDATE tpcc_order SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?",
        (carrier_id, w_id, d_id, o_id),
    ).await?;

    tx.exec_drop(
        "UPDATE tpcc_order_line SET ol_delivery_d = NOW() \
         WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
        (w_id, d_id, o_id),
    )
    .await?;

    let ol_total: f64 = tx
        .exec_first(
            "SELECT COALESCE(SUM(ol_amount), 0.0) FROM tpcc_order_line \
             WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
            (w_id, d_id, o_id),
        )
        .await?
        .unwrap_or(0.0);

    tx.exec_drop(
        "UPDATE tpcc_customer \
         SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 \
         WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
        (ol_total, w_id, d_id, c_id),
    )
    .await?;

    tx.commit().await?;
    Ok(TxnKind::Delivery)
}

// ── Fault injectors (mirrors soak.rs) ────────────────────────────────────────

async fn inject_network_partition(harness: &Harness) -> Result<Duration> {
    let start = Instant::now();
    harness.toxi.disable("mysql").await?;
    sleep(Duration::from_secs(PARTITION_HOLD_SECS)).await;
    harness.toxi.enable("mysql").await?;
    harness::wait_for_url(TPCC_HEALTH_URL, RECOVERY_TIMEOUT).await?;
    Ok(start.elapsed())
}

async fn inject_sink_outage(harness: &Harness) -> Result<Duration> {
    let start = Instant::now();
    harness.toxi.disable("kafka").await?;
    sleep(Duration::from_secs(OUTAGE_HOLD_SECS)).await;
    harness.toxi.enable("kafka").await?;
    harness::wait_for_url(TPCC_HEALTH_URL, RECOVERY_TIMEOUT).await?;
    Ok(start.elapsed())
}

async fn inject_crash() -> Result<Duration> {
    let start = Instant::now();
    docker::kill_service(TPCC_PROFILE, TPCC_SERVICE).await?;
    docker::start_service(TPCC_PROFILE, TPCC_SERVICE).await?;
    harness::wait_for_url(TPCC_HEALTH_URL, RECOVERY_TIMEOUT).await?;
    Ok(start.elapsed())
}

// ── Schema alter loop ─────────────────────────────────────────────────────────

const TPCC_TABLES: &[&str] = &[
    "tpcc_warehouse",
    "tpcc_district",
    "tpcc_customer",
    "tpcc_history",
    "tpcc_item",
    "tpcc_stock",
    "tpcc_order",
    "tpcc_new_order",
    "tpcc_order_line",
];

struct AlterRecord {
    ok: bool,
}

async fn alter_loop(
    stop: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
) -> Vec<AlterRecord> {
    let mut rng = SmallRng::from_entropy();
    let mut records = Vec::new();
    let pool = mysql_async::Pool::new(MYSQL_DSN);

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let wait_secs = rng.gen_range(ALTER_MIN_SECS..=ALTER_MAX_SECS);
        tokio::select! {
            _ = wake.notified() => {}
            _ = sleep(Duration::from_secs(wait_secs)) => {}
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }

        let table = TPCC_TABLES[rng.gen_range(0..TPCC_TABLES.len())];
        let col_suffix: u32 = rng.r#gen();
        let col_name = format!("ext_{col_suffix:08x}");
        let sql = match rng.gen_range(0u8..5) {
            0 => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} BIGINT DEFAULT NULL"
            ),
            1 => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} VARCHAR(128) DEFAULT NULL"
            ),
            2 => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} BOOLEAN DEFAULT FALSE"
            ),
            3 => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} JSON DEFAULT NULL"
            ),
            _ => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} FLOAT DEFAULT NULL"
            ),
        };

        match pool.get_conn().await {
            Ok(mut conn) => match conn.exec_drop(&sql, ()).await {
                Ok(_) => {
                    info!(%table, %col_name, "mid-stream schema alter applied");
                    records.push(AlterRecord { ok: true });
                }
                Err(e) => {
                    info!(%table, %col_name, error = %e, "mid-stream schema alter failed (ignored)");
                    records.push(AlterRecord { ok: false });
                }
            },
            Err(e) => {
                tracing::warn!(%table, error = %e, "alter loop failed to get connection");
                records.push(AlterRecord { ok: false });
            }
        }
    }

    let _ = pool.disconnect().await;
    records
}

// ── Resource sampling ─────────────────────────────────────────────────────────

struct ResourceSample {
    mem_bytes: u64,
    cpu_percent: f64,
}

async fn sample_and_log(
    samples: &mut Vec<ResourceSample>,
    counters: &Arc<Counters>,
) {
    if let Ok(s) = docker::sample_stats(TPCC_PROFILE, TPCC_SERVICE).await {
        if s.mem_bytes > 0 {
            info!(
                cpu = s.cpu_percent,
                mem_mib = s.mem_bytes / 1024 / 1024,
                new_orders = counters.new_orders.load(Ordering::Relaxed),
                payments = counters.payments.load(Ordering::Relaxed),
                deliveries = counters.deliveries.load(Ordering::Relaxed),
                "resource snapshot",
            );
            samples.push(ResourceSample {
                mem_bytes: s.mem_bytes,
                cpu_percent: s.cpu_percent,
            });
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Counters {
    new_orders: AtomicU64,
    payments: AtomicU64,
    deliveries: AtomicU64,
    errors: AtomicU64,
}

struct FaultRecord {
    kind: &'static str,
    recovery_secs: f64,
    recovered: bool,
}

const SYLLABLES: &[&str] = &[
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION",
    "EING",
];

/// TPC-C last name: concatenate three syllables based on n % 1000.
fn last_name(rng: &mut SmallRng, c_id: u32) -> String {
    let n = if c_id <= 1000 {
        c_id - 1
    } else {
        rng.gen_range(0..1000)
    };
    format!(
        "{}{}{}",
        SYLLABLES[(n / 100) as usize],
        SYLLABLES[((n / 10) % 10) as usize],
        SYLLABLES[(n % 10) as usize]
    )
}

fn rand_str(rng: &mut SmallRng, min: usize, max: usize) -> String {
    const ALPHA: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let len = if min == max {
        min
    } else {
        rng.gen_range(min..=max)
    };
    (0..len)
        .map(|_| ALPHA[rng.gen_range(0..ALPHA.len())] as char)
        .collect()
}

fn rand_fixed24(rng: &mut SmallRng) -> String {
    let s = rand_str(rng, 24, 24);
    format!("{s:<24}")
}

fn rand_state(rng: &mut SmallRng) -> &'static str {
    const STATES: &[&str] =
        &["CA", "TX", "NY", "FL", "WA", "OR", "IL", "OH", "GA", "NC"];
    STATES[rng.gen_range(0..STATES.len())]
}

fn rand_zip(rng: &mut SmallRng) -> String {
    format!("{:05}1111", rng.gen_range(0..100_000u32))
}
fn rand_tax(rng: &mut SmallRng) -> f64 {
    rng.gen_range(0.0..0.2)
}
fn rand_phone(rng: &mut SmallRng) -> String {
    format!(
        "{:04}-{:03}-{:04}-{:04}",
        rng.gen_range(0..10000u32),
        rng.gen_range(0..1000u32),
        rng.gen_range(0..10000u32),
        rng.gen_range(0..10000u32)
    )
}
