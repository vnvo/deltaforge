//! Source-backend abstraction for chaos scenarios.
//!
//! Generic scenarios (network_partition, sink_outage, crash_recovery,
//! schema_drift) are parameterised over `SourceBackend` so the same scenario
//! logic runs against both MySQL and PostgreSQL without duplication.
//!
//! Source-specific scenarios (failover, binlog_purge, pg_failover,
//! slot_dropped) are NOT generic — they reach directly into the database
//! using their own connection helpers.

use anyhow::Result;

// ── Direct DSNs (bypasses Toxiproxy — used for data insertion) ──────────────

/// MySQL root DSN — connects directly to the host-exposed port, not through
/// Toxiproxy, so inserts succeed even when the proxy is cut.
pub const MYSQL_DSN: &str = "mysql://root:password@127.0.0.1:3306/orders";

/// Postgres superuser DSN — same intent as MYSQL_DSN.
pub const PG_DSN: &str =
    "host=localhost port=5432 user=postgres password=postgres dbname=orders";

// ── Trait ────────────────────────────────────────────────────────────────────

/// Encapsulates everything a generic chaos scenario needs to know about the
/// CDC source being tested.
///
/// All implementors are used through generics (`<B: SourceBackend>`), not
/// `dyn SourceBackend`, so async fn in traits works without boxing.
pub trait SourceBackend: Send + Sync {
    /// Short identifier shown in log messages and scenario result names.
    fn name(&self) -> &str;

    /// Toxiproxy proxy name to cut/restore for source-partition scenarios.
    fn proxy(&self) -> &str;

    /// Docker Compose profile that owns this backend's DeltaForge service.
    fn compose_profile(&self) -> &str;

    /// Docker Compose service name for DeltaForge.
    fn compose_service(&self) -> &str;

    /// Insert `n` test rows directly into the source DB (bypasses Toxiproxy).
    /// `tag` is embedded in the row data so log messages can identify callers.
    async fn insert_rows(&self, tag: &str, n: i64) -> Result<i64>;

    /// ALTER TABLE customers ADD COLUMN notes TEXT DEFAULT NULL.
    async fn schema_drift_add(&self) -> Result<()>;

    /// ALTER TABLE customers DROP COLUMN notes (reverts schema_drift_add).
    async fn schema_drift_drop(&self) -> Result<()>;

    /// Insert a single row, optionally populating the `notes` column.
    /// Called by schema_drift before (notes=None) and after (notes=Some) the
    /// DDL to verify the column is handled correctly by DeltaForge.
    async fn schema_drift_insert(&self, notes: Option<String>) -> Result<()>;
}

// ── MySQL backend ─────────────────────────────────────────────────────────────

pub struct MysqlBackend;

impl SourceBackend for MysqlBackend {
    fn name(&self) -> &str {
        "mysql"
    }
    fn proxy(&self) -> &str {
        "mysql"
    }
    fn compose_profile(&self) -> &str {
        "app"
    }
    fn compose_service(&self) -> &str {
        "deltaforge"
    }

    async fn insert_rows(&self, tag: &str, n: i64) -> Result<i64> {
        use mysql_async::prelude::Queryable;
        let pool = mysql_async::Pool::new(MYSQL_DSN);
        let mut conn = pool.get_conn().await?;
        let ts = now_ms();
        for i in 0..n {
            conn.exec_drop(
                "INSERT INTO customers (name, email, balance) VALUES (?, ?, ?)",
                (
                    format!("{tag}-{ts}-{i}"),
                    format!("{tag}-{ts}-{i}@test.com"),
                    0.0_f64,
                ),
            )
            .await?;
        }
        conn.disconnect().await?;
        let _ = pool.disconnect().await;
        Ok(n)
    }

    async fn schema_drift_add(&self) -> Result<()> {
        mysql_exec("ALTER TABLE customers ADD COLUMN notes TEXT DEFAULT NULL")
            .await
    }

    async fn schema_drift_drop(&self) -> Result<()> {
        mysql_exec("ALTER TABLE customers DROP COLUMN notes").await
    }

    async fn schema_drift_insert(&self, notes: Option<String>) -> Result<()> {
        use mysql_async::prelude::Queryable;
        let pool = mysql_async::Pool::new(MYSQL_DSN);
        let mut conn = pool.get_conn().await?;
        let ts = now_ms();
        match notes {
            Some(n) => {
                conn.exec_drop(
                    "INSERT INTO customers (name, email, balance, notes) VALUES (?, ?, ?, ?)",
                    (format!("drift-{ts}"), format!("drift-{ts}@test.com"), 0.0_f64, n),
                )
                .await?
            }
            None => {
                conn.exec_drop(
                    "INSERT INTO customers (name, email, balance) VALUES (?, ?, ?)",
                    (format!("drift-{ts}"), format!("drift-{ts}@test.com"), 0.0_f64),
                )
                .await?
            }
        }
        conn.disconnect().await?;
        let _ = pool.disconnect().await;
        Ok(())
    }
}

// ── PostgreSQL backend ────────────────────────────────────────────────────────

pub struct PgBackend;

impl SourceBackend for PgBackend {
    fn name(&self) -> &str {
        "postgres"
    }
    fn proxy(&self) -> &str {
        "postgres"
    }
    fn compose_profile(&self) -> &str {
        "pg-app"
    }
    fn compose_service(&self) -> &str {
        "deltaforge-pg"
    }

    async fn insert_rows(&self, tag: &str, n: i64) -> Result<i64> {
        let (client, conn) =
            tokio_postgres::connect(PG_DSN, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let ts = now_ms();
        for i in 0..n {
            let name = format!("{tag}-{ts}-{i}");
            let email = format!("{tag}-{ts}-{i}@test.com");
            let balance = 0.0_f64;
            client
                .execute(
                    "INSERT INTO customers (name, email, balance) VALUES ($1, $2, $3)",
                    &[&name, &email, &balance],
                )
                .await?;
        }
        Ok(n)
    }

    async fn schema_drift_add(&self) -> Result<()> {
        pg_exec("ALTER TABLE customers ADD COLUMN notes TEXT DEFAULT NULL")
            .await
    }

    async fn schema_drift_drop(&self) -> Result<()> {
        pg_exec("ALTER TABLE customers DROP COLUMN notes").await
    }

    async fn schema_drift_insert(&self, notes: Option<String>) -> Result<()> {
        let (client, conn) =
            tokio_postgres::connect(PG_DSN, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let ts = now_ms();
        let name = format!("drift-{ts}");
        let email = format!("drift-{ts}@test.com");
        let balance = 0.0_f64;
        match notes {
            Some(n) => {
                client
                    .execute(
                        "INSERT INTO customers (name, email, balance, notes) \
                         VALUES ($1, $2, $3, $4)",
                        &[&name, &email, &balance, &n],
                    )
                    .await?;
            }
            None => {
                client
                    .execute(
                        "INSERT INTO customers (name, email, balance) VALUES ($1, $2, $3)",
                        &[&name, &email, &balance],
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

/// Run a single DDL/DML statement against MySQL (no parameters).
async fn mysql_exec(sql: &str) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let pool = mysql_async::Pool::new(MYSQL_DSN);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(sql).await?;
    conn.disconnect().await?;
    let _ = pool.disconnect().await;
    Ok(())
}

/// Run a single DDL/DML statement against Postgres (no parameters).
async fn pg_exec(sql: &str) -> Result<()> {
    let (client, conn) =
        tokio_postgres::connect(PG_DSN, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client.execute(sql, &[]).await?;
    Ok(())
}
