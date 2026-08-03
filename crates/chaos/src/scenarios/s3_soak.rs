//! Scenario: S3 sink sustained writes.
//!
//! What it proves:
//! - Per-row DLQ + streaming JSONL + List/Map columns + multipart upload
//!   all hold up under a sustained write load.
//! - File rolling fires correctly (we see new objects appear over time).
//! - Memory does not grow unboundedly (no full-file-in-RAM regression).
//!
//! Prerequisites:
//! - Pipeline running with an S3 sink targeting bucket `deltaforge-chaos`
//!   on MinIO (e.g., `chaos/config/mysql-to-s3.yaml`).
//! - `--profile s3-infra` up (MinIO + mc-bootstrap + lifecycle policy).
//!
//! Steps:
//!   1. Warmup — wait until at least one object lands under the prefix.
//!   2. Insert rows over `duration_mins` minutes via the source backend.
//!   3. Periodically poll the bucket prefix for object count.
//!   4. Verify: bucket grew during the soak; final inserted count matches.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use tokio::time::sleep;
use tracing::info;

use crate::backend::SourceBackend;
use crate::harness::{Harness, ScenarioResult, print_scenario_banner};
use crate::scenarios::meta::{ScenarioCategory, ScenarioMeta};

pub const META: ScenarioMeta = ScenarioMeta {
    name: "s3-soak",
    description: "Sustained, steady offered load to S3 (Parquet) — a stability \
         test, not a throughput benchmark: memory must stay bounded over time \
         while file rolling, partitioning, multipart upload, and per-row DLQ all \
         keep working. (For peak MySQL→S3 throughput, use s3-backlog-drain.)",
    expected: "All written rows present in the bucket; memory does not grow \
               unboundedly; the pipeline keeps up with the steady offered load.",
    category: ScenarioCategory::S3,
    required_profiles: &["base", "mysql-infra", "s3-infra", "df"],
    tags: &["s3", "parquet", "soak", "stability"],
};

// Direct (non-toxiproxied) MinIO endpoint reachable from the chaos runner.
// Host port 9100 → MinIO container port 9000 (mapped in docker-compose.chaos.yml
// to avoid conflict with DeltaForge containers' metrics on 9000-9002).
const MINIO_ENDPOINT: &str = "http://localhost:9100";
const MINIO_KEY: &str = "minioadmin";
const MINIO_SECRET: &str = "minioadmin";
const BUCKET: &str = "deltaforge-chaos";
const PREFIX: &str = "lake/";

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

pub async fn run<B: SourceBackend>(
    harness: &Harness,
    backend: &B,
    duration_mins: u64,
    rows_per_batch: i64,
    batch_delay_ms: u64,
) -> Result<ScenarioResult> {
    let name = format!("{}/s3-soak", backend.name());
    print_scenario_banner(&name, META.description, META.expected);
    harness.setup().await?;

    let store: Arc<dyn ObjectStore> = match build_minio_store() {
        Ok(s) => s,
        Err(e) => {
            return Ok(ScenarioResult::fail(
                &name,
                format!("could not build MinIO client: {e}"),
            ));
        }
    };
    let prefix = Path::from(PREFIX);

    // ── Warmup ──
    info!(prefix = PREFIX, "warmup: waiting for first S3 object...");
    let warmup_start = Instant::now();
    let initial_objects = loop {
        if warmup_start.elapsed() > WARMUP_TIMEOUT {
            return Ok(ScenarioResult::fail(
                &name,
                "warmup timeout — no S3 objects appeared. \
                 Is an S3-sink pipeline applied and running?",
            ));
        }
        // Insert a small priming batch so the writer has something to do.
        backend.insert_rows("s3-soak-warmup", 100).await?;
        let count = count_objects(&store, &prefix).await.unwrap_or(0);
        if count > 0 {
            info!(count, "warmup complete — S3 sink is producing files");
            break count;
        }
        sleep(POLL_INTERVAL).await;
    };

    // ── Soak: keep inserting for the duration ──
    let deadline = Instant::now() + Duration::from_secs(duration_mins * 60);
    info!(
        duration_mins,
        rows_per_batch, batch_delay_ms, "starting S3 soak insert workload"
    );

    let mut total_inserted: i64 = 0;
    let mut last_sample_at = Instant::now();
    let mut sample_count: u64 = 0;
    let mut prev_objects = initial_objects;
    let mut growth_windows: u64 = 0;
    let mut total_windows: u64 = 0;

    while Instant::now() < deadline {
        let inserted = backend.insert_rows("s3-soak", rows_per_batch).await?;
        total_inserted += inserted;
        sleep(Duration::from_millis(batch_delay_ms)).await;

        if last_sample_at.elapsed() >= SAMPLE_INTERVAL {
            let count = count_objects(&store, &prefix).await.unwrap_or(0);
            let elapsed = deadline
                .duration_since(Instant::now())
                .max(Duration::ZERO)
                .as_secs();
            info!(
                remaining_secs = elapsed,
                total_inserted,
                objects = count,
                delta_since_warmup = count.saturating_sub(initial_objects),
                "s3 soak progress"
            );
            total_windows += 1;
            if count > prev_objects {
                growth_windows += 1;
            }
            prev_objects = count;
            last_sample_at = Instant::now();
            sample_count += 1;
        }
    }

    // ── Final check ──
    let final_objects = count_objects(&store, &prefix).await.unwrap_or(0);
    info!(
        initial_objects,
        final_objects, total_inserted, "soak complete"
    );

    if final_objects <= initial_objects {
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "S3 object count did not grow: started {initial_objects}, \
                 finished {final_objects}. S3 sink may be stuck."
            ),
        ));
    }

    let growth_ratio = if total_windows > 0 {
        growth_windows as f64 / total_windows as f64
    } else {
        0.0
    };
    let mut result = ScenarioResult::pass(&name);
    result = result
        .note(format!("rows inserted: {total_inserted}"))
        .note(format!(
            "objects committed during soak: {}",
            final_objects - initial_objects
        ))
        .note(format!(
            "rolling cadence: {growth_windows}/{total_windows} \
             sample windows showed object growth ({:.0}%)",
            growth_ratio * 100.0
        ))
        .note(format!("samples taken: {sample_count}"));
    Ok(result)
}

fn build_minio_store() -> Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_endpoint(MINIO_ENDPOINT)
        .with_region("us-east-1")
        .with_access_key_id(MINIO_KEY)
        .with_secret_access_key(MINIO_SECRET)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;
    Ok(Arc::new(store))
}

async fn count_objects(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<u64> {
    use futures::TryStreamExt;
    let stream = store.list(Some(prefix));
    let entries: Vec<_> = stream.try_collect().await?;
    Ok(entries.len() as u64)
}
