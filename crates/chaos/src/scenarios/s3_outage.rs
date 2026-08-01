//! Scenario: MinIO becomes unavailable mid-stream (S3 sink).
//!
//! What it proves:
//! - DeltaForge backpressures the source while S3 is down (required sink).
//! - No partial files appear at the destination during the outage
//!   (file-level atomicity).
//! - The pipeline recovers cleanly when S3 returns — the in-flight batch
//!   that timed out gets replayed and lands in a new file.
//!
//! Prerequisites:
//! - Pipeline running with an S3 sink targeting `deltaforge-chaos` on
//!   MinIO via the toxiproxy route (`http://toxiproxy:5104`).
//! - `--profile s3-infra` up (MinIO + toxiproxy route).
//!
//! Steps:
//!   1. Warmup: insert rows, wait for objects to appear.
//!   2. Cut the MinIO proxy.
//!   3. Insert more rows; object count should NOT grow.
//!   4. Restore the proxy.
//!   5. Verify object count grows again after restore.

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
    name: "s3-outage",
    description: "Cuts MinIO via toxiproxy mid-stream then restores it.",
    expected: "Required S3 sink backpressures the source; no partial files \
               visible; pipeline catches up cleanly after MinIO returns.",
    category: ScenarioCategory::S3,
    required_profiles: &["base", "mysql-infra", "s3-infra", "df"],
    tags: &["s3", "resilience", "backpressure"],
};

// Host port 9100 → MinIO container port 9000 (see s3_soak.rs comment).
const MINIO_ENDPOINT: &str = "http://localhost:9100";
const MINIO_KEY: &str = "minioadmin";
const MINIO_SECRET: &str = "minioadmin";
const BUCKET: &str = "deltaforge-chaos";
const PREFIX: &str = "lake/";
const S3_PROXY: &str = "minio";

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const OUTAGE_HOLD: Duration = Duration::from_secs(20);
const RECOVERY_WAIT: Duration = Duration::from_secs(45);
const POLL_INTERVAL: Duration = Duration::from_secs(3);

pub async fn run<B: SourceBackend>(
    harness: &Harness,
    backend: &B,
) -> Result<ScenarioResult> {
    let name = format!("{}/s3-outage", backend.name());
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
    info!("warmup: priming S3 sink and waiting for first object...");
    let warmup_start = Instant::now();
    let initial_objects = loop {
        if warmup_start.elapsed() > WARMUP_TIMEOUT {
            return Ok(ScenarioResult::fail(
                &name,
                "warmup timeout — no S3 objects appeared. Is an S3 \
                 pipeline running and targeting toxiproxy:5104?",
            ));
        }
        backend.insert_rows("s3-outage-warmup", 100).await?;
        let count = count_objects(&store, &prefix).await.unwrap_or(0);
        if count > 0 {
            info!(count, "warmup complete");
            break count;
        }
        sleep(POLL_INTERVAL).await;
    };

    // ── Phase 1: Cut MinIO ──
    info!("cutting MinIO proxy ({S3_PROXY})...");
    harness.toxi.disable(S3_PROXY).await?;

    // Insert more rows. Required S3 sink will backpressure; the events
    // pile up in DeltaForge's in-memory buffers (bounded by pool max_bytes
    // × active_partitions). No NEW objects should land at the destination
    // during the outage.
    info!("inserting rows while MinIO is down (expect backpressure)...");
    let inserted_during_outage =
        backend.insert_rows("s3-outage-down", 500).await?;
    info!(
        inserted_during_outage,
        "rows inserted into source during outage"
    );
    sleep(OUTAGE_HOLD).await;

    let objects_during_outage =
        count_objects(&store, &prefix).await.unwrap_or(0);
    let delta_during_outage =
        objects_during_outage as i64 - initial_objects as i64;
    info!(
        objects_during_outage,
        delta_during_outage,
        "S3 object count during outage (delta should be 0 — no partial files)"
    );

    if delta_during_outage > 0 {
        // Restore before failing.
        harness.toxi.enable(S3_PROXY).await?;
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "S3 object count grew by {delta_during_outage} during the \
                 outage — partial-file atomicity violated"
            ),
        ));
    }

    // ── Phase 2: Restore MinIO ──
    info!("restoring MinIO proxy...");
    harness.toxi.enable(S3_PROXY).await?;

    // After restore, the pipeline should drain the backlog. Object count
    // must grow within RECOVERY_WAIT.
    let restore_deadline = Instant::now() + RECOVERY_WAIT;
    let recovered = loop {
        if Instant::now() > restore_deadline {
            break false;
        }
        sleep(POLL_INTERVAL).await;
        let count = count_objects(&store, &prefix).await.unwrap_or(0);
        if count > objects_during_outage {
            info!(
                objects = count,
                delta = count - objects_during_outage,
                "recovery complete — S3 objects landing again"
            );
            break true;
        }
    };

    if !recovered {
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "no S3 objects landed within {}s after restore — pipeline \
                 may be stuck",
                RECOVERY_WAIT.as_secs()
            ),
        ));
    }

    let final_objects = count_objects(&store, &prefix).await.unwrap_or(0);
    let recovered_objects = final_objects - objects_during_outage;
    let mut result = ScenarioResult::pass(&name);
    result = result
        .note(format!(
            "rows inserted during outage: {inserted_during_outage}"
        ))
        .note(format!(
            "objects during outage: {objects_during_outage} \
             (delta 0 — atomicity preserved)"
        ))
        .note(format!("objects after recovery: {final_objects}"))
        .note(format!("objects landed post-restore: {recovered_objects}"));
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
