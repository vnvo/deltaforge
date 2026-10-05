//! A container the gate self-test (`scripts/gate-selftest.sh`) runs inside a
//! real gate: it starts one owned container and never removes it (like a
//! suite's shared static container), then fails or hangs as
//! `GATE_PROBE` asks. Not part of any gate tier.

use gate_ownership::GateOwned;
use testcontainers::{GenericImage, ImageExt, runners::AsyncRunner};

#[tokio::test]
#[ignore = "run by scripts/gate-selftest.sh"]
async fn gate_probe() {
    let container = GenericImage::new("postgres", "17")
        .with_env_var("POSTGRES_PASSWORD", "probe")
        .gate_owned()
        .start()
        .await
        .expect("start the probe container");
    let id = container.id().to_string();
    // Never dropped: the gate, not the test, must remove it.
    std::mem::forget(container);
    if let Ok(marker) = std::env::var("GATE_PROBE_MARKER") {
        std::fs::write(marker, &id).expect("write the probe marker");
    }
    match std::env::var("GATE_PROBE").as_deref() {
        Ok("hang") => {
            tokio::time::sleep(std::time::Duration::from_secs(900)).await
        }
        _ => panic!("forced failure with container {id} left running"),
    }
}
