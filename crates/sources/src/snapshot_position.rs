//! The resume position a snapshot boundary commits to the sinks' checkpoints
//! while the snapshot is incomplete.
//!
//! A sink commits a snapshot boundary as its resume checkpoint. Until the
//! snapshot is complete that position must not read as a stream position: a
//! restart resuming the stream at the anchor would skip every snapshot row not
//! yet delivered. So an incomplete snapshot's boundaries carry this explicit
//! shape, `{"snapshot": {"format", "generation", "anchor"}}`, with no
//! top-level stream position (no PostgreSQL `lsn`, no MySQL `file`/`pos`): a
//! release that does not know it cannot parse it as a position and fails
//! closed. Only the boundary that completes the snapshot carries an ordinary
//! stream position at the anchor.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// The format this release writes and reads.
pub const SNAPSHOT_POSITION_FORMAT: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position<A> {
    snapshot: Mark<A>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Mark<A> {
    format: u32,
    generation: u64,
    anchor: A,
}

/// The incomplete-snapshot position of `generation` with its `anchor`.
pub fn encode<A: Serialize>(generation: u64, anchor: A) -> Vec<u8> {
    serde_json::to_vec(&Position {
        snapshot: Mark {
            format: SNAPSHOT_POSITION_FORMAT,
            generation,
            anchor,
        },
    })
    .expect("a snapshot position always serializes")
}

/// `Ok(None)`: not a snapshot position (a JSON object without a `snapshot`
/// member, or not JSON). `Ok(Some((generation, anchor)))`: one this release
/// reads. `Err`: a snapshot position of another format or malformed - never
/// taken for anything else.
pub fn decode<A: DeserializeOwned>(
    raw: &[u8],
) -> Result<Option<(u64, A)>, String> {
    let Ok(serde_json::Value::Object(map)) =
        serde_json::from_slice::<serde_json::Value>(raw)
    else {
        return Ok(None);
    };
    let Some(mark) = map.get("snapshot") else {
        return Ok(None);
    };
    let format = mark.get("format").and_then(serde_json::Value::as_u64);
    if format != Some(u64::from(SNAPSHOT_POSITION_FORMAT)) {
        return Err(format!(
            "snapshot position of format {format:?}, which this release does \
             not know"
        ));
    }
    let p: Position<A> = serde_json::from_slice(raw)
        .map_err(|e| format!("malformed snapshot position: {e}"))?;
    Ok(Some((p.snapshot.generation, p.snapshot.anchor)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_position_round_trips_and_is_told_apart() {
        let raw = encode(7, "0/16B3748");
        assert_eq!(
            decode::<String>(&raw).unwrap(),
            Some((7, "0/16B3748".to_string()))
        );
        // Stream positions and other bytes are not snapshot positions.
        assert_eq!(
            decode::<String>(br#"{"lsn":"0/1","tx_id":null}"#),
            Ok(None)
        );
        assert_eq!(decode::<String>(b"0/16B3748"), Ok(None));
    }

    #[test]
    fn an_unknown_or_malformed_snapshot_position_is_refused() {
        let future =
            br#"{"snapshot":{"format":2,"generation":1,"anchor":"0/1"}}"#;
        assert!(decode::<String>(future).is_err());
        let extra =
            br#"{"snapshot":{"format":1,"generation":1,"anchor":"0/1"},"lsn":"0/9"}"#;
        assert!(decode::<String>(extra).is_err());
        let wrong =
            br#"{"snapshot":{"format":1,"generation":"x","anchor":"0/1"}}"#;
        assert!(decode::<String>(wrong).is_err());
    }
}
