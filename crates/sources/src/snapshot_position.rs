//! The resume position a snapshot boundary commits to the sinks' checkpoints
//! while the snapshot is incomplete, the adoption position, and their order
//! against each other and against stream positions.
//!
//! A sink commits a snapshot boundary as its resume checkpoint. Until the
//! snapshot is complete that position must not read as a stream position: a
//! restart resuming the stream at the anchor would skip every snapshot row not
//! yet delivered. So an incomplete snapshot's boundaries carry this explicit
//! shape, `{"snapshot": {"format", ["snapshot_chain",] "generation",
//! "anchor"}}`, with no top-level stream position (no PostgreSQL `lsn`, no
//! MySQL `file`/`pos`): a release that does not know it cannot parse it as a
//! position and fails closed. Only the boundary that completes the snapshot
//! carries an ordinary stream position at the anchor.
//!
//! Format 1 (no chain) is what #131 wrote; format 2 binds the snapshot chain
//! (see `docs/design/snapshot-durable-queue.md`, section 3.5): within one
//! chain a later generation orders after an earlier one, different chains
//! never order. Every generation starts with each sink moving its own state
//! into it (the generation start barrier, design section 5.4), which leaves
//! the start position `{"snapshot_adopted": {"format", "snapshot_chain",
//! "generation", "replaced_digest"}}`. Every order here uses only the two
//! positions; a move into a generation is a local check
//! ([`generation_start`]), never an order.

use deltaforge_core::CheckpointOrder;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// The format [`encode`] writes (#131, no chain).
pub const SNAPSHOT_POSITION_FORMAT: u32 = 1;
/// The format [`encode_chained`] writes.
pub const SNAPSHOT_POSITION_FORMAT_CHAINED: u32 = 2;
/// The generation start position's format.
pub const ADOPTION_POSITION_FORMAT: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position<A> {
    snapshot: Mark<A>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Mark<A> {
    format: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snapshot_chain: Option<String>,
    generation: u64,
    anchor: A,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdoptionPosition {
    snapshot_adopted: Adopted,
}

/// A sink's start of a generation of a snapshot chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Adopted {
    pub format: u32,
    pub snapshot_chain: String,
    pub generation: u64,
    pub replaced_digest: String,
}

/// The incomplete-snapshot position of `generation` with its `anchor`
/// (format 1, no chain).
pub fn encode<A: Serialize>(generation: u64, anchor: A) -> Vec<u8> {
    serde_json::to_vec(&Position {
        snapshot: Mark {
            format: SNAPSHOT_POSITION_FORMAT,
            snapshot_chain: None,
            generation,
            anchor,
        },
    })
    .expect("a snapshot position always serializes")
}

/// The incomplete-snapshot position of `generation` of `snapshot_chain`
/// with its `anchor` (format 2).
pub fn encode_chained<A: Serialize>(
    snapshot_chain: &str,
    generation: u64,
    anchor: A,
) -> Vec<u8> {
    serde_json::to_vec(&Position {
        snapshot: Mark {
            format: SNAPSHOT_POSITION_FORMAT_CHAINED,
            snapshot_chain: Some(snapshot_chain.to_string()),
            generation,
            anchor,
        },
    })
    .expect("a snapshot position always serializes")
}

/// The start position of `generation` of `snapshot_chain`, replacing the
/// state with digest `replaced_digest`.
pub fn encode_adopted(
    snapshot_chain: &str,
    generation: u64,
    replaced_digest: &str,
) -> Vec<u8> {
    serde_json::to_vec(&AdoptionPosition {
        snapshot_adopted: Adopted {
            format: ADOPTION_POSITION_FORMAT,
            snapshot_chain: snapshot_chain.to_string(),
            generation,
            replaced_digest: replaced_digest.to_string(),
        },
    })
    .expect("an adoption position always serializes")
}

/// An incomplete-snapshot position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incomplete<A> {
    /// `None`: written before chains (format 1, or an engine's bare legacy
    /// form).
    pub snapshot_chain: Option<String>,
    /// `None` only for an engine's bare legacy form, which records none.
    pub generation: Option<u64>,
    pub anchor: A,
}

/// What stored checkpoint bytes are, as far as snapshots go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Classified<A> {
    Incomplete(Incomplete<A>),
    Adopted(Adopted),
    /// Not a snapshot or adoption position: the engine's stream position (or
    /// whatever the engine's own parsing makes of it).
    Stream,
}

/// Classify stored bytes. `Err`: a snapshot or adoption position of a format
/// this release does not know, or malformed - never taken for anything else.
pub fn classify<A: DeserializeOwned>(
    raw: &[u8],
) -> Result<Classified<A>, String> {
    let Ok(serde_json::Value::Object(map)) =
        serde_json::from_slice::<serde_json::Value>(raw)
    else {
        return Ok(Classified::Stream);
    };
    if let Some(adopted) = map.get("snapshot_adopted") {
        let format = adopted.get("format").and_then(serde_json::Value::as_u64);
        if format != Some(u64::from(ADOPTION_POSITION_FORMAT)) {
            return Err(format!(
                "adoption position of format {format:?}, which this release \
                 does not know"
            ));
        }
        let p: AdoptionPosition = serde_json::from_slice(raw)
            .map_err(|e| format!("malformed adoption position: {e}"))?;
        return Ok(Classified::Adopted(p.snapshot_adopted));
    }
    let Some(mark) = map.get("snapshot") else {
        return Ok(Classified::Stream);
    };
    let format = mark.get("format").and_then(serde_json::Value::as_u64);
    let p: Position<A> = match format {
        Some(f)
            if f == u64::from(SNAPSHOT_POSITION_FORMAT)
                || f == u64::from(SNAPSHOT_POSITION_FORMAT_CHAINED) =>
        {
            serde_json::from_slice(raw)
                .map_err(|e| format!("malformed snapshot position: {e}"))?
        }
        _ => {
            return Err(format!(
                "snapshot position of format {format:?}, which this release \
                 does not know"
            ));
        }
    };
    let chained = p.snapshot.format == SNAPSHOT_POSITION_FORMAT_CHAINED;
    if chained != p.snapshot.snapshot_chain.is_some() {
        return Err("malformed snapshot position: format and chain disagree"
            .to_string());
    }
    Ok(Classified::Incomplete(Incomplete {
        snapshot_chain: p.snapshot.snapshot_chain,
        generation: Some(p.snapshot.generation),
        anchor: p.snapshot.anchor,
    }))
}

/// `Ok(None)`: not a snapshot position. `Ok(Some((generation, anchor)))`: an
/// incomplete-snapshot position of either format. `Err`: one of another
/// format or malformed.
pub fn decode<A: DeserializeOwned>(
    raw: &[u8],
) -> Result<Option<(u64, A)>, String> {
    match classify::<A>(raw)? {
        Classified::Incomplete(i) => Ok(Some((
            i.generation
                .expect("a decoded position records its generation"),
            i.anchor,
        ))),
        Classified::Adopted(_) | Classified::Stream => Ok(None),
    }
}

/// What an engine supplies to order its snapshot and stream positions.
pub trait EngineOrder {
    /// The engine's anchor type.
    type Anchor: DeserializeOwned + PartialEq;

    /// Order two stream positions.
    fn stream_order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder;

    /// The order of `anchor`, as a stream position, against the stream
    /// position `stream` (`Before`: the stream position is strictly after it).
    fn anchor_vs_stream(
        &self,
        anchor: &Self::Anchor,
        stream: &[u8],
    ) -> CheckpointOrder;

    /// The snapshot completion mark a stream position carries:
    /// `(snapshot_chain, generation)`, the chain absent on marks written
    /// before chains.
    fn completion_mark(&self, stream: &[u8]) -> Option<(Option<String>, u64)>;

    /// An engine's bare legacy snapshot form (PostgreSQL: the anchor LSN as
    /// text), as an incomplete position without chain or generation.
    fn bare_legacy(&self, _raw: &[u8]) -> Option<Self::Anchor> {
        None
    }

    /// The source lineage a stream position records, when the engine's
    /// checkpoints record one (MySQL: the server lineage hash).
    fn stream_lineage(&self, _stream: &[u8]) -> Option<String> {
        None
    }
}

/// Classify stored bytes with the engine's bare legacy form (`None`: an
/// unknown format or malformed).
pub fn classify_stored<E: EngineOrder>(
    engine: &E,
    raw: &[u8],
) -> Option<Classified<E::Anchor>> {
    classify_with(engine, raw).ok()
}

fn classify_with<E: EngineOrder>(
    engine: &E,
    raw: &[u8],
) -> Result<Classified<E::Anchor>, ()> {
    match classify::<E::Anchor>(raw).map_err(|_| ())? {
        Classified::Stream => Ok(match engine.bare_legacy(raw) {
            Some(anchor) => Classified::Incomplete(Incomplete {
                snapshot_chain: None,
                generation: None,
                anchor,
            }),
            None => Classified::Stream,
        }),
        other => Ok(other),
    }
}

fn flip(o: CheckpointOrder) -> CheckpointOrder {
    match o {
        CheckpointOrder::Before => CheckpointOrder::After,
        CheckpointOrder::After => CheckpointOrder::Before,
        other => other,
    }
}

/// The order of two stored checkpoints (design section 3.5). Fail closed:
/// anything not proven ordered is `Incomparable`.
pub fn order<E: EngineOrder>(
    engine: &E,
    a: &[u8],
    b: &[u8],
) -> CheckpointOrder {
    use CheckpointOrder::*;
    use Classified::*;
    let (Ok(ca), Ok(cb)) = (classify_with(engine, a), classify_with(engine, b))
    else {
        tracing::warn!(
            "incomparable checkpoints: unreadable snapshot position"
        );
        return Incomparable;
    };
    match (ca, cb) {
        (Stream, Stream) => engine.stream_order(a, b),
        (Incomplete(x), Incomplete(y)) => incomplete_pair(&x, &y),
        (Adopted(x), Adopted(y)) => {
            if x.snapshot_chain == y.snapshot_chain {
                scalar(x.generation, y.generation)
            } else {
                Incomparable
            }
        }
        (Adopted(d), Incomplete(i)) => {
            match (&i.snapshot_chain, i.generation) {
                (Some(c), Some(g)) => start_vs(&d, c, g),
                _ => Incomparable,
            }
        }
        (Incomplete(i), Adopted(d)) => {
            match (&i.snapshot_chain, i.generation) {
                (Some(c), Some(g)) => flip(start_vs(&d, c, g)),
                _ => Incomparable,
            }
        }
        (Adopted(d), Stream) => match engine.completion_mark(b) {
            Some((Some(c), g)) => start_vs(&d, &c, g),
            _ => Incomparable,
        },
        (Stream, Adopted(d)) => match engine.completion_mark(a) {
            Some((Some(c), g)) => flip(start_vs(&d, &c, g)),
            _ => Incomparable,
        },
        (Incomplete(i), Stream) => incomplete_vs_stream(engine, &i, b),
        (Stream, Incomplete(i)) => flip(incomplete_vs_stream(engine, &i, a)),
    }
}

fn incomplete_pair<A: PartialEq>(
    x: &Incomplete<A>,
    y: &Incomplete<A>,
) -> CheckpointOrder {
    use CheckpointOrder::*;
    match (&x.snapshot_chain, &y.snapshot_chain) {
        (None, None) => {
            if x == y {
                Equal
            } else {
                Incomparable
            }
        }
        (Some(cx), Some(cy)) if cx == cy => {
            match x.generation.cmp(&y.generation) {
                std::cmp::Ordering::Less => Before,
                std::cmp::Ordering::Greater => After,
                // One generation has one anchor.
                std::cmp::Ordering::Equal if x.anchor == y.anchor => Equal,
                std::cmp::Ordering::Equal => Incomparable,
            }
        }
        _ => Incomparable,
    }
}

fn scalar(a: u64, b: u64) -> CheckpointOrder {
    match a.cmp(&b) {
        std::cmp::Ordering::Less => CheckpointOrder::Before,
        std::cmp::Ordering::Equal => CheckpointOrder::Equal,
        std::cmp::Ordering::Greater => CheckpointOrder::After,
    }
}

/// A generation start against a position of generation `g` of chain `c`.
fn start_vs(d: &Adopted, c: &str, g: u64) -> CheckpointOrder {
    if d.snapshot_chain != c {
        CheckpointOrder::Incomparable
    } else if g >= d.generation {
        CheckpointOrder::Before
    } else {
        CheckpointOrder::After
    }
}

pub use crate::durable_checkpoint::StartDecision;

/// The digest a start position records of the state it replaced (`empty`
/// for none).
pub fn replaced_digest(prev: Option<&[u8]>) -> String {
    use sha2::{Digest, Sha256};
    match prev {
        None => "empty".to_string(),
        Some(b) => hex::encode(Sha256::digest(b)),
    }
}

/// An engine's whole start step on a stored checkpoint: the local check,
/// and on `Move` the start checkpoint binding the replaced state's digest.
pub fn checkpoint_start<E: EngineOrder>(
    engine: &E,
    prev: Option<&[u8]>,
    lineage: Option<&str>,
    start: &deltaforge_core::GenerationStart,
) -> deltaforge_core::CheckpointStart {
    use deltaforge_core::CheckpointStart;
    match generation_start(
        engine,
        prev,
        lineage,
        &start.snapshot_chain,
        start.generation,
        start.legacy_through,
    ) {
        StartDecision::Move => CheckpointStart::Move(
            deltaforge_core::CheckpointMeta::from_vec(encode_adopted(
                &start.snapshot_chain,
                start.generation,
                &replaced_digest(prev),
            )),
        ),
        StartDecision::Already => CheckpointStart::Already,
        StartDecision::Refuse => CheckpointStart::Refuse,
    }
}

/// The generation start barrier's local check on a sink's stored checkpoint
/// (design section 5.4): `prev` is the exact checkpoint the sink holds
/// (`None`: empty). Accepted previous states: empty; a legacy (chain-less)
/// snapshot position up to `legacy_through`; any position of chain `chain`
/// below `generation`; a stream position of `lineage` (checked where the
/// engine's checkpoints record a lineage).
pub fn generation_start<E: EngineOrder>(
    engine: &E,
    prev: Option<&[u8]>,
    lineage: Option<&str>,
    chain: &str,
    generation: u64,
    legacy_through: Option<u64>,
) -> StartDecision {
    use StartDecision::*;
    let Some(prev) = prev else {
        return Move;
    };
    // A later generation of the chain means the caller's control state is
    // stale, rewound or corrupt: never acknowledged.
    let in_chain = |c: &str, g: u64| {
        if c != chain {
            Refuse
        } else {
            match g.cmp(&generation) {
                std::cmp::Ordering::Less => Move,
                std::cmp::Ordering::Equal => Already,
                std::cmp::Ordering::Greater => Refuse,
            }
        }
    };
    let adopted_legacy = |g: Option<u64>| match (g, legacy_through) {
        (_, None) => Refuse,
        (None, Some(_)) => Move,
        (Some(g), Some(k)) if g <= k => Move,
        _ => Refuse,
    };
    match classify_with(engine, prev) {
        Err(()) => Refuse,
        Ok(Classified::Adopted(d)) => in_chain(&d.snapshot_chain, d.generation),
        Ok(Classified::Incomplete(i)) => {
            match (&i.snapshot_chain, i.generation) {
                (Some(c), Some(g)) => in_chain(c, g),
                (Some(_), None) => Refuse,
                (None, g) => adopted_legacy(g),
            }
        }
        Ok(Classified::Stream) => {
            if let (Some(want), Some(have)) =
                (lineage, engine.stream_lineage(prev))
                && want != have
            {
                return Refuse;
            }
            match engine.completion_mark(prev) {
                Some((Some(c), g)) => in_chain(&c, g),
                // A completion written before chains, or a plain stream
                // position: the lineage's stream.
                Some((None, _)) | None => Move,
            }
        }
    }
}

fn incomplete_vs_stream<E: EngineOrder>(
    engine: &E,
    i: &Incomplete<E::Anchor>,
    stream: &[u8],
) -> CheckpointOrder {
    use CheckpointOrder::*;
    let rel = engine.anchor_vs_stream(&i.anchor, stream);
    let Some(chain) = &i.snapshot_chain else {
        // Before chains (#131): ordered before a stream position at or
        // after its anchor.
        return if matches!(rel, Before | Equal) {
            Before
        } else {
            Incomparable
        };
    };
    let generation = i.generation.unwrap_or_default();
    match engine.completion_mark(stream) {
        Some((Some(c), g)) if c == *chain => match g.cmp(&generation) {
            std::cmp::Ordering::Greater => Before,
            std::cmp::Ordering::Less => After,
            std::cmp::Ordering::Equal if rel == Equal => Before,
            std::cmp::Ordering::Equal => Incomparable,
        },
        // Another chain's completion, or a chain-less (legacy) one.
        Some(_) => Incomparable,
        None if rel == Before => Before,
        None => Incomparable,
    }
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

#[cfg(test)]
mod order_tests {
    use super::*;
    use CheckpointOrder::*;

    /// A test engine: anchors are numbers; a stream position is
    /// `{"p": n, "mark": [chain or null, generation]?}`.
    struct T;

    fn stream(p: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "p": p })).unwrap()
    }

    fn marked(p: u64, chain: Option<&str>, g: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "p": p, "mark": [chain, g] }))
            .unwrap()
    }

    fn p(raw: &[u8]) -> Option<u64> {
        serde_json::from_slice::<serde_json::Value>(raw).ok()?["p"].as_u64()
    }

    impl EngineOrder for T {
        type Anchor = u64;
        fn stream_order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
            match (p(a), p(b)) {
                (Some(a), Some(b)) => match a.cmp(&b) {
                    std::cmp::Ordering::Less => Before,
                    std::cmp::Ordering::Equal => Equal,
                    std::cmp::Ordering::Greater => After,
                },
                _ => Incomparable,
            }
        }
        fn anchor_vs_stream(&self, anchor: &u64, s: &[u8]) -> CheckpointOrder {
            self.stream_order(&stream(*anchor), s)
        }
        fn completion_mark(&self, s: &[u8]) -> Option<(Option<String>, u64)> {
            let v: serde_json::Value = serde_json::from_slice(s).ok()?;
            let m = v.get("mark")?;
            Some((m[0].as_str().map(str::to_string), m[1].as_u64()?))
        }
    }

    fn o(a: &[u8], b: &[u8]) -> CheckpointOrder {
        let ab = order(&T, a, b);
        assert_eq!(order(&T, b, a), flip(ab), "antisymmetric");
        ab
    }

    #[test]
    fn a_chain_orders_its_generations_and_never_another_chain() {
        let g3 = encode_chained("c", 3, 10u64);
        assert_eq!(o(&g3, &encode_chained("c", 3, 10u64)), Equal);
        assert_eq!(o(&g3, &encode_chained("c", 4, 20u64)), Before);
        assert_eq!(o(&g3, &encode_chained("c", 3, 11u64)), Incomparable);
        assert_eq!(o(&g3, &encode_chained("d", 4, 20u64)), Incomparable);
        // A chain-less (#131) position never orders against a chain.
        assert_eq!(o(&g3, &encode(2, 5u64)), Incomparable);
        assert_eq!(o(&encode(2, 5u64), &encode(2, 5u64)), Equal);
        assert_eq!(o(&encode(2, 5u64), &encode(3, 5u64)), Incomparable);
    }

    #[test]
    fn a_generation_start_precedes_its_generation_and_later_ones() {
        let d = encode_adopted("c", 4, "digest");
        assert_eq!(o(&d, &encode_chained("c", 4, 10u64)), Before);
        assert_eq!(o(&d, &encode_chained("c", 5, 10u64)), Before);
        assert_eq!(o(&d, &encode_chained("c", 3, 10u64)), After);
        assert_eq!(o(&d, &marked(10, Some("c"), 4)), Before);
        assert_eq!(o(&d, &marked(10, Some("c"), 3)), After);
        assert_eq!(o(&d, &encode_chained("d", 5, 10u64)), Incomparable);
        assert_eq!(o(&d, &encode(5, 10u64)), Incomparable);
        assert_eq!(o(&d, &encode_adopted("c", 4, "other")), Equal);
        assert_eq!(o(&d, &encode_adopted("c", 5, "digest")), Before);
        // Never ordered against a plain stream position or another chain's
        // completion: entering a generation is a checked move.
        assert_eq!(o(&d, &stream(99)), Incomparable);
        assert_eq!(o(&d, &marked(10, Some("d"), 9)), Incomparable);
    }

    fn start(
        prev: Option<&[u8]>,
        generation: u64,
        legacy_through: Option<u64>,
    ) -> StartDecision {
        generation_start(&T, prev, None, "c", generation, legacy_through)
    }

    /// Completed `g`, then CDC, then a re-snapshot `g+1`: the sink moves from
    /// its CDC checkpoint into `g+1`, whose positions order after the start.
    #[test]
    fn a_resnapshot_after_cdc_starts_from_the_cdc_checkpoint() {
        assert_eq!(start(Some(&stream(500)), 4, None), StartDecision::Move);
        assert_eq!(
            start(Some(&marked(10, Some("c"), 3)), 4, None),
            StartDecision::Move
        );
        let d = encode_adopted("c", 4, "digest");
        assert_eq!(o(&d, &encode_chained("c", 4, 600u64)), Before);
    }

    #[test]
    fn a_generation_start_is_a_checked_local_move() {
        use StartDecision::*;
        assert_eq!(start(None, 4, None), Move);
        assert_eq!(start(Some(&encode_chained("c", 3, 10u64)), 4, None), Move);
        // A partial start of 3 (a crash), then 3 replaced by 4.
        assert_eq!(start(Some(&encode_adopted("c", 3, "d")), 4, None), Move);
        assert_eq!(start(Some(&encode_adopted("c", 4, "d")), 4, None), Already);
        assert_eq!(
            start(Some(&encode_chained("c", 4, 10u64)), 4, None),
            Already
        );
        assert_eq!(start(Some(&marked(10, Some("c"), 4)), 4, None), Already);
        // A sink in a later generation than the one starting: the caller's
        // control state went backwards; refused.
        assert_eq!(
            start(Some(&encode_chained("c", 5, 10u64)), 4, None),
            Refuse
        );
        assert_eq!(start(Some(&encode_adopted("c", 5, "d")), 4, None), Refuse);
        assert_eq!(start(Some(&marked(10, Some("c"), 5)), 4, None), Refuse);
        // Another chain, an unadopted legacy position, an unknown format.
        assert_eq!(
            start(Some(&encode_chained("d", 3, 10u64)), 4, None),
            Refuse
        );
        assert_eq!(start(Some(&encode_adopted("d", 3, "x")), 4, None), Refuse);
        assert_eq!(start(Some(&marked(10, Some("d"), 3)), 4, None), Refuse);
        assert_eq!(start(Some(&encode(3, 10u64)), 4, None), Refuse);
        assert_eq!(start(Some(&encode(3, 10u64)), 4, Some(3)), Move);
        assert_eq!(start(Some(&encode(5, 10u64)), 6, Some(3)), Refuse);
        let unknown = br#"{"snapshot":{"format":9,"generation":1,"anchor":1}}"#;
        assert_eq!(start(Some(unknown), 4, None), Refuse);
    }

    /// A stream position of a foreign lineage is refused where the engine's
    /// checkpoints record the lineage.
    #[test]
    fn a_foreign_lineage_stream_position_is_refused() {
        struct WithLineage;
        impl EngineOrder for WithLineage {
            type Anchor = u64;
            fn stream_order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
                T.stream_order(a, b)
            }
            fn anchor_vs_stream(&self, a: &u64, s: &[u8]) -> CheckpointOrder {
                T.anchor_vs_stream(a, s)
            }
            fn completion_mark(
                &self,
                s: &[u8],
            ) -> Option<(Option<String>, u64)> {
                T.completion_mark(s)
            }
            fn stream_lineage(&self, s: &[u8]) -> Option<String> {
                let v: serde_json::Value = serde_json::from_slice(s).ok()?;
                v["lineage"].as_str().map(str::to_string)
            }
        }
        let decide = |prev: &[u8]| {
            generation_start(
                &WithLineage,
                Some(prev),
                Some("mine"),
                "c",
                4,
                None,
            )
        };
        assert_eq!(
            decide(br#"{"p":5,"lineage":"other"}"#),
            StartDecision::Refuse
        );
        assert_eq!(decide(br#"{"p":5,"lineage":"mine"}"#), StartDecision::Move);
    }

    #[test]
    fn a_chained_position_orders_against_completions_and_streams() {
        let i = encode_chained("c", 3, 10u64);
        // Its own completion, exactly at the anchor.
        assert_eq!(o(&i, &marked(10, Some("c"), 3)), Before);
        assert_eq!(o(&i, &marked(11, Some("c"), 3)), Incomparable);
        // Completions of other generations of the chain.
        assert_eq!(o(&i, &marked(20, Some("c"), 4)), Before);
        assert_eq!(o(&i, &marked(5, Some("c"), 2)), After);
        // Another chain's, or a chain-less, completion.
        assert_eq!(o(&i, &marked(10, Some("d"), 3)), Incomparable);
        assert_eq!(o(&i, &marked(10, None, 3)), Incomparable);
        // Unmarked stream positions: only strictly after the anchor.
        assert_eq!(o(&i, &stream(11)), Before);
        assert_eq!(o(&i, &stream(10)), Incomparable);
        assert_eq!(o(&i, &stream(9)), Incomparable);
    }

    #[test]
    fn a_chainless_position_keeps_its_131_order() {
        let i = encode(3, 10u64);
        assert_eq!(o(&i, &stream(10)), Before);
        assert_eq!(o(&i, &stream(11)), Before);
        assert_eq!(o(&i, &stream(9)), Incomparable);
    }

    #[test]
    fn unknown_or_inconsistent_positions_are_incomparable() {
        let i = encode_chained("c", 3, 10u64);
        for bad in [
            br#"{"snapshot":{"format":3,"generation":1,"anchor":1}}"#.to_vec(),
            br#"{"snapshot":{"format":2,"generation":1,"anchor":1}}"#.to_vec(),
            br#"{"snapshot":{"format":1,"snapshot_chain":"c","generation":1,"anchor":1}}"#.to_vec(),
            br#"{"snapshot_adopted":{"format":2,"snapshot_chain":"c","generation":1,"replaced_digest":"d"}}"#.to_vec(),
        ] {
            assert!(classify::<u64>(&bad).is_err());
            assert_eq!(order(&T, &i, &bad), Incomparable);
        }
    }
}
