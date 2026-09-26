//! Deterministic, key-sorted JSON serialization.
//!
//! Produces compact JSON with object keys sorted lexicographically at every level
//! (arrays keep their order), so the bytes never depend on `serde_json`'s map key
//! ordering (which the `preserve_order` feature can flip). Used wherever content must
//! hash the same regardless of key order - the durable S3 encoder and the event-replay
//! envelope both rely on it. Kept in `deltaforge-core` so no higher-level crate has to
//! be pulled in as a dependency just to canonicalize.

use serde_json::Value;

/// Canonical bytes for a JSON value: compact, with object keys sorted at every level.
pub fn canonical_json_bytes(v: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    write_canonical_json(&mut buf, v);
    buf
}

/// Write `v` as compact JSON with object keys sorted lexicographically at every level
/// (arrays keep their order). Matches `serde_json`'s compact spacing, so the only
/// difference from `to_writer` is the guaranteed key order.
pub fn write_canonical_json(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Object(map) => {
            buf.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                serde_json::to_writer(&mut *buf, k)
                    .expect("string key serializes");
                buf.push(b':');
                write_canonical_json(buf, &map[*k]);
            }
            buf.push(b'}');
        }
        Value::Array(arr) => {
            buf.push(b'[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                write_canonical_json(buf, item);
            }
            buf.push(b']');
        }
        // Scalars (null/bool/number/string) have a single canonical compact form.
        other => {
            serde_json::to_writer(&mut *buf, other).expect("scalar serializes")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keys_sorted_recursively_regardless_of_insertion_order() {
        let a = json!({"b": 1, "a": {"y": 2, "x": 3}});
        let b = json!({"a": {"x": 3, "y": 2}, "b": 1});
        assert_eq!(canonical_json_bytes(&a), canonical_json_bytes(&b));
        assert_eq!(
            String::from_utf8(canonical_json_bytes(&a)).unwrap(),
            r#"{"a":{"x":3,"y":2},"b":1}"#
        );
    }

    #[test]
    fn arrays_keep_order() {
        let v = json!([3, 1, 2]);
        assert_eq!(
            String::from_utf8(canonical_json_bytes(&v)).unwrap(),
            "[3,1,2]"
        );
    }
}
