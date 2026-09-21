//! Stable **processor identity digests** — the `processor_digest` component of a
//! synthetic [`EventId`](deltaforge_core::EventId).
//!
//! The digest must be deterministic and sensitive to anything that changes a
//! processor's output: for JavaScript, the exact source bytes (whitespace
//! included) plus its canonical config; for built-ins, the processor kind, an
//! explicit implementation version, and its canonical config. Config is
//! canonicalized **structurally** (recursively sorted keys) so it never depends
//! on incidental serialization order. Digests are computed **once at
//! construction** and stored, never recomputed per event.

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Domain separator between the JS source bytes and its config in the hash.
const JS_SEP: &[u8] = b"\0df.js.config\0";

/// Serialize a JSON value to a canonical byte form with **recursively sorted**
/// object keys, so structurally-equal configs always hash identically.
fn canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"n"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"t" } else { b"f" }),
        Value::Number(n) => {
            out.push(b'#');
            out.extend_from_slice(n.to_string().as_bytes());
            out.push(b';');
        }
        Value::String(s) => {
            out.push(b'"');
            out.extend_from_slice(&(s.len() as u64).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Array(items) => {
            out.push(b'[');
            out.extend_from_slice(&(items.len() as u64).to_be_bytes());
            for item in items {
                canonical(item, out);
            }
        }
        Value::Object(map) => {
            out.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.extend_from_slice(&(keys.len() as u64).to_be_bytes());
            for k in keys {
                out.extend_from_slice(&(k.len() as u64).to_be_bytes());
                out.extend_from_slice(k.as_bytes());
                canonical(&map[k], out);
            }
        }
    }
}

fn canonical_config<C: Serialize>(config: &C) -> Vec<u8> {
    let value = serde_json::to_value(config).unwrap_or(Value::Null);
    let mut buf = Vec::new();
    canonical(&value, &mut buf);
    buf
}

/// Digest for a built-in processor: `"<kind>:v<impl_version>:<hex sha256 of
/// canonical config>"`. Bump `impl_version` whenever a code change alters the
/// processor's output for identical config.
pub fn builtin_digest<C: Serialize>(
    kind: &str,
    impl_version: u32,
    config: &C,
) -> String {
    let mut h = Sha256::new();
    h.update(canonical_config(config));
    format!("{kind}:v{impl_version}:{}", hex::encode(h.finalize()))
}

/// Digest for the JavaScript processor: `"js:<hex sha256 of exact source bytes
/// || sep || canonical config>"`. The exact source bytes (whitespace included)
/// are hashed so any source change yields a new identity.
pub fn js_digest<C: Serialize>(source: &str, config: &C) -> String {
    let mut h = Sha256::new();
    h.update(source.as_bytes());
    h.update(JS_SEP);
    h.update(canonical_config(config));
    format!("js:{}", hex::encode(h.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_is_key_order_independent() {
        let a = json!({"b": 1, "a": {"y": 2, "x": 3}});
        let b = json!({"a": {"x": 3, "y": 2}, "b": 1});
        let (mut ba, mut bb) = (Vec::new(), Vec::new());
        canonical(&a, &mut ba);
        canonical(&b, &mut bb);
        assert_eq!(ba, bb);
    }

    #[test]
    fn canonical_distinguishes_structure() {
        // {"a":"bc"} vs {"ab":"c"} must not collide (length-prefixing).
        let (mut x, mut y) = (Vec::new(), Vec::new());
        canonical(&json!({"a": "bc"}), &mut x);
        canonical(&json!({"ab": "c"}), &mut y);
        assert_ne!(x, y);
        // string "1" vs number 1 differ.
        let (mut s, mut n) = (Vec::new(), Vec::new());
        canonical(&json!("1"), &mut s);
        canonical(&json!(1), &mut n);
        assert_ne!(s, n);
    }

    #[test]
    fn js_digest_sensitive_to_source_and_config() {
        let cfg = json!({"cpu_ms": 100});
        let base = js_digest("return events;", &cfg);
        // Whitespace change → different digest (exact bytes).
        assert_ne!(base, js_digest("return events; ", &cfg));
        // Config change → different digest.
        assert_ne!(base, js_digest("return events;", &json!({"cpu_ms": 200})));
        // Stable.
        assert_eq!(base, js_digest("return events;", &cfg));
    }

    #[test]
    fn builtin_digest_sensitive_to_kind_version_config() {
        let cfg = json!({"prefix": "outbox_"});
        let base = builtin_digest("outbox", 1, &cfg);
        assert_ne!(base, builtin_digest("flatten", 1, &cfg));
        assert_ne!(base, builtin_digest("outbox", 2, &cfg));
        assert_ne!(base, builtin_digest("outbox", 1, &json!({"prefix": "x"})));
        assert_eq!(base, builtin_digest("outbox", 1, &cfg));
        assert!(base.starts_with("outbox:v1:"));
    }
}
