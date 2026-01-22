//! JSON encoding - standard JSON serialization.

use bytes::Bytes;
use serde::Serialize;

use super::EncodingError;

/// JSON encoding - serializes to UTF-8 JSON bytes.
#[derive(Debug, Clone, Copy, Default)]
pub struct Json;

impl Json {
    pub const fn name(&self) -> &'static str {
        "json"
    }

    pub const fn content_type(&self) -> &'static str {
        "application/json"
    }

    #[inline]
    pub fn encode<T: Serialize>(
        &self,
        value: &T,
    ) -> Result<Bytes, EncodingError> {
        let bytes = serde_json::to_vec(value)?;
        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_encodes_value() {
        let encoding = Json;
        let value = json!({"id": 1, "name": "Alice"});

        let bytes = encoding.encode(&value).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(parsed["name"], "Alice");
    }
}
