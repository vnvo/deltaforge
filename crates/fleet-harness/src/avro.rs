//! Confluent-framed Avro events, decoded to the JSON the verifier reads.
//!
//! A framed record is a zero magic byte, the writer schema's registry id
//! (4 bytes, big-endian) and the Avro datum. The writer schema is fetched
//! from the registry once per id.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use apache_avro::Schema;
use serde_json::Value;

pub struct Registry {
    url: String,
    http: reqwest::Client,
    schemas: HashMap<u32, Schema>,
}

impl Registry {
    pub fn new(url: &str) -> Result<Registry> {
        Ok(Registry {
            url: url.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            schemas: HashMap::new(),
        })
    }

    /// Whether `payload` is a framed Avro record.
    pub fn is_framed(payload: &[u8]) -> bool {
        payload.len() >= 5 && payload[0] == 0
    }

    /// The JSON value of a framed Avro record.
    pub async fn decode(&mut self, payload: &[u8]) -> Result<Value> {
        if !Self::is_framed(payload) {
            bail!("not a framed Avro record");
        }
        let id = u32::from_be_bytes([
            payload[1], payload[2], payload[3], payload[4],
        ]);
        if !self.schemas.contains_key(&id) {
            let schema = self.fetch(id).await?;
            self.schemas.insert(id, schema);
        }
        decode_datum(&self.schemas[&id], &payload[5..])
    }

    async fn fetch(&self, id: u32) -> Result<Schema> {
        let url = format!("{}/schemas/ids/{id}", self.url);
        let body: Value = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()
            .with_context(|| format!("GET {url}"))?
            .json()
            .await
            .with_context(|| format!("GET {url}: body"))?;
        let text = body["schema"]
            .as_str()
            .with_context(|| format!("GET {url}: no schema"))?;
        Schema::parse_str(text).with_context(|| format!("schema {id}"))
    }
}

/// One Avro datum of `schema` as JSON.
pub fn decode_datum(schema: &Schema, datum: &[u8]) -> Result<Value> {
    let mut reader = datum;
    let v = apache_avro::from_avro_datum(schema, &mut reader, None)
        .context("Avro datum")?;
    if !reader.is_empty() {
        bail!("{} bytes after the Avro datum", reader.len());
    }
    Value::try_from(v).context("Avro value as JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Record;
    use serde_json::json;

    #[test]
    fn a_datum_decodes_to_the_json_the_verifier_reads() {
        let schema = Schema::parse_str(
            r#"{"type":"record","name":"Envelope","fields":[
                {"name":"after","type":["null",{"type":"record","name":"V",
                  "fields":[{"name":"id","type":"long"},
                            {"name":"committed_at","type":"string"}]}]},
                {"name":"op","type":"string"}]}"#,
        )
        .unwrap();
        let mut r = Record::new(&schema).unwrap();
        let after = apache_avro::types::Value::Record(vec![
            ("id".into(), apache_avro::types::Value::Long(7)),
            (
                "committed_at".into(),
                apache_avro::types::Value::String(
                    "2026-11-22 10:00:00.5".into(),
                ),
            ),
        ]);
        r.put(
            "after",
            apache_avro::types::Value::Union(1, Box::new(after)),
        );
        r.put("op", "c");
        let bytes = apache_avro::to_avro_datum(&schema, r).unwrap();
        assert_eq!(
            decode_datum(&schema, &bytes).unwrap(),
            json!({"after": {"id": 7, "committed_at": "2026-11-22 10:00:00.5"}, "op": "c"})
        );
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(decode_datum(&schema, &extra).is_err());
        assert!(!Registry::is_framed(b"{\"op\":\"c\"}"));
    }
}
