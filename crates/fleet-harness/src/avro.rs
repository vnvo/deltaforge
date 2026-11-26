//! Confluent-framed Avro events, decoded to the JSON the verifier reads.
//!
//! A framed record is a zero magic byte, the writer schema's registry id
//! (4 bytes, big-endian) and the Avro datum. The writer schema is fetched
//! from the registry once per id, several ids at a time, and compiled once
//! into a [`Decoder`].

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use apache_avro::Schema;
use apache_avro::schema::{Name, Namespace, ResolvedSchema};
use serde_json::{Map, Number, Value};
use tokio::task::JoinSet;

/// Schema fetches in flight at once.
pub const FETCHES: usize = 16;
/// Attempts per schema fetch before the id is failed.
const ATTEMPTS: u32 = 3;

/// The writer schema id of a framed Avro record.
pub fn frame_id(payload: &[u8]) -> Option<u32> {
    (payload.len() >= 5 && payload[0] == 0).then(|| {
        u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]])
    })
}

/// What the registry holds for one schema id.
pub enum Slot<'a> {
    Ready(&'a Arc<Decoder>),
    Failed(&'a str),
    /// Being fetched, or waiting for a fetch slot.
    Pending,
    /// Never requested.
    Unknown,
}

pub struct Registry {
    url: String,
    http: reqwest::Client,
    ready: HashMap<u32, Arc<Decoder>>,
    failed: HashMap<u32, String>,
    pending: HashSet<u32>,
    waiting: VecDeque<u32>,
    fetches: JoinSet<(u32, Result<Decoder>)>,
    /// Cross-check every n-th decode (see [`Decoder::decode_crosschecked`]).
    crosscheck_every: Option<u64>,
    decoded: std::cell::Cell<u64>,
}

impl Registry {
    pub fn new(url: &str, crosscheck_every: Option<u64>) -> Result<Registry> {
        Ok(Registry {
            url: url.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            ready: HashMap::new(),
            failed: HashMap::new(),
            pending: HashSet::new(),
            waiting: VecDeque::new(),
            fetches: JoinSet::new(),
            crosscheck_every: crosscheck_every.filter(|&n| n > 0),
            decoded: std::cell::Cell::new(0),
        })
    }

    pub fn slot(&self, id: u32) -> Slot<'_> {
        if let Some(d) = self.ready.get(&id) {
            Slot::Ready(d)
        } else if let Some(e) = self.failed.get(&id) {
            Slot::Failed(e)
        } else if self.pending.contains(&id) {
            Slot::Pending
        } else {
            Slot::Unknown
        }
    }

    /// Decode one datum of `d`. With cross-checks asked, the first datum
    /// of each schema and every n-th datum are cross-checked.
    pub fn decode(&self, d: &Decoder, datum: &[u8]) -> Result<Value> {
        let Some(every) = self.crosscheck_every else {
            return d.decode(datum);
        };
        let n = self.decoded.get();
        self.decoded.set(n + 1);
        let first = !d.checked.swap(true, std::sync::atomic::Ordering::Relaxed);
        if first || n.is_multiple_of(every) {
            d.decode_crosschecked(datum)
        } else {
            d.decode(datum)
        }
    }

    /// Fetch `id` unless it is held, failed or already requested.
    pub fn request(&mut self, id: u32) {
        if matches!(self.slot(id), Slot::Unknown) {
            self.pending.insert(id);
            self.waiting.push_back(id);
            self.start();
        }
    }

    /// Whether a requested schema has not arrived yet.
    pub fn in_flight(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Wait for one requested schema to arrive (or fail).
    pub async fn arrive(&mut self) {
        let Some(joined) = self.fetches.join_next().await else {
            return;
        };
        let (id, result) = match joined {
            Ok(r) => r,
            Err(e) => panic!("schema fetch task: {e}"),
        };
        self.pending.remove(&id);
        match result {
            Ok(d) => {
                if !d.compiled() {
                    eprintln!(
                        "  verifier: writer schema {id} has a type the compiled \
                         decoder does not cover; decoded through apache-avro"
                    );
                }
                self.ready.insert(id, Arc::new(d));
            }
            Err(e) => {
                self.failed.insert(id, format!("{e:#}"));
            }
        }
        self.start();
    }

    fn start(&mut self) {
        while self.fetches.len() < FETCHES {
            let Some(id) = self.waiting.pop_front() else {
                break;
            };
            let (http, url) = (self.http.clone(), self.url.clone());
            self.fetches.spawn(async move {
                let mut last = anyhow!("no attempt");
                for attempt in 0..ATTEMPTS {
                    if attempt > 0 {
                        tokio::time::sleep(Duration::from_millis(
                            200 << attempt,
                        ))
                        .await;
                    }
                    match fetch(&http, &url, id).await {
                        Ok(d) => return (id, Ok(d)),
                        Err(e) => last = e,
                    }
                }
                (id, Err(last))
            });
        }
    }
}

async fn fetch(http: &reqwest::Client, base: &str, id: u32) -> Result<Decoder> {
    let url = format!("{base}/schemas/ids/{id}");
    let body: Value = http
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
    let schema =
        Schema::parse_str(text).with_context(|| format!("schema {id}"))?;
    Decoder::new(schema).with_context(|| format!("schema {id}"))
}

/// One writer schema, compiled once: its datums decode straight to JSON,
/// or, for a schema with a type the compiled form does not cover, through
/// apache-avro.
pub struct Decoder {
    schema: Schema,
    plan: Option<Vec<Node>>,
    /// Whether a datum of this schema has been cross-checked.
    checked: std::sync::atomic::AtomicBool,
}

/// A compiled schema node; children are indices into the plan.
enum Node {
    Null,
    Boolean,
    /// `int`, `date`, `time-millis`.
    Int,
    /// `long` and the other long-based logical types.
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(usize),
    Map(usize),
    Union(Vec<usize>),
    Record(Vec<(String, usize)>),
    Enum(Vec<String>),
    Fixed(usize),
}

impl Decoder {
    pub fn new(schema: Schema) -> Result<Decoder> {
        let plan = {
            let resolved = ResolvedSchema::try_from(&schema)
                .context("resolve the schema's names")?;
            let mut c = Compiler {
                names: resolved.get_names(),
                nodes: Vec::new(),
                named: HashMap::new(),
            };
            c.node(&schema, &None).map(|_| c.nodes)
        };
        Ok(Decoder {
            schema,
            plan,
            checked: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Whether datums decode through the compiled form.
    pub fn compiled(&self) -> bool {
        self.plan.is_some()
    }

    /// [`Decoder::decode`], and the same datum through apache-avro: a
    /// difference is an error.
    pub fn decode_crosschecked(&self, datum: &[u8]) -> Result<Value> {
        let v = self.decode(datum)?;
        if self.plan.is_some() {
            let oracle = decode_datum(&self.schema, datum)
                .context("cross-check through apache-avro")?;
            if v != oracle {
                bail!(
                    "compiled decode {v} differs from apache-avro's {oracle}"
                );
            }
        }
        Ok(v)
    }

    /// One Avro datum as JSON: the value apache-avro decodes, converted
    /// to JSON. Bytes after the datum are an error.
    pub fn decode(&self, datum: &[u8]) -> Result<Value> {
        let Some(plan) = &self.plan else {
            return decode_datum(&self.schema, datum);
        };
        let mut r = Reader { b: datum };
        let v = r.value(plan, 0).context("Avro datum")?;
        if !r.b.is_empty() {
            bail!("{} bytes after the Avro datum", r.b.len());
        }
        Ok(v)
    }
}

struct Compiler<'s> {
    names: &'s HashMap<Name, &'s Schema>,
    nodes: Vec<Node>,
    /// Named types already compiled, by full name.
    named: HashMap<Name, usize>,
}

impl Compiler<'_> {
    /// Compile `s` (in `ns`) and return its index, or `None` for a type
    /// the compiled form does not cover.
    fn node(&mut self, s: &Schema, ns: &Namespace) -> Option<usize> {
        let leaf = match s {
            Schema::Null => Some(Node::Null),
            Schema::Boolean => Some(Node::Boolean),
            Schema::Int | Schema::Date | Schema::TimeMillis => Some(Node::Int),
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => Some(Node::Long),
            Schema::Float => Some(Node::Float),
            Schema::Double => Some(Node::Double),
            Schema::Bytes => Some(Node::Bytes),
            Schema::String => Some(Node::String),
            _ => None,
        };
        if let Some(n) = leaf {
            return Some(self.push(n));
        }
        match s {
            Schema::Array(a) => {
                let at = self.push(Node::Null);
                let items = self.node(&a.items, ns)?;
                self.nodes[at] = Node::Array(items);
                Some(at)
            }
            Schema::Map(m) => {
                let at = self.push(Node::Null);
                let values = self.node(&m.types, ns)?;
                self.nodes[at] = Node::Map(values);
                Some(at)
            }
            Schema::Union(u) => {
                let at = self.push(Node::Null);
                let mut vs = Vec::with_capacity(u.variants().len());
                for v in u.variants() {
                    vs.push(self.node(v, ns)?);
                }
                self.nodes[at] = Node::Union(vs);
                Some(at)
            }
            Schema::Record(r) => {
                let full = r.name.fully_qualified_name(ns);
                // Registered before its fields, so a recursive reference
                // resolves to it.
                let at = self.push(Node::Null);
                self.named.insert(full.clone(), at);
                let mut fields = Vec::with_capacity(r.fields.len());
                for f in &r.fields {
                    fields.push((
                        f.name.clone(),
                        self.node(&f.schema, &full.namespace)?,
                    ));
                }
                self.nodes[at] = Node::Record(fields);
                Some(at)
            }
            Schema::Enum(e) => {
                let full = e.name.fully_qualified_name(ns);
                let at = self.push(Node::Enum(e.symbols.clone()));
                self.named.insert(full, at);
                Some(at)
            }
            Schema::Fixed(f) => {
                let full = f.name.fully_qualified_name(ns);
                let at = self.push(Node::Fixed(f.size));
                self.named.insert(full, at);
                Some(at)
            }
            Schema::Ref { name } => {
                let full = name.fully_qualified_name(ns);
                if let Some(&at) = self.named.get(&full) {
                    return Some(at);
                }
                let s = *self.names.get(&full)?;
                self.node(s, &full.namespace)
            }
            // Decimal, big-decimal, uuid and duration: through apache-avro.
            _ => None,
        }
    }

    fn push(&mut self, n: Node) -> usize {
        self.nodes.push(n);
        self.nodes.len() - 1
    }
}

struct Reader<'a> {
    b: &'a [u8],
}

impl Reader<'_> {
    fn long(&mut self) -> Result<i64> {
        let mut z: u64 = 0;
        for (i, &byte) in self.b.iter().enumerate().take(10) {
            z |= u64::from(byte & 0x7f) << (7 * i);
            if byte & 0x80 == 0 {
                self.b = &self.b[i + 1..];
                return Ok(if z & 1 == 0 {
                    (z >> 1) as i64
                } else {
                    !(z >> 1) as i64
                });
            }
        }
        bail!("variable-length integer truncated or longer than 10 bytes")
    }

    fn len(&mut self) -> Result<usize> {
        let n = self.long()?;
        usize::try_from(n).map_err(|_| anyhow!("negative length {n}"))
    }

    fn take(&mut self, n: usize) -> Result<&[u8]> {
        if n > self.b.len() {
            bail!("{n} bytes wanted, {} left", self.b.len());
        }
        let (head, rest) = self.b.split_at(n);
        self.b = rest;
        Ok(head)
    }

    /// The item count of the next block of an array or map (0 at its end).
    fn block(&mut self) -> Result<usize> {
        let n = self.long()?;
        if n < 0 {
            self.long()?; // the block's size in bytes
            return usize::try_from(n.unsigned_abs())
                .map_err(|_| anyhow!("block of {n} items"));
        }
        Ok(n as usize)
    }

    fn float(f: f64) -> Result<Value> {
        Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(|| anyhow!("{f} has no JSON form"))
    }

    fn value(&mut self, plan: &[Node], at: usize) -> Result<Value> {
        Ok(match &plan[at] {
            Node::Null => Value::Null,
            Node::Boolean => match self.take(1)?[0] {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                b => bail!("boolean byte {b}"),
            },
            Node::Int => {
                let n = self.long()?;
                Value::from(i32::try_from(n).map_err(|_| anyhow!("int {n}"))?)
            }
            Node::Long => Value::from(self.long()?),
            Node::Float => {
                let b = self.take(4)?;
                Self::float(
                    f32::from_le_bytes([b[0], b[1], b[2], b[3]]).into(),
                )?
            }
            Node::Double => {
                let b: [u8; 8] = self.take(8)?.try_into().expect("8 bytes");
                Self::float(f64::from_le_bytes(b))?
            }
            Node::Bytes => {
                let n = self.len()?;
                Value::Array(self.take(n)?.iter().map(|&b| b.into()).collect())
            }
            Node::String => {
                let n = self.len()?;
                Value::String(
                    std::str::from_utf8(self.take(n)?)
                        .context("string is not UTF-8")?
                        .to_owned(),
                )
            }
            Node::Fixed(n) => {
                Value::Array(self.take(*n)?.iter().map(|&b| b.into()).collect())
            }
            Node::Enum(symbols) => {
                let i = self.long()?;
                let s = usize::try_from(i)
                    .ok()
                    .and_then(|i| symbols.get(i))
                    .ok_or_else(|| anyhow!("enum index {i}"))?;
                Value::String(s.clone())
            }
            Node::Union(variants) => {
                let i = self.long()?;
                let &v = usize::try_from(i)
                    .ok()
                    .and_then(|i| variants.get(i))
                    .ok_or_else(|| anyhow!("union index {i}"))?;
                self.value(plan, v)?
            }
            Node::Array(items) => {
                let mut out = Vec::new();
                loop {
                    let n = self.block()?;
                    if n == 0 {
                        break;
                    }
                    out.reserve(n.min(self.b.len()));
                    for _ in 0..n {
                        out.push(self.value(plan, *items)?);
                    }
                }
                Value::Array(out)
            }
            Node::Map(values) => {
                let mut out = Map::new();
                loop {
                    let n = self.block()?;
                    if n == 0 {
                        break;
                    }
                    for _ in 0..n {
                        let k = self.len()?;
                        let key = std::str::from_utf8(self.take(k)?)
                            .context("map key is not UTF-8")?
                            .to_owned();
                        let v = self.value(plan, *values)?;
                        out.insert(key, v);
                    }
                }
                Value::Object(out)
            }
            Node::Record(fields) => {
                let mut out = Map::new();
                for (name, f) in fields {
                    let v = self.value(plan, *f)?;
                    out.insert(name.clone(), v);
                }
                Value::Object(out)
            }
        })
    }
}

/// One Avro datum of `schema` as JSON, through apache-avro.
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
    use apache_avro::types::{Record, Value as Avro};
    use serde_json::json;

    fn compiled(schema: &str) -> (Schema, Decoder) {
        let schema = Schema::parse_str(schema).unwrap();
        let d = Decoder::new(schema.clone()).unwrap();
        (schema, d)
    }

    /// The compiled decoder agrees with apache-avro on `value`.
    fn agrees(schema: &Schema, d: &Decoder, value: Avro) -> Value {
        let bytes = apache_avro::to_avro_datum(schema, value).unwrap();
        let fast = d.decode(&bytes).unwrap();
        assert_eq!(fast, decode_datum(schema, &bytes).unwrap());
        fast
    }

    #[test]
    fn a_datum_decodes_to_the_json_the_verifier_reads() {
        let (schema, d) = compiled(
            r#"{"type":"record","name":"Envelope","fields":[
                {"name":"after","type":["null",{"type":"record","name":"V",
                  "fields":[{"name":"id","type":"long"},
                            {"name":"committed_at","type":"string"}]}]},
                {"name":"op","type":"string"}]}"#,
        );
        assert!(d.compiled());
        let mut r = Record::new(&schema).unwrap();
        let after = Avro::Record(vec![
            ("id".into(), Avro::Long(7)),
            (
                "committed_at".into(),
                Avro::String("2026-11-22 10:00:00.5".into()),
            ),
        ]);
        r.put("after", Avro::Union(1, Box::new(after)));
        r.put("op", "c");
        let bytes = apache_avro::to_avro_datum(&schema, r).unwrap();
        assert_eq!(
            d.decode(&bytes).unwrap(),
            json!({"after": {"id": 7, "committed_at": "2026-11-22 10:00:00.5"}, "op": "c"})
        );
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(d.decode(&extra).is_err());
        assert!(d.decode(&bytes[..bytes.len() - 1]).is_err());
        assert_eq!(frame_id(b"{\"op\":\"c\"}"), None);
        assert_eq!(frame_id(&[0, 0, 0, 1, 2, 9]), Some(258));
    }

    #[test]
    fn every_compiled_type_decodes_as_apache_avro_does() {
        let (schema, d) = compiled(
            r#"{"type":"record","name":"All","namespace":"n","fields":[
                {"name":"b","type":"boolean"},
                {"name":"i","type":"int"},
                {"name":"l","type":"long"},
                {"name":"f","type":"float"},
                {"name":"d","type":"double"},
                {"name":"y","type":"bytes"},
                {"name":"s","type":"string"},
                {"name":"dt","type":{"type":"int","logicalType":"date"}},
                {"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}},
                {"name":"tm","type":{"type":"long","logicalType":"time-micros"}},
                {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
                {"name":"x","type":{"type":"fixed","name":"F","size":3}},
                {"name":"a","type":{"type":"array","items":"long"}},
                {"name":"m","type":{"type":"map","values":["null","string"]}},
                {"name":"e2","type":"E"},
                {"name":"inner","type":["null",{"type":"record","name":"In",
                   "namespace":"o","fields":[{"name":"x2","type":"n.F"}]}]},
                {"name":"inner2","type":"o.In"}]}"#,
        );
        assert!(d.compiled());
        let rec = |i: i32, l: i64, many: bool| {
            let n = if many { 300 } else { 0 };
            Avro::Record(vec![
                ("b".into(), Avro::Boolean(many)),
                ("i".into(), Avro::Int(i)),
                ("l".into(), Avro::Long(l)),
                ("f".into(), Avro::Float(1.5)),
                ("d".into(), Avro::Double(-2.25e300)),
                ("y".into(), Avro::Bytes(vec![0, 255, 7])),
                ("s".into(), Avro::String("zß€".repeat(n / 30 + 1))),
                ("dt".into(), Avro::Date(i)),
                ("ts".into(), Avro::TimestampMicros(l)),
                ("tm".into(), Avro::TimeMicros(l.max(0))),
                ("e".into(), Avro::Enum(1, "B".into())),
                ("x".into(), Avro::Fixed(3, vec![1, 2, 3])),
                (
                    "a".into(),
                    Avro::Array((0..n as i64).map(Avro::Long).collect()),
                ),
                (
                    "m".into(),
                    Avro::Map(
                        (0..n.min(40))
                            .map(|k| {
                                let v = if k % 2 == 0 {
                                    Avro::Union(0, Box::new(Avro::Null))
                                } else {
                                    Avro::Union(
                                        1,
                                        Box::new(Avro::String(k.to_string())),
                                    )
                                };
                                (format!("k{k}"), v)
                            })
                            .collect(),
                    ),
                ),
                ("e2".into(), Avro::Enum(0, "A".into())),
                (
                    "inner".into(),
                    Avro::Union(
                        1,
                        Box::new(Avro::Record(vec![(
                            "x2".into(),
                            Avro::Fixed(3, vec![9, 8, 7]),
                        )])),
                    ),
                ),
                (
                    "inner2".into(),
                    Avro::Record(vec![(
                        "x2".into(),
                        Avro::Fixed(3, vec![0; 3]),
                    )]),
                ),
            ])
        };
        for (i, l) in
            [(0, 0), (i32::MIN, i64::MIN), (i32::MAX, i64::MAX), (-1, 63)]
        {
            agrees(&schema, &d, rec(i, l, false));
            agrees(&schema, &d, rec(i, l, true));
        }
    }

    #[test]
    fn a_recursive_schema_compiles_and_decodes() {
        let (schema, d) = compiled(
            r#"{"type":"record","name":"Node","fields":[
                {"name":"v","type":"long"},
                {"name":"next","type":["null","Node"]}]}"#,
        );
        assert!(d.compiled());
        let list = Avro::Record(vec![
            ("v".into(), Avro::Long(1)),
            (
                "next".into(),
                Avro::Union(
                    1,
                    Box::new(Avro::Record(vec![
                        ("v".into(), Avro::Long(2)),
                        ("next".into(), Avro::Union(0, Box::new(Avro::Null))),
                    ])),
                ),
            ),
        ]);
        assert_eq!(
            agrees(&schema, &d, list),
            json!({"v": 1, "next": {"v": 2, "next": null}})
        );
    }

    #[test]
    fn a_schema_with_a_decimal_decodes_through_apache_avro() {
        let (schema, d) = compiled(
            r#"{"type":"record","name":"R","fields":[
                {"name":"p","type":{"type":"bytes","logicalType":"decimal",
                  "precision":10,"scale":2}}]}"#,
        );
        assert!(!d.compiled());
        agrees(
            &schema,
            &d,
            Avro::Record(vec![(
                "p".into(),
                Avro::Decimal(apache_avro::Decimal::from(vec![1u8, 2])),
            )]),
        );
    }

    /// A registry serving `ids` (others 404), counting the most requests
    /// served at once.
    async fn serve(
        ids: std::ops::RangeInclusive<u32>,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (now, most) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (n, m) = (now.clone(), most.clone());
        let app = axum::Router::new().route(
            "/schemas/ids/{id}",
            axum::routing::get(move |axum::extract::Path(id): axum::extract::Path<u32>| {
                let (n, m, ids) = (n.clone(), m.clone(), ids.clone());
                async move {
                    m.fetch_max(n.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    n.fetch_sub(1, Ordering::SeqCst);
                    if !ids.contains(&id) {
                        return Err(axum::http::StatusCode::NOT_FOUND);
                    }
                    let schema = format!(
                        r#"{{"type":"record","name":"T{id}","fields":[{{"name":"v","type":"long"}}]}}"#
                    );
                    Ok(axum::Json(json!({ "schema": schema })))
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        (url, most)
    }

    #[tokio::test]
    async fn schemas_are_fetched_several_at_a_time_and_a_missing_one_fails() {
        let (url, most) = serve(1..=40).await;
        let mut r = Registry::new(&url, Some(1)).unwrap();
        for id in (1..=40).chain([1, 2, 99]) {
            r.request(id);
        }
        assert!(matches!(r.slot(7), Slot::Pending));
        while r.in_flight() {
            r.arrive().await;
        }
        let most = most.load(std::sync::atomic::Ordering::SeqCst);
        assert!((2..=FETCHES).contains(&most), "{most} at once");
        for id in 1..=40 {
            let Slot::Ready(d) = r.slot(id) else {
                panic!("{id} not ready")
            };
            assert!(d.compiled());
            // zigzag 5 = -3
            assert_eq!(r.decode(d, &[5]).unwrap(), json!({"v": -3}));
        }
        assert!(matches!(r.slot(99), Slot::Failed(e) if e.contains("404")));
        assert!(matches!(r.slot(100), Slot::Unknown));
    }

    #[test]
    fn malformed_datums_are_errors() {
        let (_, d) = compiled(
            r#"{"type":"record","name":"R","fields":[
                {"name":"i","type":"int"},{"name":"b","type":"boolean"},
                {"name":"u","type":["null","long"]}]}"#,
        );
        // int out of range (zigzag of 2^31)
        assert!(d.decode(&[0x80, 0x80, 0x80, 0x80, 0x10, 0, 0]).is_err());
        // boolean byte 2
        assert!(d.decode(&[0, 2, 0]).is_err());
        // union index 5
        assert!(d.decode(&[0, 1, 10]).is_err());
        // unterminated varint
        assert!(d.decode(&[0xff; 12]).is_err());
        assert_eq!(
            d.decode(&[2, 1, 2, 4]).unwrap(),
            json!({"i": 1, "b": true, "u": 2})
        );
    }
}
