//! The lossless durable form of canonical values.
//!
//! Used wherever an event is kept to be read again (the replay journal, the
//! DLQ, durable S3 objects): every value is a tagged JSON object that reads
//! back to the identical [`CanonicalValue`]. 64-bit integers are decimal
//! text, floats their IEEE bit pattern in hex, bytes base64, documents their
//! canonical JSON text. SQL NULL is JSON `null`; every other value is an
//! object, so nothing else can be mistaken for it.
//!
//! This is also the serde form of [`CanonicalValue`] and [`CanonicalRow`]:
//! the native JSON rendering is explicit, through [`super::render`].

use base64::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value, json};

use super::{
    ArrayValue, Bits, CanonicalError, CanonicalRow, CanonicalValue, CivilDate,
    DateTimeValue, DateValue, DbValue, Decimal, Document, EnumValue, IntWidth,
    JsonText, OpaqueText, Precision, SetValue, SourceType, TimeValue,
    TimestampValue,
};

/// A durable value that does not read back as a canonical value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed durable canonical value: {0}")]
pub struct DurableError(pub String);

impl From<CanonicalError> for DurableError {
    fn from(e: CanonicalError) -> Self {
        DurableError(e.0)
    }
}

fn bad<T>(msg: impl Into<String>) -> Result<T, DurableError> {
    Err(DurableError(msg.into()))
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

fn precision_json(p: Precision) -> Value {
    match p {
        Precision::Fixed(d) => json!(d),
        Precision::Variable => json!("var"),
    }
}

fn date_fields(m: &mut Map<String, Value>, d: &CivilDate) {
    m.insert("y".into(), json!(d.year()));
    m.insert("m".into(), json!(d.month()));
    m.insert("d".into(), json!(d.day()));
}

fn tagged(tag: &str, fields: Value) -> Value {
    let mut m = match fields {
        Value::Object(m) => m,
        _ => Map::new(),
    };
    m.insert("t".into(), json!(tag));
    Value::Object(m)
}

/// A value's durable form.
pub fn value_to_durable(v: &CanonicalValue) -> Value {
    match v {
        CanonicalValue::Null => Value::Null,
        CanonicalValue::Unchanged => tagged("unchanged", json!({})),
        CanonicalValue::Db(db) => db_to_durable(db),
        CanonicalValue::Opaque(o) => {
            tagged("opaque", json!({ "s": o.source_type, "v": o.text }))
        }
        CanonicalValue::Document(d) => {
            tagged("doc", json!({ "v": d.canonical_text() }))
        }
        CanonicalValue::Bytes(b) => {
            tagged("bytes", json!({ "v": BASE64_STANDARD.encode(b) }))
        }
    }
}

fn db_to_durable(v: &DbValue) -> Value {
    match v {
        DbValue::Int { value, width } => {
            tagged("int", json!({ "w": width.bits(), "v": value.to_string() }))
        }
        DbValue::UInt { value, width } => {
            tagged("uint", json!({ "w": width.bits(), "v": value.to_string() }))
        }
        DbValue::Decimal(d) => {
            tagged("dec", json!({ "v": super::render::decimal_text(d) }))
        }
        DbValue::Float32(f) => {
            tagged("f32", json!({ "v": format!("{:08x}", f.to_bits()) }))
        }
        DbValue::Float64(f) => {
            tagged("f64", json!({ "v": format!("{:016x}", f.to_bits()) }))
        }
        DbValue::Bool(b) => tagged("bool", json!({ "v": b })),
        DbValue::Text(s) => tagged("text", json!({ "v": s })),
        DbValue::Binary(b) => {
            tagged("bin", json!({ "v": BASE64_STANDARD.encode(b) }))
        }
        DbValue::Bits(b) => tagged(
            "bits",
            json!({ "n": b.len(), "v": BASE64_STANDARD.encode(b.bytes()) }),
        ),
        DbValue::Enum(e) => {
            tagged("enum", json!({ "v": e.label, "i": e.index }))
        }
        DbValue::Set(s) => tagged(
            "set",
            json!({ "b": s.bitmask().to_string(), "v": s.members() }),
        ),
        DbValue::Json(j) => tagged("json", json!({ "v": j.as_str() })),
        DbValue::Date(d) => match d {
            DateValue::Civil(c) => {
                let mut m = Map::new();
                date_fields(&mut m, c);
                tagged("date", Value::Object(m))
            }
            DateValue::PosInfinity => tagged("date", json!({ "inf": 1 })),
            DateValue::NegInfinity => tagged("date", json!({ "inf": -1 })),
        },
        DbValue::Time(t) => tagged(
            "time",
            json!({
                "us": t.micros().to_string(),
                "p": precision_json(t.precision()),
            }),
        ),
        DbValue::DateTime(dt) => match dt {
            DateTimeValue::Civil {
                date,
                micros_of_day,
                precision,
            } => {
                let mut m = Map::new();
                date_fields(&mut m, date);
                m.insert("us".into(), json!(micros_of_day.to_string()));
                m.insert("p".into(), precision_json(*precision));
                tagged("dt", Value::Object(m))
            }
            DateTimeValue::PosInfinity => tagged("dt", json!({ "inf": 1 })),
            DateTimeValue::NegInfinity => tagged("dt", json!({ "inf": -1 })),
        },
        DbValue::Timestamp(ts) => match ts {
            TimestampValue::Instant { micros, precision } => tagged(
                "ts",
                json!({
                    "us": micros.to_string(),
                    "p": precision_json(*precision),
                }),
            ),
            TimestampValue::Zero { precision } => tagged(
                "ts",
                json!({ "zero": true, "p": precision_json(*precision) }),
            ),
            TimestampValue::PosInfinity => tagged("ts", json!({ "inf": 1 })),
            TimestampValue::NegInfinity => tagged("ts", json!({ "inf": -1 })),
        },
        DbValue::Year(y) => tagged("year", json!({ "v": y })),
        DbValue::Uuid(u) => tagged("uuid", json!({ "v": hex::encode(u) })),
        DbValue::Array(a) => tagged(
            "array",
            json!({
                "e": a.element_type(),
                "v": a.items().iter().map(value_to_durable).collect::<Vec<_>>(),
            }),
        ),
    }
}

/// A row's durable form: `{"c": [[name, value], ...]}` in column order,
/// with `"x": {name: canonical JSON text}` when it has extensions.
pub fn row_to_durable(row: &CanonicalRow) -> Value {
    let mut m = Map::new();
    m.insert(
        "c".into(),
        Value::Array(
            row.iter()
                .map(|(n, v)| json!([n, value_to_durable(v)]))
                .collect(),
        ),
    );
    if !row.extensions().is_empty() {
        m.insert(
            "x".into(),
            Value::Object(
                row.extensions()
                    .iter()
                    .map(|(n, d)| {
                        (n.clone(), Value::String(d.canonical_text()))
                    })
                    .collect(),
            ),
        );
    }
    Value::Object(m)
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

struct Fields<'a> {
    tag: &'a str,
    m: &'a Map<String, Value>,
}

impl<'a> Fields<'a> {
    fn get(&self, k: &str) -> Result<&'a Value, DurableError> {
        match self.m.get(k) {
            Some(v) => Ok(v),
            None => bad(format!("{} without `{k}`", self.tag)),
        }
    }

    fn str(&self, k: &str) -> Result<&'a str, DurableError> {
        match self.get(k)?.as_str() {
            Some(s) => Ok(s),
            None => bad(format!("{}.{k} is not a string", self.tag)),
        }
    }

    fn u64(&self, k: &str) -> Result<u64, DurableError> {
        match self.get(k)?.as_u64() {
            Some(n) => Ok(n),
            None => bad(format!("{}.{k} is not an unsigned integer", self.tag)),
        }
    }

    fn i64(&self, k: &str) -> Result<i64, DurableError> {
        match self.get(k)?.as_i64() {
            Some(n) => Ok(n),
            None => bad(format!("{}.{k} is not an integer", self.tag)),
        }
    }

    /// A decimal-text integer.
    fn parse<T: std::str::FromStr>(&self, k: &str) -> Result<T, DurableError> {
        self.str(k)?
            .parse()
            .or_else(|_| bad(format!("{}.{k} is not decimal text", self.tag)))
    }

    fn bytes(&self, k: &str) -> Result<Vec<u8>, DurableError> {
        BASE64_STANDARD
            .decode(self.str(k)?)
            .or_else(|_| bad(format!("{}.{k} is not base64", self.tag)))
    }

    fn width(&self) -> Result<IntWidth, DurableError> {
        let bits = self.u64("w")?;
        u32::try_from(bits)
            .ok()
            .and_then(IntWidth::from_bits)
            .map_or_else(|| bad(format!("integer width {bits}")), Ok)
    }

    fn precision(&self) -> Result<Precision, DurableError> {
        match self.get("p")? {
            Value::String(s) if s == "var" => Ok(Precision::Variable),
            v => match v.as_u64().and_then(|d| u8::try_from(d).ok()) {
                Some(d) => Ok(Precision::fixed(d)?),
                None => bad(format!("{}.p is not a precision", self.tag)),
            },
        }
    }

    /// `Some(+1/-1)` for an infinity.
    fn infinity(&self) -> Result<Option<i64>, DurableError> {
        match self.m.get("inf") {
            None => Ok(None),
            Some(_) => match self.i64("inf")? {
                s @ (1 | -1) => Ok(Some(s)),
                _ => bad(format!("{}.inf is not 1 or -1", self.tag)),
            },
        }
    }

    fn date(&self) -> Result<CivilDate, DurableError> {
        let y = i32::try_from(self.i64("y")?).or_else(|_| bad("year range"))?;
        let m = u8::try_from(self.u64("m")?).or_else(|_| bad("month range"))?;
        let d = u8::try_from(self.u64("d")?).or_else(|_| bad("day range"))?;
        Ok(CivilDate::mysql(y, m, d)?)
    }
}

/// Read a value's durable form.
pub fn value_from_durable(v: &Value) -> Result<CanonicalValue, DurableError> {
    let m = match v {
        Value::Null => return Ok(CanonicalValue::Null),
        Value::Object(m) => m,
        _ => return bad("a value is neither null nor a tagged object"),
    };
    let tag = match m.get("t").and_then(Value::as_str) {
        Some(t) => t,
        None => return bad("a value without a tag"),
    };
    let f = Fields { tag, m };
    let db = |v: DbValue| Ok(CanonicalValue::Db(v));
    match tag {
        "unchanged" => Ok(CanonicalValue::Unchanged),
        "opaque" => {
            let source_type: SourceType =
                serde_json::from_value(f.get("s")?.clone())
                    .or_else(|e| bad(format!("opaque.s: {e}")))?;
            Ok(CanonicalValue::Opaque(OpaqueText {
                source_type,
                text: f.str("v")?.to_string(),
            }))
        }
        "doc" => {
            let value: Value = serde_json::from_str(f.str("v")?)
                .or_else(|_| bad("doc.v is not JSON"))?;
            Ok(CanonicalValue::Document(Document::new(value)))
        }
        "bytes" => Ok(CanonicalValue::Bytes(f.bytes("v")?)),
        "int" => db(DbValue::int(f.parse("v")?, f.width()?)?),
        "uint" => db(DbValue::uint(f.parse("v")?, f.width()?)?),
        "dec" => db(DbValue::Decimal(match f.str("v")? {
            "NaN" => Decimal::NaN,
            "Infinity" => Decimal::PosInfinity,
            "-Infinity" => Decimal::NegInfinity,
            s => {
                let d = Decimal::parse(s)?;
                if super::render::decimal_text(&d) != s {
                    return bad("dec.v is not canonical decimal text");
                }
                d
            }
        })),
        "f32" => {
            let bits = u32::from_str_radix(f.str("v")?, 16)
                .or_else(|_| bad("f32.v is not hex bits"))?;
            db(DbValue::Float32(f32::from_bits(bits)))
        }
        "f64" => {
            let bits = u64::from_str_radix(f.str("v")?, 16)
                .or_else(|_| bad("f64.v is not hex bits"))?;
            db(DbValue::Float64(f64::from_bits(bits)))
        }
        "bool" => match f.get("v")?.as_bool() {
            Some(b) => db(DbValue::Bool(b)),
            None => bad("bool.v is not a boolean"),
        },
        "text" => db(DbValue::Text(f.str("v")?.to_string())),
        "bin" => db(DbValue::Binary(f.bytes("v")?)),
        "bits" => {
            let n =
                u8::try_from(f.u64("n")?).or_else(|_| bad("bits.n range"))?;
            db(DbValue::Bits(Bits::new(n, f.bytes("v")?)?))
        }
        "enum" => {
            let index =
                u16::try_from(f.u64("i")?).or_else(|_| bad("enum.i range"))?;
            db(DbValue::Enum(EnumValue::mysql(
                index,
                f.str("v")?.to_string(),
            )?))
        }
        "set" => {
            let members = match f.get("v")?.as_array() {
                Some(a) => a
                    .iter()
                    .map(|m| m.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>(),
                None => None,
            };
            let Some(members) = members else {
                return bad("set.v is not a list of labels");
            };
            db(DbValue::Set(SetValue::from_parts(f.parse("b")?, members)?))
        }
        "json" => db(DbValue::Json(JsonText::new(f.str("v")?.to_string())?)),
        "date" => db(DbValue::Date(match f.infinity()? {
            Some(1) => DateValue::PosInfinity,
            Some(_) => DateValue::NegInfinity,
            None => DateValue::Civil(f.date()?),
        })),
        "time" => {
            let micros: i64 = f.parse("us")?;
            let p = f.precision()?;
            db(DbValue::Time(
                TimeValue::mysql(micros, p)
                    .or_else(|_| TimeValue::time_of_day(micros, p))?,
            ))
        }
        "dt" => db(DbValue::DateTime(match f.infinity()? {
            Some(1) => DateTimeValue::PosInfinity,
            Some(_) => DateTimeValue::NegInfinity,
            None => {
                DateTimeValue::civil(f.date()?, f.parse("us")?, f.precision()?)?
            }
        })),
        "ts" => db(DbValue::Timestamp(match f.infinity()? {
            Some(1) => TimestampValue::PosInfinity,
            Some(_) => TimestampValue::NegInfinity,
            None if m.contains_key("zero") => match f.get("zero")?.as_bool() {
                Some(true) => TimestampValue::Zero {
                    precision: f.precision()?,
                },
                _ => return bad("ts.zero is not true"),
            },
            None => TimestampValue::instant(f.parse("us")?, f.precision()?)?,
        })),
        "year" => {
            let y =
                u16::try_from(f.u64("v")?).or_else(|_| bad("year range"))?;
            db(DbValue::year(y)?)
        }
        "uuid" => {
            let bytes = hex::decode(f.str("v")?)
                .or_else(|_| bad("uuid.v is not hex"))?;
            match <[u8; 16]>::try_from(bytes) {
                Ok(u) => db(DbValue::Uuid(u)),
                Err(_) => bad("uuid.v is not 16 bytes"),
            }
        }
        "array" => {
            let element_type: SourceType =
                serde_json::from_value(f.get("e")?.clone())
                    .or_else(|e| bad(format!("array.e: {e}")))?;
            let Some(items) = f.get("v")?.as_array() else {
                return bad("array.v is not a list");
            };
            let items = items
                .iter()
                .map(value_from_durable)
                .collect::<Result<Vec<_>, _>>()?;
            db(DbValue::Array(ArrayValue::new(element_type, items)?))
        }
        other => bad(format!("unknown tag {other}")),
    }
}

/// Read a row's durable form.
pub fn row_from_durable(v: &Value) -> Result<CanonicalRow, DurableError> {
    let Some(m) = v.as_object() else {
        return bad("a row is not an object");
    };
    if m.keys().any(|k| k != "c" && k != "x") {
        return bad("a row has an unknown part");
    }
    let Some(pairs) = m.get("c").and_then(Value::as_array) else {
        return bad("a row without its columns");
    };
    let mut columns = Vec::with_capacity(pairs.len());
    for pair in pairs {
        match pair.as_array().map(Vec::as_slice) {
            Some([Value::String(name), value]) => {
                columns.push((name.clone(), value_from_durable(value)?));
            }
            _ => return bad("a row entry is not [name, value]"),
        }
    }
    let mut extensions = Vec::new();
    match m.get("x") {
        None => {}
        Some(Value::Object(x)) if !x.is_empty() => {
            for (name, text) in x {
                let Some(text) = text.as_str() else {
                    return bad("an extension is not JSON text");
                };
                let doc: Value = serde_json::from_str(text)
                    .or_else(|_| bad("an extension is not JSON text"))?;
                extensions.push((name.clone(), Document::new(doc)));
            }
        }
        Some(_) => return bad("a row's extensions are not a non-empty object"),
    }
    Ok(CanonicalRow::with_extensions(columns, extensions)?)
}

// ---------------------------------------------------------------------------
// serde
// ---------------------------------------------------------------------------

impl Serialize for CanonicalValue {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        value_to_durable(self).serialize(s)
    }
}

impl<'de> Deserialize<'de> for CanonicalValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        value_from_durable(&v).map_err(serde::de::Error::custom)
    }
}

impl Serialize for CanonicalRow {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        row_to_durable(self).serialize(s)
    }
}

impl<'de> Deserialize<'de> for CanonicalRow {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        row_from_durable(&v).map_err(serde::de::Error::custom)
    }
}
