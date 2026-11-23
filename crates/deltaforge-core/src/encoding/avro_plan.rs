//! Compact compiled Avro schemas ("plans") and the outputs conversion writes
//! to.
//!
//! A registered schema is parsed once by apache-avro (its validation kept)
//! and compiled into a [`Plan`]: the types encoding needs, the field names
//! and declared source types conversion reads, and named references
//! resolved. The parsed schema is then dropped. A plan is a few KB for a
//! table of a dozen columns where the parsed schema and an apache-avro
//! writer are about 270 KB.
//!
//! Conversion (in [`super::avro`]) reads a plan and writes to an [`Out`]:
//! [`BytesOut`] writes the Avro binary encoding directly (DDL-derived
//! schemas, Path A); [`ValueOut`] builds the apache-avro value its writer
//! encodes (inferred and DDL-event schemas, and the differential tests).
//! The two outputs share every conversion rule.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::Name;
use apache_avro::types::Value as AvroValue;

use super::EncodingError;

/// The `deltaforge.source_type` a DDL-derived schema records on a field.
pub(super) const SOURCE_TYPE_ATTR: &str = "deltaforge.source_type";

/// A record whose every input field must be defined by the schema.
pub(super) const STRICT_ATTR: &str = "deltaforge.strict";

/// A compiled schema node.
#[derive(Debug, Clone)]
pub(super) enum Plan {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Uuid,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    LocalTimestampMillis,
    LocalTimestampMicros,
    Fixed(usize),
    Enum(Arc<[Box<str>]>),
    Union(Box<[Plan]>),
    Array(Box<Plan>),
    Map(Box<Plan>),
    Record(Arc<RecordPlan>),
    /// A named reference, resolved: conversion treats it as the schema's
    /// reference node (its failures name a "named type"), then its target.
    Ref(Box<Plan>),
    /// A reference to an enclosing record (a recursive schema): weak, so a
    /// plan holds no reference cycle.
    Back(Weak<RecordPlan>),
    /// A reference whose name resolves to no named type.
    Unresolved,
    /// A type the converter refuses (decimal, big-decimal, duration): its
    /// name for failures.
    Unsupported(&'static str),
    /// A type no value converts to (the nanosecond timestamps): its name.
    Unmatched(&'static str),
}

/// A compiled record.
#[derive(Debug)]
pub(super) struct RecordPlan {
    pub(super) fields: Box<[FieldPlan]>,
    /// Field indices by field name, for [`Self::defines`].
    by_name: Box<[u32]>,
    /// The fields' aliases, sorted (rare).
    aliases: Box<[Box<str>]>,
    /// A row image: some field records its source column type.
    pub(super) row_image: bool,
    /// Every input field must be defined (a row image, or `deltaforge.strict`).
    pub(super) strict: bool,
}

#[derive(Debug)]
pub(super) struct FieldPlan {
    pub(super) name: Box<str>,
    /// The field's declared source type (`deltaforge.source_type`).
    pub(super) source: Option<Box<str>>,
    pub(super) plan: Plan,
}

impl RecordPlan {
    fn empty() -> Self {
        RecordPlan {
            fields: Box::new([]),
            by_name: Box::new([]),
            aliases: Box::new([]),
            row_image: false,
            strict: false,
        }
    }

    /// Whether `name` is a field (or a field's alias) of the record.
    pub(super) fn defines(&self, name: &str) -> bool {
        self.by_name
            .binary_search_by(|&i| (*self.fields[i as usize].name).cmp(name))
            .is_ok()
            || self.aliases.binary_search_by(|a| (**a).cmp(name)).is_ok()
    }

    fn heap_bytes(&self) -> u64 {
        let fields: u64 = self
            .fields
            .iter()
            .map(|f| {
                (std::mem::size_of::<FieldPlan>()
                    + f.name.len()
                    + f.source.as_ref().map_or(0, |s| s.len()))
                    as u64
                    + f.plan.heap_bytes()
            })
            .sum();
        fields
            + (self.by_name.len() * 4) as u64
            + self
                .aliases
                .iter()
                .map(|a| (std::mem::size_of::<Box<str>>() + a.len()) as u64)
                .sum::<u64>()
    }
}

impl Plan {
    /// Compile a parsed schema. Named references resolve as conversion
    /// always resolved them: by full name, else by name alone.
    pub(super) fn compile(schema: &AvroSchema) -> Result<Plan, EncodingError> {
        let mut refs = std::collections::HashSet::new();
        collect_refs(schema, &mut refs);
        let names = if refs.is_empty() {
            HashMap::new()
        } else {
            let resolved = apache_avro::schema::ResolvedSchema::try_from(
                schema,
            )
            .map_err(|e| {
                EncodingError::Avro(format!("unresolvable schema: {e}"))
            })?;
            resolved
                .get_names()
                .iter()
                .filter(|(n, _)| refs.contains(n.name.as_str()))
                .map(|(n, s)| (n.clone(), *s))
                .collect()
        };
        Compiler {
            names,
            done: HashMap::new(),
            open: Vec::new(),
        }
        .plan(schema)
    }

    /// The Avro type name for failures.
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Plan::Null => "null",
            Plan::Boolean => "boolean",
            Plan::Int => "int",
            Plan::Long => "long",
            Plan::Float => "float",
            Plan::Double => "double",
            Plan::Bytes => "bytes",
            Plan::String => "string",
            Plan::Uuid => "uuid",
            Plan::Date => "date",
            Plan::TimeMillis => "time-millis",
            Plan::TimeMicros => "time-micros",
            Plan::TimestampMillis => "timestamp-millis",
            Plan::TimestampMicros => "timestamp-micros",
            Plan::LocalTimestampMillis => "local-timestamp-millis",
            Plan::LocalTimestampMicros => "local-timestamp-micros",
            Plan::Fixed(_) => "fixed",
            Plan::Enum(_) => "enum",
            Plan::Union(_) => "union",
            Plan::Array(_) => "array",
            Plan::Map(_) => "map",
            Plan::Record(_) => "record",
            Plan::Ref(_) | Plan::Back(_) | Plan::Unresolved => "named type",
            Plan::Unsupported(k) | Plan::Unmatched(k) => k,
        }
    }

    /// The heap bytes the plan holds (not counting the node itself; a
    /// referenced record or enum counted once, where it is defined).
    pub(super) fn heap_bytes(&self) -> u64 {
        let node = std::mem::size_of::<Plan>() as u64;
        match self {
            Plan::Enum(symbols) => {
                16 + symbols
                    .iter()
                    .map(|s| (std::mem::size_of::<Box<str>>() + s.len()) as u64)
                    .sum::<u64>()
            }
            Plan::Union(variants) => {
                variants.iter().map(|v| node + v.heap_bytes()).sum()
            }
            Plan::Array(p) | Plan::Map(p) => node + p.heap_bytes(),
            Plan::Record(r) => {
                16 + std::mem::size_of::<RecordPlan>() as u64 + r.heap_bytes()
            }
            // The target is a definition's shared record or enum, or a
            // fixed size: only the box.
            Plan::Ref(_) => node,
            _ => 0,
        }
    }
}

/// The names (without namespace) of every reference in `schema`.
fn collect_refs<'a>(
    schema: &'a AvroSchema,
    refs: &mut std::collections::HashSet<&'a str>,
) {
    match schema {
        AvroSchema::Ref { name } => {
            refs.insert(name.name.as_str());
        }
        AvroSchema::Record(r) => {
            r.fields.iter().for_each(|f| collect_refs(&f.schema, refs))
        }
        AvroSchema::Union(u) => {
            u.variants().iter().for_each(|v| collect_refs(v, refs))
        }
        AvroSchema::Array(a) => collect_refs(&a.items, refs),
        AvroSchema::Map(m) => collect_refs(&m.types, refs),
        AvroSchema::Decimal(d) => collect_refs(&d.inner, refs),
        _ => {}
    }
}

struct Compiler<'s> {
    /// The named types references can resolve to.
    names: HashMap<Name, &'s AvroSchema>,
    /// Named records and enums compiled, shared by every reference.
    done: HashMap<Name, Plan>,
    /// The records being compiled: a reference to one is a back reference
    /// (a recursive schema).
    open: Vec<(Name, Weak<RecordPlan>)>,
}

impl<'s> Compiler<'s> {
    fn plan(&mut self, schema: &'s AvroSchema) -> Result<Plan, EncodingError> {
        Ok(match schema {
            AvroSchema::Null => Plan::Null,
            AvroSchema::Boolean => Plan::Boolean,
            AvroSchema::Int => Plan::Int,
            AvroSchema::Long => Plan::Long,
            AvroSchema::Float => Plan::Float,
            AvroSchema::Double => Plan::Double,
            AvroSchema::Bytes => Plan::Bytes,
            AvroSchema::String => Plan::String,
            AvroSchema::Uuid => Plan::Uuid,
            AvroSchema::Date => Plan::Date,
            AvroSchema::TimeMillis => Plan::TimeMillis,
            AvroSchema::TimeMicros => Plan::TimeMicros,
            AvroSchema::TimestampMillis => Plan::TimestampMillis,
            AvroSchema::TimestampMicros => Plan::TimestampMicros,
            AvroSchema::LocalTimestampMillis => Plan::LocalTimestampMillis,
            AvroSchema::LocalTimestampMicros => Plan::LocalTimestampMicros,
            AvroSchema::TimestampNanos => Plan::Unmatched("timestamp-nanos"),
            AvroSchema::LocalTimestampNanos => {
                Plan::Unmatched("local-timestamp-nanos")
            }
            AvroSchema::Decimal(_) | AvroSchema::BigDecimal => {
                Plan::Unsupported("decimal")
            }
            AvroSchema::Duration => Plan::Unsupported("duration"),
            AvroSchema::Fixed(f) => Plan::Fixed(f.size),
            AvroSchema::Enum(e) => {
                if let Some(p) = self.done.get(&e.name) {
                    return Ok(p.clone());
                }
                let p = Plan::Enum(
                    e.symbols.iter().map(|s| s.as_str().into()).collect(),
                );
                self.done.insert(e.name.clone(), p.clone());
                p
            }
            AvroSchema::Union(u) => Plan::Union(
                u.variants()
                    .iter()
                    .map(|v| self.plan(v))
                    .collect::<Result<_, _>>()?,
            ),
            AvroSchema::Array(a) => Plan::Array(Box::new(self.plan(&a.items)?)),
            AvroSchema::Map(m) => Plan::Map(Box::new(self.plan(&m.types)?)),
            AvroSchema::Record(r) => {
                if let Some(p) = self.done.get(&r.name) {
                    return Ok(p.clone());
                }
                let mut failed = None;
                let record = Arc::new_cyclic(|me| {
                    self.open.push((r.name.clone(), me.clone()));
                    let built = self.record(r);
                    self.open.pop();
                    built.unwrap_or_else(|e| {
                        failed = Some(e);
                        RecordPlan::empty()
                    })
                });
                if let Some(e) = failed {
                    return Err(e);
                }
                let p = Plan::Record(record);
                self.done.insert(r.name.clone(), p.clone());
                p
            }
            AvroSchema::Ref { name } => {
                let found = self.names.get(name).copied().or_else(|| {
                    self.names
                        .iter()
                        .find(|(n, _)| n.name == name.name)
                        .map(|(_, s)| *s)
                });
                let Some(target) = found else {
                    return Ok(Plan::Unresolved);
                };
                if let AvroSchema::Record(r) = target
                    && let Some((_, me)) =
                        self.open.iter().find(|(n, _)| *n == r.name)
                {
                    return Ok(Plan::Back(me.clone()));
                }
                Plan::Ref(Box::new(self.plan(target)?))
            }
        })
    }

    fn record(
        &mut self,
        r: &'s apache_avro::schema::RecordSchema,
    ) -> Result<RecordPlan, EncodingError> {
        let mut fields = Vec::with_capacity(r.fields.len());
        let mut row_image = false;
        for f in &r.fields {
            row_image |= f.custom_attributes.contains_key(SOURCE_TYPE_ATTR);
            fields.push(FieldPlan {
                name: f.name.as_str().into(),
                source: f
                    .custom_attributes
                    .get(SOURCE_TYPE_ATTR)
                    .and_then(|v| v.as_str())
                    .map(Into::into),
                plan: self.plan(&f.schema)?,
            });
        }
        let mut by_name: Vec<u32> = (0..fields.len() as u32).collect();
        by_name.sort_by(|&a, &b| {
            fields[a as usize].name.cmp(&fields[b as usize].name)
        });
        let mut aliases: Vec<Box<str>> = r
            .lookup
            .keys()
            .filter(|k| !fields.iter().any(|f| *f.name == ***k))
            .map(|k| k.as_str().into())
            .collect();
        aliases.sort();
        let strict = row_image
            || r.attributes
                .get(STRICT_ATTR)
                .is_some_and(|v| v.as_bool() == Some(true));
        Ok(RecordPlan {
            fields: fields.into(),
            by_name: by_name.into(),
            aliases: aliases.into(),
            row_image,
            strict,
        })
    }
}

/// Where conversion writes a value. Values arrive in schema order: a
/// record's fields in its field order (each named first), an array's items,
/// a map's entries (each key first), a union's branch index then its value.
pub(super) trait Out {
    type Mark: Copy;
    /// The output's position, to [`Out::reset`] a failed union branch to.
    fn mark(&self) -> Self::Mark;
    fn reset(&mut self, mark: Self::Mark);

    fn null(&mut self);
    fn boolean(&mut self, b: bool);
    fn int(&mut self, v: i32);
    fn long(&mut self, v: i64);
    fn float(&mut self, v: f32);
    fn double(&mut self, v: f64);
    fn date(&mut self, days: i32);
    fn time_millis(&mut self, v: i32);
    fn time_micros(&mut self, v: i64);
    fn timestamp_millis(&mut self, v: i64);
    fn timestamp_micros(&mut self, v: i64);
    fn local_timestamp_millis(&mut self, v: i64);
    fn local_timestamp_micros(&mut self, v: i64);
    fn bytes(&mut self, b: &[u8]);
    fn string(&mut self, s: &str);
    /// A UUID for a `uuid` field (written as its hyphenated text).
    fn uuid(&mut self, u: &[u8; 16]);
    fn fixed(&mut self, b: &[u8]);
    fn enum_symbol(&mut self, index: u32, label: &str);
    fn union_branch(&mut self, index: u32);
    fn record_begin(&mut self, fields: usize);
    fn field(&mut self, name: &str);
    fn record_end(&mut self);
    fn array_begin(&mut self, items: usize);
    fn array_end(&mut self);
    fn map_begin(&mut self, entries: usize);
    fn map_key(&mut self, key: &str);
    fn map_end(&mut self);
}

/// The Avro binary encoding, written directly.
pub(super) struct BytesOut {
    buf: Vec<u8>,
}

impl BytesOut {
    /// An output starting with `prefix` (the wire-format header).
    pub(super) fn with_prefix(prefix: &[u8]) -> Self {
        let mut buf = Vec::with_capacity(256);
        buf.extend_from_slice(prefix);
        Self { buf }
    }

    pub(super) fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    fn varint(&mut self, n: i64) {
        let mut z = ((n << 1) ^ (n >> 63)) as u64;
        while z >= 0x80 {
            self.buf.push((z as u8) | 0x80);
            z >>= 7;
        }
        self.buf.push(z as u8);
    }

    fn len_bytes(&mut self, b: &[u8]) {
        self.varint(b.len() as i64);
        self.buf.extend_from_slice(b);
    }
}

impl Out for BytesOut {
    type Mark = usize;
    fn mark(&self) -> usize {
        self.buf.len()
    }
    fn reset(&mut self, mark: usize) {
        self.buf.truncate(mark);
    }
    fn null(&mut self) {}
    fn boolean(&mut self, b: bool) {
        self.buf.push(u8::from(b));
    }
    fn int(&mut self, v: i32) {
        self.varint(v as i64);
    }
    fn long(&mut self, v: i64) {
        self.varint(v);
    }
    fn float(&mut self, v: f32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn double(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn date(&mut self, days: i32) {
        self.varint(days as i64);
    }
    fn time_millis(&mut self, v: i32) {
        self.varint(v as i64);
    }
    fn time_micros(&mut self, v: i64) {
        self.varint(v);
    }
    fn timestamp_millis(&mut self, v: i64) {
        self.varint(v);
    }
    fn timestamp_micros(&mut self, v: i64) {
        self.varint(v);
    }
    fn local_timestamp_millis(&mut self, v: i64) {
        self.varint(v);
    }
    fn local_timestamp_micros(&mut self, v: i64) {
        self.varint(v);
    }
    fn bytes(&mut self, b: &[u8]) {
        self.len_bytes(b);
    }
    fn string(&mut self, s: &str) {
        self.len_bytes(s.as_bytes());
    }
    fn uuid(&mut self, u: &[u8; 16]) {
        let mut text = [0u8; uuid::fmt::Hyphenated::LENGTH];
        let s = uuid::Uuid::from_bytes(*u)
            .hyphenated()
            .encode_lower(&mut text);
        self.len_bytes(s.as_bytes());
    }
    fn fixed(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }
    fn enum_symbol(&mut self, index: u32, _: &str) {
        self.varint(index as i64);
    }
    fn union_branch(&mut self, index: u32) {
        self.varint(index as i64);
    }
    fn record_begin(&mut self, _: usize) {}
    fn field(&mut self, _: &str) {}
    fn record_end(&mut self) {}
    // A non-empty array or map is one block: its count, its items, then
    // the zero count that ends it; an empty one only the zero.
    fn array_begin(&mut self, items: usize) {
        if items > 0 {
            self.varint(items as i64);
        }
    }
    fn array_end(&mut self) {
        self.buf.push(0);
    }
    fn map_begin(&mut self, entries: usize) {
        if entries > 0 {
            self.varint(entries as i64);
        }
    }
    fn map_key(&mut self, key: &str) {
        self.len_bytes(key.as_bytes());
    }
    fn map_end(&mut self) {
        self.buf.push(0);
    }
}

/// The apache-avro value of the converted data.
#[derive(Default)]
pub(super) struct ValueOut {
    stack: Vec<Frame>,
    done: Option<AvroValue>,
}

enum Frame {
    Record(Vec<(String, AvroValue)>, Option<String>),
    Array(Vec<AvroValue>),
    Map(HashMap<String, AvroValue>, Option<String>),
    Union(u32),
}

impl ValueOut {
    /// The value written (`None` if no complete value was).
    pub(super) fn finish(self) -> Option<AvroValue> {
        self.done.filter(|_| self.stack.is_empty())
    }

    fn push(&mut self, v: AvroValue) {
        match self.stack.last_mut() {
            None => self.done = Some(v),
            Some(Frame::Record(fields, name)) => {
                fields.push((name.take().unwrap_or_default(), v))
            }
            Some(Frame::Array(items)) => items.push(v),
            Some(Frame::Map(entries, key)) => {
                entries.insert(key.take().unwrap_or_default(), v);
            }
            Some(Frame::Union(i)) => {
                let i = *i;
                self.stack.pop();
                self.push(AvroValue::Union(i, Box::new(v)));
            }
        }
    }
}

impl Out for ValueOut {
    /// A failed branch's partial values are all above the stack depth it
    /// started at: nothing below is touched before the branch completes.
    type Mark = usize;
    fn mark(&self) -> usize {
        self.stack.len()
    }
    fn reset(&mut self, mark: usize) {
        self.stack.truncate(mark);
    }
    fn null(&mut self) {
        self.push(AvroValue::Null);
    }
    fn boolean(&mut self, b: bool) {
        self.push(AvroValue::Boolean(b));
    }
    fn int(&mut self, v: i32) {
        self.push(AvroValue::Int(v));
    }
    fn long(&mut self, v: i64) {
        self.push(AvroValue::Long(v));
    }
    fn float(&mut self, v: f32) {
        self.push(AvroValue::Float(v));
    }
    fn double(&mut self, v: f64) {
        self.push(AvroValue::Double(v));
    }
    fn date(&mut self, days: i32) {
        self.push(AvroValue::Date(days));
    }
    fn time_millis(&mut self, v: i32) {
        self.push(AvroValue::TimeMillis(v));
    }
    fn time_micros(&mut self, v: i64) {
        self.push(AvroValue::TimeMicros(v));
    }
    fn timestamp_millis(&mut self, v: i64) {
        self.push(AvroValue::TimestampMillis(v));
    }
    fn timestamp_micros(&mut self, v: i64) {
        self.push(AvroValue::TimestampMicros(v));
    }
    fn local_timestamp_millis(&mut self, v: i64) {
        self.push(AvroValue::LocalTimestampMillis(v));
    }
    fn local_timestamp_micros(&mut self, v: i64) {
        self.push(AvroValue::LocalTimestampMicros(v));
    }
    fn bytes(&mut self, b: &[u8]) {
        self.push(AvroValue::Bytes(b.to_vec()));
    }
    fn string(&mut self, s: &str) {
        self.push(AvroValue::String(s.to_string()));
    }
    fn uuid(&mut self, u: &[u8; 16]) {
        self.push(AvroValue::Uuid(uuid::Uuid::from_bytes(*u)));
    }
    fn fixed(&mut self, b: &[u8]) {
        self.push(AvroValue::Fixed(b.len(), b.to_vec()));
    }
    fn enum_symbol(&mut self, index: u32, label: &str) {
        self.push(AvroValue::Enum(index, label.to_string()));
    }
    fn union_branch(&mut self, index: u32) {
        self.stack.push(Frame::Union(index));
    }
    fn record_begin(&mut self, fields: usize) {
        self.stack
            .push(Frame::Record(Vec::with_capacity(fields), None));
    }
    fn field(&mut self, name: &str) {
        if let Some(Frame::Record(_, n)) = self.stack.last_mut() {
            *n = Some(name.to_string());
        }
    }
    fn record_end(&mut self) {
        if let Some(Frame::Record(fields, _)) = self.stack.pop() {
            self.push(AvroValue::Record(fields));
        }
    }
    fn array_begin(&mut self, items: usize) {
        self.stack.push(Frame::Array(Vec::with_capacity(items)));
    }
    fn array_end(&mut self) {
        if let Some(Frame::Array(items)) = self.stack.pop() {
            self.push(AvroValue::Array(items));
        }
    }
    fn map_begin(&mut self, entries: usize) {
        self.stack
            .push(Frame::Map(HashMap::with_capacity(entries), None));
    }
    fn map_key(&mut self, key: &str) {
        if let Some(Frame::Map(_, k)) = self.stack.last_mut() {
            *k = Some(key.to_string());
        }
    }
    fn map_end(&mut self) {
        if let Some(Frame::Map(entries, _)) = self.stack.pop() {
            self.push(AvroValue::Map(entries));
        }
    }
}
