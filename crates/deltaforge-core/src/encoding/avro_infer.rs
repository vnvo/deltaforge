//! Inferred Avro schemas (Path C): one schema per subject and record name
//! that only ever widens.
//!
//! A value never narrows the schema. A field first seen null stays nullable
//! and gains the kinds later values carry; a field is never dropped. Sibling
//! `before` and `after` images share their fields, so a schema inferred from
//! an insert already carries the before image of later updates and deletes.
//! Nested record names never change once assigned, so each widened schema
//! stays backward compatible with the previous one.

use std::collections::HashSet;

use serde_json::{Map, Value as J, json};

const BOOL: u8 = 1;
const LONG: u8 = 2;
const DOUBLE: u8 = 4;
const STRING: u8 = 8;
/// Union branch order: a value takes the first branch that holds it exactly.
const KINDS: [(u8, &str); 4] = [
    (BOOL, "boolean"),
    (LONG, "long"),
    (DOUBLE, "double"),
    (STRING, "string"),
];

/// A field: always nullable, plus every kind seen.
#[derive(Clone, Default, Debug, PartialEq)]
struct Field {
    kinds: u8,
    array: Option<Items>,
    record: Option<Record>,
}

/// Array items: scalars only; a nested array or object is its JSON text.
#[derive(Clone, Copy, Default, Debug, PartialEq)]
struct Items {
    nullable: bool,
    kinds: u8,
}

#[derive(Clone, Debug, PartialEq)]
struct Record {
    name: String,
    fields: Vec<(String, Field)>,
}

/// The widened schema of one subject and record name.
#[derive(Clone, Debug)]
pub(super) struct Inferred {
    root: Record,
    names: HashSet<String>,
}

/// The kind of a non-null scalar: integers in the signed 64-bit range are
/// `long`, larger integers their exact decimal text, other numbers `double`.
fn scalar_kind(v: &J) -> Option<u8> {
    match v {
        J::Bool(_) => Some(BOOL),
        J::Number(n) if n.is_i64() => Some(LONG),
        J::Number(n) if n.is_u64() => Some(STRING),
        J::Number(_) => Some(DOUBLE),
        J::String(_) => Some(STRING),
        _ => None,
    }
}

fn item_kind(v: &J) -> Option<u8> {
    match v {
        J::Null => None,
        J::Array(_) | J::Object(_) => Some(STRING),
        other => scalar_kind(other),
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

/// `deltaforge.{Key}`, or `deltaforge.{Key}_{n}` when that name is taken.
fn fresh_name(key: &str, names: &mut HashSet<String>) -> String {
    let base = format!("deltaforge.{}", capitalize(key));
    if names.insert(base.clone()) {
        return base;
    }
    (2..)
        .map(|n| format!("{base}_{n}"))
        .find(|c| names.insert(c.clone()))
        .expect("an unused name")
}

impl Inferred {
    pub(super) fn new(name: &str) -> Self {
        Self {
            root: Record {
                name: name.to_string(),
                fields: Vec::new(),
            },
            names: HashSet::from([name.to_string()]),
        }
    }

    /// Resume from a schema registered earlier for the subject, so a restart
    /// widens it instead of starting narrower. `None` when it is not a schema
    /// this inference produces, or names another record.
    pub(super) fn seeded(name: &str, schema_json: &str) -> Option<Self> {
        let schema: J = serde_json::from_str(schema_json).ok()?;
        let mut names = HashSet::new();
        let root = parse_record(&schema, None, &mut names)?;
        (root.name == name).then_some(Self { root, names })
    }

    /// Whether the current schema holds `value` exactly.
    pub(super) fn covers(&self, value: &Map<String, J>) -> bool {
        covers_record(&self.root, value)
    }

    /// Widen the schema to hold `value`.
    pub(super) fn absorb(&mut self, value: &Map<String, J>) {
        absorb_record(&mut self.root, value, &mut self.names);
    }

    pub(super) fn schema_json(&self) -> String {
        record_json(&self.root).to_string()
    }
}

fn covers_record(r: &Record, value: &Map<String, J>) -> bool {
    value.iter().all(|(k, v)| {
        r.fields
            .iter()
            .find(|(n, _)| n == k)
            .is_some_and(|(_, f)| covers_field(f, v))
    })
}

fn covers_field(f: &Field, v: &J) -> bool {
    match v {
        J::Null => true,
        J::Array(items) => f.array.is_some_and(|it| {
            items.iter().all(|x| match item_kind(x) {
                None => it.nullable,
                Some(k) => it.kinds & k != 0,
            })
        }),
        J::Object(m) => f.record.as_ref().is_some_and(|r| covers_record(r, m)),
        scalar => scalar_kind(scalar).is_some_and(|k| f.kinds & k != 0),
    }
}

fn field_mut<'a>(r: &'a mut Record, key: &str) -> &'a mut Field {
    let i = match r.fields.iter().position(|(n, _)| n == key) {
        Some(i) => i,
        None => {
            r.fields.push((key.to_string(), Field::default()));
            r.fields.len() - 1
        }
    };
    &mut r.fields[i].1
}

fn absorb_record(
    r: &mut Record,
    value: &Map<String, J>,
    names: &mut HashSet<String>,
) {
    for (k, v) in value {
        absorb_field(field_mut(r, k), k, v, names);
    }
    share_row_images(r, names);
}

fn absorb_field(f: &mut Field, key: &str, v: &J, names: &mut HashSet<String>) {
    match v {
        J::Null => {}
        J::Array(items) => {
            let it = f.array.get_or_insert_with(Items::default);
            for x in items {
                match item_kind(x) {
                    None => it.nullable = true,
                    Some(k) => it.kinds |= k,
                }
            }
        }
        J::Object(m) => {
            let rec = f.record.get_or_insert_with(|| Record {
                name: fresh_name(key, names),
                fields: Vec::new(),
            });
            absorb_record(rec, m, names);
        }
        scalar => {
            if let Some(k) = scalar_kind(scalar) {
                f.kinds |= k;
            }
        }
    }
}

/// `before` and `after` are images of the same row: give each every field
/// and kind of the other.
fn share_row_images(r: &mut Record, names: &mut HashSet<String>) {
    let has = |r: &Record, k: &str| r.fields.iter().any(|(n, _)| n == k);
    if !has(r, "before") && !has(r, "after") {
        return;
    }
    let after = field_mut(r, "after").clone();
    merge_field(field_mut(r, "before"), "before", &after, names);
    let before = field_mut(r, "before").clone();
    merge_field(field_mut(r, "after"), "after", &before, names);
}

fn merge_field(
    dst: &mut Field,
    key: &str,
    src: &Field,
    names: &mut HashSet<String>,
) {
    dst.kinds |= src.kinds;
    if let Some(s) = src.array {
        let d = dst.array.get_or_insert_with(Items::default);
        d.nullable |= s.nullable;
        d.kinds |= s.kinds;
    }
    if let Some(s) = &src.record {
        let d = dst.record.get_or_insert_with(|| Record {
            name: fresh_name(key, names),
            fields: Vec::new(),
        });
        for (k, sf) in &s.fields {
            merge_field(field_mut(d, k), k, sf, names);
        }
    }
}

fn field_type(f: &Field) -> J {
    let mut variants: Vec<J> = KINDS
        .iter()
        .filter(|(k, _)| f.kinds & k != 0)
        .map(|(_, n)| json!(n))
        .collect();
    if let Some(it) = f.array {
        variants.push(json!({"type": "array", "items": items_type(it)}));
    }
    if let Some(r) = &f.record {
        variants.push(record_json(r));
    }
    if variants.is_empty() {
        return json!("null");
    }
    variants.insert(0, json!("null"));
    J::Array(variants)
}

fn items_type(it: Items) -> J {
    let kinds: Vec<J> = KINDS
        .iter()
        .filter(|(k, _)| it.kinds & k != 0)
        .map(|(_, n)| json!(n))
        .collect();
    match (it.nullable, kinds.len()) {
        (_, 0) => json!("null"),
        (false, 1) => kinds[0].clone(),
        (nullable, _) => {
            let mut u = if nullable {
                vec![json!("null")]
            } else {
                vec![]
            };
            u.extend(kinds);
            J::Array(u)
        }
    }
}

fn record_json(r: &Record) -> J {
    let fields: Vec<J> = r
        .fields
        .iter()
        .map(
            |(n, f)| json!({"name": n, "type": field_type(f), "default": null}),
        )
        .collect();
    json!({"type": "record", "name": r.name, "fields": fields})
}

fn primitive_kind(s: &str) -> Option<u8> {
    KINDS.iter().find(|(_, n)| *n == s).map(|(k, _)| *k)
}

fn full_name(obj: &Map<String, J>, namespace: Option<&str>) -> Option<String> {
    let name = obj.get("name")?.as_str()?;
    let ns = obj.get("namespace").and_then(J::as_str).or(namespace);
    Some(match ns {
        Some(ns) if !name.contains('.') && !ns.is_empty() => {
            format!("{ns}.{name}")
        }
        _ => name.to_string(),
    })
}

fn namespace_of(full: &str) -> Option<&str> {
    full.rsplit_once('.').map(|(ns, _)| ns)
}

fn parse_record(
    v: &J,
    namespace: Option<&str>,
    names: &mut HashSet<String>,
) -> Option<Record> {
    let obj = v.as_object()?;
    if obj.get("type")?.as_str()? != "record" {
        return None;
    }
    let name = full_name(obj, namespace)?;
    if !names.insert(name.clone()) {
        return None;
    }
    let ns = namespace_of(&name).map(str::to_string);
    let mut fields = Vec::new();
    for f in obj.get("fields")?.as_array()? {
        let fname = f.get("name")?.as_str()?.to_string();
        let field = parse_field(f.get("type")?, ns.as_deref(), names)?;
        fields.push((fname, field));
    }
    Some(Record { name, fields })
}

fn parse_field(
    t: &J,
    namespace: Option<&str>,
    names: &mut HashSet<String>,
) -> Option<Field> {
    let mut f = Field::default();
    let variants: Vec<&J> = match t {
        J::Array(u) => u.iter().collect(),
        other => vec![other],
    };
    for v in variants {
        match v {
            J::String(s) if s == "null" => {}
            J::String(s) => f.kinds |= primitive_kind(s)?,
            J::Object(o) => match o.get("type")?.as_str()? {
                "array" if f.array.is_none() => {
                    f.array = Some(parse_items(o.get("items")?)?)
                }
                "record" if f.record.is_none() => {
                    f.record = Some(parse_record(v, namespace, names)?)
                }
                _ => return None,
            },
            _ => return None,
        }
    }
    Some(f)
}

fn parse_items(t: &J) -> Option<Items> {
    let mut it = Items::default();
    let variants: Vec<&J> = match t {
        J::Array(u) => u.iter().collect(),
        other => vec![other],
    };
    for v in variants {
        match v.as_str()? {
            "null" => it.nullable = true,
            s => it.kinds |= primitive_kind(s)?,
        }
    }
    Some(it)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(v: J) -> Map<String, J> {
        v.as_object().unwrap().clone()
    }

    fn widened(values: &[J]) -> Inferred {
        let mut s = Inferred::new("deltaforge.Event");
        for v in values {
            s.absorb(&obj(v.clone()));
        }
        s
    }

    #[test]
    fn a_null_first_field_gains_the_kinds_of_later_values() {
        let s = widened(&[json!({"a": null}), json!({"a": 5})]);
        let schema: J = serde_json::from_str(&s.schema_json()).unwrap();
        assert_eq!(schema["fields"][0]["type"], json!(["null", "long"]));
        assert!(s.covers(&obj(json!({"a": null}))));
        assert!(s.covers(&obj(json!({"a": 7}))));
        assert!(!s.covers(&obj(json!({"a": "x"}))));
        assert!(!s.covers(&obj(json!({"b": 1}))), "unknown field");
    }

    #[test]
    fn an_insert_shapes_the_before_image_too() {
        let s = widened(&[json!({
            "before": null,
            "after": {"id": 1, "name": "a", "j": {"k": 1}},
            "op": "c"
        })]);
        assert!(s.covers(&obj(json!({
            "before": {"id": 1, "name": "a", "j": {"k": 2}},
            "after": {"id": 1, "name": "b", "j": {"k": 3}},
            "op": "u"
        }))));
        apache_avro::Schema::parse_str(&s.schema_json())
            .expect("nested records of both images have distinct names");
    }

    #[test]
    fn widening_is_deterministic_and_names_are_stable() {
        let a = widened(&[
            json!({"after": {"x": 1}}),
            json!({"after": {"y": 2.5}}),
        ]);
        let b = widened(&[
            json!({"after": {"x": 1}}),
            json!({"after": {"y": 2.5}}),
        ]);
        assert_eq!(a.schema_json(), b.schema_json());

        let mut c = a.clone();
        c.absorb(&obj(json!({"after": {"x": "big"}})));
        let names = |s: &Inferred| {
            let mut n: Vec<_> = s.names.iter().cloned().collect();
            n.sort();
            n
        };
        assert_eq!(names(&a), names(&c), "widening renames nothing");
    }

    #[test]
    fn integers_beyond_long_are_text_and_floats_double() {
        let s = widened(&[json!({"u": 18446744073709551615u64, "f": 1.5})]);
        let schema: J = serde_json::from_str(&s.schema_json()).unwrap();
        let type_of = |name: &str| {
            schema["fields"]
                .as_array()
                .unwrap()
                .iter()
                .find(|f| f["name"] == name)
                .unwrap()["type"]
                .clone()
        };
        assert_eq!(type_of("u"), json!(["null", "string"]));
        assert_eq!(type_of("f"), json!(["null", "double"]));
    }

    #[test]
    fn a_registered_schema_seeds_the_same_widening() {
        let s = widened(&[
            json!({"before": null, "after": {"id": 1, "tags": [1, null]}}),
            json!({"after": {"id": 1, "note": "n"}}),
        ]);
        let seeded =
            Inferred::seeded("deltaforge.Event", &s.schema_json()).unwrap();
        assert_eq!(seeded.schema_json(), s.schema_json());
        assert!(
            Inferred::seeded("deltaforge.Other", &s.schema_json()).is_none()
        );
        assert!(
            Inferred::seeded(
                "deltaforge.Event",
                r#"{"type":"record","name":"deltaforge.Event","fields":[{"name":"d","type":{"type":"int","logicalType":"date"}}]}"#
            )
            .is_none(),
            "not a schema this inference produces"
        );
    }
}
