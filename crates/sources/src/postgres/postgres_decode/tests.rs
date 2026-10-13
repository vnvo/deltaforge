//! Per-type decoding of PostgreSQL output text (pinned session).

use deltaforge_core::canonical::render::{RenderOpts, render_value};
use deltaforge_core::canonical::{CanonicalValue, TypeMapping};
use serde_json::{Value, json};

use super::*;

fn kind(oid: u32, typmod: i32) -> PgKind {
    PgKind::of(oid, typmod, None)
}

fn v2(oid: u32, typmod: i32, text: &str) -> Value {
    let v = decode_text(&kind(oid, typmod), text)
        .unwrap_or_else(|e| panic!("{text}: {e}"));
    render_value(&v, RenderOpts::new(TypeMapping::V2)).unwrap()
}

fn refused(oid: u32, typmod: i32, text: &str) -> bool {
    decode_text(&kind(oid, typmod), text).is_err()
}

#[test]
fn booleans_and_integers() {
    assert_eq!(v2(16, -1, "t"), json!(true));
    assert_eq!(v2(16, -1, "f"), json!(false));
    assert!(refused(16, -1, "true"));
    assert_eq!(v2(21, -1, "-32768"), json!(-32768));
    assert!(refused(21, -1, "32768"));
    assert_eq!(v2(23, -1, "2147483647"), json!(2147483647));
    assert_eq!(
        v2(20, -1, "-9223372036854775808"),
        json!("-9223372036854775808")
    );
    assert!(refused(20, -1, "9223372036854775808"));
    assert_eq!(v2(26, -1, "4294967295"), json!(4294967295u64));
}

#[test]
fn numeric_extremes_stay_exact() {
    let big = format!("{}.{}", "9".repeat(131_072), "1".repeat(16_383));
    assert_eq!(v2(1700, -1, &big), json!(big));
    assert_eq!(
        v2(1700, -1, "-0.000000000000000000000000001"),
        json!("-0.000000000000000000000000001")
    );
    assert_eq!(v2(1700, -1, "NaN"), json!("NaN"));
    assert_eq!(v2(1700, -1, "Infinity"), json!("Infinity"));
    assert_eq!(v2(1700, -1, "-Infinity"), json!("-Infinity"));
    assert!(refused(1700, -1, "1e5"));
}

#[test]
fn floats_keep_their_width_and_non_finite_values() {
    assert_eq!(v2(700, -1, "1.1").to_string(), "1.1");
    assert_eq!(v2(700, -1, "3.4028235e+38").to_string(), "3.4028235e+38");
    assert_eq!(v2(701, -1, "0.1").to_string(), "0.1");
    assert_eq!(
        v2(701, -1, "-1.7976931348623157e+308").to_string(),
        "-1.7976931348623157e+308"
    );
    assert_eq!(v2(701, -1, "NaN"), json!("NaN"));
    assert_eq!(v2(700, -1, "-Infinity"), json!("-Infinity"));
    assert!(refused(701, -1, "nan"));
}

#[test]
fn text_bytea_uuid_json() {
    assert_eq!(
        v2(1042, 9, "ab   "),
        json!("ab   "),
        "bpchar keeps its padding"
    );
    assert_eq!(
        v2(25, -1, "{\"_base64\":\"x\"}"),
        json!("{\"_base64\":\"x\"}")
    );
    assert_eq!(v2(17, -1, "\\x00ff"), json!({"_base64": "AP8="}));
    assert_eq!(v2(17, -1, "\\x"), json!({"_base64": ""}));
    assert!(
        refused(17, -1, "\\000"),
        "escape format is not the pinned output"
    );
    assert_eq!(
        v2(2950, -1, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        json!("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    );
    assert!(refused(2950, -1, "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"));
    assert_eq!(v2(3802, -1, "{\"a\": 1}"), json!("{\"a\": 1}"));
    assert_eq!(v2(114, -1, "null"), json!("null"));
    assert!(refused(114, -1, "{"));
}

#[test]
fn dates_and_times() {
    assert_eq!(v2(1082, -1, "2024-02-29"), json!("2024-02-29"));
    assert_eq!(v2(1082, -1, "0044-03-15 BC"), json!("-00043-03-15"));
    assert_eq!(v2(1082, -1, "0001-01-01 BC"), json!("0000-01-01"));
    assert_eq!(v2(1082, -1, "294276-12-31"), json!("+294276-12-31"));
    assert_eq!(v2(1082, -1, "infinity"), json!("infinity"));
    assert_eq!(v2(1082, -1, "-infinity"), json!("-infinity"));
    assert!(refused(1082, -1, "2023-02-29"));
    assert!(refused(1082, -1, "02/03/2024"));

    assert_eq!(v2(1083, -1, "24:00:00"), json!("24:00:00"));
    assert_eq!(v2(1083, -1, "12:34:56.5"), json!("12:34:56.5"));
    assert_eq!(v2(1083, 3, "12:34:56.5"), json!("12:34:56.500"));
    assert!(refused(1083, 3, "12:34:56.5001"));
    assert!(refused(1083, -1, "25:00:00"));

    assert_eq!(
        v2(1114, -1, "2024-03-09 01:02:03.000123"),
        json!("2024-03-09T01:02:03.000123")
    );
    assert_eq!(
        v2(1114, 0, "2024-03-09 01:02:03"),
        json!("2024-03-09T01:02:03")
    );
    assert_eq!(v2(1114, -1, "infinity"), json!("infinity"));
    assert_eq!(
        v2(1114, -1, "4713-01-01 00:00:00 BC"),
        json!("-04712-01-01T00:00:00")
    );

    assert_eq!(
        v2(1184, -1, "2024-10-27 00:50:30.654321+00"),
        json!("2024-10-27T00:50:30.654321Z")
    );
    assert_eq!(
        v2(1184, 0, "1969-12-31 23:59:59+00"),
        json!("1969-12-31T23:59:59Z")
    );
    assert_eq!(v2(1184, -1, "-infinity"), json!("-infinity"));
    assert!(
        refused(1184, -1, "2024-10-27 02:50:30+02"),
        "an unpinned zone fails closed"
    );
}

#[test]
fn arrays_use_their_declared_element_type() {
    assert_eq!(v2(1016, -1, "{1,NULL,-3}"), json!(["1", null, "-3"]));
    assert_eq!(v2(1007, -1, "{{1,2},{3,4}}"), json!([[1, 2], [3, 4]]));
    assert_eq!(v2(1009, -1, "{}"), json!([]));
    assert_eq!(
        v2(1009, -1, r#"{"a,b","NULL",NULL,"q\"\\",plain}"#),
        json!(["a,b", "NULL", null, "q\"\\", "plain"])
    );
    assert_eq!(v2(1001, -1, r#"{"\\x00ff"}"#), json!([{"_base64": "AP8="}]));
    assert_eq!(
        v2(1185, -1, r#"{"2024-01-01 00:00:00+00",infinity}"#),
        json!(["2024-01-01T00:00:00Z", "infinity"])
    );
    assert_eq!(v2(1231, -1, "{1.50,NaN}"), json!(["1.50", "NaN"]));
    // An element that does not parse fails the whole value.
    assert!(refused(1007, -1, "{1,x}"));
    assert!(refused(1007, -1, "{1,2"));
    assert!(refused(1007, -1, "[0:1]={1,2}"));
    // The element type is recorded.
    let v = decode_text(&kind(1007, -1), "{1}").unwrap();
    let CanonicalValue::Db(DbValue::Array(a)) = v else {
        panic!()
    };
    assert_eq!(a.element_type().name, "int4");
    assert_eq!(a.element_type().oid, Some(23));
}

#[test]
fn unmapped_types_are_opaque_with_their_identity() {
    let k = PgKind::of(869, -1, None);
    let v = decode_text(&k, "192.168.0.1").unwrap();
    let CanonicalValue::Opaque(o) = &v else {
        panic!("{v:?}")
    };
    assert_eq!(o.source_type.name, "inet");
    assert_eq!(o.source_type.oid, Some(869));
    assert_eq!(o.text, "192.168.0.1");
    // A user-defined type is named by the decoding schema.
    let k = PgKind::of(16_390, -1, Some("mood"));
    let CanonicalValue::Opaque(o) = decode_text(&k, "happy").unwrap() else {
        panic!()
    };
    assert_eq!(
        (o.source_type.name.as_str(), o.source_type.oid),
        ("mood", Some(16_390))
    );
    // Arrays of an unmapped element type are opaque as a whole.
    assert!(
        matches!(PgKind::of(1041, -1, None), PgKind::Opaque(_)),
        "inet[]"
    );
    assert!(
        matches!(PgKind::of(1186, -1, None), PgKind::Opaque(_)),
        "interval"
    );
}

fn row() -> PgRowDecoder {
    PgRowDecoder {
        columns: vec![
            PgColumnDecoder::new("id", 23, -1, None).with_key(true),
            PgColumnDecoder::new("doc", 25, -1, None),
        ],
    }
}

#[test]
fn a_key_only_image_holds_only_the_key() {
    let r = row()
        .decode_tuple(
            &[TupleValue::Text(b"1"), TupleValue::Null],
            Image::KeyOnly,
        )
        .unwrap();
    assert_eq!(r.names().collect::<Vec<_>>(), ["id"]);
    // Outside the key PostgreSQL sends nothing: a value there is malformed.
    assert!(
        row()
            .decode_tuple(
                &[TupleValue::Text(b"1"), TupleValue::Text(b"x")],
                Image::KeyOnly
            )
            .is_err()
    );
}

#[test]
fn unchanged_toast_is_legal_only_in_an_update_new_row() {
    let r = row()
        .decode_tuple(
            &[TupleValue::Text(b"1"), TupleValue::Unchanged],
            Image::UpdateAfter,
        )
        .unwrap();
    assert_eq!(r.get("doc"), Some(&CanonicalValue::Unchanged));
    assert!(
        row()
            .decode_tuple(
                &[TupleValue::Text(b"1"), TupleValue::Unchanged],
                Image::Other
            )
            .is_err()
    );
    // Never mistaken for a value a customer supplied.
    let r = row()
        .decode_tuple(
            &[
                TupleValue::Text(b"1"),
                TupleValue::Text(br#"{"_unchanged":true}"#),
            ],
            Image::UpdateAfter,
        )
        .unwrap();
    assert!(matches!(
        r.get("doc"),
        Some(CanonicalValue::Db(DbValue::Text(_)))
    ));
}

#[test]
fn tuples_fail_closed_on_shape_and_format() {
    assert!(
        row()
            .decode_tuple(&[TupleValue::Text(b"1")], Image::Other)
            .is_err()
    );
    assert!(
        row()
            .decode_tuple(
                &[TupleValue::Text(b"1"), TupleValue::Binary],
                Image::Other
            )
            .is_err()
    );
    assert!(
        row()
            .decode_tuple(
                &[TupleValue::Text(b"x"), TupleValue::Null],
                Image::Other
            )
            .is_err()
    );
    let err = row()
        .decode_tuple(
            &[TupleValue::Text(b"secret-value"), TupleValue::Null],
            Image::Other,
        )
        .unwrap_err();
    assert!(!err.to_string().contains("secret-value"), "{err}");
    let r = row().decode_texts(&[Some("1"), None]).unwrap();
    assert_eq!(r.get("doc"), Some(&CanonicalValue::Null));
}

#[test]
fn the_pinned_session_sets_every_output_setting() {
    let sql = pinned_session_sql();
    for k in [
        "DateStyle = 'ISO, YMD'",
        "TimeZone = 'UTC'",
        "IntervalStyle = 'postgres'",
        "extra_float_digits = '3'",
        "bytea_output = 'hex'",
        "lc_monetary = 'C'",
        "search_path = 'pg_catalog'",
    ] {
        assert!(sql.contains(k), "{k} missing from {sql}");
    }
}

// ── Unchanged-TOAST reconstruction ────────────────────────────────────────

fn canon(cols: &[(&str, CanonicalValue)]) -> CanonicalRow {
    CanonicalRow::new(
        cols.iter()
            .map(|(n, v)| (n.to_string(), v.clone()))
            .collect(),
    )
    .unwrap()
}

fn text(s: &str) -> CanonicalValue {
    CanonicalValue::Db(DbValue::Text(s.to_string()))
}

#[test]
fn unchanged_values_are_copied_from_the_full_old_row() {
    let big = "x".repeat(100_000);
    let before = canon(&[
        ("id", text("1")),
        ("body", text(&big)),
        ("blob", CanonicalValue::Db(DbValue::Binary(vec![7; 50_000]))),
        ("small", text("old")),
        ("gone", CanonicalValue::Null),
    ]);
    let mut after = canon(&[
        ("id", text("1")),
        ("body", CanonicalValue::Unchanged),
        ("blob", CanonicalValue::Unchanged),
        ("small", text("new")),
        ("gone", CanonicalValue::Null),
    ]);
    let r = PgRowDecoder::reconstruct_unchanged(&mut after, Some(&before));
    assert_eq!(
        r,
        Reconstruction {
            reconstructed: 2,
            unavailable: 0
        }
    );
    assert_eq!(after.get("body"), Some(&text(&big)));
    assert_eq!(after.get("blob"), before.get("blob"));
    // A changed value and SQL NULL are the new row's own.
    assert_eq!(after.get("small"), Some(&text("new")));
    assert_eq!(after.get("gone"), Some(&CanonicalValue::Null));
}

#[test]
fn an_old_null_is_copied_as_null() {
    let before = canon(&[("v", CanonicalValue::Null)]);
    let mut after = canon(&[("v", CanonicalValue::Unchanged)]);
    let r = PgRowDecoder::reconstruct_unchanged(&mut after, Some(&before));
    assert_eq!(r.reconstructed, 1);
    assert_eq!(after.get("v"), Some(&CanonicalValue::Null));
}

#[test]
fn without_a_usable_old_value_the_value_stays_unavailable() {
    let unchanged = || canon(&[("v", CanonicalValue::Unchanged)]);
    // No full old row (REPLICA IDENTITY DEFAULT or a key-only image).
    let mut after = unchanged();
    let r = PgRowDecoder::reconstruct_unchanged(&mut after, None);
    assert_eq!(
        r,
        Reconstruction {
            reconstructed: 0,
            unavailable: 1
        }
    );
    assert_eq!(after.get("v"), Some(&CanonicalValue::Unchanged));
    // The old row lacks the column.
    let mut after = unchanged();
    let r = PgRowDecoder::reconstruct_unchanged(
        &mut after,
        Some(&canon(&[("w", text("1"))])),
    );
    assert_eq!(r.unavailable, 1);
    // The old value is itself unavailable.
    let mut after = unchanged();
    let r = PgRowDecoder::reconstruct_unchanged(&mut after, Some(&unchanged()));
    assert_eq!(r.unavailable, 1);
    assert_eq!(after.get("v"), Some(&CanonicalValue::Unchanged));
}
