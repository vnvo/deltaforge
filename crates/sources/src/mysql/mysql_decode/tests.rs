//! The decoder's per-type rules, and snapshot/CDC parity: the same stored
//! value, as each path's wire value, decodes to the same canonical value.

use deltaforge_core::canonical::render::{RenderOpts, render_value};
use deltaforge_core::canonical::{
    CanonicalValue, DbValue, IntWidth, Precision, TimestampValue, TypeMapping,
};
use mysql_async::Value as Snap;
use mysql_binlog_connector_rust::column::column_value::{
    ColumnValue as Cdc, MysqlDate, MysqlDateTime, MysqlTime,
};
use serde_json::json;

use super::*;

fn col(column_type: &str, data_type: &str) -> MySqlColumn {
    let mut c = MySqlColumn::new("c", column_type, data_type, true, 1);
    c.collation_id = Some(255);
    c
}

fn dec(c: &MySqlColumn) -> ColumnDecoder {
    ColumnDecoder::new(c)
}

fn cdc(c: &MySqlColumn, v: Cdc) -> Result<CanonicalValue, DecodeError> {
    dec(c).decode(Raw::from_binlog(&v))
}

fn snap(c: &MySqlColumn, v: Snap) -> Result<CanonicalValue, DecodeError> {
    let d = dec(c);
    let kind = d.kind.clone().unwrap();
    d.decode(Raw::from_snapshot(&v, &kind))
}

/// Both paths decode to the same value; returns it rendered as v2 JSON.
fn parity(c: &MySqlColumn, binlog: Cdc, snapshot: Snap) -> serde_json::Value {
    let a = cdc(c, binlog).unwrap();
    let b = snap(c, snapshot).unwrap();
    assert_eq!(a, b, "{} snapshot and CDC differ", c.column_type);
    render_value(&a, RenderOpts::new(TypeMapping::V2)).unwrap()
}

#[test]
fn unsigned_integers_reinterpret_binlog_bits_at_their_width() {
    let t = col("tinyint unsigned", "tinyint");
    assert_eq!(parity(&t, Cdc::Tiny(-1), Snap::UInt(255)), json!(255));
    let m = col("mediumint unsigned", "mediumint");
    assert_eq!(
        parity(&m, Cdc::Long(-1), Snap::Int(16_777_215)),
        json!(16_777_215)
    );
    let b = col("bigint unsigned", "bigint");
    assert_eq!(
        parity(&b, Cdc::LongLong(-1), Snap::UInt(u64::MAX)),
        json!("18446744073709551615")
    );
    let s = col("smallint", "smallint");
    assert_eq!(parity(&s, Cdc::Short(-5), Snap::Int(-5)), json!(-5));
    // A snapshot value can never need reinterpretation.
    assert!(snap(&t, Snap::Int(-1)).is_err());
    assert!(snap(&col("tinyint", "tinyint"), Snap::Int(128)).is_err());
}

#[test]
fn decimals_keep_their_scale() {
    let d = col("decimal(10,2)", "decimal");
    assert_eq!(
        parity(
            &d,
            Cdc::Decimal("-0.50".into()),
            Snap::Bytes(b"-0.50".to_vec())
        ),
        json!("-0.50")
    );
    assert!(snap(&d, Snap::Bytes(b"1e5".to_vec())).is_err());
}

#[test]
fn floats_keep_their_width() {
    let f = col("float", "float");
    assert_eq!(
        parity(&f, Cdc::Float(1.1), Snap::Float(1.1)).to_string(),
        "1.1"
    );
    let d = col("double", "double");
    assert_eq!(
        parity(&d, Cdc::Double(0.1), Snap::Double(0.1)).to_string(),
        "0.1"
    );
}

#[test]
fn bits_are_fixed_width_bytes_or_a_boolean() {
    let one = col("bit(1)", "bit");
    assert_eq!(parity(&one, Cdc::Bit(1), Snap::Bytes(vec![1])), json!(true));
    let twelve = col("bit(12)", "bit");
    assert_eq!(
        parity(&twelve, Cdc::Bit(0x0abc), Snap::Bytes(vec![0x0a, 0xbc])),
        json!({"_base64": "Crw="})
    );
    assert!(cdc(&twelve, Cdc::Bit(0x1000)).is_err());
}

#[test]
fn text_decodes_from_the_column_charset() {
    let mut latin1 = col("varchar(10)", "varchar");
    latin1.collation_id = Some(8);
    assert_eq!(
        parity(
            &latin1,
            Cdc::String(vec![0x80, 0xE9, 0x81]),
            Snap::Bytes(vec![0x80, 0xE9, 0x81])
        ),
        json!("\u{20AC}\u{e9}\u{81}")
    );
    let utf8 = col("text", "text");
    assert_eq!(
        parity(
            &utf8,
            Cdc::Blob("héllo".into()),
            Snap::Bytes("héllo".into())
        ),
        json!("héllo")
    );
    // Undecodable bytes fail closed; nothing is replaced or wrapped.
    assert!(cdc(&utf8, Cdc::Blob(vec![0xff])).is_err());
    let mut ascii = col("varchar(3)", "varchar");
    ascii.collation_id = Some(11);
    assert!(cdc(&ascii, Cdc::String(vec![0x80])).is_err());
    let mut mb3 = col("varchar(3)", "varchar");
    mb3.collation_id = Some(33);
    assert!(cdc(&mb3, Cdc::String("😀".into())).is_err());
    let mut unknown = col("varchar(3)", "varchar");
    unknown.collation_id = Some(28); // gbk
    assert!(cdc(&unknown, Cdc::String(b"a".to_vec())).is_err());
    unknown.collation_id = None;
    assert!(cdc(&unknown, Cdc::String(b"a".to_vec())).is_err());
}

#[test]
fn char_drops_trailing_spaces_and_varchar_keeps_them() {
    let c = col("char(5)", "char");
    assert_eq!(
        parity(
            &c,
            Cdc::String(b"ab".to_vec()),
            Snap::Bytes(b"ab   ".to_vec())
        ),
        json!("ab")
    );
    let v = col("varchar(5)", "varchar");
    assert_eq!(
        parity(
            &v,
            Cdc::String(b"ab ".to_vec()),
            Snap::Bytes(b"ab ".to_vec())
        ),
        json!("ab ")
    );
}

#[test]
fn binary_keeps_its_padding_and_never_becomes_text() {
    let mut b = col("binary(4)", "binary");
    b.char_octet_length = Some(4);
    assert_eq!(
        parity(
            &b,
            Cdc::String(b"ab".to_vec()),
            Snap::Bytes(b"ab\0\0".to_vec())
        ),
        json!({"_base64": "YWIAAA=="})
    );
    assert!(cdc(&b, Cdc::String(b"abcde".to_vec())).is_err());
    let vb = col("varbinary(8)", "varbinary");
    assert_eq!(
        parity(
            &vb,
            Cdc::String(b"ok".to_vec()),
            Snap::Bytes(b"ok".to_vec())
        ),
        json!({"_base64": "b2s="})
    );
    let blob = col("blob", "blob");
    assert_eq!(
        parity(&blob, Cdc::Blob(vec![]), Snap::Bytes(vec![])),
        json!({"_base64": ""})
    );
}

#[test]
fn enum_and_set_decode_from_the_stored_number() {
    let e = col("enum('a','','b''c')", "enum");
    assert_eq!(parity(&e, Cdc::Enum(2), Snap::Int(2)), json!(""));
    assert_eq!(parity(&e, Cdc::Enum(3), Snap::UInt(3)), json!("b'c"));
    // Index 0 is MySQL's error value, never the declared '' label.
    let err = cdc(&e, Cdc::Enum(0)).unwrap_err();
    assert!(err.reason.contains("index 0"), "{err}");
    assert!(snap(&e, Snap::Int(0)).is_err());
    assert!(cdc(&e, Cdc::Enum(4)).is_err());

    let s = col("set('x','y','z')", "set");
    assert_eq!(parity(&s, Cdc::Set(0b101), Snap::Int(0b101)), json!("x,z"));
    assert_eq!(parity(&s, Cdc::Set(0), Snap::Int(0)), json!(""));
    assert!(cdc(&s, Cdc::Set(0b1000)).is_err());
    let d = dec(&s);
    assert!(
        TableDecoder { columns: vec![d] }
            .snapshot_select_list()
            .contains("CAST(`c` AS UNSIGNED) AS `c`")
    );
}

#[test]
fn json_text_comes_from_binary_json_and_snapshot_text_alike() {
    // {"a": 1, "bb": "x"} as MySQL's binary JSON (small object).
    let jsonb = vec![
        0x00, 0x02, 0x00, 0x17, 0x00, // small object: 2 entries, 23 bytes
        0x12, 0x00, 0x01, 0x00, 0x13, 0x00, 0x02, 0x00, // key entries
        0x05, 0x01, 0x00, 0x0c, 0x15,
        0x00, // values: int16 1 inline, string at 21
        b'a', b'b', b'b', 0x01, b'x',
    ];
    let j = col("json", "json");
    assert_eq!(
        parity(
            &j,
            Cdc::Json(jsonb),
            Snap::Bytes(br#"{"a": 1, "bb": "x"}"#.to_vec())
        ),
        json!(r#"{"a": 1, "bb": "x"}"#)
    );
    // JSON null is the text "null", never SQL NULL.
    assert_eq!(
        parity(
            &j,
            Cdc::Json(vec![0x04, 0x00]),
            Snap::Bytes(b"null".to_vec())
        ),
        json!("null")
    );
    assert_eq!(cdc(&j, Cdc::None).unwrap(), CanonicalValue::Null);
    assert!(cdc(&j, Cdc::Json(vec![0x7f])).is_err());
}

#[test]
fn temporal_values_keep_their_category_and_precision() {
    let d = col("date", "date");
    let date = |y, m, d| MysqlDate {
        year: y,
        month: m,
        day: d,
    };
    assert_eq!(
        parity(
            &d,
            Cdc::Date(date(2024, 2, 29)),
            Snap::Date(2024, 2, 29, 0, 0, 0, 0)
        ),
        json!("2024-02-29")
    );
    assert_eq!(
        parity(
            &d,
            Cdc::Date(date(0, 0, 0)),
            Snap::Date(0, 0, 0, 0, 0, 0, 0)
        ),
        json!("0000-00-00")
    );
    assert!(cdc(&d, Cdc::Date(date(2023, 2, 29))).is_err());

    let mut t = col("time(3)", "time");
    t.datetime_precision = Some(3);
    let tv = MysqlTime {
        negative: true,
        hours: 838,
        minutes: 59,
        seconds: 58,
        micros: 500_000,
    };
    assert_eq!(
        parity(&t, Cdc::Time(tv), Snap::Time(true, 34, 22, 59, 58, 500_000)),
        json!("-838:59:58.500")
    );
    // A fraction finer than the declared precision is not this column's value.
    let fine = MysqlTime {
        micros: 500_001,
        ..tv
    };
    assert!(cdc(&t, Cdc::Time(fine)).is_err());

    let dt = col("datetime", "datetime");
    let dtv = MysqlDateTime {
        date: date(2024, 3, 9),
        hour: 1,
        minute: 2,
        second: 3,
        micros: 0,
    };
    assert_eq!(
        parity(&dt, Cdc::DateTime(dtv), Snap::Date(2024, 3, 9, 1, 2, 3, 0)),
        json!("2024-03-09T01:02:03")
    );
    let zero = MysqlDateTime {
        date: date(0, 0, 0),
        hour: 0,
        minute: 0,
        second: 0,
        micros: 0,
    };
    assert_eq!(
        parity(&dt, Cdc::DateTime(zero), Snap::Date(0, 0, 0, 0, 0, 0, 0)),
        json!("0000-00-00 00:00:00")
    );

    let mut ts = col("timestamp(6)", "timestamp");
    ts.datetime_precision = Some(6);
    assert_eq!(
        parity(
            &ts,
            Cdc::Timestamp(1_729_990_230_654_321),
            Snap::Date(2024, 10, 27, 0, 50, 30, 654_321)
        ),
        json!("2024-10-27T00:50:30.654321Z")
    );
    assert_eq!(
        cdc(&ts, Cdc::Timestamp(0)).unwrap(),
        snap(&ts, Snap::Date(0, 0, 0, 0, 0, 0, 0)).unwrap()
    );
    assert_eq!(
        cdc(&ts, Cdc::Timestamp(0)).unwrap(),
        CanonicalValue::Db(DbValue::Timestamp(TimestampValue::Zero {
            precision: Precision::Fixed(6)
        }))
    );
}

#[test]
fn year_zero_stays_zero() {
    let y = col("year", "year");
    assert_eq!(parity(&y, Cdc::Year(0), Snap::Int(0)), json!(0));
    assert_eq!(parity(&y, Cdc::Year(2155), Snap::Int(2155)), json!(2155));
    assert!(snap(&y, Snap::Int(1900)).is_err());
}

#[test]
fn an_unsupported_type_fails_only_for_a_value() {
    let v = col("vector(3)", "vector");
    assert_eq!(cdc(&v, Cdc::None).unwrap(), CanonicalValue::Null);
    let err = cdc(&v, Cdc::Blob(vec![0; 12])).unwrap_err();
    assert_eq!(err.column, "c");
    assert!(err.reason.contains("vector"), "{err}");
}

#[test]
fn errors_never_carry_the_value() {
    let i = col("int", "int");
    let err = snap(&i, Snap::Bytes(b"topsecret".to_vec()))
        .unwrap_err()
        .to_string();
    assert!(!err.contains("topsecret"), "{err}");
    let t = col("varchar(9)", "varchar");
    let err = cdc(&t, Cdc::String(b"topsecret\xff".to_vec()))
        .unwrap_err()
        .to_string();
    assert!(!err.contains("topsecret"), "{err}");
}

#[test]
fn rows_decode_by_ordinal_with_the_image_bitmap() {
    let schema = MySqlTableSchema::new(vec![
        MySqlColumn::new("id", "int", "int", false, 1),
        MySqlColumn::new("e", "enum('a','b')", "enum", true, 2),
        MySqlColumn::new("n", "int", "int", true, 3),
    ]);
    let t = TableDecoder::new(&schema);
    let row = t
        .decode_binlog_row(&[true, false, true], &[Cdc::Long(7), Cdc::None])
        .unwrap();
    assert_eq!(row.names().collect::<Vec<_>>(), ["id", "n"]);
    assert_eq!(
        row.get("id"),
        Some(&CanonicalValue::Db(DbValue::int(7, IntWidth::W32).unwrap()))
    );
    assert!(t.decode_binlog_row(&[true, true, true, true], &[]).is_err());
    assert_eq!(
        t.snapshot_select_list(),
        "`id`, CAST(`e` AS UNSIGNED) AS `e`, `n`"
    );
    let snap_row = t
        .decode_snapshot_row(&[Snap::Int(7), Snap::Int(2), Snap::NULL])
        .unwrap();
    assert_eq!(
        snap_row
            .get("e")
            .map(|v| render_value(v, RenderOpts::default()).unwrap()),
        Some(json!("b"))
    );
    assert!(t.decode_snapshot_row(&[Snap::Int(7)]).is_err());
}
