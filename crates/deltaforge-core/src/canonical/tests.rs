use serde_json::{Value, json};

use super::durable::{
    row_from_durable, row_to_durable, value_from_durable, value_to_durable,
};
use super::render::{
    RenderFailure, RenderOpts, WideIntegers, render_row, render_value,
};
use super::*;

fn v2() -> RenderOpts {
    RenderOpts::new(TypeMapping::V2)
}

fn v1() -> RenderOpts {
    RenderOpts::new(TypeMapping::V1)
}

fn db(v: DbValue) -> CanonicalValue {
    CanonicalValue::Db(v)
}

fn r(v: &CanonicalValue, opts: RenderOpts) -> Value {
    render_value(v, opts).unwrap()
}

fn date(y: i32, m: u8, d: u8) -> CivilDate {
    CivilDate::new(y, m, d).unwrap()
}

fn pg_type(name: &str, oid: u32) -> SourceType {
    SourceType {
        engine: Engine::Postgres,
        name: name.into(),
        oid: Some(oid),
        typmod: None,
    }
}

// ----- invariants ---------------------------------------------------------

#[test]
fn integers_must_fit_their_width() {
    assert!(DbValue::int(127, IntWidth::W8).is_ok());
    assert!(DbValue::int(128, IntWidth::W8).is_err());
    assert!(DbValue::int(-128, IntWidth::W8).is_ok());
    assert!(DbValue::int(-129, IntWidth::W8).is_err());
    assert!(DbValue::int(-(1 << 23), IntWidth::W24).is_ok());
    assert!(DbValue::int(1 << 23, IntWidth::W24).is_err());
    assert!(DbValue::int(i64::MIN, IntWidth::W64).is_ok());
    assert!(DbValue::uint(16_777_215, IntWidth::W24).is_ok());
    assert!(DbValue::uint(16_777_216, IntWidth::W24).is_err());
    assert!(DbValue::uint(u64::MAX, IntWidth::W64).is_ok());
}

#[test]
fn decimals_have_one_canonical_text() {
    let t =
        |s: &str| render::decimal_text(&Decimal::parse(s).unwrap()).to_string();
    assert_eq!(t("012.50"), "12.50");
    assert_eq!(t("-0.00"), "0.00");
    assert_eq!(t("+3"), "3");
    assert_eq!(t(".5"), "0.5");
    assert_eq!(t("-000"), "0");
    assert_eq!(t("-1.0"), "-1.0");
    for bad in ["", "-", "1.", "1e5", "1.2.3", "abc", "NaN", " 1"] {
        assert!(Decimal::parse(bad).is_err(), "{bad:?} accepted");
    }
}

#[test]
fn bits_hold_exactly_their_width() {
    assert!(Bits::new(1, vec![1]).is_ok());
    assert!(Bits::new(1, vec![2]).is_err(), "bit above BIT(1)");
    assert!(Bits::new(12, vec![0x0f, 0xff]).is_ok());
    assert!(Bits::new(12, vec![0x1f, 0xff]).is_err());
    assert!(Bits::new(12, vec![0xff]).is_err(), "too few bytes");
    assert!(Bits::new(0, vec![]).is_err());
    assert!(Bits::new(65, vec![0; 9]).is_err());
    assert_eq!(Bits::from_u64(64, u64::MAX).unwrap().to_u64(), u64::MAX);
    assert!(Bits::from_u64(8, 256).is_err());
}

#[test]
fn enum_index_zero_is_refused_but_an_empty_label_is_valid() {
    assert!(EnumValue::mysql(0, String::new()).is_err());
    let empty = EnumValue::mysql(2, String::new()).unwrap();
    assert_eq!(r(&db(DbValue::Enum(empty)), v2()), json!(""));
}

#[test]
fn set_members_follow_the_definition_order() {
    let def: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
    let s = SetValue::from_bitmask(0b101, &def).unwrap();
    assert_eq!(s.members(), ["a", "c"]);
    assert!(SetValue::from_bitmask(0b1000, &def).is_err());
    assert_eq!(r(&db(DbValue::Set(s)), v2()), json!("a,c"));
    let empty = SetValue::from_bitmask(0, &def).unwrap();
    assert_eq!(r(&db(DbValue::Set(empty)), v2()), json!(""));
    assert!(SetValue::from_parts(0b11, vec!["a".into()]).is_err());
}

#[test]
fn dates_refuse_invalid_calendar_days_but_keep_mysql_zero_parts() {
    assert!(CivilDate::new(2023, 2, 29).is_err());
    assert!(CivilDate::new(2024, 2, 29).is_ok());
    assert!(CivilDate::new(2024, 0, 1).is_err());
    assert!(CivilDate::mysql(0, 0, 0).is_ok());
    assert!(CivilDate::mysql(2024, 0, 15).is_ok());
    assert!(
        CivilDate::mysql(2024, 2, 30).is_err(),
        "only zero parts relax the calendar"
    );
    assert!(CivilDate::mysql(2024, 13, 0).is_err());
}

#[test]
fn temporal_values_respect_their_precision() {
    assert!(TimeValue::mysql(1_500_000, Precision::Fixed(1)).is_ok());
    assert!(TimeValue::mysql(1_550_000, Precision::Fixed(1)).is_err());
    assert!(
        TimeValue::mysql(
            -((838 * 3600 + 59 * 60 + 59) * 1_000_000),
            Precision::Fixed(0)
        )
        .is_ok()
    );
    assert!(
        TimeValue::mysql((839 * 3600) * 1_000_000, Precision::Fixed(0))
            .is_err()
    );
    assert!(
        TimeValue::time_of_day(MICROS_PER_DAY, Precision::Variable).is_ok()
    );
    assert!(TimeValue::time_of_day(-1, Precision::Variable).is_err());
    assert!(TimestampValue::instant(-1, Precision::Fixed(6)).is_ok());
    assert!(TimestampValue::instant(-1, Precision::Fixed(0)).is_err());
    assert!(TimestampValue::instant(-1_000_000, Precision::Fixed(0)).is_ok());
    assert!(Precision::fixed(7).is_err());
    assert!(
        DateTimeValue::civil(
            date(2024, 1, 1),
            MICROS_PER_DAY,
            Precision::Fixed(0)
        )
        .is_err()
    );
}

#[test]
fn year_is_zero_or_1901_through_2155() {
    assert!(DbValue::year(0).is_ok());
    assert!(DbValue::year(1900).is_err());
    assert!(DbValue::year(1901).is_ok());
    assert!(DbValue::year(2155).is_ok());
    assert!(DbValue::year(2156).is_err());
}

#[test]
fn rows_refuse_duplicate_names_and_keep_order() {
    assert!(
        CanonicalRow::new(vec![
            ("a".into(), CanonicalValue::Null),
            ("a".into(), CanonicalValue::Null)
        ])
        .is_err()
    );
    let mut row = CanonicalRow::new(vec![
        ("b".into(), CanonicalValue::Null),
        ("a".into(), db(DbValue::Bool(true))),
    ])
    .unwrap();
    row.set("a", db(DbValue::Bool(false)));
    row.set("c", CanonicalValue::Null);
    assert_eq!(row.names().collect::<Vec<_>>(), ["b", "a", "c"]);
    assert_eq!(row.remove("b"), Some(CanonicalValue::Null));
    assert_eq!(row.names().collect::<Vec<_>>(), ["a", "c"]);
}

#[test]
fn array_items_are_typed_values_only() {
    let t = pg_type("int4", 23);
    assert!(ArrayValue::new(t.clone(), vec![CanonicalValue::Null]).is_ok());
    for item in [
        CanonicalValue::Unchanged,
        CanonicalValue::Bytes(vec![1]),
        CanonicalValue::Document(Document::new(json!(1))),
    ] {
        assert!(ArrayValue::new(t.clone(), vec![item]).is_err());
    }
}

// ----- native rendering ---------------------------------------------------

#[test]
fn wide_integers_render_as_strings_by_default_in_v2() {
    let big = db(DbValue::int(i64::MIN, IntWidth::W64).unwrap());
    let ubig = db(DbValue::uint(u64::MAX, IntWidth::W64).unwrap());
    let small = db(DbValue::uint(u32::MAX as u64, IntWidth::W32).unwrap());
    assert_eq!(r(&big, v2()), json!("-9223372036854775808"));
    assert_eq!(r(&ubig, v2()), json!("18446744073709551615"));
    assert_eq!(r(&small, v2()), json!(4294967295u64));

    let num = RenderOpts {
        wide_integers: WideIntegers::Number,
        ..v2()
    };
    assert_eq!(r(&big, num), json!(i64::MIN));
    assert_eq!(
        render_value(&ubig, num),
        Err(RenderFailure::WideUnsignedNumber)
    );
    let fits = db(DbValue::uint(i64::MAX as u64, IntWidth::W64).unwrap());
    assert_eq!(r(&fits, num), json!(i64::MAX));

    // v1 keeps numbers, exact for the full unsigned range.
    assert_eq!(r(&ubig, v1()), json!(u64::MAX));
}

#[test]
fn floats_render_shortest_and_non_finite_as_strings() {
    let f = db(DbValue::Float32(1.1));
    assert_eq!(r(&f, v2()).to_string(), "1.1");
    assert_eq!(r(&f, v1()).to_string(), "1.100000023841858");
    assert_eq!(r(&db(DbValue::Float64(0.1)), v2()).to_string(), "0.1");
    assert_eq!(r(&db(DbValue::Float64(f64::NAN)), v2()), json!("NaN"));
    assert_eq!(
        r(&db(DbValue::Float32(f32::INFINITY)), v2()),
        json!("Infinity")
    );
    assert_eq!(
        r(&db(DbValue::Float64(f64::NEG_INFINITY)), v2()),
        json!("-Infinity")
    );
}

#[test]
fn binary_text_and_bits_render_by_type_never_by_content() {
    // UTF-8-looking bytes in a binary column still render as bytes.
    assert_eq!(
        r(&db(DbValue::Binary(b"abc".to_vec())), v2()),
        json!({"_base64": "YWJj"})
    );
    assert_eq!(
        r(&CanonicalValue::Bytes(b"abc".to_vec()), v2()),
        json!({"_base64": "YWJj"})
    );
    assert_eq!(r(&db(DbValue::Text("YWJj".into())), v2()), json!("YWJj"));
    let one = db(DbValue::Bits(Bits::new(1, vec![1]).unwrap()));
    assert_eq!(r(&one, v2()), json!(true));
    let twelve = db(DbValue::Bits(Bits::new(12, vec![0x0a, 0xbc]).unwrap()));
    assert_eq!(r(&twelve, v2()), json!({"_base64": "Crw="}));
    assert_eq!(r(&twelve, v1()), json!(0x0abc));
    assert_eq!(r(&one, v1()), json!(1));
}

#[test]
fn sql_json_null_is_text_and_sql_null_is_null() {
    let jnull = db(DbValue::Json(JsonText::new("null".into()).unwrap()));
    assert_eq!(r(&jnull, v2()), json!("null"));
    assert_eq!(r(&CanonicalValue::Null, v2()), Value::Null);
    assert!(JsonText::new("{bad".into()).is_err());
    // A document embeds its JSON.
    let doc =
        CanonicalValue::Document(Document::new(json!({"b": 1, "a": [null]})));
    assert_eq!(r(&doc, v2()), json!({"a": [null], "b": 1}));
}

#[test]
fn temporal_text_per_version() {
    let t = db(DbValue::Time(
        TimeValue::mysql(
            -(3600 + 1) * 1_000_000 - 500_000,
            Precision::Fixed(1),
        )
        .unwrap(),
    ));
    assert_eq!(r(&t, v2()), json!("-01:00:01.5"));
    assert_eq!(r(&t, v1()), json!("-01:00:01.500000"));
    let long = db(DbValue::Time(
        TimeValue::mysql(838 * 3_600_000_000, Precision::Fixed(0)).unwrap(),
    ));
    assert_eq!(r(&long, v2()), json!("838:00:00"));

    let dt = db(DbValue::DateTime(
        DateTimeValue::civil(
            date(2024, 3, 9),
            (13 * 3600 + 5) * 1_000_000 + 120_000,
            Precision::Fixed(3),
        )
        .unwrap(),
    ));
    assert_eq!(r(&dt, v2()), json!("2024-03-09T13:00:05.120"));
    assert_eq!(r(&dt, v1()), json!("2024-03-09 13:00:05.120000"));
    let zero = db(DbValue::DateTime(
        DateTimeValue::civil(
            CivilDate::mysql(0, 0, 0).unwrap(),
            0,
            Precision::Fixed(0),
        )
        .unwrap(),
    ));
    assert_eq!(r(&zero, v2()), json!("0000-00-00 00:00:00"));

    let ts = db(DbValue::Timestamp(
        TimestampValue::instant(1_711_841_400_000_000, Precision::Fixed(0))
            .unwrap(),
    ));
    assert_eq!(r(&ts, v2()), json!("2024-03-30T23:30:00Z"));
    assert_eq!(r(&ts, v1()), json!(1_711_841_400_000_000i64));
    let pre = db(DbValue::Timestamp(
        TimestampValue::instant(-1, Precision::Fixed(6)).unwrap(),
    ));
    assert_eq!(r(&pre, v2()), json!("1969-12-31T23:59:59.999999Z"));
    let tz = db(DbValue::Timestamp(TimestampValue::Zero {
        precision: Precision::Fixed(2),
    }));
    assert_eq!(r(&tz, v2()), json!("0000-00-00 00:00:00.00"));
    assert_eq!(r(&tz, v1()), json!("0000-00-00 00:00:00.00"));

    let var = db(DbValue::Timestamp(
        TimestampValue::instant(1_500, Precision::Variable).unwrap(),
    ));
    assert_eq!(r(&var, v2()), json!("1970-01-01T00:00:00.0015Z"));
    let inf = db(DbValue::Date(DateValue::NegInfinity));
    assert_eq!(r(&inf, v2()), json!("-infinity"));
    let bc = db(DbValue::Date(DateValue::Civil(date(-44, 3, 15))));
    assert_eq!(r(&bc, v2()), json!("-00044-03-15"));
    let far = db(DbValue::Date(DateValue::Civil(date(12345, 1, 1))));
    assert_eq!(r(&far, v2()), json!("+12345-01-01"));
}

#[test]
fn days_since_epoch_round_trip() {
    for days in [-719_528, -1, 0, 1, 19_000, 2_932_896] {
        let d = CivilDate::from_days_since_epoch(days).unwrap();
        assert_eq!(d.days_since_epoch(), Some(days));
    }
    assert_eq!(date(1970, 1, 1).days_since_epoch(), Some(0));
    assert_eq!(
        CivilDate::mysql(2024, 0, 1).unwrap().days_since_epoch(),
        None
    );
}

#[test]
fn unchanged_columns_are_listed_never_rendered() {
    let row = CanonicalRow::new(vec![
        ("id".into(), db(DbValue::int(1, IntWidth::W32).unwrap())),
        ("big".into(), CanonicalValue::Unchanged),
        ("n".into(), CanonicalValue::Null),
    ])
    .unwrap();
    let out = render_row(&row, v2()).unwrap();
    assert_eq!(Value::Object(out.values), json!({"id": 1, "n": null}));
    assert_eq!(out.unchanged, ["big"]);
    assert_eq!(
        render_value(&CanonicalValue::Unchanged, v2()),
        Err(RenderFailure::Unchanged)
    );
}

#[test]
fn uuid_opaque_and_arrays_render_as_text_and_lists() {
    let u = db(DbValue::Uuid(
        *b"\x12\x34\x56\x78\x9a\xbc\xde\xf0\x12\x34\x56\x78\x9a\xbc\xde\xf0",
    ));
    assert_eq!(r(&u, v2()), json!("12345678-9abc-def0-1234-56789abcdef0"));
    let o = CanonicalValue::Opaque(OpaqueText {
        source_type: pg_type("int4range", 3904),
        text: "[1,5)".into(),
    });
    assert_eq!(r(&o, v2()), json!("[1,5)"));
    let inner = ArrayValue::new(
        pg_type("int8", 20),
        vec![
            db(DbValue::int(1, IntWidth::W64).unwrap()),
            CanonicalValue::Null,
        ],
    )
    .unwrap();
    let outer = db(DbValue::Array(
        ArrayValue::new(pg_type("int8", 20), vec![db(DbValue::Array(inner))])
            .unwrap(),
    ));
    assert_eq!(r(&outer, v2()), json!([["1", null]]));
}

// ----- durable form -------------------------------------------------------

fn every_value() -> Vec<CanonicalValue> {
    let def: Vec<String> = ["x", "y"].map(String::from).to_vec();
    vec![
        CanonicalValue::Null,
        CanonicalValue::Unchanged,
        db(DbValue::int(i64::MIN, IntWidth::W64).unwrap()),
        db(DbValue::int(-5, IntWidth::W8).unwrap()),
        db(DbValue::uint(u64::MAX, IntWidth::W64).unwrap()),
        db(DbValue::Decimal(Decimal::parse("-12.3400").unwrap())),
        db(DbValue::Decimal(Decimal::NaN)),
        db(DbValue::Decimal(Decimal::NegInfinity)),
        db(DbValue::Float32(1.1)),
        db(DbValue::Float32(-0.0)),
        db(DbValue::Float64(f64::NAN)),
        db(DbValue::Float64(-0.0)),
        db(DbValue::Float64(f64::INFINITY)),
        db(DbValue::Bool(false)),
        db(DbValue::Text("héllo\u{0}".into())),
        db(DbValue::Binary(vec![0, 255, 0])),
        db(DbValue::Bits(Bits::new(12, vec![0x0f, 0x01]).unwrap())),
        db(DbValue::Enum(EnumValue::mysql(3, String::new()).unwrap())),
        db(DbValue::Set(SetValue::from_bitmask(0b10, &def).unwrap())),
        db(DbValue::Json(
            JsonText::new("{\"a\": 12345678901234567890}".into()).unwrap(),
        )),
        db(DbValue::Date(DateValue::Civil(
            CivilDate::mysql(2024, 0, 0).unwrap(),
        ))),
        db(DbValue::Date(DateValue::PosInfinity)),
        db(DbValue::Time(
            TimeValue::mysql(-1_000, Precision::Fixed(3)).unwrap(),
        )),
        db(DbValue::Time(
            TimeValue::time_of_day(MICROS_PER_DAY, Precision::Variable)
                .unwrap(),
        )),
        db(DbValue::DateTime(
            DateTimeValue::civil(date(1, 1, 1), 5, Precision::Fixed(6))
                .unwrap(),
        )),
        db(DbValue::DateTime(DateTimeValue::NegInfinity)),
        db(DbValue::Timestamp(
            TimestampValue::instant(
                -62_135_596_800_000_000,
                Precision::Variable,
            )
            .unwrap(),
        )),
        db(DbValue::Timestamp(TimestampValue::Zero {
            precision: Precision::Fixed(0),
        })),
        db(DbValue::Timestamp(TimestampValue::PosInfinity)),
        db(DbValue::year(0).unwrap()),
        db(DbValue::Uuid([7; 16])),
        db(DbValue::Array(
            ArrayValue::new(
                pg_type("text", 25),
                vec![db(DbValue::Text("a".into())), CanonicalValue::Null],
            )
            .unwrap(),
        )),
        CanonicalValue::Opaque(OpaqueText {
            source_type: SourceType {
                engine: Engine::Postgres,
                name: "mood".into(),
                oid: Some(16_390),
                typmod: Some(-1),
            },
            text: "happy".into(),
        }),
        CanonicalValue::Document(Document::new(json!(null))),
        CanonicalValue::Document(Document::new(
            json!({"z": 1, "a": {"y": [1.5, "s"], "b": true}}),
        )),
        CanonicalValue::Bytes(vec![]),
    ]
}

/// Bitwise equality, so NaN payloads and -0.0 count.
fn same(a: &CanonicalValue, b: &CanonicalValue) -> bool {
    value_to_durable(a) == value_to_durable(b)
        && format!("{a:?}") == format!("{b:?}")
}

#[test]
fn every_value_round_trips_through_the_durable_form() {
    for v in every_value() {
        let text = serde_json::to_string(&v).unwrap();
        let back: CanonicalValue = serde_json::from_str(&text).unwrap();
        assert!(same(&v, &back), "{v:?} came back as {back:?}");
    }
}

#[test]
fn durable_form_keeps_sql_null_json_null_and_unchanged_apart() {
    let forms: Vec<Value> = [
        CanonicalValue::Null,
        CanonicalValue::Document(Document::new(json!(null))),
        db(DbValue::Json(JsonText::new("null".into()).unwrap())),
        CanonicalValue::Unchanged,
    ]
    .iter()
    .map(value_to_durable)
    .collect();
    for (i, a) in forms.iter().enumerate() {
        for b in &forms[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn durable_documents_are_canonical_text() {
    let a = CanonicalValue::Document(Document::new(json!({"b": 1, "a": 2})));
    assert_eq!(
        value_to_durable(&a),
        json!({"t": "doc", "v": "{\"a\":2,\"b\":1}"})
    );
}

#[test]
fn durable_rows_keep_column_order() {
    let row = CanonicalRow::new(vec![
        ("z".into(), db(DbValue::Bool(true))),
        ("a".into(), CanonicalValue::Unchanged),
    ])
    .unwrap();
    let back = row_from_durable(&row_to_durable(&row)).unwrap();
    assert_eq!(back.names().collect::<Vec<_>>(), ["z", "a"]);
    assert_eq!(back, row);
    assert!(row_from_durable(&json!([["a", null], ["a", null]])).is_err());
}

#[test]
fn malformed_durable_values_are_refused() {
    for bad in [
        json!(1),
        json!("x"),
        json!({}),
        json!({"t": "nope"}),
        json!({"t": "int", "w": 8, "v": "128"}),
        json!({"t": "int", "w": 12, "v": "1"}),
        json!({"t": "int", "w": 8, "v": 1}),
        json!({"t": "dec", "v": "01.0"}),
        json!({"t": "enum", "v": "", "i": 0}),
        json!({"t": "bits", "n": 1, "v": "Ag=="}),
        json!({"t": "date", "y": 2023, "m": 2, "d": 29}),
        json!({"t": "date", "inf": 2}),
        json!({"t": "ts", "us": "-1", "p": 0}),
        json!({"t": "ts", "zero": false, "p": 0}),
        json!({"t": "time", "us": "0", "p": 7}),
        json!({"t": "year", "v": 1900}),
        json!({"t": "uuid", "v": "00"}),
        json!({"t": "json", "v": "{"}),
        json!({"t": "set", "b": "3", "v": ["a"]}),
        json!({"t": "array", "e": {"engine": "postgres", "name": "int4"}, "v": [{"t": "unchanged"}]}),
        json!({"t": "f64", "v": "zz"}),
    ] {
        assert!(value_from_durable(&bad).is_err(), "{bad} accepted");
    }
}

#[test]
fn type_mapping_parses_only_known_versions() {
    assert_eq!(TypeMapping::default(), TypeMapping::V2);
    assert_eq!(TypeMapping::parse("v1"), Some(TypeMapping::V1));
    assert_eq!(TypeMapping::parse("v3"), None);
    assert_eq!(serde_json::to_value(TypeMapping::V1).unwrap(), json!("v1"));
    assert!(serde_json::from_value::<TypeMapping>(json!("v3")).is_err());
}

// ----- extensions -----------------------------------------------------------

fn ext_row() -> CanonicalRow {
    CanonicalRow::with_extensions(
        vec![("id".into(), db(DbValue::int(1, IntWidth::W32).unwrap()))],
        vec![
            ("tags".to_string(), Document::new(json!(["a"]))),
            ("meta".to_string(), Document::new(json!({"b": 2, "a": 1}))),
        ],
    )
    .unwrap()
}

#[test]
fn extensions_never_take_a_column_name_or_a_conflicting_path() {
    let mut row = ext_row();
    assert!(row.set_extension("id", Document::new(json!(1))).is_err());
    assert!(
        row.set_extension("meta.a", Document::new(json!(1)))
            .is_err()
    );
    let mut row2 = CanonicalRow::with_extensions(
        vec![],
        vec![("a.b".to_string(), Document::new(json!(1)))],
    )
    .unwrap();
    assert!(row2.set_extension("a", Document::new(json!(1))).is_err());
    // "ab" is not a path under "a".
    assert!(row2.set_extension("ab", Document::new(json!(1))).is_ok());
    // Replacing an extension is a modification.
    assert!(row.set_extension("tags", Document::new(json!([]))).is_ok());
    assert_eq!(row.extensions()["tags"], Document::new(json!([])));
}

#[test]
fn extensions_render_merged_and_round_trip_durably() {
    let row = ext_row();
    let out = render_row(&row, v2()).unwrap();
    assert_eq!(
        Value::Object(out.values),
        json!({"id": 1, "tags": ["a"], "meta": {"a": 1, "b": 2}})
    );
    let back = row_from_durable(&row_to_durable(&row)).unwrap();
    assert_eq!(back, row);
    // Deterministic: canonical text, names in order; no extensions, no part.
    assert_eq!(
        row_to_durable(&row)["x"],
        json!({"meta": "{\"a\":1,\"b\":2}", "tags": "[\"a\"]"})
    );
    let plain =
        CanonicalRow::new(vec![("id".into(), CanonicalValue::Null)]).unwrap();
    assert!(row_to_durable(&plain).get("x").is_none());
    assert!(
        row_from_durable(&json!({"c": [], "x": {}})).is_err(),
        "one form for no extensions"
    );
}

#[test]
fn an_extension_never_overwrites_an_unchanged_column_in_rendering() {
    let mut row =
        CanonicalRow::new(vec![("doc".into(), CanonicalValue::Unchanged)])
            .unwrap();
    // set_extension refuses a column name outright...
    assert!(row.set_extension("doc", Document::new(json!(1))).is_err());
    // ...and the renderer refuses any collision it meets.
    let row = CanonicalRow::with_extensions(
        vec![],
        vec![("x".to_string(), Document::new(json!(1)))],
    )
    .unwrap();
    assert!(render_row(&row, v2()).is_ok());
}

// ----- rendering golden ---------------------------------------------------

/// Temporal values at every precision form, the year edges and MySQL's
/// zero parts.
fn temporal_values() -> Vec<CanonicalValue> {
    let mut out = Vec::new();
    for (p, frac) in [
        (Precision::Fixed(0), 0),
        (Precision::Fixed(1), 500_000),
        (Precision::Fixed(3), 123_000),
        (Precision::Fixed(5), 987_650),
        (Precision::Fixed(6), 123_456),
        (Precision::Fixed(6), 0),
        (Precision::Variable, 120_000),
        (Precision::Variable, 0),
        (Precision::Variable, 7),
    ] {
        let max_time = (838 * 3600 + 59 * 60 + 59) * 1_000_000;
        out.extend([
            db(DbValue::Time(TimeValue::mysql(frac, p).unwrap())),
            db(DbValue::Time(
                TimeValue::mysql(-(max_time + frac), p).unwrap(),
            )),
            db(DbValue::Time(
                TimeValue::mysql(25 * 3_600_000_000 + frac, p).unwrap(),
            )),
            db(DbValue::Time(
                TimeValue::time_of_day(3_723_000_000 + frac, p).unwrap(),
            )),
            db(DbValue::DateTime(
                DateTimeValue::civil(
                    date(9999, 12, 31),
                    86_399_000_000 + frac,
                    p,
                )
                .unwrap(),
            )),
            db(DbValue::DateTime(
                DateTimeValue::civil(
                    CivilDate::mysql(0, 0, 0).unwrap(),
                    frac,
                    p,
                )
                .unwrap(),
            )),
            db(DbValue::DateTime(
                DateTimeValue::civil(
                    CivilDate::mysql(2024, 2, 0).unwrap(),
                    45_296_000_000 + frac,
                    p,
                )
                .unwrap(),
            )),
            db(DbValue::Timestamp(
                TimestampValue::instant(1_760_000_000_000_000 + frac, p)
                    .unwrap(),
            )),
            db(DbValue::Timestamp(
                TimestampValue::instant(-100_000_000_000_000_000 + frac, p)
                    .unwrap(),
            )),
            db(DbValue::Timestamp(
                TimestampValue::instant(300_000_000_000_000_000 + frac, p)
                    .unwrap(),
            )),
            db(DbValue::Timestamp(TimestampValue::Zero { precision: p })),
        ]);
    }
    for days in [0, -1, -719_528, -1_000_000, 2_932_896, 3_000_000] {
        out.push(db(DbValue::Date(DateValue::Civil(
            CivilDate::from_days_since_epoch(days).unwrap(),
        ))));
    }
    out.push(db(DbValue::Date(DateValue::NegInfinity)));
    out
}

/// Checks a row's streamed image against its rendered map.
type StreamCheck =
    fn(&CanonicalRow, RenderOpts, &serde_json::Map<String, Value>);

/// A rendered map's JSON with its keys in the row's order (then the
/// extensions'), whatever order the map keeps.
fn in_row_order(
    row: &CanonicalRow,
    map: &serde_json::Map<String, Value>,
) -> String {
    let names = row
        .names()
        .chain(row.extensions().keys().map(String::as_str))
        .filter(|n| map.contains_key(*n));
    let entries: Vec<String> = names
        .map(|n| format!("{}:{}", Value::from(n), map[n]))
        .collect();
    assert_eq!(entries.len(), map.len());
    format!("{{{}}}", entries.join(","))
}

/// Every option set's rendering of a row holding every value (each value
/// a column; values the options refuse are left out), one line each.
fn rendering_golden(streamed: Option<StreamCheck>) -> String {
    let mut values = every_value();
    values.extend(temporal_values());
    let mut out = String::new();
    for mapping in [TypeMapping::V1, TypeMapping::V2] {
        for wide_integers in [WideIntegers::String, WideIntegers::Number] {
            let opts = RenderOpts {
                mapping,
                wide_integers,
            };
            let cols = values
                .iter()
                .enumerate()
                .filter(|(_, v)| {
                    matches!(v, CanonicalValue::Unchanged)
                        || render_value(v, opts).is_ok()
                })
                .map(|(i, v)| (format!("c{i}"), v.clone()))
                .collect();
            let row = CanonicalRow::new(cols).unwrap();
            let rendered = render_row(&row, opts).unwrap();
            if let Some(check) = streamed {
                check(&row, opts, &rendered.values);
            }
            out.push_str(&format!(
                "{mapping:?} {wide_integers:?} {} {:?}\n",
                in_row_order(&row, &rendered.values),
                rendered.unchanged
            ));
        }
    }
    out
}

/// The native rendering is exactly the reviewed one (generated before
/// rendering was made to stream, `DF_WRITE_RENDER_GOLDEN=1` rewrites it).
#[test]
fn rendering_matches_the_golden_bytes() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/canonical/render_golden.txt"
    );
    // The streamed image writes exactly the bytes of the rendered map.
    let actual = rendering_golden(Some(|row, opts, map| {
        let (image, _) = super::render::row_image(row, opts).unwrap();
        assert_eq!(
            serde_json::to_string(&image).unwrap(),
            in_row_order(row, map)
        );
    }));
    if std::env::var_os("DF_WRITE_RENDER_GOLDEN").is_some() {
        std::fs::write(path, &actual).unwrap();
    }
    let golden = std::fs::read_to_string(path).unwrap();
    for (a, g) in actual.lines().zip(golden.lines()) {
        assert_eq!(a, g);
    }
    assert_eq!(actual.lines().count(), golden.lines().count());
}

#[test]
fn a_hinted_lookup_is_a_lookup_by_name() {
    let row = CanonicalRow::new(vec![
        ("a".into(), CanonicalValue::Null),
        ("b".into(), db(DbValue::Bool(true))),
    ])
    .unwrap();
    for hint in 0..4 {
        assert_eq!(row.get_hinted(hint, "b"), row.get("b"));
        assert_eq!(row.get_hinted(hint, "a"), row.get("a"));
        assert_eq!(row.get_hinted(hint, "c"), None);
    }
}
