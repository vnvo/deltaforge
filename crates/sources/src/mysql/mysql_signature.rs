//! Canonical TableMap signature and its comparison with a recorded schema
//! (design spec 7.14 #4, Round 32 rulings).
//!
//! Every field the TableMap carries that can affect decoding or the logical
//! schema assignment is compared against the schema's own definition:
//! column count and order, the exact wire type code, the type metadata
//! (decimal precision/scale, temporal fractional precision, bit width,
//! string/ENUM/SET packing and byte length, BLOB pack length), nullability;
//! and under `binlog_row_metadata=FULL` names, signedness, ENUM/SET values,
//! collations, visibility, geometry subtype and primary-key metadata.
//!
//! The outcome is three-valued. A field the schema cannot answer reliably
//! (a byte length or collation of a version captured before those were
//! recorded) makes the schema UNVERIFIABLE for this TableMap: neither
//! a match nor a non-match, so it can never be selected or contribute to a
//! unique match.

use mysql_binlog_connector_rust::column::column_type::ColumnType;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use sha2::{Digest, Sha256};

use super::mysql_table_schema::{MySqlColumn, MySqlTableSchema};

/// Format of the canonical signature (bound into FULL evidence).
pub(crate) const SIGNATURE_FORMAT: &str = "mysql-tablemap-signature-v1";

/// How a schema relates to a TableMap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Every carried field matches.
    Match,
    /// A carried field contradicts the schema.
    Mismatch(String),
    /// A carried field cannot be compared with this schema.
    Unverifiable(String),
}

/// Whether the TableMap carries FULL metadata (a name for every column).
pub(crate) fn is_full(tm: &TableMapEvent) -> bool {
    tm.table_metadata.as_ref().is_some_and(|m| {
        m.columns.len() == tm.column_types.len()
            && m.columns.iter().all(|c| c.column_name.is_some())
    })
}

/// Every field of a TableMap the comparison uses (not the table id or
/// names, which do not affect decoding), as stored in FULL evidence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Signature {
    pub column_types: Vec<u8>,
    pub column_metas: Vec<u16>,
    pub null_bits: Vec<bool>,
    pub table_metadata: Option<
        mysql_binlog_connector_rust::event::table_map::table_metadata::TableMetadata,
    >,
}

impl Signature {
    pub(crate) fn of(tm: &TableMapEvent) -> Self {
        Self {
            column_types: tm.column_types.clone(),
            column_metas: tm.column_metas.clone(),
            null_bits: tm.null_bits.clone(),
            table_metadata: tm.table_metadata.clone(),
        }
    }

    /// A TableMap with this signature (for comparison).
    pub(crate) fn table_map(&self) -> TableMapEvent {
        TableMapEvent {
            table_id: 0,
            database_name: String::new(),
            table_name: String::new(),
            column_types: self.column_types.clone(),
            column_metas: self.column_metas.clone(),
            null_bits: self.null_bits.clone(),
            table_metadata: self.table_metadata.clone(),
        }
    }

    /// Deterministic digest (with the signature format).
    pub(crate) fn digest(&self) -> String {
        let bytes = serde_json::to_vec(&(SIGNATURE_FORMAT, self))
            .expect("signature serializes");
        hex::encode(Sha256::digest(bytes))
    }
}

/// Base type, lower-cased, without length/attributes ("varchar", "int").
fn base(c: &MySqlColumn) -> String {
    c.data_type.trim().to_ascii_lowercase()
}

/// MySQL flags YEAR unsigned in the binlog although its type does not say so.
fn unsigned(c: &MySqlColumn) -> bool {
    base(c) == "year" || c.column_type.to_ascii_lowercase().contains("unsigned")
}

/// The quoted values of `enum('a','b')` / `set(...)`.
fn quoted_values(column_type: &str) -> Option<Vec<String>> {
    let open = column_type.find('(')?;
    let close = column_type.rfind(')')?;
    let inner: Vec<char> = column_type[open + 1..close].chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < inner.len() {
        if inner[i] != '\'' {
            i += 1;
            continue;
        }
        let mut v = String::new();
        i += 1;
        loop {
            match inner.get(i) {
                None => return None,
                Some('\'') if inner.get(i + 1) == Some(&'\'') => {
                    v.push('\'');
                    i += 2;
                }
                Some('\'') => {
                    i += 1;
                    break;
                }
                Some('\\') => {
                    v.push(*inner.get(i + 1)?);
                    i += 2;
                }
                Some(&ch) => {
                    v.push(ch);
                    i += 1;
                }
            }
        }
        out.push(v);
    }
    Some(out)
}

/// The fractional-seconds precision of a temporal column: recorded, or the
/// `(n)` suffix of its type, or 0.
fn fsp(c: &MySqlColumn) -> i64 {
    if let Some(p) = c.datetime_precision {
        return p;
    }
    let t = c.column_type.to_ascii_lowercase();
    t.find('(')
        .and_then(|o| t[o + 1..].split(')').next())
        .and_then(|n| n.trim().parse().ok())
        .unwrap_or(0)
}

/// What the TableMap must carry for column `c`: exact wire type and the
/// interpretation metadata; `Err` = unverifiable from this schema.
enum Expect {
    /// Exact `(type code, meta)`.
    Exact(u8, u16),
    /// A string-family column (CHAR/BINARY, ENUM, SET) sent as
    /// MYSQL_TYPE_STRING: the real type and its decoded length.
    Str { real: u8, length: u16 },
}

fn expect(c: &MySqlColumn) -> Result<Expect, String> {
    use ColumnType as T;
    let code = |t: T| t as u8;
    let b = base(c);
    let octets = || {
        c.char_octet_length
            .map(|n| n as u16)
            .ok_or_else(|| format!("{}: byte length not recorded", c.name))
    };
    Ok(match b.as_str() {
        "tinyint" | "bool" | "boolean" => Expect::Exact(code(T::Tiny), 0),
        "smallint" => Expect::Exact(code(T::Short), 0),
        "mediumint" => Expect::Exact(code(T::Int24), 0),
        "int" | "integer" => Expect::Exact(code(T::Long), 0),
        "bigint" => Expect::Exact(code(T::LongLong), 0),
        "float" => Expect::Exact(code(T::Float), 4),
        "double" | "real" => Expect::Exact(code(T::Double), 8),
        "decimal" | "numeric" => {
            let p = c.numeric_precision.ok_or("decimal precision unknown")?;
            let s = c.numeric_scale.ok_or("decimal scale unknown")?;
            Expect::Exact(code(T::NewDecimal), ((s as u16) << 8) | (p as u16))
        }
        "bit" => {
            let w = c.numeric_precision.ok_or("bit width unknown")? as u16;
            Expect::Exact(code(T::Bit), ((w / 8) << 8) | (w % 8))
        }
        "date" => Expect::Exact(code(T::Date), 0),
        "year" => Expect::Exact(code(T::Year), 0),
        "time" => Expect::Exact(code(T::Time2), fsp(c) as u16),
        "datetime" => Expect::Exact(code(T::DateTime2), fsp(c) as u16),
        "timestamp" => Expect::Exact(code(T::TimeStamp2), fsp(c) as u16),
        "varchar" => Expect::Exact(code(T::VarChar), octets()?),
        // Binary strings: one byte per character.
        "varbinary" => Expect::Exact(
            code(T::VarChar),
            c.char_max_length.ok_or("varbinary length unknown")? as u16,
        ),
        "char" => Expect::Str {
            real: code(T::String),
            length: octets()?,
        },
        "binary" => Expect::Str {
            real: code(T::String),
            length: c.char_max_length.ok_or("binary length unknown")? as u16,
        },
        "enum" => {
            let n = quoted_values(&c.column_type)
                .ok_or("enum values unparseable")?
                .len();
            Expect::Str {
                real: code(T::Enum),
                length: if n > 255 { 2 } else { 1 },
            }
        }
        "set" => {
            let n = quoted_values(&c.column_type)
                .ok_or("set values unparseable")?
                .len();
            let bytes = n.div_ceil(8);
            Expect::Str {
                real: code(T::Set),
                length: if bytes > 4 { 8 } else { bytes as u16 },
            }
        }
        "tinyblob" | "tinytext" => Expect::Exact(code(T::Blob), 1),
        "blob" | "text" => Expect::Exact(code(T::Blob), 2),
        "mediumblob" | "mediumtext" => Expect::Exact(code(T::Blob), 3),
        "longblob" | "longtext" => Expect::Exact(code(T::Blob), 4),
        "json" => Expect::Exact(code(T::Json), 4),
        "geometry" | "point" | "linestring" | "polygon" | "multipoint"
        | "multilinestring" | "multipolygon" | "geometrycollection"
        | "geomcollection" => Expect::Exact(code(T::Geometry), 4),
        other => {
            return Err(format!("{}: type {other} not comparable", c.name));
        }
    })
}

fn geometry_code(b: &str) -> Option<u32> {
    Some(match b {
        "geometry" => 0,
        "point" => 1,
        "linestring" => 2,
        "polygon" => 3,
        "multipoint" => 4,
        "multilinestring" => 5,
        "multipolygon" => 6,
        "geometrycollection" | "geomcollection" => 7,
        _ => return None,
    })
}

fn is_character(b: &str) -> bool {
    matches!(
        b,
        "char"
            | "varchar"
            | "tinytext"
            | "text"
            | "mediumtext"
            | "longtext"
            | "enum"
            | "set"
    )
}

/// Compare `schema` with every field `tm` carries.
pub(crate) fn compare(
    schema: &MySqlTableSchema,
    tm: &TableMapEvent,
) -> Verdict {
    let mut unverifiable: Option<String> = None;
    macro_rules! mismatch {
        ($($t:tt)*) => { return Verdict::Mismatch(format!($($t)*)) };
    }
    let n = tm.column_types.len();
    if schema.columns.len() != n || tm.column_metas.len() != n {
        mismatch!(
            "{} columns in the binlog, {} in the schema",
            n,
            schema.columns.len()
        );
    }
    let md = tm.table_metadata.as_ref();
    for (i, c) in schema.columns.iter().enumerate() {
        let (code, meta) = (tm.column_types[i], tm.column_metas[i]);
        match expect(c) {
            Err(why) => {
                unverifiable.get_or_insert(why);
            }
            Ok(Expect::Exact(want, want_meta)) => {
                if code != want || meta != want_meta {
                    mismatch!(
                        "column {} ({}): binlog type {code}/{meta}, schema {want}/{want_meta}",
                        i + 1,
                        c.name
                    );
                }
            }
            Ok(Expect::Str { real, length }) => {
                if code != ColumnType::String as u8 {
                    mismatch!(
                        "column {} ({}): binlog type {code}",
                        i + 1,
                        c.name
                    );
                }
                match ColumnType::parse_string_column_meta(meta, code) {
                    Ok((r, l)) if r == real && l == length => {}
                    Ok((r, l)) => mismatch!(
                        "column {} ({}): binlog {r}/{l}, schema {real}/{length}",
                        i + 1,
                        c.name
                    ),
                    Err(_) => mismatch!("column {} meta unparseable", i + 1),
                }
            }
        }
        if let Some(&nullable) = tm.null_bits.get(i) {
            if nullable != c.nullable {
                mismatch!("column {} ({}): nullability", i + 1, c.name);
            }
        }
        let Some(cm) = md.and_then(|m| m.columns.get(i)) else {
            continue;
        };
        let b = base(c);
        if let Some(name) = &cm.column_name {
            if name != &c.name {
                mismatch!(
                    "column {}: binlog name {name}, schema {}",
                    i + 1,
                    c.name
                );
            }
        }
        if let Some(signed) = cm.is_signed {
            if signed == unsigned(c) {
                mismatch!("column {} ({}): signedness", i + 1, c.name);
            }
        }
        let values = match b.as_str() {
            "enum" => cm.enum_string_values.as_ref(),
            "set" => cm.set_string_values.as_ref(),
            _ => None,
        };
        if let Some(values) = values {
            match quoted_values(&c.column_type) {
                Some(v) if &v == values => {}
                Some(_) => {
                    mismatch!("column {} ({}): value list", i + 1, c.name)
                }
                None => {
                    unverifiable
                        .get_or_insert(format!("{}: value list", c.name));
                }
            }
        }
        let collation = if b == "enum" || b == "set" {
            cm.enum_and_set_charset_collation
        } else {
            cm.charset_collation
        };
        if let Some(id) = collation.filter(|_| is_character(&b)) {
            match c.collation_id {
                Some(own) if own == i64::from(id) => {}
                Some(_) => {
                    mismatch!("column {} ({}): collation", i + 1, c.name)
                }
                None => {
                    unverifiable.get_or_insert(format!(
                        "{}: collation not recorded",
                        c.name
                    ));
                }
            }
        }
        if let Some(visible) = cm.is_visible {
            let invisible = c
                .extra
                .as_deref()
                .is_some_and(|e| e.to_ascii_uppercase().contains("INVISIBLE"));
            if visible == invisible {
                mismatch!("column {} ({}): visibility", i + 1, c.name);
            }
        }
        if let (Some(g), Some(own)) = (cm.geometry_type, geometry_code(&b)) {
            if g != own {
                mismatch!("column {} ({}): geometry type", i + 1, c.name);
            }
        }
    }

    // Primary key (FULL): the columns in key order with their prefixes.
    if let Some(md) = md {
        let carried = !md.primary_key.is_empty()
            || md.columns.iter().any(|c| {
                c.is_simple_primary_key.is_some()
                    || c.primary_key_prefix.is_some()
            });
        if carried {
            let tm_pk: Vec<(usize, u32)> = md.primary_key.clone();
            let schema_pk: Option<Vec<(usize, u32)>> = schema
                .primary_key
                .iter()
                .map(|name| {
                    let i =
                        schema.columns.iter().position(|c| &c.name == name)?;
                    let prefix =
                        schema.columns[i].primary_key_prefix.unwrap_or(0);
                    Some((i, prefix as u32))
                })
                .collect();
            if schema_pk.as_ref() != Some(&tm_pk) {
                mismatch!(
                    "primary key {tm_pk:?} vs schema {:?}",
                    schema.primary_key
                );
            }
        }
    }

    match unverifiable {
        Some(why) => Verdict::Unverifiable(why),
        None => Verdict::Match,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enum_and_set_values_parse_with_escapes() {
        assert_eq!(
            quoted_values("enum('a','b''c','d\\\\e')"),
            Some(vec!["a".into(), "b'c".into(), "d\\e".into()])
        );
        assert_eq!(quoted_values("set('x')"), Some(vec!["x".into()]));
        assert_eq!(quoted_values("enum('open"), None);
    }

    fn col(name: &str, data_type: &str, column_type: &str) -> MySqlColumn {
        MySqlColumn::new(name, column_type, data_type, true, 1)
    }

    fn tm(types: &[(u8, u16)]) -> TableMapEvent {
        TableMapEvent {
            table_id: 1,
            database_name: "d".into(),
            table_name: "t".into(),
            column_types: types.iter().map(|t| t.0).collect(),
            column_metas: types.iter().map(|t| t.1).collect(),
            null_bits: vec![true; types.len()],
            table_metadata: None,
        }
    }

    #[test]
    fn exact_types_and_metadata_are_compared() {
        let schema = MySqlTableSchema::new(vec![
            col("a", "int", "int"),
            {
                let mut c = col("b", "decimal", "decimal(10,2)");
                c.numeric_precision = Some(10);
                c.numeric_scale = Some(2);
                c
            },
            col("c", "datetime", "datetime(3)"),
        ]);
        let ok = tm(&[(3, 0), (246, (2 << 8) | 10), (18, 3)]);
        assert_eq!(compare(&schema, &ok), Verdict::Match);
        // Another exact type in the same family, another scale, another fsp.
        for bad in [
            tm(&[(8, 0), (246, (2 << 8) | 10), (18, 3)]),
            tm(&[(3, 0), (246, (3 << 8) | 10), (18, 3)]),
            tm(&[(3, 0), (246, (2 << 8) | 10), (18, 6)]),
            tm(&[(3, 0), (246, (2 << 8) | 10)]),
        ] {
            assert!(matches!(compare(&schema, &bad), Verdict::Mismatch(_)));
        }
    }

    #[test]
    fn a_length_the_schema_did_not_record_is_unverifiable_not_a_match() {
        let old =
            MySqlTableSchema::new(vec![col("s", "varchar", "varchar(10)")]);
        assert!(matches!(
            compare(&old, &tm(&[(15, 40)])),
            Verdict::Unverifiable(_)
        ));
        let mut c = col("s", "varchar", "varchar(10)");
        c.char_octet_length = Some(40);
        let new = MySqlTableSchema::new(vec![c]);
        assert_eq!(compare(&new, &tm(&[(15, 40)])), Verdict::Match);
        assert!(matches!(
            compare(&new, &tm(&[(15, 30)])),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn nullability_is_compared() {
        let mut c = col("a", "int", "int");
        c.nullable = false;
        let schema = MySqlTableSchema::new(vec![c]);
        assert!(matches!(
            compare(&schema, &tm(&[(3, 0)])),
            Verdict::Mismatch(_)
        ));
    }

    /// Live MySQL 8.4 (Docker): the TableMap a real server writes for every
    /// supported type matches the schema captured from its catalog exactly,
    /// with FULL and with MINIMAL row metadata.
    mod live {
        use super::*;
        use crate::mysql::mysql_schema_loader::{Live, fetch_table_schema_on};
        use crate::mysql::mysql_session::open_replication_session;
        use mysql_async::prelude::Queryable;
        use mysql_binlog_connector_rust::binlog_client::BinlogClient;
        use mysql_binlog_connector_rust::event::event_data::EventData;
        use std::time::Duration;
        use testcontainers::core::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        const TABLE: &str = "CREATE TABLE sig.t (
            id INT PRIMARY KEY,
            a TINYINT, b SMALLINT UNSIGNED, c MEDIUMINT, d BIGINT UNSIGNED,
            e FLOAT, f DOUBLE, g DECIMAL(10,2), h BIT(5), i DATE, j YEAR,
            k TIME(2), l DATETIME(3), m TIMESTAMP(6) NULL,
            n VARCHAR(10) CHARACTER SET utf8mb4, o VARBINARY(7),
            p CHAR(5) CHARACTER SET latin1, q BINARY(3),
            r ENUM('x','y''z'), s SET('a','b','c'),
            t1 TINYTEXT, t2 TEXT, t3 MEDIUMBLOB, t4 LONGTEXT, u JSON,
            v POINT, w GEOMETRY, x INT NOT NULL DEFAULT 0
        )";

        /// A composite key out of column order with a prefix, an invisible
        /// column, and mostly one charset with an exception.
        const KEYED: &str = "CREATE TABLE sig.k (
            a INT, b VARCHAR(20) CHARACTER SET utf8mb4, c BLOB,
            d VARCHAR(5) CHARACTER SET latin1, e INT INVISIBLE,
            f TEXT CHARACTER SET utf8mb4,
            PRIMARY KEY (b, a, c(10))
        )";

        async fn table_map(
            port: u16,
            uuid: &str,
            file: String,
            pos: u32,
            table: &str,
        ) -> TableMapEvent {
            let client = BinlogClient {
                url: format!("mysql://root:pw@127.0.0.1:{port}/"),
                server_id: 4_100_001,
                binlog_filename: file,
                binlog_position: pos,
                timeout_secs: 30,
                ..Default::default()
            };
            let mut s = open_replication_session(&client, uuid).await.unwrap();
            loop {
                let (_, data) = s.read().await.unwrap();
                if let EventData::TableMap(tm) = data {
                    if tm.table_name == table {
                        return tm;
                    }
                }
            }
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_real_table_map_matches_its_captured_schema() {
            let c = GenericImage::new("mysql", "8.4")
                .with_wait_for(WaitFor::message_on_stderr(
                    "ready for connections. Version: '8.4",
                ))
                .with_env_var("MYSQL_ROOT_PASSWORD", "pw")
                .with_cmd([
                    "--server-id=71",
                    "--log-bin=mysql-bin",
                    "--binlog-format=ROW",
                ])
                .start()
                .await
                .unwrap();
            let port = c.get_host_port_ipv4(3306).await.unwrap();
            let dsn = format!("mysql://root:pw@127.0.0.1:{port}/");
            let mut conn = loop {
                if let Ok(conn) =
                    mysql_async::Conn::from_url(dsn.as_str()).await
                {
                    break conn;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            };
            let uuid: String = conn
                .query_first("SELECT @@GLOBAL.server_uuid")
                .await
                .unwrap()
                .unwrap();
            conn.query_drop("CREATE DATABASE sig").await.unwrap();
            conn.query_drop(TABLE).await.unwrap();
            conn.query_drop(KEYED).await.unwrap();
            for (mode, full) in [("FULL", true), ("MINIMAL", false)] {
                conn.query_drop(format!(
                    "SET GLOBAL binlog_row_metadata = {mode}"
                ))
                .await
                .unwrap();
                let row: mysql_async::Row = conn
                    .query_first("SHOW BINARY LOG STATUS")
                    .await
                    .unwrap()
                    .unwrap();
                let (file, pos): (String, u64) =
                    (row.get(0).unwrap(), row.get(1).unwrap());
                // A fresh session sees the new global value.
                let mut w =
                    mysql_async::Conn::from_url(dsn.as_str()).await.unwrap();
                let id = if full { 1 } else { 2 };
                w.query_drop(format!("INSERT INTO sig.t (id) VALUES ({id})"))
                    .await
                    .unwrap();
                w.query_drop(format!(
                    "INSERT INTO sig.k (a, b, c) VALUES ({id}, 'k', 'blob')"
                ))
                .await
                .unwrap();
                let keyed =
                    table_map(port, &uuid, file.clone(), pos as u32, "k").await;
                let Live::Found(keyed_schema) =
                    fetch_table_schema_on(&mut conn, &uuid, "sig", "k")
                        .await
                        .unwrap()
                else {
                    panic!("same server");
                };
                assert_eq!(
                    compare(&keyed_schema, &keyed),
                    Verdict::Match,
                    "{mode}: {keyed:?}"
                );
                if full {
                    // The key order matters.
                    let mut swapped = keyed_schema.clone();
                    swapped.primary_key.swap(0, 1);
                    assert!(matches!(
                        compare(&swapped, &keyed),
                        Verdict::Mismatch(_)
                    ));
                }
                let tm = table_map(port, &uuid, file, pos as u32, "t").await;
                assert_eq!(is_full(&tm), full, "{mode}");
                let Live::Found(schema) =
                    fetch_table_schema_on(&mut conn, &uuid, "sig", "t")
                        .await
                        .unwrap()
                else {
                    panic!("same server");
                };
                assert_eq!(
                    compare(&schema, &tm),
                    Verdict::Match,
                    "{mode}: {tm:?}"
                );
                // A schema captured before byte lengths and collations were
                // recorded cannot verify the carried character columns.
                let mut old = schema.clone();
                for col in &mut old.columns {
                    col.char_octet_length = None;
                    col.collation_id = None;
                }
                assert!(
                    matches!(compare(&old, &tm), Verdict::Unverifiable(_)),
                    "{mode}"
                );
            }
        }
    }
}
