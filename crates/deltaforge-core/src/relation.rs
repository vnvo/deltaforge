//! The identity of a source relation (a table).
//!
//! A relation is connector + source + database + namespace + table. Caches,
//! registry bindings, idempotency keys and stored records key on its
//! [canonical encoding](SourceRelationIdentity::canonical); names that must
//! follow a target's grammar (Avro) use a deterministic escaping of its
//! parts. [`SourceInfo::namespace`](crate::SourceInfo::namespace) and
//! `namespace.table` are for display, table selection and routing, never a
//! general-purpose uniqueness key.

use std::fmt::Write;

use crate::SourceInfo;

/// A relation of a source.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceRelationIdentity {
    /// `mysql`, `postgresql`, ...
    pub connector: String,
    /// The source that read it (`SourceInfo::name`).
    pub source: String,
    pub database: String,
    /// The namespace inside the database: the PostgreSQL schema; MySQL has
    /// none, so its database.
    pub namespace: String,
    pub table: String,
}

impl SourceRelationIdentity {
    /// The relation an event's source block names.
    pub fn of(source: &SourceInfo) -> Self {
        SourceRelationIdentity {
            connector: source.connector.clone(),
            source: source.name.clone(),
            database: source.db.clone(),
            namespace: source.namespace().to_string(),
            table: source.table.clone(),
        }
    }

    /// The canonical encoding: every part percent-escaped (`%` and `/`),
    /// joined by `/`. Injective: two relations share it only if every part
    /// is equal.
    pub fn canonical(&self) -> String {
        let mut out = String::new();
        for (i, part) in [
            &self.connector,
            &self.source,
            &self.database,
            &self.namespace,
            &self.table,
        ]
        .into_iter()
        .enumerate()
        {
            if i > 0 {
                out.push('/');
            }
            for c in part.chars() {
                match c {
                    '%' => out.push_str("%25"),
                    '/' => out.push_str("%2F"),
                    c => out.push(c),
                }
            }
        }
        out
    }

    /// The relation as one storage-key segment within its source:
    /// database, namespace and table, each escaped as by [`avro_name`]
    /// (ASCII letters, digits and `_` only), joined by `.`. Unambiguous (the
    /// escaped parts hold no `.`) and safe in any object key or path (no
    /// `/`, `%` or `=`), so a hostile identifier neither adds a segment nor
    /// changes when a stored key is read back.
    pub fn key_segment(&self) -> String {
        format!(
            "{}.{}.{}",
            avro_name(&self.database),
            avro_name(&self.namespace),
            avro_name(&self.table)
        )
    }

    /// Decode [`Self::canonical`]: exactly five `/`-separated parts, each
    /// unescaped (`%25` and `%2F` only; any other `%` is refused).
    pub fn from_canonical(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('/').collect();
        let [connector, source, database, namespace, table] = parts[..] else {
            return Err(format!(
                "a relation identity has 5 parts, not {}",
                parts.len()
            ));
        };
        let unescape = |p: &str| -> Result<String, String> {
            let mut out = String::with_capacity(p.len());
            let mut rest = p;
            while let Some(i) = rest.find('%') {
                out.push_str(&rest[..i]);
                match rest.get(i..i + 3) {
                    Some("%25") => out.push('%'),
                    Some("%2F") => out.push('/'),
                    _ => return Err(format!("invalid escape in {p:?}")),
                }
                rest = &rest[i + 3..];
            }
            out.push_str(rest);
            Ok(out)
        };
        Ok(SourceRelationIdentity {
            connector: unescape(connector)?,
            source: unescape(source)?,
            database: unescape(database)?,
            namespace: unescape(namespace)?,
            table: unescape(table)?,
        })
    }

    /// The relation's Avro name path below the connector: database,
    /// namespace (PostgreSQL) and table, each an escaped Avro name (see
    /// [`avro_name`]). MySQL relations have two parts, PostgreSQL relations
    /// three, so the path is unambiguous for a connector.
    pub fn avro_path(&self) -> (String, String) {
        avro_path(&self.database, &self.namespace, &self.table)
    }
}

/// `(namespace path, record-name part)` for a relation: the escaped
/// database, then the escaped namespace when it differs from the database
/// (PostgreSQL), and the escaped table.
pub fn avro_path(
    database: &str,
    namespace: &str,
    table: &str,
) -> (String, String) {
    let path = if namespace == database {
        avro_name(database)
    } else {
        format!("{}.{}", avro_name(database), avro_name(namespace))
    };
    (path, avro_name(table))
}

/// `s` as an Avro name (`[A-Za-z_][A-Za-z0-9_]*`), deterministically and
/// reversibly: ASCII letters and digits stay, except a leading digit; `_`
/// becomes `__`; every other byte (and a leading digit) becomes `_` and two
/// lowercase hex digits. The empty string is `_`.
pub fn avro_name(s: &str) -> String {
    if s.is_empty() {
        return "_".to_string();
    }
    let mut out = String::with_capacity(s.len());
    for (i, b) in s.bytes().enumerate() {
        match b {
            b'_' => out.push_str("__"),
            b'0'..=b'9' if i == 0 => {
                let _ = write!(out, "_{b:02x}");
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' => out.push(b as char),
            _ => {
                let _ = write!(out, "_{b:02x}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(db: &str, ns: &str, table: &str) -> SourceRelationIdentity {
        SourceRelationIdentity {
            connector: "postgresql".into(),
            source: "src".into(),
            database: db.into(),
            namespace: ns.into(),
            table: table.into(),
        }
    }

    #[test]
    fn the_canonical_encoding_is_injective() {
        // Concatenation would make these equal.
        assert_ne!(
            rel("a/b", "c", "t").canonical(),
            rel("a", "b/c", "t").canonical()
        );
        assert_ne!(
            rel("a%2Fb", "c", "t").canonical(),
            rel("a/b", "c", "t").canonical()
        );
        assert_eq!(rel("db", "s", "t").canonical(), "postgresql/src/db/s/t");
    }

    fn full(
        connector: &str,
        source: &str,
        db: &str,
        ns: &str,
        table: &str,
    ) -> SourceRelationIdentity {
        SourceRelationIdentity {
            connector: connector.into(),
            source: source.into(),
            database: db.into(),
            namespace: ns.into(),
            table: table.into(),
        }
    }

    /// Pinned vectors: `/`, `%`, `.`, `_`, Unicode and empty parts, both
    /// encodings, and the canonical round trip.
    #[test]
    fn pinned_identity_vectors() {
        let cases = [
            (
                full("postgresql", "p1", "shop", "public", "orders"),
                "postgresql/p1/shop/public/orders",
                "shop.public.orders",
            ),
            (
                full("mysql", "p/1", "a%b", "a%b", "t/x"),
                "mysql/p%2F1/a%25b/a%25b/t%2Fx",
                "a_25b.a_25b.t_2fx",
            ),
            (
                full("postgresql", "p", "d.b", "s_1", "Ünï"),
                "postgresql/p/d.b/s_1/Ünï",
                "d_2eb.s__1._c3_9cn_c3_af",
            ),
            (
                full("postgresql", "", "", "", ""),
                "postgresql////",
                "_._._",
            ),
            (
                full("postgresql", "p", "1db", "%2F", "_"),
                "postgresql/p/1db/%252F/_",
                "_31db._252F.__",
            ),
        ];
        for (rel, canonical, segment) in cases {
            assert_eq!(rel.canonical(), canonical, "{rel:?}");
            assert_eq!(rel.key_segment(), segment, "{rel:?}");
            assert_eq!(
                SourceRelationIdentity::from_canonical(canonical).unwrap(),
                rel
            );
        }
        // Values that concatenation (`.` or `/` joined) would merge stay apart.
        let pairs = [
            (
                full("c", "s", "a.b", "c", "t"),
                full("c", "s", "a", "b.c", "t"),
            ),
            (
                full("c", "s", "a/b", "c", "t"),
                full("c", "s", "a", "b/c", "t"),
            ),
            (
                full("c", "s/x", "d", "n", "t"),
                full("c", "s", "x/d", "n", "t"),
            ),
            (
                full("c", "s", "a_", "b", "t"),
                full("c", "s", "a", "_b", "t"),
            ),
        ];
        for (a, b) in pairs {
            assert_ne!(a.canonical(), b.canonical(), "{a:?} {b:?}");
            assert_ne!(a.key_segment(), b.key_segment(), "{a:?} {b:?}");
        }
        for bad in ["a/b/c/d", "a/b/c/d/e/f", "a/b/c/d/%2", "a/b/c/d/%41"] {
            assert!(
                SourceRelationIdentity::from_canonical(bad).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_key_segment_is_storage_safe_and_injective() {
        let seg = rel("a/b%", "s=c", "t.u").key_segment();
        assert!(!seg.contains(['/', '%', '=']), "{seg}");
        assert_ne!(
            rel("a.b", "c", "t").key_segment(),
            rel("a", "b.c", "t").key_segment()
        );
        assert_eq!(
            rel("db", "public", "orders").key_segment(),
            "db.public.orders"
        );
    }

    /// MySQL (namespace = database) paths have two parts, PostgreSQL paths
    /// three.
    #[test]
    fn avro_paths_are_pinned() {
        assert_eq!(
            avro_path("shop", "shop", "orders"),
            ("shop".to_string(), "orders".to_string())
        );
        assert_eq!(
            avro_path("postgres", "public", "t_1"),
            ("postgres.public".to_string(), "t__1".to_string())
        );
    }

    #[test]
    fn avro_names_escape_deterministically_and_never_collide() {
        assert_eq!(avro_name("orders"), "orders");
        assert_eq!(avro_name("ident_a"), "ident__a");
        assert_eq!(avro_name("a.b"), "a_2eb");
        assert_eq!(avro_name("9lives"), "_39lives");
        assert_eq!(avro_name(""), "_");
        // `a_2eb` (a table literally so named) and `a.b` differ.
        assert_ne!(avro_name("a_2eb"), avro_name("a.b"));
        assert_ne!(avro_path("x", "y.z", "t"), avro_path("x.y", "z", "t"));
        let ok = |n: &str| {
            n.chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        };
        for s in ["Ünïcode", "with space", "1", "-", "x-y"] {
            assert!(ok(&avro_name(s)), "{s:?} -> {}", avro_name(s));
        }
    }
}
