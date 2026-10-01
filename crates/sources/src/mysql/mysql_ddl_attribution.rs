//! Which tables a MySQL DDL statement can change (design spec 7.5, 7.14).
//!
//! The same classifier serves streaming and the baseline pre-scan. It never
//! guesses: a statement is attributed to explicit tables only when every
//! affected base object is identified with certainty; otherwise it yields a
//! barrier, scoped to one database only when the statement positively proves
//! that every affected object lies in it, and lineage-wide in every other
//! case (uncertain identifiers, qualifiers, table lists, versioned or hint
//! comments, possible ANSI-quoted identifiers, several statements, unknown
//! object types, an unknown default database).

// Used by the MySQL source (DDL records) and the baseline pre-scan in later
// commits; this allow is removed there.
#![allow(dead_code)]

/// A `(database, table)` pair as written (after unquoting and resolving the
/// default database). Compare with [`same_name`] under the server's
/// `lower_case_table_names`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableName {
    pub db: String,
    pub table: String,
}

/// The barrier scope a statement proves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BarrierScopeOf {
    Lineage,
    Database(String),
}

/// What a statement does to base-table shapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DdlEffect {
    /// Changes no base table's columns.
    None,
    /// May change exactly these tables (renames include source AND target).
    Tables(Vec<TableName>),
    /// Recreates these tables with the same shape (TRUNCATE): no record.
    SameShape(Vec<TableName>),
    /// Cannot be attributed to explicit tables.
    Barrier(BarrierScopeOf),
}

/// Compare identifiers as the server does: `lower_case_table_names` 0 is
/// case-sensitive; 1 and 2 compare case-insensitively.
pub(crate) fn same_name(a: &str, b: &str, lower_case_table_names: u8) -> bool {
    if lower_case_table_names == 0 {
        a == b
    } else {
        a.to_lowercase() == b.to_lowercase()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    /// Unquoted word (keyword or identifier).
    Word(String),
    /// Backtick-quoted identifier (unescaped).
    Quoted(String),
    /// Single-quoted string literal.
    Str,
    /// Double-quoted token: a string, or an identifier under ANSI_QUOTES.
    DoubleQuoted,
    Punct(char),
}

/// Tokenize; `None` on anything the classifier must not interpret (versioned
/// or optimizer-hint comments, unterminated quotes or comments).
fn tokenize(sql: &str) -> Option<Vec<Tok>> {
    let c: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '/' && c.get(i + 1) == Some(&'*') {
            // `/*! ... */` executes on MySQL; `/*+ ... */` is a hint.
            if matches!(c.get(i + 2), Some('!') | Some('+')) {
                return None;
            }
            let end = (i + 2..c.len().saturating_sub(1))
                .find(|&j| c[j] == '*' && c[j + 1] == '/')?;
            i = end + 2;
        } else if ch == '#'
            || (ch == '-'
                && c.get(i + 1) == Some(&'-')
                && c.get(i + 2).is_none_or(|x| x.is_whitespace()))
        {
            while i < c.len() && c[i] != '\n' {
                i += 1;
            }
        } else if ch == '`' {
            let mut s = String::new();
            i += 1;
            loop {
                match c.get(i) {
                    None => return None,
                    Some('`') if c.get(i + 1) == Some(&'`') => {
                        s.push('`');
                        i += 2;
                    }
                    Some('`') => {
                        i += 1;
                        break;
                    }
                    Some(&x) => {
                        s.push(x);
                        i += 1;
                    }
                }
            }
            out.push(Tok::Quoted(s));
        } else if ch == '\'' || ch == '"' {
            let q = ch;
            i += 1;
            loop {
                match c.get(i) {
                    None => return None,
                    Some('\\') => i += 2,
                    Some(&x) if x == q && c.get(i + 1) == Some(&q) => i += 2,
                    Some(&x) if x == q => {
                        i += 1;
                        break;
                    }
                    Some(_) => i += 1,
                }
            }
            out.push(if q == '\'' {
                Tok::Str
            } else {
                Tok::DoubleQuoted
            });
        } else if ch.is_alphanumeric() || ch == '_' || ch == '$' {
            let start = i;
            while i < c.len()
                && (c[i].is_alphanumeric() || c[i] == '_' || c[i] == '$')
            {
                i += 1;
            }
            out.push(Tok::Word(c[start..i].iter().collect()));
        } else {
            out.push(Tok::Punct(ch));
            i += 1;
        }
    }
    Some(out)
}

struct P<'a> {
    t: &'a [Tok],
    i: usize,
    default_db: Option<&'a str>,
}

impl P<'_> {
    fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.t.get(self.i), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw))
    }
    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.peek_kw(kw) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn eat_kws(&mut self, kws: &[&str]) -> bool {
        let save = self.i;
        for k in kws {
            if !self.eat_kw(k) {
                self.i = save;
                return false;
            }
        }
        true
    }
    fn eat_punct(&mut self, p: char) -> bool {
        if self.t.get(self.i) == Some(&Tok::Punct(p)) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    /// An identifier: backtick-quoted or an unquoted word.
    fn ident(&mut self) -> Option<String> {
        match self.t.get(self.i)? {
            Tok::Quoted(s) => {
                self.i += 1;
                Some(s.clone())
            }
            Tok::Word(w) => {
                self.i += 1;
                Some(w.clone())
            }
            _ => None,
        }
    }
    /// An identifier or a single-quoted string (account names).
    fn ident_or_str(&mut self) -> Option<()> {
        if self.t.get(self.i) == Some(&Tok::Str) {
            self.i += 1;
            return Some(());
        }
        self.ident().map(|_| ())
    }
    /// `[db.]table`, resolving the default database. `None` when uncertain.
    fn table_name(&mut self) -> Option<TableName> {
        let first = self.ident()?;
        if self.eat_punct('.') {
            let table = self.ident()?;
            Some(TableName { db: first, table })
        } else {
            let db = self.default_db?.to_string();
            Some(TableName { db, table: first })
        }
    }
    fn at_end(&self) -> bool {
        self.i >= self.t.len()
    }
}

/// Classify one QueryEvent statement. `default_db` is the event's own
/// current database (`None` / empty when unknown).
pub(crate) fn classify(sql: &str, default_db: Option<&str>) -> DdlEffect {
    let lineage = DdlEffect::Barrier(BarrierScopeOf::Lineage);
    let default_db = default_db.filter(|d| !d.is_empty());
    let Some(mut toks) = tokenize(sql) else {
        return lineage;
    };
    // One statement, at most one trailing `;`. Strings and comments are
    // already consumed, so every remaining `;` is a statement separator; any
    // other than the last token means several statements (including stored
    // program bodies, rejected conservatively). Checked before any branch.
    if toks.last() == Some(&Tok::Punct(';')) {
        toks.pop();
    }
    if toks.contains(&Tok::Punct(';')) {
        return lineage;
    }
    if toks.contains(&Tok::DoubleQuoted) {
        // Under ANSI_QUOTES a double-quoted token is an identifier.
        if is_ddl_family(&toks) {
            return lineage;
        }
    }
    let mut p = P {
        t: &toks,
        i: 0,
        default_db,
    };
    classify_tokens(&mut p).unwrap_or(lineage)
}

fn is_ddl_family(toks: &[Tok]) -> bool {
    matches!(toks.first(), Some(Tok::Word(w)) if ["ALTER", "CREATE", "DROP", "RENAME", "TRUNCATE"]
        .iter()
        .any(|k| w.eq_ignore_ascii_case(k)))
}

/// `None` = uncertain -> lineage barrier.
fn classify_tokens(p: &mut P<'_>) -> Option<DdlEffect> {
    let Some(Tok::Word(first)) = p.t.first() else {
        return Some(DdlEffect::None);
    };
    let first = first.to_ascii_uppercase();
    match first.as_str() {
        "ALTER" => alter(p),
        "CREATE" => create(p),
        "DROP" => drop_(p),
        "RENAME" => rename(p),
        "TRUNCATE" => {
            p.i = 1;
            p.eat_kw("TABLE");
            let t = p.table_name()?;
            p.at_end().then_some(DdlEffect::SameShape(vec![t]))
        }
        // Statements that never change a base table's columns.
        "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" | "XA"
        | "GRANT" | "REVOKE" | "SET" | "FLUSH" | "ANALYZE" | "OPTIMIZE"
        | "CHECK" | "REPAIR" | "INSTALL" | "UNINSTALL" => Some(DdlEffect::None),
        // Anything else: not ours to interpret.
        _ => None,
    }
}

/// Object kinds whose DDL never changes a base table's columns.
const NON_TABLE_OBJECTS: &[&str] = &[
    "VIEW",
    "TRIGGER",
    "PROCEDURE",
    "FUNCTION",
    "EVENT",
    "USER",
    "ROLE",
];

/// Skip `CREATE` modifiers that precede the object kind.
fn skip_create_modifiers(p: &mut P<'_>) -> Option<()> {
    loop {
        if p.eat_kws(&["OR", "REPLACE"]) || p.eat_kw("TEMPORARY") {
            continue;
        }
        if p.eat_kw("ALGORITHM")
            || p.eat_kw("DEFINER")
            || p.eat_kws(&["SQL", "SECURITY"])
        {
            p.eat_punct('=');
            // value: a word, `user`@`host` / 'user'@'host', CURRENT_USER[()]
            p.ident_or_str()?;
            if p.eat_punct('@') {
                p.ident_or_str()?;
            }
            if p.eat_punct('(') {
                p.eat_punct(')').then_some(())?;
            }
            continue;
        }
        return Some(());
    }
}

fn non_table_object(p: &P<'_>) -> bool {
    NON_TABLE_OBJECTS.iter().any(|k| p.peek_kw(k))
}

fn create(p: &mut P<'_>) -> Option<DdlEffect> {
    p.i = 1;
    skip_create_modifiers(p)?;
    if non_table_object(p) {
        return Some(DdlEffect::None);
    }
    if p.eat_kw("DATABASE") || p.eat_kw("SCHEMA") {
        // A new database has no tables yet.
        return Some(DdlEffect::None);
    }
    if p.eat_kw("TABLE") {
        p.eat_kws(&["IF", "NOT", "EXISTS"]);
        let t = p.table_name()?;
        // `LIKE`, `AS SELECT`, column definitions: only `t` is created.
        return Some(DdlEffect::Tables(vec![t]));
    }
    // CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX i [USING x] ON t ...
    let _ = p.eat_kw("UNIQUE") || p.eat_kw("FULLTEXT") || p.eat_kw("SPATIAL");
    if p.eat_kw("INDEX") {
        p.ident()?;
        if p.eat_kw("USING") {
            p.ident()?;
        }
        p.eat_kw("ON").then_some(())?;
        let t = p.table_name()?;
        return Some(DdlEffect::Tables(vec![t]));
    }
    None
}

fn drop_(p: &mut P<'_>) -> Option<DdlEffect> {
    p.i = 1;
    p.eat_kw("TEMPORARY");
    if non_table_object(p) {
        return Some(DdlEffect::None);
    }
    if p.eat_kw("DATABASE") || p.eat_kw("SCHEMA") {
        p.eat_kws(&["IF", "EXISTS"]);
        let db = p.ident()?;
        return p
            .at_end()
            .then_some(DdlEffect::Barrier(BarrierScopeOf::Database(db)));
    }
    if p.eat_kw("TABLE") || p.eat_kw("TABLES") {
        p.eat_kws(&["IF", "EXISTS"]);
        let mut tables = vec![p.table_name()?];
        while p.eat_punct(',') {
            tables.push(p.table_name()?);
        }
        let _ = p.eat_kw("RESTRICT") || p.eat_kw("CASCADE");
        return p.at_end().then_some(DdlEffect::Tables(tables));
    }
    if p.eat_kw("INDEX") {
        p.ident()?;
        p.eat_kw("ON").then_some(())?;
        let t = p.table_name()?;
        return Some(DdlEffect::Tables(vec![t]));
    }
    None
}

fn rename(p: &mut P<'_>) -> Option<DdlEffect> {
    p.i = 1;
    if !(p.eat_kw("TABLE") || p.eat_kw("TABLES")) {
        return None; // RENAME USER etc. are not handled here.
    }
    let mut tables = Vec::new();
    loop {
        tables.push(p.table_name()?);
        p.eat_kw("TO").then_some(())?;
        tables.push(p.table_name()?);
        if !p.eat_punct(',') {
            break;
        }
    }
    p.at_end().then_some(DdlEffect::Tables(tables))
}

fn alter(p: &mut P<'_>) -> Option<DdlEffect> {
    p.i = 1;
    let _ = p.eat_kw("ONLINE") || p.eat_kw("OFFLINE");
    p.eat_kw("IGNORE");
    if non_table_object(p) || p.peek_kw("ALGORITHM") || p.peek_kw("DEFINER") {
        // ALTER [ALGORITHM=..] [DEFINER=..] VIEW / ALTER EVENT etc.
        skip_create_modifiers(p)?;
        return non_table_object(p).then_some(DdlEffect::None);
    }
    if p.eat_kw("DATABASE") || p.eat_kw("SCHEMA") {
        // Changes defaults only, but positively scoped: database barrier.
        return match p.ident() {
            Some(db)
                if !NON_DB_ALTER_WORDS
                    .iter()
                    .any(|w| db.eq_ignore_ascii_case(w)) =>
            {
                Some(DdlEffect::Barrier(BarrierScopeOf::Database(db)))
            }
            // `ALTER DATABASE CHARACTER SET ...` applies to the default db.
            _ => p.default_db.map(|d| {
                DdlEffect::Barrier(BarrierScopeOf::Database(d.to_string()))
            }),
        };
    }
    if !p.eat_kw("TABLE") {
        return None;
    }
    let t = p.table_name()?;
    let mut tables = vec![t];
    // A table rename anywhere among the alter specifications:
    // RENAME [TO|AS] new  (not RENAME COLUMN / INDEX / KEY); and the other
    // table of EXCHANGE PARTITION ... WITH TABLE other.
    while p.i < p.t.len() {
        if p.eat_kws(&["WITH", "TABLE"]) {
            tables.push(p.table_name()?);
        } else if p.eat_kw("RENAME") {
            if p.peek_kw("COLUMN") || p.peek_kw("INDEX") || p.peek_kw("KEY") {
                continue;
            }
            let _ = p.eat_kw("TO") || p.eat_kw("AS");
            tables.push(p.table_name()?);
        } else {
            p.i += 1;
        }
    }
    Some(DdlEffect::Tables(tables))
}

/// Words that may follow `ALTER DATABASE` when the name is omitted.
const NON_DB_ALTER_WORDS: &[&str] = &[
    "CHARACTER",
    "CHARSET",
    "COLLATE",
    "DEFAULT",
    "ENCRYPTION",
    "READ",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn t(db: &str, table: &str) -> TableName {
        TableName {
            db: db.into(),
            table: table.into(),
        }
    }
    fn tables(v: &[(&str, &str)]) -> DdlEffect {
        DdlEffect::Tables(v.iter().map(|(d, x)| t(d, x)).collect())
    }
    const LINEAGE: DdlEffect = DdlEffect::Barrier(BarrierScopeOf::Lineage);

    #[test]
    fn table_ddl_is_attributed_with_the_default_database() {
        for (sql, want) in [
            (
                "ALTER TABLE orders ADD COLUMN x INT",
                tables(&[("app", "orders")]),
            ),
            (
                "alter table `orders` drop column `a`",
                tables(&[("app", "orders")]),
            ),
            (
                "ALTER TABLE shop.orders MODIFY a BIGINT",
                tables(&[("shop", "orders")]),
            ),
            (
                "ALTER TABLE `my``db`.`we.ird` ADD b INT",
                tables(&[("my`db", "we.ird")]),
            ),
            (
                "ALTER ONLINE IGNORE TABLE orders ENGINE=InnoDB",
                tables(&[("app", "orders")]),
            ),
            (
                "CREATE TABLE t2 (id INT PRIMARY KEY)",
                tables(&[("app", "t2")]),
            ),
            (
                "CREATE TABLE IF NOT EXISTS shop.t2 LIKE app.t1",
                tables(&[("shop", "t2")]),
            ),
            (
                "CREATE TABLE t3 AS SELECT * FROM t1",
                tables(&[("app", "t3")]),
            ),
            (
                "CREATE TEMPORARY TABLE tmp (a INT)",
                tables(&[("app", "tmp")]),
            ),
            (
                "CREATE UNIQUE INDEX i ON orders (a)",
                tables(&[("app", "orders")]),
            ),
            (
                "CREATE INDEX i USING BTREE ON shop.orders (a)",
                tables(&[("shop", "orders")]),
            ),
            ("DROP INDEX i ON orders", tables(&[("app", "orders")])),
            ("DROP TABLE a", tables(&[("app", "a")])),
            (
                "DROP TABLE IF EXISTS a, shop.b, `c` CASCADE",
                tables(&[("app", "a"), ("shop", "b"), ("app", "c")]),
            ),
            (
                "DROP TEMPORARY TABLE IF EXISTS tmp",
                tables(&[("app", "tmp")]),
            ),
            ("RENAME TABLE a TO b", tables(&[("app", "a"), ("app", "b")])),
            (
                "RENAME TABLE a TO shop.b, shop.c TO d",
                tables(&[
                    ("app", "a"),
                    ("shop", "b"),
                    ("shop", "c"),
                    ("app", "d"),
                ]),
            ),
            (
                "ALTER TABLE a RENAME TO b",
                tables(&[("app", "a"), ("app", "b")]),
            ),
            (
                "ALTER TABLE a ADD x INT, RENAME AS shop.b",
                tables(&[("app", "a"), ("shop", "b")]),
            ),
            (
                "ALTER TABLE a RENAME b",
                tables(&[("app", "a"), ("app", "b")]),
            ),
            (
                "ALTER TABLE a RENAME COLUMN x TO y",
                tables(&[("app", "a")]),
            ),
            ("ALTER TABLE a RENAME INDEX i TO j", tables(&[("app", "a")])),
            (
                "/* note */ ALTER TABLE a ADD x INT -- trailing\n",
                tables(&[("app", "a")]),
            ),
            ("ALTER TABLE a ADD x INT # comment", tables(&[("app", "a")])),
            (
                "ALTER TABLE a ADD x INT COMMENT 'RENAME TO b'",
                tables(&[("app", "a")]),
            ),
            ("DROP TABLE a;", tables(&[("app", "a")])),
            (
                "ALTER TABLE a EXCHANGE PARTITION p0 WITH TABLE shop.b",
                tables(&[("app", "a"), ("shop", "b")]),
            ),
        ] {
            assert_eq!(classify(sql, Some("app")), want, "{sql}");
        }
    }

    #[test]
    fn truncate_keeps_the_shape() {
        assert_eq!(
            classify("TRUNCATE TABLE a", Some("app")),
            DdlEffect::SameShape(vec![t("app", "a")])
        );
        assert_eq!(
            classify("truncate shop.a", Some("app")),
            DdlEffect::SameShape(vec![t("shop", "a")])
        );
    }

    #[test]
    fn statements_that_never_change_table_columns() {
        for sql in [
            "BEGIN",
            "COMMIT",
            "XA START 'x'",
            "GRANT SELECT ON *.* TO u",
            "SET PASSWORD FOR u = 'x'",
            "FLUSH TABLES",
            "ANALYZE TABLE a",
            "OPTIMIZE TABLE a",
            "CREATE USER u",
            "DROP USER u",
            "ALTER USER u",
            "CREATE VIEW v AS SELECT 1",
            "CREATE OR REPLACE VIEW v AS SELECT 1",
            "CREATE ALGORITHM=MERGE DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW v AS SELECT 1",
            "CREATE DEFINER=CURRENT_USER() TRIGGER tr BEFORE INSERT ON a FOR EACH ROW SET @x=1",
            "CREATE DEFINER=`u`@`h` PROCEDURE p() BEGIN END",
            "CREATE DEFINER='u'@'h' VIEW v AS SELECT 1",
            "DROP VIEW v",
            "DROP TRIGGER tr",
            "DROP PROCEDURE p",
            "DROP EVENT e",
            "ALTER VIEW v AS SELECT 2",
            "ALTER ALGORITHM=MERGE VIEW v AS SELECT 2",
            "ALTER EVENT e DISABLE",
            "CREATE DATABASE newdb",
        ] {
            assert_eq!(classify(sql, Some("app")), DdlEffect::None, "{sql}");
        }
    }

    #[test]
    fn database_barriers_need_positive_scope() {
        assert_eq!(
            classify("DROP DATABASE shop", Some("app")),
            DdlEffect::Barrier(BarrierScopeOf::Database("shop".into()))
        );
        assert_eq!(
            classify("DROP SCHEMA IF EXISTS `sh``op`", None),
            DdlEffect::Barrier(BarrierScopeOf::Database("sh`op".into()))
        );
        assert_eq!(
            classify("ALTER DATABASE shop CHARACTER SET utf8mb4", None),
            DdlEffect::Barrier(BarrierScopeOf::Database("shop".into()))
        );
        assert_eq!(
            classify("ALTER DATABASE CHARACTER SET utf8mb4", Some("app")),
            DdlEffect::Barrier(BarrierScopeOf::Database("app".into()))
        );
        assert_eq!(
            classify("ALTER DATABASE CHARACTER SET utf8mb4", None),
            LINEAGE
        );
    }

    #[test]
    fn uncertainty_is_a_lineage_barrier() {
        for (sql, db) in [
            // No default database for an unqualified name.
            ("ALTER TABLE a ADD x INT", None),
            ("ALTER TABLE a ADD x INT", Some("")),
            ("RENAME TABLE a TO b", None),
            // Versioned / hint comments execute or change behaviour.
            ("/*!50100 ALTER TABLE a ADD x INT */", Some("app")),
            ("ALTER /*+ hint */ TABLE a ADD x INT", Some("app")),
            // Possible ANSI_QUOTES identifiers.
            ("ALTER TABLE \"a\" ADD x INT", Some("app")),
            ("DROP TABLE \"a\"", Some("app")),
            // Several statements, trailing tokens, malformed lists.
            ("DROP TABLE a; DROP TABLE b", Some("app")),
            ("RENAME TABLE a b", Some("app")),
            ("DROP TABLE a,", Some("app")),
            ("RENAME TABLE a TO", Some("app")),
            // Unknown object kinds and statements.
            ("ALTER TABLESPACE ts ADD DATAFILE 'x'", Some("app")),
            ("CREATE SPATIAL REFERENCE SYSTEM 1 NAME 'x'", Some("app")),
            ("ALTER INSTANCE ROTATE INNODB MASTER KEY", Some("app")),
            ("IMPORT TABLE FROM '/tmp/a.sdi'", Some("app")),
            ("CREATE RESOURCE GROUP g TYPE = USER", Some("app")),
            // Unterminated quoting or comments.
            ("ALTER TABLE `a ADD x INT", Some("app")),
            ("ALTER TABLE a /* x", Some("app")),
        ] {
            assert_eq!(classify(sql, db), LINEAGE, "{sql} / {db:?}");
        }
    }

    #[test]
    fn several_statements_are_a_lineage_barrier_whatever_comes_first() {
        for sql in [
            // Non-table DDL followed by table DDL.
            "CREATE VIEW v AS SELECT 1; ALTER TABLE orders ADD COLUMN x INT",
            "CREATE DATABASE d; DROP TABLE orders",
            "DROP VIEW v; DROP TABLE orders",
            "ALTER VIEW v AS SELECT 2; ALTER TABLE orders ADD x INT",
            "ALTER EVENT e DISABLE; ALTER TABLE orders ADD x INT",
            "ALTER DATABASE app CHARACTER SET utf8mb4; DROP TABLE orders",
            "DROP DATABASE shop; DROP TABLE app.orders",
            "GRANT SELECT ON *.* TO u; ALTER TABLE orders ADD x INT",
            "SET @a = 1; ALTER TABLE orders ADD x INT",
            "BEGIN; ALTER TABLE orders ADD x INT",
            // Table DDL followed by another table's DDL.
            "CREATE TABLE t2 (id INT); ALTER TABLE customers ADD x INT",
            "CREATE TABLE t2 LIKE t1; DROP TABLE customers",
            "CREATE INDEX i ON orders (a); ALTER TABLE customers ADD x INT",
            "DROP INDEX i ON orders; ALTER TABLE customers ADD COLUMN x INT",
            "ALTER TABLE orders ADD x INT; DROP TABLE customers",
            "ALTER TABLE orders ADD x INT; ALTER TABLE customers ADD y INT",
            "ALTER TABLE orders RENAME TO o2; RENAME TABLE c TO d",
            "TRUNCATE TABLE orders; ALTER TABLE customers ADD x INT",
            "RENAME TABLE a TO b; DROP TABLE customers",
            "DROP TABLE a; DROP TABLE b",
            // Stored-program bodies with statement separators: conservative.
            "CREATE PROCEDURE p() BEGIN ALTER TABLE orders ADD x INT; END",
            // Empty statements between separators.
            "ALTER TABLE orders ADD x INT;;",
            ";ALTER TABLE orders ADD x INT",
        ] {
            assert_eq!(classify(sql, Some("app")), LINEAGE, "{sql}");
        }
    }

    #[test]
    fn semicolons_in_strings_and_comments_and_one_trailing_are_not_separators()
    {
        for (sql, want) in [
            (
                "ALTER TABLE orders ADD x INT COMMENT 'a; DROP TABLE b'",
                tables(&[("app", "orders")]),
            ),
            (
                "ALTER TABLE orders ADD x INT /* ; DROP TABLE b */",
                tables(&[("app", "orders")]),
            ),
            (
                "ALTER TABLE orders ADD x INT -- ; DROP TABLE b\n",
                tables(&[("app", "orders")]),
            ),
            (
                "ALTER TABLE orders ADD x INT # ; DROP TABLE b",
                tables(&[("app", "orders")]),
            ),
            (
                "ALTER TABLE `or;ders` ADD x INT",
                tables(&[("app", "or;ders")]),
            ),
            (
                "ALTER TABLE orders ADD x INT;",
                tables(&[("app", "orders")]),
            ),
            (
                "ALTER TABLE orders ADD x INT ; -- done",
                tables(&[("app", "orders")]),
            ),
            (
                "CREATE INDEX i ON orders (a);",
                tables(&[("app", "orders")]),
            ),
            ("CREATE VIEW v AS SELECT ';';", DdlEffect::None),
        ] {
            assert_eq!(classify(sql, Some("app")), want, "{sql}");
        }
    }

    #[test]
    fn names_compare_by_lower_case_table_names() {
        assert!(same_name("Orders", "Orders", 0));
        assert!(!same_name("Orders", "orders", 0));
        assert!(same_name("Orders", "orders", 1));
        assert!(same_name("Orders", "ORDERS", 2));
    }
}
