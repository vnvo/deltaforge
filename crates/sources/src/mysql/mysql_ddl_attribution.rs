//! Which tables a MySQL DDL statement can change (design spec 7.5, 7.14).
//!
//! The same classifier serves streaming and the baseline pre-scan. It never
//! guesses: a statement is attributed to explicit tables only when every
//! affected base object is identified with certainty; otherwise it yields a
//! barrier, scoped to one database only when the statement positively proves
//! that every affected object lies in it, and lineage-wide in every other
//! case (uncertain identifiers, qualifiers, table lists, hint or uncertain
//! executable comments, possible ANSI-quoted identifiers, several
//! statements, unknown object types, an unknown default database).
//!
//! Executable comments are part of the statement the server ran:
//! `/*! ... */` always, `/*!NNNNN ... */` when the version that wrote the
//! binlog is at least `NNNNN`; a non-applicable guard is a plain comment.
//! The statement is classified as executed, never by stripping comment
//! markers.

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

/// The kind of a named object a statement acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectKind {
    Table,
    View,
    Database,
}

impl ObjectKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Table => "table",
            ObjectKind::View => "view",
            ObjectKind::Database => "database",
        }
    }
}

/// One object a statement acts on, as written (after unquoting and
/// resolving the default database). A database object's `name` is the
/// database itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DdlObject {
    pub kind: ObjectKind,
    pub db: String,
    pub name: String,
}

/// A statement's attribution, computed once and used for every purpose:
/// activation records, schema-cache invalidation and the DDL event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Classified {
    pub effect: DdlEffect,
    /// The objects the statement acts on, in statement order (a rename:
    /// source then target). Empty when unattributed.
    pub objects: Vec<DdlObject>,
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

/// A MySQL server version as executable-comment guards compare it:
/// `8.0.36-log` is `80036`. `None` when the text is not `major.minor.patch`.
pub(crate) fn parse_server_version(text: &str) -> Option<u32> {
    let numeric: String = text
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let mut parts = numeric.split('.').map(|p| p.parse::<u32>().ok());
    let (major, minor, patch) =
        (parts.next()??, parts.next()??, parts.next()??);
    (minor < 100 && patch < 100).then_some(major * 10_000 + minor * 100 + patch)
}

/// Tokenize the statement as the server executed it: the contents of an
/// applicable executable comment are part of it, a non-applicable versioned
/// comment and plain comments are not. `server_version` is the version that
/// wrote the binlog (its format description).
///
/// `None` on anything the classifier must not interpret: an optimizer hint;
/// a versioned comment whose guard is not five digits or whose server
/// version is unknown; a MariaDB executable comment; a comment inside an
/// executable comment; unterminated quotes or comments.
fn tokenize(sql: &str, server_version: Option<u32>) -> Option<Vec<Tok>> {
    let c: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    // Inside an executable comment, whose `*/` ends it.
    let mut executable = false;
    while i < c.len() {
        let ch = c[i];
        if executable && ch == '*' && c.get(i + 1) == Some(&'/') {
            executable = false;
            i += 2;
        } else if ch.is_whitespace() {
            i += 1;
        } else if ch == '/' && c.get(i + 1) == Some(&'*') {
            if executable {
                return None;
            }
            match (c.get(i + 2), c.get(i + 3)) {
                // An optimizer hint, or a MariaDB executable comment.
                (Some('+'), _) | (Some('M'), Some('!')) => return None,
                (Some('!'), _) => {
                    let digits = c[i + 3..]
                        .iter()
                        .take_while(|d| d.is_ascii_digit())
                        .count();
                    let body = i + 3 + digits;
                    let applies = match digits {
                        0 => true,
                        5 => {
                            let guard: u32 = c[i + 3..body]
                                .iter()
                                .collect::<String>()
                                .parse()
                                .ok()?;
                            server_version? >= guard
                        }
                        _ => return None,
                    };
                    if applies {
                        executable = true;
                        i = body;
                    } else {
                        i = comment_end(&c, body)?;
                    }
                }
                _ => i = comment_end(&c, i + 2)?,
            }
        } else if ch == '#'
            || (ch == '-'
                && c.get(i + 1) == Some(&'-')
                && c.get(i + 2).is_none_or(|x| x.is_whitespace()))
        {
            if executable {
                return None;
            }
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
    (!executable).then_some(out)
}

/// The index after the `*/` that closes a comment whose body starts at
/// `from`.
fn comment_end(c: &[char], from: usize) -> Option<usize> {
    (from..c.len().saturating_sub(1))
        .find(|&j| c[j] == '*' && c[j + 1] == '/')
        .map(|j| j + 2)
}

/// The barewords (keywords and unquoted identifiers, uppercase) of the
/// statement as the server executed it, in order; strings and quoted
/// identifiers are not words. `None` when it cannot be lexed with
/// certainty (see `tokenize`).
pub(crate) fn executable_words(
    sql: &str,
    server_version: Option<u32>,
) -> Option<Vec<String>> {
    tokenize(sql, server_version).map(|toks| {
        toks.into_iter()
            .filter_map(|t| match t {
                Tok::Word(w) => Some(w.to_ascii_uppercase()),
                _ => None,
            })
            .collect()
    })
}

/// Whether the executed statement is DDL (it starts with `ALTER`, `CREATE`,
/// `DROP`, `RENAME` or `TRUNCATE`). When it cannot be lexed with certainty,
/// the text with comment markers removed decides: an uncertain statement
/// that may be DDL is emitted (as a lineage barrier), never dropped.
pub(crate) fn is_ddl_statement(sql: &str, server_version: Option<u32>) -> bool {
    let first = match executable_words(sql, server_version) {
        Some(words) => words.into_iter().next(),
        None => words_including_comments(sql)
            .into_iter()
            .find(|w| !w.chars().all(|c| c.is_ascii_digit())),
    };
    first.is_some_and(|w| {
        ["ALTER", "CREATE", "DROP", "RENAME", "TRUNCATE"].contains(&w.as_str())
    })
}

/// Every bareword of `sql`, uppercase, comment contents included (comment
/// markers are separators): for conservative decisions only (a credential
/// mentioned anywhere is redacted), never for attribution.
pub(crate) fn words_including_comments(sql: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote: Option<char> = None;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if let Some(q) = quote {
            if ch == '\\' {
                chars.next();
            } else if ch == q {
                quote = None;
            }
            continue;
        }
        if ch.is_alphanumeric() || ch == '_' || ch == '$' {
            word.push(ch.to_ascii_uppercase());
            continue;
        }
        if !word.is_empty() {
            words.push(std::mem::take(&mut word));
        }
        if matches!(ch, '\'' | '"' | '`') {
            quote = Some(ch);
        }
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
}

struct P<'a> {
    t: &'a [Tok],
    i: usize,
    default_db: Option<&'a str>,
    /// Non-table objects the statement names (views), when resolvable.
    named: Vec<DdlObject>,
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
/// current database (`None` / empty when unknown); `server_version` is the
/// version that wrote the binlog (see [`parse_server_version`]).
pub(crate) fn classify(
    sql: &str,
    default_db: Option<&str>,
    server_version: Option<u32>,
) -> DdlEffect {
    classify_statement(sql, default_db, server_version).effect
}

/// Classify one statement with the objects it acts on: the attributed
/// tables (statement order), a database barrier's database, or the views it
/// names when resolvable. Uncertain statements have no objects.
pub(crate) fn classify_statement(
    sql: &str,
    default_db: Option<&str>,
    server_version: Option<u32>,
) -> Classified {
    let mut named = Vec::new();
    let effect = classify_inner(sql, default_db, server_version, &mut named);
    let table = |t: &TableName| DdlObject {
        kind: ObjectKind::Table,
        db: t.db.clone(),
        name: t.table.clone(),
    };
    let objects = match &effect {
        DdlEffect::Tables(ts) | DdlEffect::SameShape(ts) => {
            ts.iter().map(table).collect()
        }
        DdlEffect::Barrier(BarrierScopeOf::Database(db)) => vec![DdlObject {
            kind: ObjectKind::Database,
            db: db.clone(),
            name: db.clone(),
        }],
        DdlEffect::Barrier(BarrierScopeOf::Lineage) => Vec::new(),
        DdlEffect::None => named,
    };
    Classified { effect, objects }
}

fn classify_inner(
    sql: &str,
    default_db: Option<&str>,
    server_version: Option<u32>,
    named: &mut Vec<DdlObject>,
) -> DdlEffect {
    let lineage = DdlEffect::Barrier(BarrierScopeOf::Lineage);
    let default_db = default_db.filter(|d| !d.is_empty());
    let Some(mut toks) = tokenize(sql, server_version) else {
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
        named: Vec::new(),
    };
    let effect = classify_tokens(&mut p).unwrap_or(lineage);
    *named = p.named;
    effect
}

/// Whether `sql` is a `DROP [TEMPORARY] TABLE[S]` statement: its tables get
/// no forward proof (a later CREATE is its own DDL with its own proof).
pub(crate) fn is_drop_table(sql: &str, server_version: Option<u32>) -> bool {
    let Some(toks) = tokenize(sql, server_version) else {
        return false;
    };
    let mut p = P {
        t: &toks,
        i: 0,
        default_db: None,
        named: Vec::new(),
    };
    if !p.eat_kw("DROP") {
        return false;
    }
    p.eat_kw("TEMPORARY");
    p.peek_kw("TABLE") || p.peek_kw("TABLES")
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

/// `VIEW [db.]name[, ...]` at the cursor: record the views when every name
/// resolves (`multiple` for DROP's list); never changes a base table.
fn views(p: &mut P<'_>, multiple: bool) -> Option<DdlEffect> {
    if !p.eat_kw("VIEW") {
        return None;
    }
    p.eat_kws(&["IF", "NOT", "EXISTS"]);
    p.eat_kws(&["IF", "EXISTS"]);
    let mut found = Vec::new();
    loop {
        match p.table_name() {
            Some(v) => found.push(DdlObject {
                kind: ObjectKind::View,
                db: v.db,
                name: v.table,
            }),
            // Unresolvable (no default database): unattributed.
            None => return Some(DdlEffect::None),
        }
        if !(multiple && p.eat_punct(',')) {
            break;
        }
    }
    p.named = found;
    Some(DdlEffect::None)
}

fn create(p: &mut P<'_>) -> Option<DdlEffect> {
    p.i = 1;
    skip_create_modifiers(p)?;
    if p.peek_kw("VIEW") {
        return views(p, false);
    }
    if non_table_object(p) {
        return Some(DdlEffect::None);
    }
    if p.eat_kw("DATABASE") || p.eat_kw("SCHEMA") {
        // Scoped like ALTER and DROP DATABASE; the options that follow name
        // no other object.
        p.eat_kws(&["IF", "NOT", "EXISTS"]);
        let db = p.ident()?;
        return Some(DdlEffect::Barrier(BarrierScopeOf::Database(db)));
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
    if p.peek_kw("VIEW") {
        return views(p, true);
    }
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
        if p.peek_kw("VIEW") {
            return views(p, false);
        }
        return non_table_object(p).then_some(DdlEffect::None);
    }
    if p.eat_kw("DATABASE") || p.eat_kw("SCHEMA") {
        // Changes defaults only, but positively scoped: database barrier.
        // A quoted name is always the database, never an option keyword.
        let named = match p.t.get(p.i) {
            Some(Tok::Quoted(db)) => Some(db.clone()),
            Some(Tok::Word(w))
                if !NON_DB_ALTER_WORDS
                    .iter()
                    .any(|k| w.eq_ignore_ascii_case(k)) =>
            {
                Some(w.clone())
            }
            _ => None,
        };
        return match named {
            Some(db) => Some(DdlEffect::Barrier(BarrierScopeOf::Database(db))),
            // `ALTER DATABASE CHARACTER SET ...` applies to the default db.
            None => p.default_db.map(|d| {
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

    /// The server version the binlog of most tests was written by.
    const V8036: Option<u32> = Some(80036);

    fn classify(sql: &str, default_db: Option<&str>) -> DdlEffect {
        super::classify(sql, default_db, V8036)
    }

    fn is_drop_table(sql: &str) -> bool {
        super::is_drop_table(sql, V8036)
    }

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

    /// CREATE, ALTER and DROP DATABASE are scoped alike: a database
    /// barrier naming the database, with quoted names and IF [NOT] EXISTS.
    #[test]
    fn database_ddl_is_scoped_symmetrically() {
        let db = |name: &str| {
            DdlEffect::Barrier(BarrierScopeOf::Database(name.into()))
        };
        for (sql, want) in [
            ("CREATE DATABASE shop", "shop"),
            ("create schema shop", "shop"),
            ("CREATE DATABASE IF NOT EXISTS shop", "shop"),
            ("CREATE DATABASE `my shop` CHARACTER SET utf8mb4", "my shop"),
            (
                "CREATE SCHEMA IF NOT EXISTS `sh``op` DEFAULT COLLATE utf8mb4_bin",
                "sh`op",
            ),
            ("ALTER DATABASE shop READ ONLY = 1", "shop"),
            ("ALTER SCHEMA `my shop` CHARACTER SET latin1", "my shop"),
            // A quoted name is the database even when it spells an option.
            (
                "ALTER DATABASE `character` CHARACTER SET latin1",
                "character",
            ),
            ("DROP DATABASE shop", "shop"),
            ("DROP DATABASE IF EXISTS `my shop`", "my shop"),
            ("DROP SCHEMA `sh``op`", "sh`op"),
        ] {
            assert_eq!(classify(sql, Some("app")), db(want), "{sql}");
            assert_eq!(
                classify(sql, None),
                db(want),
                "{sql} without a default database"
            );
            let c = classify_statement(sql, None, V8036);
            assert_eq!(
                c.objects,
                [DdlObject {
                    kind: ObjectKind::Database,
                    db: want.into(),
                    name: want.into()
                }],
                "{sql}"
            );
        }
        // Under ANSI_QUOTES a double-quoted name may be the database: uncertain.
        assert_eq!(classify("CREATE DATABASE \"x\"", Some("app")), LINEAGE);
    }

    /// An executable comment is part of the executed statement; a
    /// non-applicable versioned comment is a plain comment.
    #[test]
    fn executable_comments_are_classified_as_executed() {
        for (sql, want) in [
            // Unversioned: always executed.
            (
                "/*! ALTER TABLE shop.a ADD x INT */",
                tables(&[("shop", "a")]),
            ),
            ("/*!ALTER TABLE a ADD x INT*/", tables(&[("app", "a")])),
            // Applicable guards (<= 8.0.36).
            (
                "/*!50100 ALTER TABLE shop.a ADD x INT */",
                tables(&[("shop", "a")]),
            ),
            (
                "/*!80036 RENAME TABLE a TO b */",
                tables(&[("app", "a"), ("app", "b")]),
            ),
            (
                "ALTER TABLE shop.a ADD x INT /*!50100 , RENAME TO shop.b */",
                tables(&[("shop", "a"), ("shop", "b")]),
            ),
            // A non-applicable guard is not executed.
            (
                "ALTER TABLE shop.a ADD x INT /*!80037 , RENAME TO shop.b */",
                tables(&[("shop", "a")]),
            ),
            (
                "DROP TABLE shop.a /*!90000 , shop.b */",
                tables(&[("shop", "a")]),
            ),
            // A plain comment before the statement.
            (
                "/* note */ ALTER TABLE a ADD x INT",
                tables(&[("app", "a")]),
            ),
            // A `*/` inside a string does not end the executable comment.
            (
                "/*!50100 ALTER TABLE a ADD x VARCHAR(5) DEFAULT '*/' */",
                tables(&[("app", "a")]),
            ),
            // mysqldump's view creation, split across executable comments.
            (
                "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */ /*!50001 VIEW `v` AS select 1 */",
                DdlEffect::None,
            ),
            (
                "/*!50001 DROP DATABASE IF EXISTS `d` */",
                DdlEffect::Barrier(BarrierScopeOf::Database("d".into())),
            ),
            ("/*!40101 SET NAMES utf8mb4 */", DdlEffect::None),
        ] {
            assert_eq!(classify(sql, Some("app")), want, "{sql}");
        }
        // The guard compares the version that wrote the binlog.
        let sql = "/*!80036 ALTER TABLE shop.a ADD x INT */";
        assert_eq!(super::classify(sql, None, Some(80035)), DdlEffect::None);
        assert_eq!(
            super::classify(sql, None, Some(80036)),
            tables(&[("shop", "a")])
        );
        assert_eq!(
            super::classify(sql, None, Some(80400)),
            tables(&[("shop", "a")])
        );
        // Its view's objects, from the executed text.
        let c = classify_statement(
            "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50001 VIEW `shop`.`v` AS select 1 */",
            None,
            V8036,
        );
        assert_eq!(
            c.objects,
            [DdlObject {
                kind: ObjectKind::View,
                db: "shop".into(),
                name: "v".into()
            }]
        );
    }

    /// Executable comments the classifier cannot interpret with certainty
    /// are lineage barriers.
    #[test]
    fn uncertain_executable_comments_are_lineage_barriers() {
        for (sql, version) in [
            // The version that wrote the binlog is unknown.
            ("/*!50100 ALTER TABLE shop.a ADD x INT */", None),
            (
                "ALTER TABLE shop.a ADD x INT /*!50100 , RENAME TO shop.b */",
                None,
            ),
            // Guards that are not five digits.
            ("/*!5010 ALTER TABLE shop.a ADD x INT */", V8036),
            ("/*!800360 ALTER TABLE shop.a ADD x INT */", V8036),
            // A comment, or a line comment, inside an executable comment.
            ("/*! ALTER TABLE shop.a /* c */ ADD x INT */", V8036),
            ("/*! ALTER TABLE shop.a -- c\n ADD x INT */", V8036),
            // Unterminated.
            ("/*!50100 ALTER TABLE shop.a ADD x INT", V8036),
            // MariaDB executable comments and optimizer hints.
            ("/*M!100000 ALTER TABLE shop.a ADD x INT */", V8036),
            ("ALTER /*+ hint */ TABLE shop.a ADD x INT", V8036),
        ] {
            assert_eq!(
                super::classify(sql, None, version),
                LINEAGE,
                "{sql} / {version:?}"
            );
        }
    }

    #[test]
    fn server_versions_parse_as_guards_compare_them() {
        assert_eq!(parse_server_version("8.0.36-log"), Some(80036));
        assert_eq!(parse_server_version("8.4.2"), Some(80402));
        assert_eq!(
            parse_server_version("5.7.44-0ubuntu0.18.04.1-log"),
            Some(50744)
        );
        assert_eq!(parse_server_version("10.11.6-MariaDB"), Some(101106));
        assert_eq!(parse_server_version("8.4"), None);
        assert_eq!(parse_server_version(""), None);
    }

    #[test]
    fn ddl_statements_are_recognized_as_executed() {
        for sql in [
            "ALTER TABLE a ADD x INT",
            "/* c */ create table a (id int)",
            "/*!50100 ALTER TABLE a ADD x INT */",
            "/*! DROP DATABASE d */",
            "-- c\nRENAME TABLE a TO b",
            // Uncertain lexing: emitted rather than dropped.
            "/*!5010 ALTER TABLE a ADD x INT */",
        ] {
            assert!(is_ddl_statement(sql, V8036), "{sql}");
        }
        for sql in [
            "BEGIN",
            "/*!40101 SET NAMES utf8mb4 */",
            "/*!90000 ALTER TABLE a ADD x INT */",
            "GRANT SELECT ON *.* TO u",
            "INSERT INTO t VALUES ('ALTER')",
        ] {
            assert!(!is_ddl_statement(sql, V8036), "{sql}");
        }
    }

    #[test]
    fn uncertainty_is_a_lineage_barrier() {
        for (sql, db) in [
            // No default database for an unqualified name.
            ("ALTER TABLE a ADD x INT", None),
            ("ALTER TABLE a ADD x INT", Some("")),
            ("RENAME TABLE a TO b", None),
            // Hint comments change behaviour.
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
    fn drops_of_tables_are_recognized() {
        for sql in [
            "DROP TABLE a",
            "drop temporary table if exists a, b",
            "/* x */ DROP TABLES a",
        ] {
            assert!(is_drop_table(sql), "{sql}");
        }
        for sql in [
            "ALTER TABLE a DROP COLUMN b",
            "DROP INDEX i ON a",
            "DROP DATABASE d",
            "DROP VIEW v",
            "CREATE TABLE a (id INT)",
        ] {
            assert!(!is_drop_table(sql), "{sql}");
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
