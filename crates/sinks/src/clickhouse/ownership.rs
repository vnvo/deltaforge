//! ClickHouse destination ownership and write fencing.
//!
//! Supported (the only topology the fencing guarantee covers): a table the
//! sink created itself, on one server, with a local `MergeTree`
//! (`changelog`) or `ReplacingMergeTree` (`upsert`) engine, written with
//! synchronous inserts. Every other engine (`Replicated*`, `Distributed`,
//! ...) is refused, as is an endpoint whose `serverUUID()` differs from the
//! one recorded at creation, and a server on which `async_insert=0` does
//! not take effect (an asynchronous insert acknowledges before the
//! constraint is evaluated).
//!
//! The table is created (`CREATE TABLE` without `IF NOT EXISTS`) with its
//! immutable marker as the table comment, the reserved last column
//! `_df_fence LowCardinality(String)` and the constraint
//! `df_fence_<fence> CHECK _df_fence = '<fence>'`. Every row carries its
//! writer's fence, so an insert by a writer whose fence was rotated away
//! fails at the server (`VIOLATED_CONSTRAINT`). A fence rotates by one
//! `ALTER TABLE ... DROP CONSTRAINT df_fence_<old>, ADD CONSTRAINT
//! df_fence_<new> ...`: it fails whole when the old constraint is gone, a
//! compare-and-swap.

use async_trait::async_trait;
use deltaforge_config::ChMode;
use deltaforge_core::ownership::{
    ClaimFailure, FencedDestination, Marker, Observation, OwnershipClass,
    RemoteOutcome, is_token,
};
use serde_json::Value;

use super::client::ChTransport;
use super::ddl::{
    FENCE_COLUMN, FENCE_COLUMN_TYPE, create_table_ddl, fence_constraint,
    fence_constraint_clause, quote_ident, quote_literal,
};
use super::types::{ChType, ColDesc};

/// The engine a mode's table must have.
pub(crate) fn engine_of(mode: &ChMode) -> &'static str {
    match mode {
        ChMode::Upsert => "ReplacingMergeTree",
        ChMode::Changelog => "MergeTree",
    }
}

/// The marker's name for a mode.
pub(crate) fn mode_name(mode: &ChMode) -> &'static str {
    match mode {
        ChMode::Upsert => "upsert",
        ChMode::Changelog => "changelog",
    }
}

/// The top-level items of a `CREATE TABLE` statement's element list (its
/// columns, indices, projections and constraints), split at commas outside
/// quotes, backticks and parentheses. `None` when there is no complete list.
pub(crate) fn element_list(create: &str) -> Option<Vec<String>> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut started = false;
    for c in create.chars() {
        if let Some(q) = quote {
            if started {
                current.push(c);
            }
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '`' | '"' => {
                quote = Some(c);
                if started {
                    current.push(c);
                }
            }
            '(' => {
                if started {
                    current.push(c);
                }
                depth += 1;
                started = true;
            }
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 && started {
                    items.push(current.trim().to_string());
                    return Some(items);
                }
                current.push(c);
            }
            ',' if started && depth == 1 => {
                items.push(current.trim().to_string());
                current.clear();
            }
            _ if started => current.push(c),
            _ => {}
        }
    }
    None
}

/// An identifier as written (unquoted or backtick-quoted).
fn unquote(ident: &str) -> &str {
    ident
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(ident)
}

/// The fence the table's constraints enforce: exactly one DeltaForge fence
/// constraint, named for its token, whose expression is exactly
/// `_df_fence = '<token>'`.
pub(crate) fn enforced_fence(create: &str) -> Result<String, String> {
    let items = element_list(create).ok_or("no element list")?;
    let mut fences = Vec::new();
    for item in &items {
        let mut words = item.splitn(4, ' ');
        if !words
            .next()
            .unwrap_or("")
            .eq_ignore_ascii_case("CONSTRAINT")
        {
            continue;
        }
        let name = unquote(words.next().unwrap_or(""));
        if !name.starts_with("df_fence") {
            continue;
        }
        let check = words.next().unwrap_or("");
        let expr = words.next().unwrap_or("").trim();
        let token = name
            .strip_prefix("df_fence_")
            .filter(|t| is_token(t))
            .ok_or_else(|| format!("constraint {name} is not a fence"))?;
        let expected = [
            format!("{FENCE_COLUMN} = '{token}'"),
            format!("`{FENCE_COLUMN}` = '{token}'"),
        ];
        if !check.eq_ignore_ascii_case("CHECK")
            || !expected.iter().any(|e| e == expr)
        {
            return Err(format!("constraint {name} has another expression"));
        }
        fences.push(token.to_string());
    }
    match fences.as_slice() {
        [one] => Ok(one.clone()),
        all => Err(format!("{} fence constraints", all.len())),
    }
}

/// The reserved fence column is the table's last column, exactly once, of
/// exactly the expected type, and no other DeltaForge-reserved column
/// exists.
pub(crate) fn fence_column_ok(
    columns: &[(String, String)],
) -> Result<(), String> {
    let reserved: Vec<_> = columns
        .iter()
        .enumerate()
        .filter(|(_, (n, _))| n.starts_with("_df_"))
        .collect();
    match reserved.as_slice() {
        [(pos, (name, ty))]
            if name == FENCE_COLUMN
                && ty == FENCE_COLUMN_TYPE
                && *pos + 1 == columns.len() =>
        {
            Ok(())
        }
        _ => Err(format!(
            "reserved columns {:?} are not exactly one last {FENCE_COLUMN} {FENCE_COLUMN_TYPE}",
            reserved.iter().map(|(_, c)| c).collect::<Vec<_>>()
        )),
    }
}

/// The target table, for a claim.
pub(crate) struct ChDestination<'a> {
    pub transport: &'a dyn ChTransport,
    pub database: &'a str,
    pub table: &'a str,
    pub mode: &'a ChMode,
    pub extensions_column: &'a str,
    /// The user columns and key it is created with.
    pub columns: &'a [(ColDesc, ChType)],
    pub primary_key: &'a [String],
}

fn unavailable(what: &str, e: impl std::fmt::Display) -> ClaimFailure {
    ClaimFailure::Unavailable(format!("clickhouse {what}: {e}"))
}

/// The ClickHouse error code in an error answer (`Code: 57. DB::...`).
pub(crate) fn error_code(text: &str) -> Option<u32> {
    let rest = &text[text.find("Code: ")? + 6..];
    rest.split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

impl ChDestination<'_> {
    async fn query(
        &self,
        what: &str,
        sql: &str,
    ) -> Result<String, ClaimFailure> {
        let (code, text) = self
            .transport
            .query_status(sql)
            .await
            .map_err(|e| unavailable(what, e))?;
        if code != 200 {
            return Err(unavailable(
                what,
                format!("status {code}: {}", head(&text)),
            ));
        }
        Ok(text)
    }
}

fn head(text: &str) -> &str {
    &text[..text.len().min(200)]
}

fn json_rows(text: &str) -> Result<Vec<Value>, ClaimFailure> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).map_err(|e| unavailable("answer", e)))
        .collect()
}

#[async_trait]
impl FencedDestination for ChDestination<'_> {
    async fn read(&self) -> Result<Observation, ClaimFailure> {
        let server = self
            .query("server identity", "SELECT serverUUID() FORMAT TabSeparated")
            .await?
            .trim()
            .to_string();
        if server.is_empty() {
            return Err(unavailable("server identity", "empty"));
        }
        // Synchronous inserts take effect (an asynchronous one acknowledges
        // before the fence constraint is evaluated).
        let (code, sync) = self
            .transport
            .query_status(
                "SELECT getSetting('async_insert') SETTINGS async_insert = 0 \
                 FORMAT TabSeparated",
            )
            .await
            .map_err(|e| unavailable("insert settings", e))?;
        // `async_insert` is a Bool: printed `false` (or `0` by older
        // servers). Anything else is not proven off.
        if code != 200 || !matches!(sync.trim(), "0" | "false") {
            tracing::error!(
                status = code,
                "clickhouse: async_insert=0 does not take effect; inserts \
                 cannot be fenced"
            );
            return Err(ClaimFailure::refused(
                OwnershipClass::FencingUnenforced,
            ));
        }
        let (db, table) =
            (quote_literal(self.database), quote_literal(self.table));
        let tables = json_rows(
            &self
                .query(
                    "table read",
                    &format!(
                        "SELECT engine, comment, create_table_query FROM system.tables \
                         WHERE database = {db} AND name = {table} FORMAT JSONEachRow"
                    ),
                )
                .await?,
        )?;
        let Some(t) = tables.first() else {
            return Ok(Observation::Absent {
                server: Some(server),
            });
        };
        let engine = t["engine"].as_str().unwrap_or("");
        if engine != engine_of(self.mode) {
            tracing::error!(
                engine,
                expected = engine_of(self.mode),
                "clickhouse: unsupported engine for a fenced destination"
            );
            return Err(ClaimFailure::refused(
                OwnershipClass::UnsupportedTopology,
            ));
        }
        let comment = t["comment"].as_str().unwrap_or("");
        let marker = if comment.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(comment)
                    .unwrap_or_else(|_| Value::String(comment.to_string())),
            )
        };
        let columns: Vec<(String, String)> = json_rows(
            &self
                .query(
                    "column read",
                    &format!(
                        "SELECT name, type FROM system.columns WHERE database = {db} \
                         AND table = {table} ORDER BY position FORMAT JSONEachRow"
                    ),
                )
                .await?,
        )?
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap_or("").to_string(),
                c["type"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect();
        let fence = fence_column_ok(&columns).and_then(|()| {
            enforced_fence(t["create_table_query"].as_str().unwrap_or(""))
        });
        Ok(Observation::Present {
            marker,
            fence,
            server: Some(server),
        })
    }

    async fn create(
        &self,
        marker: &Marker,
        fence: &str,
    ) -> Result<RemoteOutcome, ClaimFailure> {
        let ddl = create_table_ddl(
            self.database,
            self.table,
            self.columns,
            self.primary_key,
            self.mode.clone(),
            self.extensions_column,
            fence,
            &serde_json::to_string(marker).expect("a marker serializes"),
        );
        let Ok((code, text)) = self.transport.query_status(&ddl).await else {
            return Ok(RemoteOutcome::Unknown);
        };
        match (code, error_code(&text)) {
            (200, _) => Ok(RemoteOutcome::Applied),
            // TABLE_ALREADY_EXISTS
            (_, Some(57)) => Ok(RemoteOutcome::Refused),
            (_, Some(c)) => Err(unavailable(
                "table creation",
                format!("code {c}: {}", head(&text)),
            )),
            _ => Ok(RemoteOutcome::Unknown),
        }
    }

    async fn swap_fence(
        &self,
        _instance: &str,
        from: &str,
        to: &str,
    ) -> Result<RemoteOutcome, ClaimFailure> {
        let alter = format!(
            "ALTER TABLE {}.{} DROP CONSTRAINT {}, ADD {}",
            quote_ident(self.database),
            quote_ident(self.table),
            fence_constraint(from),
            fence_constraint_clause(to)
        );
        let Ok((code, text)) = self.transport.query_status(&alter).await else {
            return Ok(RemoteOutcome::Unknown);
        };
        Ok(match (code, error_code(&text)) {
            (200, _) => RemoteOutcome::Applied,
            // BAD_ARGUMENTS: the old constraint is gone (another claimer).
            (_, Some(36)) => RemoteOutcome::Refused,
            _ => RemoteOutcome::Unknown,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: &str = "0123456789abcdef0123456789abcdef";
    const G: &str = "fedcba9876543210fedcba9876543210";

    fn create(constraints: &[&str]) -> String {
        format!(
            "CREATE TABLE `my db`.`we,ird` (`id` Int64, `v` String DEFAULT 'a,b)', \
             `_df_fence` LowCardinality(String){}) ENGINE = MergeTree ORDER BY id \
             COMMENT 'x (y, z)'",
            constraints
                .iter()
                .map(|c| format!(", {c}"))
                .collect::<String>()
        )
    }

    #[test]
    fn the_element_list_respects_quotes_and_parentheses() {
        let items = element_list(&create(&[])).unwrap();
        assert_eq!(items.len(), 3, "{items:?}");
        assert_eq!(items[1], "`v` String DEFAULT 'a,b)'");
    }

    #[test]
    fn exactly_one_well_formed_fence_constraint_is_accepted() {
        let ok = format!("CONSTRAINT df_fence_{F} CHECK _df_fence = '{F}'");
        assert_eq!(enforced_fence(&create(&[&ok])), Ok(F.to_string()));
        let quoted =
            format!("CONSTRAINT `df_fence_{F}` CHECK `_df_fence` = '{F}'");
        assert_eq!(enforced_fence(&create(&[&quoted])), Ok(F.to_string()));
        // A user constraint is ignored.
        let user = "CONSTRAINT positive CHECK id > 0";
        assert_eq!(enforced_fence(&create(&[user, &ok])), Ok(F.to_string()));

        let none: [&str; 0] = [];
        assert!(enforced_fence(&create(&none)).is_err());
        let two = format!("CONSTRAINT df_fence_{G} CHECK _df_fence = '{G}'");
        assert!(enforced_fence(&create(&[&ok, &two])).is_err());
        // The name and the expression disagree.
        let mismatch =
            format!("CONSTRAINT df_fence_{F} CHECK _df_fence = '{G}'");
        assert!(enforced_fence(&create(&[&mismatch])).is_err());
        // A weakened expression.
        let weak =
            format!("CONSTRAINT df_fence_{F} CHECK _df_fence = '{F}' OR 1");
        assert!(enforced_fence(&create(&[&weak])).is_err());
        let bad_name =
            "CONSTRAINT df_fence_x CHECK _df_fence = 'x'".to_string();
        assert!(enforced_fence(&create(&[&bad_name])).is_err());
        let assume =
            format!("CONSTRAINT df_fence_{F} ASSUME _df_fence = '{F}'");
        assert!(enforced_fence(&create(&[&assume])).is_err());
    }

    #[test]
    fn the_fence_column_is_exactly_one_last_reserved_column() {
        let c = |v: &[(&str, &str)]| -> Vec<(String, String)> {
            v.iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect()
        };
        assert!(
            fence_column_ok(&c(&[
                ("id", "Int64"),
                ("_df_fence", FENCE_COLUMN_TYPE)
            ]))
            .is_ok()
        );
        assert!(fence_column_ok(&c(&[("id", "Int64")])).is_err());
        assert!(
            fence_column_ok(&c(&[
                ("_df_fence", FENCE_COLUMN_TYPE),
                ("id", "Int64")
            ]))
            .is_err()
        );
        assert!(
            fence_column_ok(&c(&[("id", "Int64"), ("_df_fence", "String")]))
                .is_err()
        );
        assert!(
            fence_column_ok(&c(&[
                ("_df_other", "String"),
                ("_df_fence", FENCE_COLUMN_TYPE)
            ]))
            .is_err()
        );
    }

    #[test]
    fn error_codes_are_read_from_the_answer() {
        assert_eq!(
            error_code(
                "Code: 57. DB::Exception: Table default.t already exists."
            ),
            Some(57)
        );
        assert_eq!(
            error_code("Code: 469. DB::Exception: Constraint"),
            Some(469)
        );
        assert_eq!(error_code("no code here"), None);
    }
}
