//! Resolve and validate a table's **effective identity columns** for stable
//! snapshot ids, with identical policy for MySQL and PostgreSQL.
//!
//! Precedence: an explicit `identity_columns` override (validated) wins;
//! otherwise the declared primary key (in its declared order) is used; a table
//! with neither is **keyless** and rejected *before* any snapshot row is
//! emitted (Postgres `ctid` is not a durable identity and is never used here).

use std::collections::BTreeSet;

use deltaforge_core::IdentityKind;

/// A column reduced to what identity resolution needs: its name and the
/// canonical [`IdentityKind`] its source type maps to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityColumnInfo {
    /// Column name.
    pub name: String,
    /// Canonical identity type category.
    pub kind: IdentityKind,
}

/// A table's schema reduced to what identity resolution needs. Each source
/// builds this from its own schema type, mapping DB types to [`IdentityKind`].
pub struct IdentitySchemaView<'a> {
    /// All columns.
    pub columns: &'a [IdentityColumnInfo],
    /// Declared primary key, in declared order (empty if none).
    pub primary_key: &'a [String],
    /// Column sets that each form a UNIQUE constraint (order within a set is
    /// irrelevant to uniqueness).
    pub unique_constraints: &'a [Vec<String>],
}

/// The resolved effective identity for a table — ordered, typed columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedIdentity {
    /// Identity columns in the order that participates in the id.
    pub columns: Vec<IdentityColumnInfo>,
}

/// Why identity resolution failed. Every variant is caught before allocation
/// or row emission.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityResolutionError {
    /// No primary key and no `identity_columns` — cannot mint stable ids.
    #[error(
        "table {db}.{table} has no primary key and no identity_columns; \
         stable snapshot ids require a key (configure identity_columns)"
    )]
    KeylessTable {
        /// Database name.
        db: String,
        /// Table name.
        table: String,
    },
    /// `identity_columns` was configured empty.
    #[error("table {table}: identity_columns must not be empty")]
    EmptyIdentityColumns {
        /// Table name.
        table: String,
    },
    /// A column appears twice in `identity_columns`.
    #[error("table {table}: duplicate identity column {column:?}")]
    DuplicateIdentityColumn {
        /// Table name.
        table: String,
        /// The duplicated column.
        column: String,
    },
    /// An `identity_columns` entry is not a column of the table.
    #[error("table {table}: identity column {column:?} does not exist")]
    UnknownIdentityColumn {
        /// Table name.
        table: String,
        /// The missing column.
        column: String,
    },
    /// The columns are not provably unique and uniqueness was not acknowledged.
    #[error(
        "table {table}: identity_columns {columns:?} are not provably unique \
         (no matching primary key or unique constraint); set assume_unique: \
         true to acknowledge"
    )]
    UnprovableUniqueness {
        /// Table name.
        table: String,
        /// The configured columns.
        columns: Vec<String>,
    },
}

fn lookup<'a>(
    schema: &'a IdentitySchemaView<'_>,
    table: &str,
    name: &str,
) -> Result<&'a IdentityColumnInfo, IdentityResolutionError> {
    schema
        .columns
        .iter()
        .find(|c| c.name == name)
        .ok_or_else(|| IdentityResolutionError::UnknownIdentityColumn {
            table: table.to_string(),
            column: name.to_string(),
        })
}

/// Resolve the effective identity columns for one table.
///
/// `identity_columns` / `assume_unique` come from the table's configured
/// `TableOptions` (the override; `None` means "use the primary key").
pub fn resolve_identity(
    db: &str,
    table: &str,
    schema: &IdentitySchemaView<'_>,
    identity_columns: Option<&[String]>,
    assume_unique: bool,
) -> Result<ResolvedIdentity, IdentityResolutionError> {
    match identity_columns {
        Some(cols) => {
            if cols.is_empty() {
                return Err(IdentityResolutionError::EmptyIdentityColumns {
                    table: table.to_string(),
                });
            }
            // Reject duplicates.
            let mut seen = BTreeSet::new();
            for c in cols {
                if !seen.insert(c.as_str()) {
                    return Err(
                        IdentityResolutionError::DuplicateIdentityColumn {
                            table: table.to_string(),
                            column: c.clone(),
                        },
                    );
                }
            }
            // Existence + type, preserving configured order.
            let mut resolved = Vec::with_capacity(cols.len());
            for c in cols {
                resolved.push(lookup(schema, table, c)?.clone());
            }
            // Uniqueness proof: match the PK set, or any unique-constraint set
            // (as sets — column order does not affect uniqueness). Otherwise
            // require an explicit acknowledgement.
            let want: BTreeSet<&str> =
                cols.iter().map(String::as_str).collect();
            let pk: BTreeSet<&str> =
                schema.primary_key.iter().map(String::as_str).collect();
            let matches_pk = !pk.is_empty() && pk == want;
            let matches_unique = schema.unique_constraints.iter().any(|uc| {
                let s: BTreeSet<&str> = uc.iter().map(String::as_str).collect();
                s == want
            });
            if !(matches_pk || matches_unique || assume_unique) {
                return Err(IdentityResolutionError::UnprovableUniqueness {
                    table: table.to_string(),
                    columns: cols.to_vec(),
                });
            }
            Ok(ResolvedIdentity { columns: resolved })
        }
        None => {
            if schema.primary_key.is_empty() {
                return Err(IdentityResolutionError::KeylessTable {
                    db: db.to_string(),
                    table: table.to_string(),
                });
            }
            let mut resolved = Vec::with_capacity(schema.primary_key.len());
            for c in schema.primary_key {
                resolved.push(lookup(schema, table, c)?.clone());
            }
            Ok(ResolvedIdentity { columns: resolved })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, kind: IdentityKind) -> IdentityColumnInfo {
        IdentityColumnInfo {
            name: name.into(),
            kind,
        }
    }

    fn schema<'a>(
        columns: &'a [IdentityColumnInfo],
        pk: &'a [String],
        uniques: &'a [Vec<String>],
    ) -> IdentitySchemaView<'a> {
        IdentitySchemaView {
            columns,
            primary_key: pk,
            unique_constraints: uniques,
        }
    }

    fn cols() -> Vec<IdentityColumnInfo> {
        vec![
            col("tenant_id", IdentityKind::Int),
            col("order_id", IdentityKind::UInt),
            col("email", IdentityKind::Text),
        ]
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn primary_key_used_in_declared_order_when_no_override() {
        let c = cols();
        let pk = s(&["tenant_id", "order_id"]);
        let sv = schema(&c, &pk, &[]);
        let r = resolve_identity("d", "orders", &sv, None, false).unwrap();
        assert_eq!(
            r.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant_id", "order_id"]
        );
        assert_eq!(r.columns[1].kind, IdentityKind::UInt);
    }

    #[test]
    fn keyless_table_is_rejected() {
        let c = cols();
        let sv = schema(&c, &[], &[]);
        let err = resolve_identity("d", "logs", &sv, None, false).unwrap_err();
        assert_eq!(
            err,
            IdentityResolutionError::KeylessTable {
                db: "d".into(),
                table: "logs".into()
            }
        );
    }

    #[test]
    fn override_matching_pk_out_of_order_is_ok_and_preserves_config_order() {
        let c = cols();
        let pk = s(&["tenant_id", "order_id"]);
        let sv = schema(&c, &pk, &[]);
        // Override lists the PK columns in the opposite order — still unique.
        let cols_cfg = s(&["order_id", "tenant_id"]);
        let r = resolve_identity("d", "orders", &sv, Some(&cols_cfg), false)
            .unwrap();
        assert_eq!(
            r.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["order_id", "tenant_id"] // configured order preserved
        );
    }

    #[test]
    fn override_matching_unique_constraint_is_ok() {
        let c = cols();
        let pk = s(&["order_id"]);
        let uniques = vec![s(&["tenant_id", "email"])];
        let sv = schema(&c, &pk, &uniques);
        let cfg = s(&["tenant_id", "email"]);
        assert!(
            resolve_identity("d", "orders", &sv, Some(&cfg), false).is_ok()
        );
    }

    #[test]
    fn unprovable_override_rejected_without_ack_but_ok_with_ack() {
        let c = cols();
        let pk = s(&["order_id"]);
        let sv = schema(&c, &pk, &[]);
        let cfg = s(&["email"]); // not a PK/unique
        let err = resolve_identity("d", "orders", &sv, Some(&cfg), false)
            .unwrap_err();
        assert!(matches!(
            err,
            IdentityResolutionError::UnprovableUniqueness { .. }
        ));
        // With the explicit acknowledgement it resolves.
        assert!(resolve_identity("d", "orders", &sv, Some(&cfg), true).is_ok());
    }

    #[test]
    fn empty_override_is_rejected() {
        let c = cols();
        let pk = s(&["order_id"]);
        let sv = schema(&c, &pk, &[]);
        let err =
            resolve_identity("d", "orders", &sv, Some(&[]), false).unwrap_err();
        assert!(matches!(
            err,
            IdentityResolutionError::EmptyIdentityColumns { .. }
        ));
    }

    #[test]
    fn duplicate_override_column_is_rejected() {
        let c = cols();
        let pk = s(&["order_id"]);
        let sv = schema(&c, &pk, &[]);
        let cfg = s(&["order_id", "order_id"]);
        let err =
            resolve_identity("d", "orders", &sv, Some(&cfg), true).unwrap_err();
        assert!(matches!(
            err,
            IdentityResolutionError::DuplicateIdentityColumn { .. }
        ));
    }

    #[test]
    fn unknown_override_column_is_rejected() {
        let c = cols();
        let pk = s(&["order_id"]);
        let sv = schema(&c, &pk, &[]);
        let cfg = s(&["nope"]);
        let err =
            resolve_identity("d", "orders", &sv, Some(&cfg), true).unwrap_err();
        assert!(matches!(
            err,
            IdentityResolutionError::UnknownIdentityColumn { .. }
        ));
    }
}
