//! Resolve and validate a table's **effective identity columns** for stable
//! snapshot ids, with identical policy for MySQL and PostgreSQL.
//!
//! Precedence: an explicit `identity_columns` override (validated) wins;
//! otherwise the declared primary key (in its declared order) is used; a table
//! with neither is **keyless** and rejected *before* any snapshot row is
//! emitted (Postgres `ctid` is not a durable identity and is never used here).
//!
//! This stage is purely **name-based** (existence, duplicates, emptiness, and
//! uniqueness are all name checks). Mapping each resolved column to its
//! canonical identity type — and rejecting unsupported types — is the source's
//! job (schema-directed), done after resolution and still before allocation.

use std::collections::BTreeSet;

/// A table's schema reduced to what identity resolution needs.
pub struct IdentitySchemaView<'a> {
    /// All column names in the table.
    pub columns: &'a [String],
    /// Declared primary key, in declared order (empty if none).
    pub primary_key: &'a [String],
    /// Column sets that each form a UNIQUE constraint (order within a set is
    /// irrelevant to uniqueness).
    pub unique_constraints: &'a [Vec<String>],
}

/// The resolved effective identity for a table: the ordered column names whose
/// values form the row identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedIdentity {
    /// Identity columns in the order that participates in the id.
    pub columns: Vec<String>,
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

fn require_exists(
    schema: &IdentitySchemaView<'_>,
    table: &str,
    name: &str,
) -> Result<(), IdentityResolutionError> {
    if schema.columns.iter().any(|c| c == name) {
        Ok(())
    } else {
        Err(IdentityResolutionError::UnknownIdentityColumn {
            table: table.to_string(),
            column: name.to_string(),
        })
    }
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
            // Existence, preserving configured order.
            for c in cols {
                require_exists(schema, table, c)?;
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
            Ok(ResolvedIdentity {
                columns: cols.to_vec(),
            })
        }
        None => {
            if schema.primary_key.is_empty() {
                return Err(IdentityResolutionError::KeylessTable {
                    db: db.to_string(),
                    table: table.to_string(),
                });
            }
            for c in schema.primary_key {
                require_exists(schema, table, c)?;
            }
            Ok(ResolvedIdentity {
                columns: schema.primary_key.to_vec(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn cols() -> Vec<String> {
        s(&["tenant_id", "order_id", "email"])
    }

    fn schema<'a>(
        columns: &'a [String],
        pk: &'a [String],
        uniques: &'a [Vec<String>],
    ) -> IdentitySchemaView<'a> {
        IdentitySchemaView {
            columns,
            primary_key: pk,
            unique_constraints: uniques,
        }
    }

    #[test]
    fn primary_key_used_in_declared_order_when_no_override() {
        let c = cols();
        let pk = s(&["tenant_id", "order_id"]);
        let sv = schema(&c, &pk, &[]);
        let r = resolve_identity("d", "orders", &sv, None, false).unwrap();
        assert_eq!(r.columns, s(&["tenant_id", "order_id"]));
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
        let cfg = s(&["order_id", "tenant_id"]); // opposite order, still unique
        let r =
            resolve_identity("d", "orders", &sv, Some(&cfg), false).unwrap();
        assert_eq!(r.columns, s(&["order_id", "tenant_id"])); // config order kept
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
