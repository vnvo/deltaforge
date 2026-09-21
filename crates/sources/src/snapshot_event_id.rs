//! Provisional stable-`EventId` derivation for snapshot rows.
//!
//! Mints the `snap` id from the (re)snapshot's coordinates: the source lineage,
//! the durable generation, the table identity, and the row's typed identity
//! values (extracted per the table's resolved identity columns). Provisional:
//! not yet stored in `Event.event_id` (that lands at the cutover).
//!
//! Row values are carried in an **owned** form here because they are extracted
//! from a decoded row; [`OwnedIdentityValue::as_borrowed`] adapts them to the
//! borrowed [`IdentityValue`] the core encoder consumes.

use deltaforge_core::{
    EventId, IdentityCell, IdentityKind, IdentityValue, SourceLineage,
};

/// Owned mirror of [`IdentityCell`] (see module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedIdentityCell {
    /// Signed integer value.
    Int(i64),
    /// Unsigned integer value.
    UInt(u64),
    /// UTF-8 text value.
    Text(String),
    /// Raw byte-string value.
    Bytes(Vec<u8>),
    /// A null value in a column of the given type category.
    Null(IdentityKind),
}

/// Owned mirror of [`IdentityValue`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedIdentityValue {
    /// Column name.
    pub name: String,
    /// Typed cell value.
    pub cell: OwnedIdentityCell,
}

impl OwnedIdentityValue {
    /// Borrow as the core [`IdentityValue`] for encoding.
    pub fn as_borrowed(&self) -> IdentityValue<'_> {
        let cell = match &self.cell {
            OwnedIdentityCell::Int(v) => IdentityCell::Int(*v),
            OwnedIdentityCell::UInt(v) => IdentityCell::UInt(*v),
            OwnedIdentityCell::Text(s) => IdentityCell::Text(s),
            OwnedIdentityCell::Bytes(b) => IdentityCell::Bytes(b),
            OwnedIdentityCell::Null(k) => IdentityCell::Null(*k),
        };
        IdentityValue {
            name: &self.name,
            cell,
        }
    }
}

/// Derive the provisional `snap` [`EventId`] for one snapshot row.
pub fn snapshot_row_event_id(
    lineage: &SourceLineage<'_>,
    generation: u64,
    db: &str,
    table: &str,
    identity: &[OwnedIdentityValue],
) -> EventId {
    let borrowed: Vec<IdentityValue<'_>> = identity
        .iter()
        .map(OwnedIdentityValue::as_borrowed)
        .collect();
    EventId::snapshot(lineage, generation, db, table, &borrowed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg() -> SourceLineage<'static> {
        SourceLineage::Postgres {
            system_identifier: 0x1234_5678_90AB_CDEF,
        }
    }

    #[test]
    fn matches_core_pinned_snapshot_vector() {
        // Same coordinates as the core `pinned_vector_snapshot`: the owned
        // wrapper must reproduce the exact id, proving it is faithful.
        let id = snapshot_row_event_id(
            &pg(),
            1,
            "orders",
            "customers",
            &[OwnedIdentityValue {
                name: "id".into(),
                cell: OwnedIdentityCell::Int(42),
            }],
        );
        assert_eq!(
            id.to_string(),
            "dfid:v1:snap:a50e51a36b9842393e2a4283c0c7d341"
        );
    }

    #[test]
    fn owned_preserves_type_distinctions() {
        let mk = |cell| {
            snapshot_row_event_id(
                &pg(),
                1,
                "d",
                "t",
                &[OwnedIdentityValue {
                    name: "id".into(),
                    cell,
                }],
            )
        };
        let as_int = mk(OwnedIdentityCell::Int(42));
        let as_text = mk(OwnedIdentityCell::Text("42".into()));
        let as_bytes = mk(OwnedIdentityCell::Bytes(b"42".to_vec()));
        assert_ne!(as_int, as_text);
        assert_ne!(as_int, as_bytes);
        assert_ne!(as_text, as_bytes);
    }
}
