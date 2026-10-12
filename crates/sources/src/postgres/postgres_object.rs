//! pgoutput tuple data: the relation's columns and a strict tuple parser.
//! Values are decoded by [`super::postgres_decode`].

use super::postgres_decode::TupleValue;

/// Relation column metadata from pgoutput.
#[derive(Debug, Clone)]
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    /// Flags: 1 = part of key
    pub flags: u8,
}

#[cfg(test)]
impl RelationColumn {
    pub fn is_key(&self) -> bool {
        self.flags & 1 != 0
    }
}

/// Parse one pgoutput TupleData: its column count, then per column `n`
/// (NULL), `u` (unchanged TOAST), `t` (text) or `b` (binary) with a 4-byte
/// length. Returns the values (borrowing `data`) and the bytes consumed.
/// Anything malformed (truncated data, an unknown kind) is an error, never
/// a guessed value.
pub fn parse_tuple(
    data: &[u8],
) -> Result<(Vec<TupleValue<'_>>, usize), String> {
    let count = data
        .get(..2)
        .map(|b| u16::from_be_bytes([b[0], b[1]]) as usize)
        .ok_or("truncated tuple: no column count")?;
    let mut offset = 2;
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let kind = *data
            .get(offset)
            .ok_or_else(|| format!("truncated tuple at column {i}"))?;
        offset += 1;
        let value = match kind {
            b'n' => TupleValue::Null,
            b'u' => TupleValue::Unchanged,
            b't' | b'b' => {
                let len = data
                    .get(offset..offset + 4)
                    .map(|b| {
                        u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize
                    })
                    .ok_or_else(|| format!("truncated length at column {i}"))?;
                offset += 4;
                let bytes = data
                    .get(offset..offset + len)
                    .ok_or_else(|| format!("truncated value at column {i}"))?;
                offset += len;
                if kind == b't' {
                    TupleValue::Text(bytes)
                } else {
                    TupleValue::Binary
                }
            }
            other => {
                return Err(format!(
                    "unknown tuple value kind {:?} at column {i}",
                    char::from(other)
                ));
            }
        };
        values.push(value);
    }
    Ok((values, offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_kind_and_reports_what_it_consumed() {
        let mut data = vec![0, 4, b'n', b'u', b't', 0, 0, 0, 2, b'4', b'2'];
        data.extend([b'b', 0, 0, 0, 1, 7, 0xAA]);
        let (values, used) = parse_tuple(&data).unwrap();
        assert_eq!(
            values,
            vec![
                TupleValue::Null,
                TupleValue::Unchanged,
                TupleValue::Text(b"42"),
                TupleValue::Binary
            ]
        );
        assert_eq!(
            used,
            data.len() - 1,
            "the trailing byte is not the tuple's"
        );
    }

    #[test]
    fn malformed_tuples_fail_closed() {
        for bad in [
            vec![],
            vec![0],
            vec![0, 1],
            vec![0, 1, b'x'],
            vec![0, 1, b't', 0, 0],
            vec![0, 1, b't', 0, 0, 0, 5, b'a'],
        ] {
            assert!(parse_tuple(&bad).is_err(), "{bad:?}");
        }
    }
}
