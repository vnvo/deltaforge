//! A JSON document written again with only some of its fields changed.
//!
//! Every commit boundary writes the same checkpoint and watermark documents
//! with only the position changed. Compact JSON of a value is the
//! concatenation of its fields' bytes, so the document is written once, by
//! the production serializer, with a mark as the value of each open field,
//! and cut at the marks. A later document is the pieces joined by the new
//! values as [`deltaforge_core::json_out`] writes them: the bytes the whole
//! value serializes to.

use deltaforge_core::json_out::{write_json_str, write_json_u64};

/// The value an open string field holds when its template is written.
pub(crate) const MARK_STR: &str = "\u{0}deltaforge open field\u{0}";
/// The value an open `u64` field holds when its template is written.
pub(crate) const MARK_U64: u64 = u64::MAX;

/// An open field, by its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Open {
    Str(&'static str),
    U64(&'static str),
}

/// A value for an open field.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fill<'a> {
    Str(&'a str),
    U64(u64),
}

/// A document cut at its open fields' values.
#[derive(Clone, Debug)]
pub(crate) struct JsonTemplate {
    /// One more piece than open fields.
    pieces: Vec<Vec<u8>>,
    open: Vec<Open>,
}

impl JsonTemplate {
    /// Cut `doc` (a value written with [`MARK_STR`] / [`MARK_U64`] as the
    /// value of each field of `open`, given in document order) at those
    /// values. `None` unless each field's `"key":mark` occurs exactly once
    /// and in that order: inside a JSON string every `"` is escaped, so a
    /// key with its value cannot appear there, and anything else is
    /// refused rather than guessed.
    pub(crate) fn cut(doc: &[u8], open: &[Open]) -> Option<Self> {
        let mut pieces = Vec::with_capacity(open.len() + 1);
        let mut from = 0;
        for o in open {
            let mut needle = Vec::new();
            let value_at = match o {
                Open::Str(k) | Open::U64(k) => {
                    write_json_str(&mut needle, k);
                    needle.push(b':');
                    needle.len()
                }
            };
            match o {
                Open::Str(_) => write_json_str(&mut needle, MARK_STR),
                Open::U64(_) => write_json_u64(&mut needle, MARK_U64),
            }
            let at = find(doc, &needle)?;
            if at < from || find(&doc[at + 1..], &needle).is_some() {
                return None;
            }
            pieces.push(doc[from..at + value_at].to_vec());
            from = at + needle.len();
        }
        pieces.push(doc[from..].to_vec());
        Some(Self {
            pieces,
            open: open.to_vec(),
        })
    }

    /// The document with `fills` as its open fields' values, in order.
    /// `None` when they do not match the open fields.
    pub(crate) fn fill(&self, fills: &[Fill<'_>]) -> Option<Vec<u8>> {
        if fills.len() != self.open.len() {
            return None;
        }
        let len = self.pieces.iter().map(Vec::len).sum::<usize>()
            + fills
                .iter()
                .map(|f| match f {
                    Fill::Str(s) => s.len() + 2,
                    Fill::U64(_) => 20,
                })
                .sum::<usize>();
        let mut out = Vec::with_capacity(len);
        for (i, (f, o)) in fills.iter().zip(&self.open).enumerate() {
            out.extend_from_slice(&self.pieces[i]);
            match (f, o) {
                (Fill::Str(s), Open::Str(_)) => write_json_str(&mut out, s),
                (Fill::U64(n), Open::U64(_)) => write_json_u64(&mut out, *n),
                _ => return None,
            }
        }
        out.extend_from_slice(self.pieces.last().expect("one more piece"));
        Some(out)
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marked(doc: &str) -> Vec<u8> {
        let mut s = Vec::new();
        write_json_str(&mut s, MARK_STR);
        let mut n = Vec::new();
        write_json_u64(&mut n, MARK_U64);
        doc.replace("S", std::str::from_utf8(&s).unwrap())
            .replace("N", std::str::from_utf8(&n).unwrap())
            .into_bytes()
    }

    #[test]
    fn a_filled_document_is_the_document_with_those_values() {
        let t = JsonTemplate::cut(
            &marked(r#"{"a":"x","p":N,"g":S,"z":[1]}"#),
            &[Open::U64("p"), Open::Str("g")],
        )
        .unwrap();
        assert_eq!(
            t.fill(&[Fill::U64(7), Fill::Str("q\"\\\n")]).unwrap(),
            br#"{"a":"x","p":7,"g":"q\"\\\n","z":[1]}"#
        );
        assert!(t.fill(&[Fill::Str("x"), Fill::U64(7)]).is_none());
        assert!(t.fill(&[Fill::U64(7)]).is_none());
    }

    #[test]
    fn a_mark_that_is_not_one_field_is_refused() {
        let open = [Open::U64("p"), Open::Str("g")];
        // Twice, missing, or out of order: nothing is guessed.
        assert!(
            JsonTemplate::cut(&marked(r#"{"p":N,"g":S,"x":{"g":S}}"#), &open)
                .is_none()
        );
        assert!(
            JsonTemplate::cut(&marked(r#"{"p":N,"g":"y"}"#), &open).is_none()
        );
        assert!(
            JsonTemplate::cut(&marked(r#"{"g":S,"p":N}"#), &open).is_none()
        );
    }
}
