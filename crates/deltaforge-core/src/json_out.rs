//! Compact JSON output, byte for byte what `serde_json` writes, faster.
//!
//! [`to_writer`] and [`to_vec`] are `serde_json::to_writer` / `to_vec` for a
//! `Vec<u8>` with the compact formatter: the same bytes for every value, and
//! the same result when a value cannot be written. Strings are scanned eight
//! bytes at a time for the characters JSON must escape (`"`, `\` and the
//! control characters below U+0020) and copied in runs; everything else
//! follows `serde_json`'s serializer (integers through `itoa`, finite floats
//! through `zmij`, non-finite floats as `null`, map keys quoted as it quotes
//! them). Any error - a key that is not a string, a failing `Display`, one
//! of `serde_json`'s private marker types - truncates what was written and
//! hands the value to `serde_json` itself, whose result is returned: output
//! and errors never differ from it. `tests` hold the equivalence against the
//! `serde_json` in the lock file.

use serde::ser::{self, Impossible, Serialize};
use std::fmt::{self, Display};

/// `serde_json::to_writer(&mut *buf, value)`, appending to `buf`.
pub fn to_writer<T: ?Sized + Serialize>(
    buf: &mut Vec<u8>,
    value: &T,
) -> serde_json::Result<()> {
    let start = buf.len();
    if value.serialize(&mut Writer { out: buf }).is_ok() {
        return Ok(());
    }
    buf.truncate(start);
    serde_json::to_writer(&mut *buf, value)
}

/// `serde_json::to_vec(value)`.
pub fn to_vec<T: ?Sized + Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(128);
    to_writer(&mut buf, value)?;
    Ok(buf)
}

/// A string as [`to_vec`] writes it: quoted, with JSON's escapes.
pub fn write_json_str(out: &mut Vec<u8>, s: &str) {
    write_str(out, s);
}

/// A `u64` as [`to_vec`] writes it.
pub fn write_json_u64(out: &mut Vec<u8>, n: u64) {
    out.extend_from_slice(itoa::Buffer::new().format(n).as_bytes());
}

/// Why this writer stopped; the value is then written by `serde_json`.
#[derive(Debug)]
struct Stop;

impl Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("written by serde_json instead")
    }
}

impl std::error::Error for Stop {}

impl ser::Error for Stop {
    fn custom<T: Display>(_msg: T) -> Self {
        Stop
    }
}

type Result<T = ()> = std::result::Result<T, Stop>;

/// `serde_json`'s private marker structs (`RawValue`, arbitrary-precision
/// `Number`): written only by `serde_json`.
fn is_serde_json_private(name: &str) -> bool {
    name.starts_with("$serde_json::private::")
}

struct Writer<'a> {
    out: &'a mut Vec<u8>,
}

/// What `serde_json` escapes, by byte: 0 for none, else the escape letter
/// (`u` for `\u00XX`).
static ESCAPE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 0x20 {
        t[i] = b'u';
        i += 1;
    }
    t[0x08] = b'b';
    t[0x09] = b't';
    t[0x0A] = b'n';
    t[0x0C] = b'f';
    t[0x0D] = b'r';
    t[b'"' as usize] = b'"';
    t[b'\\' as usize] = b'\\';
    t
};

const LANES: u64 = 0x0101_0101_0101_0101;
const HIGH: u64 = 0x8080_8080_8080_8080;

/// Whether any byte of `w` may need an escape: below 0x20, `"` or `\`. A
/// true result can be spurious (the bytes are then checked one by one); a
/// false one never is.
#[inline]
fn may_escape(w: u64) -> bool {
    let control = w.wrapping_sub(LANES * 0x20) & !w;
    let q = w ^ (LANES * b'"' as u64);
    let quote = q.wrapping_sub(LANES) & !q;
    let b = w ^ (LANES * b'\\' as u64);
    let backslash = b.wrapping_sub(LANES) & !b;
    (control | quote | backslash) & HIGH != 0
}

#[inline]
fn write_escape(out: &mut Vec<u8>, byte: u8, escape: u8) {
    if escape == b'u' {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        out.extend_from_slice(&[
            b'\\',
            b'u',
            b'0',
            b'0',
            HEX[(byte >> 4) as usize],
            HEX[(byte & 0xF) as usize],
        ]);
    } else {
        out.extend_from_slice(&[b'\\', escape]);
    }
}

/// `s` escaped, without the quotes.
fn write_str_contents(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    out.reserve(b.len());
    let mut run = 0;
    let mut i = 0;
    while i + 8 <= b.len() {
        let w = u64::from_le_bytes(b[i..i + 8].try_into().expect("8 bytes"));
        if may_escape(w) {
            for (j, &byte) in b.iter().enumerate().skip(i).take(8) {
                let e = ESCAPE[byte as usize];
                if e != 0 {
                    out.extend_from_slice(&b[run..j]);
                    write_escape(out, byte, e);
                    run = j + 1;
                }
            }
        }
        i += 8;
    }
    for (j, &byte) in b.iter().enumerate().skip(i) {
        let e = ESCAPE[byte as usize];
        if e != 0 {
            out.extend_from_slice(&b[run..j]);
            write_escape(out, byte, e);
            run = j + 1;
        }
    }
    out.extend_from_slice(&b[run..]);
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    out.reserve(s.len() + 2);
    out.push(b'"');
    write_str_contents(out, s);
    out.push(b'"');
}

/// A `Display` value's text, escaped as it is formatted.
struct Escaping<'a>(&'a mut Vec<u8>);

impl fmt::Write for Escaping<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        write_str_contents(self.0, s);
        Ok(())
    }
}

fn write_display<T: ?Sized + Display>(out: &mut Vec<u8>, value: &T) -> Result {
    use fmt::Write;
    out.push(b'"');
    write!(Escaping(out), "{value}").map_err(|_| Stop)?;
    out.push(b'"');
    Ok(())
}

macro_rules! int {
    ($($name:ident: $t:ty),*) => {$(
        #[inline]
        fn $name(self, v: $t) -> Result {
            self.out
                .extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
            Ok(())
        }
    )*};
}

impl<'a, 'b> ser::Serializer for &'a mut Writer<'b> {
    type Ok = ();
    type Error = Stop;
    type SerializeSeq = Compound<'a, 'b>;
    type SerializeTuple = Compound<'a, 'b>;
    type SerializeTupleStruct = Compound<'a, 'b>;
    type SerializeTupleVariant = Compound<'a, 'b>;
    type SerializeMap = Compound<'a, 'b>;
    type SerializeStruct = Compound<'a, 'b>;
    type SerializeStructVariant = Compound<'a, 'b>;

    fn serialize_bool(self, v: bool) -> Result {
        self.out
            .extend_from_slice(if v { b"true" } else { b"false" });
        Ok(())
    }

    int!(serialize_i8: i8, serialize_i16: i16, serialize_i32: i32, serialize_i64: i64,
         serialize_i128: i128, serialize_u8: u8, serialize_u16: u16, serialize_u32: u32,
         serialize_u64: u64, serialize_u128: u128);

    fn serialize_f32(self, v: f32) -> Result {
        if v.is_finite() {
            self.out.extend_from_slice(
                zmij::Buffer::new().format_finite(v).as_bytes(),
            );
        } else {
            self.out.extend_from_slice(b"null");
        }
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result {
        if v.is_finite() {
            self.out.extend_from_slice(
                zmij::Buffer::new().format_finite(v).as_bytes(),
            );
        } else {
            self.out.extend_from_slice(b"null");
        }
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result {
        write_str(self.out, v.encode_utf8(&mut [0; 4]));
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result {
        write_str(self.out, v);
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result {
        self.out.push(b'[');
        for (i, byte) in v.iter().enumerate() {
            if i > 0 {
                self.out.push(b',');
            }
            self.out.extend_from_slice(
                itoa::Buffer::new().format(*byte).as_bytes(),
            );
        }
        self.out.push(b']');
        Ok(())
    }

    fn serialize_none(self) -> Result {
        self.serialize_unit()
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result {
        self.out.extend_from_slice(b"null");
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result {
        if is_serde_json_private(name) {
            return Err(Stop);
        }
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result {
        self.out.push(b'{');
        write_str(self.out, variant);
        self.out.push(b':');
        value.serialize(&mut *self)?;
        self.out.push(b'}');
        Ok(())
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Compound<'a, 'b>> {
        self.out.push(b'[');
        if len == Some(0) {
            self.out.push(b']');
            return Ok(Compound::new(self, State::Empty));
        }
        Ok(Compound::new(self, State::First))
    }

    fn serialize_tuple(self, len: usize) -> Result<Compound<'a, 'b>> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Compound<'a, 'b>> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Compound<'a, 'b>> {
        self.out.push(b'{');
        write_str(self.out, variant);
        self.out.push(b':');
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Compound<'a, 'b>> {
        self.out.push(b'{');
        if len == Some(0) {
            self.out.push(b'}');
            return Ok(Compound::new(self, State::Empty));
        }
        Ok(Compound::new(self, State::First))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Compound<'a, 'b>> {
        if is_serde_json_private(name) {
            return Err(Stop);
        }
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Compound<'a, 'b>> {
        self.out.push(b'{');
        write_str(self.out, variant);
        self.out.push(b':');
        self.serialize_map(Some(len))
    }

    fn collect_str<T: ?Sized + Display>(self, value: &T) -> Result {
        write_display(self.out, value)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Empty,
    First,
    Rest,
}

struct Compound<'a, 'b> {
    w: &'a mut Writer<'b>,
    state: State,
}

impl<'a, 'b> Compound<'a, 'b> {
    fn new(w: &'a mut Writer<'b>, state: State) -> Self {
        Self { w, state }
    }

    /// The separator before an element or key.
    fn next(&mut self) {
        if self.state != State::First {
            self.w.out.push(b',');
        }
        self.state = State::Rest;
    }

    fn element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result {
        self.next();
        value.serialize(&mut *self.w)
    }

    fn close(self, close: u8) {
        if self.state != State::Empty {
            self.w.out.push(close);
        }
    }

    /// A variant's array or object, then the object naming the variant.
    fn close_variant(self, close: u8) {
        let Compound { w, state } = self;
        if state != State::Empty {
            w.out.push(close);
        }
        w.out.push(b'}');
    }
}

impl ser::SerializeSeq for Compound<'_, '_> {
    type Ok = ();
    type Error = Stop;
    fn serialize_element<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> Result {
        self.element(value)
    }
    fn end(self) -> Result {
        self.close(b']');
        Ok(())
    }
}

impl ser::SerializeTuple for Compound<'_, '_> {
    type Ok = ();
    type Error = Stop;
    fn serialize_element<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> Result {
        self.element(value)
    }
    fn end(self) -> Result {
        self.close(b']');
        Ok(())
    }
}

impl ser::SerializeTupleStruct for Compound<'_, '_> {
    type Ok = ();
    type Error = Stop;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result {
        self.element(value)
    }
    fn end(self) -> Result {
        self.close(b']');
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Compound<'_, '_> {
    type Ok = ();
    type Error = Stop;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result {
        self.element(value)
    }
    fn end(self) -> Result {
        self.close_variant(b']');
        Ok(())
    }
}

impl ser::SerializeMap for Compound<'_, '_> {
    type Ok = ();
    type Error = Stop;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result {
        self.next();
        key.serialize(KeyWriter { out: self.w.out })
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result {
        self.w.out.push(b':');
        value.serialize(&mut *self.w)
    }
    fn end(self) -> Result {
        self.close(b'}');
        Ok(())
    }
}

impl ser::SerializeStruct for Compound<'_, '_> {
    type Ok = ();
    type Error = Stop;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result {
        self.next();
        write_str(self.w.out, key);
        self.w.out.push(b':');
        value.serialize(&mut *self.w)
    }
    fn end(self) -> Result {
        self.close(b'}');
        Ok(())
    }
}

impl ser::SerializeStructVariant for Compound<'_, '_> {
    type Ok = ();
    type Error = Stop;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result {
        ser::SerializeStruct::serialize_field(self, key, value)
    }
    fn end(self) -> Result {
        self.close_variant(b'}');
        Ok(())
    }
}

/// A map key: strings as they are, integers, `bool` and finite floats as
/// quoted text; anything else is not a key (`serde_json` then reports it).
struct KeyWriter<'a> {
    out: &'a mut Vec<u8>,
}

macro_rules! quoted {
    ($($name:ident: $t:ty),*) => {$(
        fn $name(self, v: $t) -> Result {
            self.out.push(b'"');
            self.out
                .extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
            self.out.push(b'"');
            Ok(())
        }
    )*};
}

impl ser::Serializer for KeyWriter<'_> {
    type Ok = ();
    type Error = Stop;
    type SerializeSeq = Impossible<(), Stop>;
    type SerializeTuple = Impossible<(), Stop>;
    type SerializeTupleStruct = Impossible<(), Stop>;
    type SerializeTupleVariant = Impossible<(), Stop>;
    type SerializeMap = Impossible<(), Stop>;
    type SerializeStruct = Impossible<(), Stop>;
    type SerializeStructVariant = Impossible<(), Stop>;

    fn serialize_str(self, v: &str) -> Result {
        write_str(self.out, v);
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result {
        write_str(self.out, variant);
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result {
        if is_serde_json_private(name) {
            return Err(Stop);
        }
        value.serialize(self)
    }

    fn serialize_bool(self, v: bool) -> Result {
        self.out
            .extend_from_slice(if v { b"\"true\"" } else { b"\"false\"" });
        Ok(())
    }

    quoted!(serialize_i8: i8, serialize_i16: i16, serialize_i32: i32, serialize_i64: i64,
            serialize_i128: i128, serialize_u8: u8, serialize_u16: u16, serialize_u32: u32,
            serialize_u64: u64, serialize_u128: u128);

    fn serialize_f32(self, v: f32) -> Result {
        if !v.is_finite() {
            return Err(Stop);
        }
        self.out.push(b'"');
        self.out
            .extend_from_slice(zmij::Buffer::new().format_finite(v).as_bytes());
        self.out.push(b'"');
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result {
        if !v.is_finite() {
            return Err(Stop);
        }
        self.out.push(b'"');
        self.out
            .extend_from_slice(zmij::Buffer::new().format_finite(v).as_bytes());
        self.out.push(b'"');
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result {
        write_str(self.out, v.encode_utf8(&mut [0; 4]));
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result {
        value.serialize(self)
    }

    fn collect_str<T: ?Sized + Display>(self, value: &T) -> Result {
        write_display(self.out, value)
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result {
        Err(Stop)
    }
    fn serialize_unit(self) -> Result {
        Err(Stop)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result {
        Err(Stop)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result {
        Err(Stop)
    }
    fn serialize_none(self) -> Result {
        Err(Stop)
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(Stop)
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(Stop)
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(Stop)
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(Stop)
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(Stop)
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct> {
        Err(Stop)
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(Stop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use std::collections::BTreeMap;

    /// The same bytes, or both an error; a value serde_json writes is
    /// written by this writer itself, not by the fallback.
    #[track_caller]
    fn same<T: ?Sized + Serialize>(value: &T) {
        if serde_json::to_vec(value).is_ok() {
            let mut own = Vec::new();
            assert!(
                value.serialize(&mut Writer { out: &mut own }).is_ok(),
                "fell back to serde_json"
            );
        }
        same_result(value);
    }

    /// The same bytes, or both an error (by either path).
    #[track_caller]
    fn same_result<T: ?Sized + Serialize>(value: &T) {
        let want = serde_json::to_vec(value);
        let got = to_vec(value);
        match (&want, &got) {
            (Ok(w), Ok(g)) => assert_eq!(
                String::from_utf8_lossy(g),
                String::from_utf8_lossy(w)
            ),
            (Err(_), Err(_)) => {}
            _ => panic!("serde_json {want:?}, json_out {got:?}"),
        }
        // Appending keeps what the buffer held.
        let mut buf = b"prefix".to_vec();
        let r = to_writer(&mut buf, value);
        assert_eq!(r.is_ok(), want.is_ok());
        if let Ok(w) = &want {
            assert_eq!(&buf[6..], &w[..]);
        }
        assert_eq!(&buf[..6], b"prefix");
    }

    /// A small deterministic generator (xorshift64*).
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn string(&mut self, max: u64) -> String {
            const PIECES: &[&str] = &[
                "a", "Z", "0", " ", "\"", "\\", "/", "\n", "\r", "\t", "\u{8}",
                "\u{c}", "\u{0}", "\u{1f}", "\u{7f}", "\u{80}", "é", "€", "𝄞",
                "\u{2028}", "\u{feff}", "abcdefgh", "xyz",
            ];
            let n = self.below(max + 1);
            (0..n)
                .map(|_| PIECES[self.below(PIECES.len() as u64) as usize])
                .collect()
        }
        fn value(&mut self, depth: u32) -> serde_json::Value {
            use serde_json::Value;
            let kinds = if depth == 0 { 6 } else { 8 };
            match self.below(kinds) {
                0 => Value::Null,
                1 => Value::Bool(self.below(2) == 0),
                2 => Value::from(self.next() as i64),
                3 => Value::from(self.next()),
                4 => serde_json::Number::from_f64(f64::from_bits(self.next()))
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
                5 => Value::String(self.string(40)),
                6 => Value::Array(
                    (0..self.below(5)).map(|_| self.value(depth - 1)).collect(),
                ),
                _ => Value::Object(
                    (0..self.below(5))
                        .map(|_| (self.string(12), self.value(depth - 1)))
                        .collect(),
                ),
            }
        }
    }

    #[test]
    fn every_byte_escapes_as_serde_json_escapes_it_at_every_offset() {
        for c in
            (0u32..0x80).chain([0x80, 0x7ff, 0x800, 0xffff, 0x10000, 0x10ffff])
        {
            let c = char::from_u32(c).unwrap();
            for before in 0..17 {
                for after in [0, 1, 7, 8, 9] {
                    let s = format!(
                        "{}{c}{}",
                        "x".repeat(before),
                        "y".repeat(after)
                    );
                    same(&s);
                }
            }
        }
        same("");
        same(&"\"".repeat(33));
        same(&"\\\u{0}\u{1f}".repeat(11));
        same(&"a".repeat(1000));
    }

    #[test]
    fn generated_strings_write_as_serde_json_writes_them() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..20_000 {
            same(&rng.string(64));
        }
    }

    #[test]
    fn generated_values_write_as_serde_json_writes_them() {
        let mut rng = Rng(0xd1b5_4a32_d192_ed03);
        for _ in 0..5_000 {
            same(&rng.value(4));
        }
    }

    #[test]
    fn numbers_write_as_serde_json_writes_them() {
        for v in [0i64, 1, -1, i64::MIN, i64::MAX, 10, -10, 999_999_999_999] {
            same(&v);
        }
        for v in [0u64, u64::MAX, 1 << 53, (1 << 53) + 1] {
            same(&v);
        }
        same(&(i8::MIN, i16::MIN, i32::MIN, u8::MAX, u16::MAX, u32::MAX));
        same(&(i128::MIN, i128::MAX, u128::MAX, 0i128));
        let floats = [
            0.0,
            -0.0,
            1.0,
            -1.5,
            0.1,
            1e21,
            1e-7,
            123456789.0,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            5e-324,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.0 / 3.0,
            2.5e15,
            1e16,
            9007199254740993.0,
        ];
        for v in floats {
            same(&v);
            same(&(v as f32));
        }
        let mut rng = Rng(42);
        for _ in 0..20_000 {
            same(&f64::from_bits(rng.next()));
            same(&f32::from_bits(rng.next() as u32));
        }
    }

    #[derive(Serialize)]
    struct Unit;
    #[derive(Serialize)]
    struct Newtype(i32);
    #[derive(Serialize)]
    struct Tuple(i32, &'static str);
    #[derive(Serialize)]
    struct Empty {}
    #[derive(Serialize)]
    enum Shape {
        Unit,
        Newtype(String),
        Pair(i32, i32),
        NoFields(),
        Named { a: i32, b: Option<i32> },
        NoNames {},
    }
    struct Bytes<'a>(&'a [u8]);
    impl Serialize for Bytes<'_> {
        fn serialize<S: ser::Serializer>(
            &self,
            s: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            s.serialize_bytes(self.0)
        }
    }
    struct Shown<T>(T);
    impl<T: Display> Serialize for Shown<T> {
        fn serialize<S: ser::Serializer>(
            &self,
            s: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            s.collect_str(&self.0)
        }
    }
    #[derive(Serialize)]
    struct Inner {
        x: i32,
        #[serde(skip_serializing_if = "Option::is_none")]
        y: Option<&'static str>,
    }
    #[derive(Serialize)]
    struct Outer {
        id: u64,
        #[serde(flatten)]
        inner: Inner,
        #[serde(flatten)]
        extra: BTreeMap<String, i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gone: Option<i32>,
    }

    #[test]
    fn every_serde_shape_writes_as_serde_json_writes_it() {
        same(&());
        same(&Unit);
        same(&Newtype(-7));
        same(&Tuple(1, "t\"wo"));
        same(&Empty {});
        same(&(1, "a", 'c', '"', true, false, None::<i32>, Some(3)));
        for e in [
            Shape::Unit,
            Shape::Newtype("n\n".into()),
            Shape::Pair(1, 2),
            Shape::NoFields(),
            Shape::Named { a: 1, b: None },
            Shape::Named { a: 1, b: Some(2) },
            Shape::NoNames {},
        ] {
            same(&e);
            same(&vec![e]);
        }
        same(&Vec::<i32>::new());
        same(&vec![Vec::<i32>::new(), vec![1], vec![1, 2]]);
        same(&Bytes(&[]));
        same(&Bytes(&[0, 1, 127, 128, 255]));
        same(&Shown(1234567));
        same(&Shown("a\"b\\c\nd"));
        same(&Shown(
            chrono::NaiveDate::from_ymd_opt(2026, 11, 16).unwrap(),
        ));
        same(&Outer {
            id: 9,
            inner: Inner { x: 1, y: Some("y") },
            extra: [("k\"".to_string(), 1), ("z".to_string(), 2)].into(),
            gone: None,
        });
        same(&Outer {
            id: 9,
            inner: Inner { x: 1, y: None },
            extra: BTreeMap::new(),
            gone: Some(1),
        });
    }

    #[test]
    fn map_keys_write_as_serde_json_writes_them() {
        same(&BTreeMap::from([(1i64, "a"), (-2, "b")]));
        same(&BTreeMap::from([(u128::MAX, 1), (0, 2)]));
        same(&BTreeMap::from([(true, 1), (false, 2)]));
        same(&BTreeMap::from([('k', 1), ('"', 2)]));
        same(&BTreeMap::from([(Newtype(3), 1)].map(|(k, v)| (k.0, v))));
        same(&std::collections::HashMap::from(
            [(Shown(5), 1)].map(|(k, v)| (k.0, v)),
        ));
        #[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
        enum K {
            A,
            B(i32),
        }
        same(&BTreeMap::from([(K::A, 1)]));
        // Not keys: both refuse.
        same(&BTreeMap::from([(K::B(1), 1)]));
        same(&BTreeMap::from([(vec![1], 1)]));
        same(&BTreeMap::from([((), 1)]));
        same(&BTreeMap::from([(None::<i32>, 1)]));
        same(&BTreeMap::from([(Some("s"), 1)]));
        // Float keys: finite ones quoted, others refused.
        struct FloatKeys(Vec<(f64, i32)>);
        impl Serialize for FloatKeys {
            fn serialize<S: ser::Serializer>(
                &self,
                s: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                use ser::SerializeMap;
                let mut m = s.serialize_map(Some(self.0.len()))?;
                for (k, v) in &self.0 {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
        }
        same(&FloatKeys(vec![(1.5, 1), (-0.0, 2), (1e300, 3)]));
        same(&FloatKeys(vec![(f64::NAN, 1)]));
        same(&FloatKeys(vec![(f64::INFINITY, 1)]));
    }

    #[test]
    fn serde_json_private_types_are_written_by_serde_json() {
        let raw =
            serde_json::value::RawValue::from_string(r#"{"a": [1, 2]}"#.into())
                .unwrap();
        same_result(&raw);
        same_result(&vec![raw.clone(), raw]);
        #[derive(Serialize)]
        struct WithRaw<'a> {
            a: i32,
            r: &'a serde_json::value::RawValue,
        }
        let raw =
            serde_json::value::RawValue::from_string("  true ".into()).unwrap();
        same_result(&WithRaw { a: 1, r: &raw });
        let mut own = Vec::new();
        assert!(raw.serialize(&mut Writer { out: &mut own }).is_err());
    }
}
