use crate::binlog_error::BinlogError;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::io::Cursor;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GtidEvent {
    pub flags: u8,
    pub gtid: String,
}

impl GtidEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html
        // refer: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Gtid__event.html
        let flags = cursor.read_u8()?;
        // `uuid:gno` written once into one buffer (DeltaForge patch: this
        // runs for every transaction).
        let mut gtid = String::with_capacity(36 + 1 + 20);
        Self::write_uuid(cursor, &mut gtid)?;
        let gno = cursor.read_u64::<LittleEndian>()?;
        write!(gtid, ":{gno}")?;
        Ok(GtidEvent { flags, gtid })
    }

    pub fn read_uuid(cursor: &mut Cursor<&Vec<u8>>) -> Result<String, BinlogError> {
        let mut s = String::with_capacity(36);
        Self::write_uuid(cursor, &mut s)?;
        Ok(s)
    }

    /// The next 16 bytes as a lowercase hyphenated UUID (8-4-4-4-12 hex digits).
    fn write_uuid(cursor: &mut Cursor<&Vec<u8>>, out: &mut String) -> Result<(), BinlogError> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut b = [0u8; 16];
        std::io::Read::read_exact(cursor, &mut b)?;
        for (i, x) in b.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                out.push('-');
            }
            out.push(HEX[(x >> 4) as usize] as char);
            out.push(HEX[(x & 0xf) as usize] as char);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(flags: u8, sid: [u8; 16], gno: u64) -> Vec<u8> {
        let mut b = vec![flags];
        b.extend_from_slice(&sid);
        b.extend_from_slice(&gno.to_le_bytes());
        b
    }

    #[test]
    fn a_gtid_reads_as_its_lowercase_uuid_and_number() {
        let sid = [
            0xf3, 0xbc, 0x6e, 0x9d, 0xe1, 0x27, 0x11, 0xf1, 0x97, 0x4c, 0x02, 0x42, 0xac, 0x1d,
            0x00, 0x03,
        ];
        let b = event(1, sid, 2402528);
        let g = GtidEvent::parse(&mut Cursor::new(&b)).unwrap();
        assert_eq!(g.flags, 1);
        assert_eq!(g.gtid, "f3bc6e9d-e127-11f1-974c-0242ac1d0003:2402528");
    }

    #[test]
    fn every_byte_value_and_the_number_extremes_read_as_hex_and_decimal() {
        for (start, gno) in [(0u8, 0u64), (16, 1), (240, u64::MAX)] {
            let sid: [u8; 16] = std::array::from_fn(|i| start.wrapping_add(i as u8));
            let b = event(0, sid, gno);
            let hex: Vec<String> = sid.iter().map(|x| format!("{x:02x}")).collect();
            let want = format!(
                "{}-{}-{}-{}-{}:{gno}",
                hex[0..4].concat(),
                hex[4..6].concat(),
                hex[6..8].concat(),
                hex[8..10].concat(),
                hex[10..16].concat()
            );
            assert_eq!(GtidEvent::parse(&mut Cursor::new(&b)).unwrap().gtid, want);
        }
    }

    #[test]
    fn a_short_event_is_an_error() {
        let b = event(0, [0; 16], 7);
        for n in [0, 1, 10, 17, 24] {
            let short = b[..n].to_vec();
            assert!(
                GtidEvent::parse(&mut Cursor::new(&short)).is_err(),
                "{n} bytes"
            );
        }
    }
}
