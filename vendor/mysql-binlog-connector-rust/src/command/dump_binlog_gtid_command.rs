use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::binlog_error::BinlogError;

use super::{command_type::CommandType, gtid_set::GtidSet};

pub struct DumpBinlogGtidCommand {
    pub server_id: u64,
    pub gtid_set: GtidSet,
    /// The binlog file and offset to start at (DeltaForge patch); `None`
    /// sends no file name and offset 4 (the server picks the file).
    pub start: Option<(String, u32)>,
}

impl DumpBinlogGtidCommand {
    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();
        buf.write_u8(CommandType::BinlogDumpGtid as u8)?;

        // BINLOG_DUMP_NEVER_STOP           = 0x00
        // BINLOG_DUMP_NON_BLOCK            = 0x01
        // BINLOG_SEND_ANNOTATE_ROWS_EVENT  = 0x02
        // BINLOG_THROUGH_POSITION          = 0x02
        // BINLOG_THROUGH_GTID              = 0x04
        let binlog_flags = 4;
        buf.write_u16::<LittleEndian>(binlog_flags)?;

        buf.write_u32::<LittleEndian>(self.server_id as u32)?;
        // binlog-filename-len, binlog-filename, binlog-pos
        match &self.start {
            Some((file, pos)) => {
                buf.write_u32::<LittleEndian>(file.len() as u32)?;
                buf.write_all(file.as_bytes())?;
                buf.write_u64::<LittleEndian>(u64::from(*pos))?;
            }
            None => {
                buf.write_u32::<LittleEndian>(0)?;
                buf.write_u64::<LittleEndian>(4)?;
            }
        }

        let mut data_size = 8; // number of uuid_sets
        for uuid_set in self.gtid_set.map.values() {
            data_size += 16; // uuid
            data_size += 8; // number of intervals
            data_size += uuid_set.intervals.len() * 16; // start to end
        }
        buf.write_u32::<LittleEndian>(data_size as u32)?;

        buf.write_u64::<LittleEndian>(self.gtid_set.map.len() as u64)?;
        for (uuid, uuid_set) in self.gtid_set.map.iter() {
            let uuid_bytes = Self::hex_to_bytes(&uuid.replace('-', ""))?;
            buf.write_all(&uuid_bytes)?;

            // intervals
            buf.write_u64::<LittleEndian>(uuid_set.intervals.len() as u64)?;
            for interval in &uuid_set.intervals {
                buf.write_u64::<LittleEndian>(interval.start)?;
                buf.write_u64::<LittleEndian>(interval.end + 1)?; // right-open
            }
        }

        Ok(buf)
    }

    fn hex_to_bytes(uuid: &str) -> Result<Vec<u8>, BinlogError> {
        let mut bytes = Vec::with_capacity(uuid.len() / 2);
        for i in (0..uuid.len()).step_by(2) {
            let hex_byte = &uuid[i..i + 2];
            bytes.push(u8::from_str_radix(hex_byte, 16)?);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(start: Option<(String, u32)>) -> Vec<u8> {
        DumpBinlogGtidCommand {
            server_id: 7,
            gtid_set: GtidSet::new("").unwrap(),
            start,
        }
        .to_bytes()
        .unwrap()
    }

    #[test]
    fn without_a_start_the_server_picks_the_file() {
        let b = command(None);
        // command, flags (2), server id (4), then name length 0 and
        // position 4.
        assert_eq!(&b[7..11], &0u32.to_le_bytes());
        assert_eq!(&b[11..19], &4u64.to_le_bytes());
    }

    #[test]
    fn a_start_sends_its_file_and_offset() {
        let b = command(Some(("mysql-bin.000003".into(), 1_010_955_828)));
        assert_eq!(&b[7..11], &16u32.to_le_bytes());
        assert_eq!(&b[11..27], b"mysql-bin.000003");
        assert_eq!(&b[27..35], &1_010_955_828u64.to_le_bytes());
        // The GTID set follows, unchanged (an empty one here).
        assert_eq!(&b[35..39], &8u32.to_le_bytes());
    }
}
