use crc::{Crc, CRC_32_ISCSI};
use log::error;

use common::error_type::ET;
use common::id::OID;
use common::result::Result;

use crate::access::clog::CLogRecType;
use crate::access::get_set_int::{get_u128, get_u32, set_u128, set_u32};

/// clog layout
/// --- |16 byte| clog header
/// --- |N  byte| command header, N depends on command type
/// --- |X  byte| payload

/// clog header layout
/// page header 16 byte
/// --- |4 byte| log record size size
///     |4 byte| checksum of the log(from 8 byte offset to the end)
///     |4 byte| LSN(log sequence number)
///     |4 byte| command type
const CLOG_HEADER_SIZE: u32 = 16;

const OFF_SIZE: u32 = 0;
const OFF_CHECKSUM: u32 = 4;
const OFF_LSN: u32 = 8;
const OFF_COMMAND_TYPE: u32 = 12;

/// update(insert, delete) key value command header layout
/// page header 44 byte
/// --- |4 byte| update type(UP_CMD_WRITE, UP_CMD_DELETE)
///     |4 byte| key size
///     |4 byte| value size
///     |16 byte| table id
///     |16 byte| tuple id
const UP_CMD_HEADER_SIZE: u32 = 44;

const OFF_UP_CMD_TYPE: u32 = 0;
const OFF_UP_CMD_KEY_SIZE: u32 = 4;
const OFF_UP_CMD_VALUE_SIZE: u32 = 8;
const OFF_UP_CMD_TABLE_ID: u32 = 12;
const OFF_UP_CMD_TUPLE_ID: u32 = 28;

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub struct CLogRec<'a> {
    slice: &'a [u8],
}

pub struct CUpdateKV<'a> {
    slice: &'a [u8],
}

impl<'a> CLogRec<'a> {
    pub fn header_size() -> usize {
        CLOG_HEADER_SIZE as usize
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn load(s: &'a [u8]) -> Result<Self> {
        if s.len() < CLOG_HEADER_SIZE as usize {
            return Err(ET::CorruptLog);
        }
        let c = CLogRec { slice: s };
        let size = c.size() as usize;
        if size > c.slice.len() {
            return Err(ET::CorruptLog);
        }
        let checksum = CRC.checksum(&c.slice[OFF_LSN as usize..size]);
        // trace!("checksum {}, range [{}, {}]", checksum,OFF_LSN, s.len());
        let log_checksum = c.checksum();
        if log_checksum != checksum {
            error!(
                "checksum expected: {} but found: {}",
                checksum, log_checksum
            );
            return Err(ET::CorruptLog);
        }
        Ok(c)
    }

    pub fn from(s: &'a [u8]) -> Self {
        CLogRec { slice: s }
    }

    pub fn format_up_kv_cmd(
        s: &'a mut [u8],
        cmd: u32,
        lsn: u32,
        table_id: OID,
        oid: OID,
        key: &[u8],
        value: &[u8],
    ) {
        let size = (CLOG_HEADER_SIZE + UP_CMD_HEADER_SIZE) as usize + key.len() + value.len();
        let mut vec = Vec::new();
        vec.resize(size, 0);
        // format log header
        set_u32(s.as_ptr(), vec.len(), OFF_SIZE, size as u32);
        set_u32(s.as_ptr(), vec.len(), OFF_LSN, lsn);
        set_u32(
            s.as_ptr(),
            vec.len(),
            OFF_COMMAND_TYPE,
            CLogRecType::CUpdateKV as u32,
        );

        // format command header
        set_u32(
            s.as_ptr(),
            vec.len(),
            CLOG_HEADER_SIZE + OFF_UP_CMD_TYPE,
            cmd,
        );
        set_u32(
            s.as_ptr(),
            vec.len(),
            CLOG_HEADER_SIZE + OFF_UP_CMD_KEY_SIZE,
            key.len() as u32,
        );
        set_u32(
            s.as_ptr(),
            vec.len(),
            CLOG_HEADER_SIZE + OFF_UP_CMD_VALUE_SIZE,
            value.len() as u32,
        );
        set_u128(
            s.as_ptr(),
            vec.len(),
            CLOG_HEADER_SIZE + OFF_UP_CMD_TABLE_ID,
            table_id,
        );
        set_u128(
            s.as_ptr(),
            vec.len(),
            CLOG_HEADER_SIZE + OFF_UP_CMD_TUPLE_ID,
            oid,
        );

        // update checksum
        let checksum = CRC.checksum(&s[OFF_LSN as usize..]);
        // debug!("checksum {}, range [{}, {}]", checksum,OFF_LSN, s.len());
        set_u32(s.as_ptr(), vec.len(), OFF_CHECKSUM, checksum);
    }

    pub fn size(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFF_SIZE)
    }

    pub fn checksum(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFF_CHECKSUM)
    }

    pub fn lsn(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFF_LSN)
    }

    pub fn clog_type(&self) -> CLogRecType {
        let v = get_u32(self.slice.as_ptr(), self.slice.len(), OFF_COMMAND_TYPE);
        let ct = unsafe { std::mem::transmute(v as i8) };
        ct
    }
}

impl<'a> CUpdateKV<'a> {
    pub fn header_size() -> usize {
        UP_CMD_HEADER_SIZE as usize
    }

    pub fn from(s: &'a [u8]) -> Self {
        CUpdateKV { slice: s }
    }

    pub fn update_cmd(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFF_UP_CMD_TYPE)
    }

    pub fn tuple_id(&self) -> OID {
        get_u128(self.slice.as_ptr(), self.slice.len(), OFF_UP_CMD_TUPLE_ID)
    }

    pub fn table_id(&self) -> OID {
        get_u128(self.slice.as_ptr(), self.slice.len(), OFF_UP_CMD_TABLE_ID)
    }

    pub fn key_size(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFF_UP_CMD_KEY_SIZE)
    }

    pub fn value_size(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFF_UP_CMD_VALUE_SIZE)
    }

    pub fn key(&self) -> &[u8] {
        let start = UP_CMD_HEADER_SIZE as usize;
        let end = (UP_CMD_HEADER_SIZE + self.key_size()) as usize;
        &self.slice[start..end]
    }

    pub fn value(&self) -> &[u8] {
        let start = (UP_CMD_HEADER_SIZE + self.key_size()) as usize;
        let end = (UP_CMD_HEADER_SIZE + self.key_size() + self.value_size()) as usize;
        &self.slice[start..end]
    }
}
