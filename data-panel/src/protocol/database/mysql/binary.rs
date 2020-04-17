use crate::protocol::database::mysql::constant::MySQLColumnType;
use std::error::Error;
use crate::protocol::database::mysql::packet::MySQLPacketPayload;

/// The `Value` is also used as a parameter to a prepared statement.
#[derive(Clone, PartialEq, PartialOrd)]
pub enum PrepareParamValue {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f64),
    /// year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32),
}

impl PrepareParamValue {
    pub fn read_bin(payload: &mut MySQLPacketPayload, column_type: MySQLColumnType, unsigned: bool) -> Result<PrepareParamValue, ()> {
        match column_type {
            MySQLColumnType::MysqlTypeString
            | MySQLColumnType::MysqlTypeVarString
            | MySQLColumnType::MysqlTypeBlob
            | MySQLColumnType::MysqlTypeTinyBlob
            | MySQLColumnType::MysqlTypeMediumBlob
            | MySQLColumnType::MysqlTypeLongBlob
            | MySQLColumnType::MysqlTypeSet
            | MySQLColumnType::MysqlTypeEnum
            | MySQLColumnType::MysqlTypeDecimal
            | MySQLColumnType::MysqlTypeVarchar
            | MySQLColumnType::MysqlTypeBit
            | MySQLColumnType::MysqlTypeNewDecimal
            | MySQLColumnType::MysqlTypeGeometry
            | MySQLColumnType::MysqlTypeJson => Ok(PrepareParamValue::Bytes(payload.get_string_lenenc())),
            MySQLColumnType::MysqlTypeTiny => {
                if unsigned {
                    Ok(PrepareParamValue::UInt(payload.get_uint(1)))
                } else {
                    Ok(PrepareParamValue::Int(payload.get_int(1)))
                }
            }
            MySQLColumnType::MysqlTypeShort | MySQLColumnType::MysqlTypeYear => {
                if unsigned {
                    Ok(PrepareParamValue::UInt(payload.get_uint_le(2)))
                } else {
                    Ok(PrepareParamValue::Int(payload.get_int_le(2)))
                }
            }
            MySQLColumnType::MysqlTypeLong | MySQLColumnType::MysqlTypeInt24 => {
                if unsigned {
                    Ok(PrepareParamValue::UInt(payload.get_uint_le(4)))
                } else {
                    Ok(PrepareParamValue::Int(payload.get_int_le(4)))
                }
            }
            MySQLColumnType::MysqlTypeLonglong => {
                if unsigned {
                    Ok(PrepareParamValue::UInt(payload.get_uint_le(8)))
                } else {
                    Ok(PrepareParamValue::Int(payload.get_int_le(8)))
                }
            }
            MySQLColumnType::MysqlTypeFloat => Ok(PrepareParamValue::Float(payload.get_f32_le().into())),
            MySQLColumnType::MysqlTypeDouble => Ok(PrepareParamValue::Float(payload.get_f64_le())),
            MySQLColumnType::MysqlTypeTimestamp
            | MySQLColumnType::MysqlTypeDate
            | MySQLColumnType::MysqlTypeDatetime => {
                let len = (payload.get_uint(1) & 0xff) as u8;
                let mut year = 0u16;
                let mut month = 0u8;
                let mut day = 0u8;
                let mut hour = 0u8;
                let mut minute = 0u8;
                let mut second = 0u8;
                let mut micro_second = 0u32;
                if len >= 4u8 {
                    year = payload.get_uint_le(2) as u16;
                    month = (payload.get_uint(1) & 0xff) as u8;
                    day = (payload.get_uint(1) & 0xff) as u8;
                } else if len >= 7u8 {
                    hour = (payload.get_uint(1) & 0xff) as u8;
                    minute = (payload.get_uint(1) & 0xff) as u8;
                    second = (payload.get_uint(1) & 0xff) as u8;
                } else if len == 11u8 {
                    micro_second = payload.get_uint_le(4) as u32;
                }
                Ok(PrepareParamValue::Date(year, month, day, hour, minute, second, micro_second))
            }
            MySQLColumnType::MysqlTypeTime => {
                let len = (payload.get_uint(1) & 0xff) as u8;
                let mut is_negative = false;
                let mut days = 0u32;
                let mut hours = 0u8;
                let mut minutes = 0u8;
                let mut seconds = 0u8;
                let mut micro_seconds = 0u32;
                if len >= 8u8 {
                    is_negative = ((payload.get_uint(1) & 0xff) as u8) == 1u8;
                    days = payload.get_uint_le(4) as u32;
                    hours = (payload.get_uint(1) & 0xff) as u8;
                    minutes = (payload.get_uint(1) & 0xff) as u8;
                    seconds = (payload.get_uint(1) & 0xff) as u8;
                } else if len == 12u8 {
                    micro_seconds = payload.get_uint_le(4) as u32;
                }
                Ok(PrepareParamValue::Time(
                    is_negative,
                    days,
                    hours,
                    minutes,
                    seconds,
                    micro_seconds,
                ))
            }
            MySQLColumnType::MysqlTypeNull => Ok(PrepareParamValue::NULL),
            x => unimplemented!("Unsupported column type {:?}", x),
        }
    }
}