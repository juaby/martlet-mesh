use crate::protocol::database::mysql::constant::MySQLColumnType;
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
                Ok(PrepareParamValue::UInt(payload.get_uint(1) & 0xff))
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

/// Writes MySql's value in binary value format.
pub fn write_bin(value: &PrepareParamValue, payload: &mut MySQLPacketPayload) {
    match *value {
        PrepareParamValue::NULL => payload.put_u8(0),
        PrepareParamValue::Bytes(ref x) => payload.put_string_lenenc(&x[..]),
        PrepareParamValue::Int(x) => {
            payload.put_i64_le(x);
        }
        PrepareParamValue::UInt(x) => {
            payload.put_u64_le(x);
        }
        PrepareParamValue::Float(x) => {
            payload.put_f64_le(x);
        }
        PrepareParamValue::Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32) => {
            payload.put_u8(0);
        }
        PrepareParamValue::Date(y, m, d, 0u8, 0u8, 0u8, 0u32) => {
            payload.put_u8(4u8);
            payload.put_u16_le(y);
            payload.put_u8(m);
            payload.put_u8(d);
        }
        PrepareParamValue::Date(y, m, d, h, i, s, 0u32) => {
            payload.put_u8(7u8);
            payload.put_u16_le(y);
            payload.put_u8(m);
            payload.put_u8(d);
            payload.put_u8(h);
            payload.put_u8(i);
            payload.put_u8(s);
        }
        PrepareParamValue::Date(y, m, d, h, i, s, u) => {
            payload.put_u8(11u8);
            payload.put_u16_le(y);
            payload.put_u8(m);
            payload.put_u8(d);
            payload.put_u8(h);
            payload.put_u8(i);
            payload.put_u8(s);
            payload.put_u32_le(u);
        }
        PrepareParamValue::Time(_, 0u32, 0u8, 0u8, 0u8, 0u32) => {
            payload.put_u8(0u8);
        }
        PrepareParamValue::Time(neg, d, h, m, s, 0u32) => {
            payload.put_u8(8u8);
            payload.put_u8(if neg { 1u8 } else { 0u8 });
            payload.put_u32_le(d);
            payload.put_u8(h);
            payload.put_u8(m);
            payload.put_u8(s);
        }
        PrepareParamValue::Time(neg, d, h, m, s, u) => {
            payload.put_u8(12u8);
            payload.put_u8(if neg { 1u8 } else { 0u8 });
            payload.put_u32_le(d);
            payload.put_u8(h);
            payload.put_u8(m);
            payload.put_u8(s);
            payload.put_u32_le(u);
        }
    }
}