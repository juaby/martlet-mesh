use crate::protocol::database::DatabasePacket;
use crate::protocol::database::mysql::constant::{MySQLColumnFlags, MySQLColumnType, MySQLNewParametersBoundFlag};
use crate::protocol::database::mysql::packet::{MySQLPacket, MySQLPacketHeader, MySQLPacketPayload};
use crate::session::mysql::SessionContext;

/**
 * COM_STMT_PREPARE command packet for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html">COM_STMT_PREPARE</a>
 */
pub struct MySQLComStmtPreparePacket {
    sequence_id: u32,
    /// MySQLCommandPacketType,
    command_type: u8,
    sql: Vec<u8>,
}

impl MySQLComStmtPreparePacket {
    pub fn new(command_type: u8) -> Self {
        MySQLComStmtPreparePacket {
            sequence_id: 0,
            command_type: command_type, // MySQLCommandPacketType::value_of(command_type & 0xff),
            sql: vec![],
        }
    }

    pub fn get_sql(&self) -> Vec<u8> {
        self.sql.clone()
    }

    pub fn get_command_type(&self) -> u8 {
        self.command_type
    }
}

impl DatabasePacket<MySQLPacketHeader, MySQLPacketPayload, SessionContext> for MySQLComStmtPreparePacket {
    fn decode<'p, 'd>(this: &'d mut Self, header: &'p MySQLPacketHeader, payload: &'p mut MySQLPacketPayload, session_ctx: &mut SessionContext) -> &'d mut Self {
        let bytes = payload.get_remaining_bytes();
        this.sql = Vec::from(bytes.as_slice());
        this
    }
}

impl MySQLPacket for MySQLComStmtPreparePacket {
    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }
}

/**
 * COM_STMT_PREPARE_OK packet for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK">COM_STMT_PREPARE_OK</a>
 */
pub struct MySQLComStmtPrepareOKPacket {
    sequence_id: u32,
    /// MySQLCommandPacketType,
    command_type: u8,
    status: u8,
    statement_id: u32,
    columns_count: u16,
    parameters_count: u16,
    warning_count: u16,
}

impl MySQLComStmtPrepareOKPacket {
    pub fn new(sequence_id: u32,
               command_type: u8, // MySQLCommandPacketType,
               statement_id: u32,
               columns_count: u16,
               parameters_count: u16,
               warning_count: u16) -> Self {
        MySQLComStmtPrepareOKPacket {
            sequence_id,
            command_type, // MySQLCommandPacketType::value_of(command_type & 0xff),
            status: 0x00,
            statement_id,
            columns_count,
            parameters_count,
            warning_count,
        }
    }
}

impl DatabasePacket<MySQLPacketHeader, MySQLPacketPayload, SessionContext> for MySQLComStmtPrepareOKPacket {
    fn encode<'p, 'd>(this: &'d mut Self, payload: &'p mut MySQLPacketPayload) -> &'p mut MySQLPacketPayload {
        payload.put_u8(this.get_sequence_id() as u8); // seq
        payload.put_u8(this.status);
        payload.put_u32_le(this.statement_id);
        payload.put_u16_le(this.columns_count);
        payload.put_u16_le(this.parameters_count);
        let reserved: [u8; 1] = [0];
        payload.put_slice(&reserved);
        payload.put_u16_le(this.warning_count);
        payload
    }
}

impl MySQLPacket for MySQLComStmtPrepareOKPacket {
    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }
}

/**
 * COM_STMT_EXECUTE command packet for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-execute.html">COM_STMT_EXECUTE</a>
 */
pub struct MySQLComStmtExecutePacket {
    sequence_id: u32,
    /// MySQLCommandPacketType,
    command_type: u8,
    statement_id: u32,
    flags: u16,
    null_bit_map: Vec<u8>,
    new_parameters_bound_flag: u8,
    iteration_count: u32,
    sql: Vec<u8>,
    parameters: Vec<PrepareParamValue>,
}

impl MySQLComStmtExecutePacket {
    pub fn new(command_type: u8) -> Self {
        MySQLComStmtExecutePacket {
            sequence_id: 0,
            command_type: command_type, // MySQLCommandPacketType::value_of(command_type & 0xff),
            statement_id: 0,
            flags: 0,
            null_bit_map: vec![],
            new_parameters_bound_flag: 0,
            iteration_count: 0,
            sql: vec![],
            parameters: vec![],
        }
    }

    pub fn get_sql(&self) -> Vec<u8> {
        self.sql.clone()
    }

    pub fn set_sql(&mut self, sql: Vec<u8>) {
        self.sql = sql;
    }

    pub fn get_statement_id(&self) -> u32 {
        self.statement_id
    }

    pub fn get_command_type(&self) -> u8 {
        self.command_type
    }

    pub fn get_parameters(&self) -> Vec<PrepareParamValue> {
        self.parameters.clone()
    }
}

impl DatabasePacket<MySQLPacketHeader, MySQLPacketPayload, SessionContext> for MySQLComStmtExecutePacket {
    fn decode<'p, 'd>(this: &'d mut Self, header: &'p MySQLPacketHeader, payload: &'p mut MySQLPacketPayload, session_ctx: &mut SessionContext) -> &'d mut Self {
        this.sequence_id = header.sequence_id;
        this.statement_id = payload.get_uint_le(4) as u32;
        this.sql = session_ctx.get_prepare_stmt_ctx_by_id(this.statement_id as u64).unwrap().get_sql();
        this.flags = (payload.get_uint(1) & 0xff) as u16;
        this.iteration_count = payload.get_uint_le(4) as u32;
        assert_eq!(1, this.iteration_count);
        let session_id = header.get_session_id();
        let parameters_count = session_ctx.get_prepare_parameters_count(this.statement_id as u64);
        //
        // Null bitmap for MySQL.
        //
        // @see <a href="https://dev.mysql.com/doc/internals/en/null-bitmap.html">NULL-Bitmap</a>
        //
        let mut null_bit_map: Vec<u8> = vec![];
        let offset = 0;
        let num_params = parameters_count as usize;
        if num_params > 0 {
            let len = (num_params + offset + 7) / 8;
            null_bit_map = vec![0u8; len]; //Vec::with_capacity(len);
            for i in 0..len {
                null_bit_map[i] = (payload.get_uint(1) & 0xff) as u8;
            }
            let new_parameters_bound_flag = (payload.get_uint(1) & 0xff) as u8;
            let mut parameter_types = Vec::with_capacity(num_params);
            if MySQLNewParametersBoundFlag::ParameterTypeExist as u8 == new_parameters_bound_flag {
                for _ in 0..num_params {
                    let column_type = (payload.get_uint(1) & 0xff) as u8;
                    let unsigned_flag = (payload.get_uint(1) & 0xff) as u8;
                    parameter_types.push((column_type, unsigned_flag));
                }
                session_ctx.set_prepare_parameter_types(this.statement_id as u64, parameter_types.clone());
            } else {
                parameter_types = session_ctx.get_prepare_parameter_types(this.statement_id as u64);
            }
            this.parameters = Vec::with_capacity(num_params);
            for i in 0..num_params {
                let null_byte_position = (i + offset) / 8;
                let null_bit_position = (i + offset) % 8;
                if (null_bit_map[null_byte_position] & (1 << null_bit_position) as u8) != 0 {
                    this.parameters.push(PrepareParamValue::NULL);
                } else {
                    let (column_type, unsigned_flag) = parameter_types.get(i).unwrap();
                    let column_flags = MySQLColumnFlags::from_bits_truncate(*unsigned_flag as u16);
                    let param_value = read_bin(payload, MySQLColumnType::from(*column_type), column_flags.contains(MySQLColumnFlags::UNSIGNED_FLAG)).unwrap();
                    this.parameters.push(param_value);
                }
            }
        }
        this
    }
}

impl MySQLPacket for MySQLComStmtExecutePacket {
    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }
}

/**
 * Binary result set row packet for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html">Binary Protocol ResultSet Row</a>
 */
pub struct MySQLBinaryResultSetRowPacket {
    sequence_id: u32,
    data: Vec<PrepareParamValue>,
}

impl MySQLBinaryResultSetRowPacket {
    pub fn new(sequence_id: u32, data: Vec<PrepareParamValue>) -> Self {
        MySQLBinaryResultSetRowPacket {
            sequence_id: sequence_id,
            data: data,
        }
    }
}

impl MySQLPacket for MySQLBinaryResultSetRowPacket {
    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }
}

impl DatabasePacket<MySQLPacketHeader, MySQLPacketPayload, SessionContext> for MySQLBinaryResultSetRowPacket {
    fn encode<'p, 'd>(this: &'d mut Self, payload: &'p mut MySQLPacketPayload) -> &'p mut MySQLPacketPayload {
        payload.put_u8(this.get_sequence_id() as u8); // seq
        payload.put_u8(0x00); // PACKET_HEADER

        let null_bitmap_offset = 2;
        let columns_numbers = this.data.len();
        let len = (columns_numbers + null_bitmap_offset + 7) / 8;
        let mut null_bit_map = vec![0u8; len];//Vec::with_capacity(len);
        for i in 0..len {
            null_bit_map[i] = 0u8;
            if let Some(v) = this.data.get(i) {
                if *v == PrepareParamValue::NULL {
                    let null_byte_position = (i + null_bitmap_offset) / 8;
                    let null_bit_position = (i + null_bitmap_offset) % 8;
                    null_bit_map[null_byte_position] = (1 << null_bit_position) as u8;
                }
            }
        }

        for v in null_bit_map.iter() {
            payload.put_u8(*v);
        }

        for v in this.data.iter() {
            write_bin(v, payload);
        }

        payload
    }
}

/**
 * COM_STMT_CLOSE command packet for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-close.html">COM_STMT_CLOSE</a>
 */
pub struct MySQLComStmtClosePacket {
    sequence_id: u32,
    /// MySQLCommandPacketType,
    command_type: u8,
    statement_id: u32,
}

impl MySQLComStmtClosePacket {
    pub fn new(command_type: u8) -> Self {
        MySQLComStmtClosePacket {
            sequence_id: 0,
            command_type: command_type, // MySQLCommandPacketType::value_of(command_type & 0xff),
            statement_id: 0,
        }
    }

    pub fn get_statement_id(&self) -> u32 {
        self.statement_id
    }
}

impl DatabasePacket<MySQLPacketHeader, MySQLPacketPayload, SessionContext> for MySQLComStmtClosePacket {
    fn decode<'p, 'd>(this: &'d mut Self, header: &'p MySQLPacketHeader, payload: &'p mut MySQLPacketPayload, session_ctx: &mut SessionContext) -> &'d mut Self {
        this.sequence_id = header.sequence_id;
        this.statement_id = payload.get_uint_le(4) as u32;

        this
    }
}

impl MySQLPacket for MySQLComStmtClosePacket {
    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }
}

/**
 * COM_STMT_RESET command packet for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-reset.html">COM_STMT_RESET</a>
 */
pub struct MySQLComStmtResetPacket {
    sequence_id: u32,
    /// MySQLCommandPacketType,
    command_type: u8,
    statement_id: u32,
}

impl MySQLComStmtResetPacket {
    pub fn new(command_type: u8) -> Self {
        MySQLComStmtResetPacket {
            sequence_id: 0,
            command_type: command_type, // MySQLCommandPacketType::value_of(command_type & 0xff),
            statement_id: 0,
        }
    }

    pub fn get_statement_id(&self) -> u32 {
        self.statement_id
    }
}

impl DatabasePacket<MySQLPacketHeader, MySQLPacketPayload, SessionContext> for MySQLComStmtResetPacket {
    fn decode<'p, 'd>(this: &'d mut Self, header: &'p MySQLPacketHeader, payload: &'p mut MySQLPacketPayload, session_ctx: &mut SessionContext) -> &'d mut Self {
        this.sequence_id = header.sequence_id;
        this.statement_id = payload.get_uint_le(4) as u32;

        this
    }
}

impl MySQLPacket for MySQLComStmtResetPacket {
    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }
}

/// The `Value` is also used as a parameter to a prepared statement.
#[derive(Clone, PartialEq, PartialOrd)]
pub enum PrepareParamValue {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
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
        MySQLColumnType::MysqlTypeDouble => Ok(PrepareParamValue::Double(payload.get_f64_le())),
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
            payload.put_f32_le(x);
        }
        PrepareParamValue::Double(x) => {
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