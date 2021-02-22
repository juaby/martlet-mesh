use crate::protocol::database::mysql::packet::{MySQLPacketPayload, MySQLPacketHeader, MySQLPacket};
use crate::protocol::database::DatabasePacket;
use crate::session::SessionContext;

/**
 * COM_QUERY command packet for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-query.html">COM_QUERY</a>
 */
pub struct MySQLComQueryPacket {
    sequence_id: u32,
    command_type: u8, // MySQLCommandPacketType,
    sql: Vec<u8>
}

impl MySQLComQueryPacket {
    pub fn new(command_type: u8) -> Self {
        MySQLComQueryPacket {
            sequence_id: 0,
            command_type: command_type, // MySQLCommandPacketType::value_of(command_type & 0xff),
            sql: vec![]
        }
    }

    pub fn get_sql(&self) -> Vec<u8> {
        self.sql.clone()
    }

    pub fn get_command_type(&self) -> u8 {
        self.command_type
    }
}

impl DatabasePacket<MySQLPacketHeader, MySQLPacketPayload> for MySQLComQueryPacket {
    fn decode<'p,'d>(this: &'d mut Self, header: &'p MySQLPacketHeader, payload: &'p mut MySQLPacketPayload, session_ctx: &mut SessionContext) -> &'d mut Self {
        let bytes = payload.get_remaining_bytes();
        this.sql = Vec::from(bytes.as_slice());
        this
    }
}

impl MySQLPacket for MySQLComQueryPacket {
    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }
}

/**
 * Text result set row packet for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow">ResultsetRow</a>
 */
pub struct MySQLTextResultSetRowPacket {
    sequence_id: u32,
    data: Vec<(bool, Vec<u8>)>, // NULL = 0xfb
}

impl MySQLTextResultSetRowPacket {
    pub fn new(sequence_id: u32, data: Vec<(bool, Vec<u8>)>) -> Self {
        MySQLTextResultSetRowPacket {
            sequence_id: sequence_id,
            data: data
        }
    }
}

impl MySQLPacket for MySQLTextResultSetRowPacket {
    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }
}

impl DatabasePacket<MySQLPacketHeader, MySQLPacketPayload> for MySQLTextResultSetRowPacket {
    fn encode<'p,'d>(this: &'d mut Self, payload: &'p mut MySQLPacketPayload) -> &'p mut MySQLPacketPayload {
        payload.put_u8(this.get_sequence_id() as u8); // seq

        for (null, col_v) in this.data.iter() {
            if !*(null) {
                payload.put_u8(0xfb);
            } else {
                payload.put_string_lenenc(col_v.as_slice());
            }
        }

        payload
    }
}