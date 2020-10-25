use crate::handler::mysql::CommandHandler;
use crate::protocol::database::mysql::packet::{MySQLPacketPayload, MySQLPacketHeader, MySQLOKPacket, MySQLEOFPacket, MySQLColumnDefinition41Packet, MySQLFieldCountPacket};
use crate::protocol::database::{DatabasePacket, PacketPayload};
use crate::session::{clear_session_prepare_stmt_context, get_session_prepare_stmt_context_statement_id, session_prepare_stmt_context_statement_id, set_session_prepare_stmt_context_sql, set_session_prepare_stmt_context_parameters_count};
use sqlparser::ast::Statement;
use mysql::{Value, Conn, Params};
use crate::protocol::database::mysql::packet::binary::{PrepareParamValue, MySQLComStmtResetPacket, MySQLComStmtPrepareOKPacket, MySQLComStmtPreparePacket, MySQLBinaryResultSetRowPacket, MySQLComStmtExecutePacket, MySQLComStmtClosePacket};
use crate::parser;
use bytes::Bytes;
use crate::protocol::database::mysql::constant::{CHARSET, MySQLColumnType};
use mysql::prelude::Queryable;

pub struct ComStmtPrepareHandler {}
impl CommandHandler<MySQLPacketPayload> for ComStmtPrepareHandler {
    fn handle(command_packet_header: Option<MySQLPacketHeader>, command_packet: Option<MySQLPacketPayload>) -> Option<Vec<Bytes>> {
        let command_packet_header = command_packet_header.unwrap();
        let command_packet_type = command_packet_header.get_command_packet_type();
        let mut command_payload = command_packet.unwrap();
        let mut prepare_packet = MySQLComStmtPreparePacket::new(command_packet_type);
        let command_packet = DatabasePacket::decode(&mut prepare_packet, &command_packet_header, &mut command_payload);
        // TODO
        let sql = command_packet.get_sql();
        let sql = String::from_utf8_lossy(sql.as_slice());
        let statements = parser::sql::mysql::parser(sql.as_ref());

        let mut payloads: Vec<Bytes> = Vec::new();

        let parameters_count = 1;
        let columns_count = 1;

        let mut global_sequence_id: u32 = 1;
        let session_id = command_packet_header.get_session_id();
        let mut statement_id = 0;
        if let Some(cache_statement_id) = get_session_prepare_stmt_context_statement_id(sql.to_string()) {
            statement_id = cache_statement_id;
        } else {
            statement_id = session_prepare_stmt_context_statement_id();
            set_session_prepare_stmt_context_parameters_count(statement_id, parameters_count);
            set_session_prepare_stmt_context_sql(statement_id, command_packet.get_sql());
        }

        let mut prepare_ok_packet = MySQLComStmtPrepareOKPacket::new(
            global_sequence_id,
            command_packet_type,
            statement_id as u32,
            columns_count,
            parameters_count,
            0);
        let mut prepare_ok_payload = MySQLPacketPayload::new();
        let prepare_ok_payload = DatabasePacket::encode(&mut prepare_ok_packet, &mut prepare_ok_payload);

        payloads.push(prepare_ok_payload.get_payload());

        if parameters_count > 0 {
            for _ in 0..parameters_count {
                global_sequence_id = global_sequence_id + 1;
                let sequence_id = global_sequence_id;
                let character_set: u16 = CHARSET as u16;
                let flags: u16 = 0;
                let schema: String = "".to_string();
                let table: String = "".to_string();
                let org_table: String = "".to_string();
                let name: String = "?".to_string();
                let org_name: String = "".to_string();
                let column_length: u32 = 0;
                let column_type: u8 = MySQLColumnType::MysqlTypeVarString as u8; // MySQLColumnType
                let decimals: u8 = 0;
                let mut column_definition41_packet =
                    MySQLColumnDefinition41Packet::new(
                        sequence_id,
                        character_set,
                        flags,
                        schema,
                        table,
                        org_table,
                        name,
                        org_name,
                        column_length,
                        column_type, // MySQLColumnType
                        decimals
                    );
                let mut column_definition41_payload = MySQLPacketPayload::new();
                let column_definition41_payload = DatabasePacket::encode(&mut column_definition41_packet, &mut column_definition41_payload);

                payloads.push(column_definition41_payload.get_payload());
            }
            global_sequence_id = global_sequence_id + 1;
            let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
            let mut eof_payload = MySQLPacketPayload::new();
            let eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);

            payloads.push(eof_payload.get_payload());
        }

        if columns_count > 0 {
            for _ in 0..columns_count {
                global_sequence_id = global_sequence_id + 1;
                let sequence_id = global_sequence_id;
                let character_set: u16 = CHARSET as u16;
                let flags: u16 = 0;
                let schema: String = "".to_string();
                let table: String = "".to_string();
                let org_table: String = "".to_string();
                let name: String = "?".to_string();
                let org_name: String = "".to_string();
                let column_length: u32 = 0;
                let column_type: u8 = MySQLColumnType::MysqlTypeVarString as u8; // MySQLColumnType
                let decimals: u8 = 0;
                let mut column_definition41_packet =
                    MySQLColumnDefinition41Packet::new(
                        sequence_id,
                        character_set,
                        flags,
                        schema,
                        table,
                        org_table,
                        name,
                        org_name,
                        column_length,
                        column_type, // MySQLColumnType
                        decimals
                    );
                let mut column_definition41_payload = MySQLPacketPayload::new();
                let column_definition41_payload = DatabasePacket::encode(&mut column_definition41_packet, &mut column_definition41_payload);

                payloads.push(column_definition41_payload.get_payload());
            }
            global_sequence_id = global_sequence_id + 1;
            let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
            let mut eof_payload = MySQLPacketPayload::new();
            let eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);

            payloads.push(eof_payload.get_payload());
        }

        Some(payloads)
    }
}

pub struct ComStmtExecuteHandler {}
impl CommandHandler<MySQLPacketPayload> for ComStmtExecuteHandler {
    fn handle(command_packet_header: Option<MySQLPacketHeader>, command_packet: Option<MySQLPacketPayload>) -> Option<Vec<Bytes>> {
        let command_packet_header = command_packet_header.unwrap();
        let command_packet_type = command_packet_header.get_command_packet_type();
        let mut command_payload = command_packet.unwrap();
        let mut stmt_execute_packet = MySQLComStmtExecutePacket::new(command_packet_type);
        let stmt_execute_packet = DatabasePacket::decode(&mut stmt_execute_packet, &command_packet_header, &mut command_payload);

        let mut payloads = Vec::new();
        let database_url = "mysql://root:root@localhost:8306/test";
        let mut conn = Conn::new(database_url).unwrap();
        let command_sql = stmt_execute_packet.get_sql();
        let cow_sql = String::from_utf8_lossy(command_sql.as_slice());
        let sql = cow_sql.to_string();
        println!("SQL = {}", sql);
        let mut statement = parser::sql::mysql::parser(cow_sql.as_ref());
        let statement = statement.pop().unwrap();

        match statement {
            Statement::Query(q) => {
                let prepare_stmt = conn.prep((*q).to_string()).unwrap();
                let params = stmt_execute_packet.get_parameters();
                let mut params_value = Vec::with_capacity(params.len());
                for v in params {
                    match v {
                        PrepareParamValue::NULL => params_value.push(Value::NULL),
                        PrepareParamValue::Bytes(bytes) => params_value.push(Value::Bytes(bytes)),
                        PrepareParamValue::Int(int) => params_value.push(Value::Int(int)),
                        PrepareParamValue::UInt(uint) => params_value.push(Value::UInt(uint)),
                        PrepareParamValue::Float(f) => params_value.push(Value::Float(f)),
                        PrepareParamValue::Double(d) => params_value.push(Value::Double(d)),
                        PrepareParamValue::Date(year, month, day, hour, minutes, seconds, micro_seconds) => params_value.push(Value::Date(year, month, day, hour, minutes, seconds, micro_seconds)),
                        PrepareParamValue::Time(is_negative, days, hours, minutes, seconds, micro_seconds) => params_value.push(Value::Time(is_negative, days, hours, minutes, seconds, micro_seconds)),
                    }
                }
                let mut result = conn.exec_iter(&prepare_stmt, Params::from(params_value)).unwrap();

                let mut global_sequence_id: u32 = 1;

                while let Some(result_set) = result.next_set() {
                    let result_set = result_set.unwrap();

                    let columns = result_set.columns();
                    let columns_ref = columns.as_ref();
                    let columns_size = columns_ref.len();
                    let mut field_count_packet = MySQLFieldCountPacket::new(global_sequence_id, columns_size as u32);
                    let mut field_count_payload = MySQLPacketPayload::new();
                    let field_count_payload = DatabasePacket::encode(&mut field_count_packet, &mut field_count_payload);

                    payloads.push(field_count_payload.get_payload());

                    for c in columns_ref {
                        global_sequence_id = global_sequence_id + 1;
                        let sequence_id = global_sequence_id;
                        let character_set: u16 = c.character_set();
                        let flags: u16 = c.flags().bits() as u16;
                        let schema: String = c.schema_str().to_string();
                        let table: String = c.table_str().to_string();
                        let org_table: String = c.org_table_str().to_string();
                        let name: String = c.name_str().to_string();
                        let org_name: String = c.org_name_str().to_string();
                        let column_length: u32 = c.column_length();
                        let column_type: u8 = c.column_type() as u8; // MySQLColumnType
                        let decimals: u8 = c.decimals();
                        let mut column_definition41_packet =
                            MySQLColumnDefinition41Packet::new(
                                sequence_id,
                                character_set,
                                flags,
                                schema,
                                table,
                                org_table,
                                name,
                                org_name,
                                column_length,
                                column_type, // MySQLColumnType
                                decimals
                            );
                        let mut column_definition41_payload = MySQLPacketPayload::new();
                        let column_definition41_payload = DatabasePacket::encode(&mut column_definition41_packet, &mut column_definition41_payload);

                        payloads.push(column_definition41_payload.get_payload());
                    }

                    global_sequence_id = global_sequence_id + 1;
                    let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
                    let mut eof_payload = MySQLPacketPayload::new();
                    let eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);

                    payloads.push(eof_payload.get_payload());

                    for row in result_set {
                        let row = row.unwrap();

                        let mut row_values = Vec::with_capacity(columns_size);
                        for column_index in 0..columns_size {
                            let v = row.get(column_index).unwrap();
                            match v {
                                Value::NULL => row_values.push(PrepareParamValue::NULL),
                                Value::Bytes(bytes) => row_values.push(PrepareParamValue::Bytes(bytes)),
                                Value::Int(int) => row_values.push(PrepareParamValue::Int(int)),
                                Value::UInt(uint) => row_values.push(PrepareParamValue::UInt(uint)),
                                Value::Float(f) => row_values.push(PrepareParamValue::Float(f)),
                                Value::Double(d) => row_values.push(PrepareParamValue::Double(d)),
                                Value::Date(year, month, day, hour, minutes, seconds, micro_seconds) => row_values.push(PrepareParamValue::Date(year, month, day, hour, minutes, seconds, micro_seconds)),
                                Value::Time(is_negative, days, hours, minutes, seconds, micro_seconds) => row_values.push(PrepareParamValue::Time(is_negative, days, hours, minutes, seconds, micro_seconds)),
                            }
                        }

                        global_sequence_id = global_sequence_id + 1;
                        let mut binary_result_set_row_packet = MySQLBinaryResultSetRowPacket::new(global_sequence_id, row_values);
                        let mut binary_result_set_row_payload = MySQLPacketPayload::new();
                        let binary_result_set_row_payload = DatabasePacket::encode(&mut binary_result_set_row_packet, &mut binary_result_set_row_payload);

                        payloads.push(binary_result_set_row_payload.get_payload());
                    }

                    global_sequence_id = global_sequence_id + 1;
                    let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
                    let mut eof_payload = MySQLPacketPayload::new();
                    let eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);

                    payloads.push(eof_payload.get_payload());
                }
            },
            Statement::SetVariable{
                local:_,
                variable:_,
                value:_,} => {

                // This query will emit two result sets.
                let mut result = conn.query_iter(sql).unwrap();

                let global_sequence_id: u32 = 1;

                while let Some(result_set) = result.next_set() {
                    let result_set = result_set.unwrap();
                    let last_insert_id = match result_set.last_insert_id() {
                        Some(last_insert_id) => last_insert_id,
                        None => 0
                    };
                    let mut ok_packet = MySQLOKPacket::new(
                        global_sequence_id,
                        result_set.affected_rows(),
                        last_insert_id);
                    let mut ok_payload = MySQLPacketPayload::new();
                    let ok_payload = DatabasePacket::encode(&mut ok_packet, &mut ok_payload);

                    payloads.push(ok_payload.get_payload());
                }
            }

            _ => {}
        }
        Some(payloads)
    }
}

pub struct ComStmtCloseHandler {}
impl CommandHandler<MySQLPacketPayload> for ComStmtCloseHandler {
    fn handle(command_packet_header: Option<MySQLPacketHeader>, command_packet: Option<MySQLPacketPayload>) -> Option<Vec<Bytes>> {
        let command_packet_header = command_packet_header.unwrap();
        let command_packet_type = command_packet_header.get_command_packet_type();
        let mut command_payload = command_packet.unwrap();
        let mut stmt_close_packet = MySQLComStmtClosePacket::new(command_packet_type);
        let stmt_close_packet = DatabasePacket::decode(&mut stmt_close_packet, &command_packet_header, &mut command_payload);

        clear_session_prepare_stmt_context(stmt_close_packet.get_statement_id() as u64);

        None
    }
}

pub struct ComStmtResetHandler {}
impl CommandHandler<MySQLPacketPayload> for ComStmtResetHandler {
    fn handle(command_packet_header: Option<MySQLPacketHeader>, command_packet: Option<MySQLPacketPayload>) -> Option<Vec<Bytes>> {
        let command_packet_header = command_packet_header.unwrap();
        let command_packet_type = command_packet_header.get_command_packet_type();
        let mut command_payload = command_packet.unwrap();
        let mut stmt_reset_packet = MySQLComStmtResetPacket::new(command_packet_type);
        let stmt_reset_packet = DatabasePacket::decode(&mut stmt_reset_packet, &command_packet_header, &mut command_payload);

        // TODO reset prepare context: fetch cursor long data

        let mut ok_packet = MySQLOKPacket::new(1, 0, 0);
        let mut ok_payload = MySQLPacketPayload::new();
        let ok_payload = DatabasePacket::encode(&mut ok_packet, &mut ok_payload);
        Some(vec![ok_payload.get_payload()])
    }
}