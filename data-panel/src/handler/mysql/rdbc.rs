use mysql::{Conn, Value, QueryResult, Text, MySqlError};
use sqlparser::ast::Statement;
use mysql::prelude::Queryable;
use bytes::Bytes;
use crate::protocol::database::mysql::packet::{MySQLColumnDefinition41Packet, MySQLFieldCountPacket, MySQLPacketPayload, MySQLEOFPacket, MySQLOKPacket, MySQLErrPacket};
use crate::protocol::database::{DatabasePacket, PacketPayload};
use crate::protocol::database::mysql::packet::text::MySQLTextResultSetRowPacket;
use crate::handler::mysql::explainplan::ExplainPlan;
use std::error::Error;

pub fn text_query(plan: &ExplainPlan<'_>) -> Option<Vec<Bytes>> {
    let sql = plan.ctx().get_sql();
    let mut payloads = Vec::new();
    let database_url = "mysql://root:root@localhost:3306/test";
    let mut conn = Conn::new(database_url).unwrap();
    match conn.query_iter(sql) {
        Ok(results) => {
            payloads = text_query_success(payloads, results, plan.ctx().get_statement());
        }
        Err(e) => {
            let (err_code, err_state, err_message) = match e {
                mysql::error::Error::IoError(ref err) => (10000 as u32, err.description(), err.description()),
                mysql::error::Error::DriverError(ref err) => (20000, err.description(), err.description()),
                mysql::error::Error::MySqlError(ref err) => (err.code as u32, err.state.as_str(), err.message.as_str()),
                mysql::error::Error::UrlError(ref err) => (40000, err.description(), err.description()),
                mysql::error::Error::TlsError(ref err) => (50000, err.description(), err.description()),
                mysql::error::Error::TlsHandshakeError(ref err) => (60000, err.description(), err.description()),
                _ => (70000, "unknown exception", "unknown exception"),
            };
            let mut err_packet = MySQLErrPacket::new(1, err_code as u32, err_state.to_string(), err_message.to_string());
            let mut err_payload = MySQLPacketPayload::new();
            let err_payload = DatabasePacket::encode(&mut err_packet, &mut err_payload);
            payloads.push(err_payload.get_payload());
        }
    };

    Some(payloads)
}

fn text_query_success(mut payloads: Vec<Bytes>, results: QueryResult<Text>, statement: &Statement) -> Vec<Bytes> {
    match statement {
        Statement::Query(q) => {
            payloads = query_result(payloads, results);
        },
        Statement::ShowVariable { variable } => {
            payloads = query_result(payloads, results);
        },
        Statement::ShowColumns { extended, full, table_name, filter } => {
            payloads = query_result(payloads, results);
        },
        Statement::SetVariable{ local, variable, value } => {
            payloads = update_result(payloads, results);
        }
        Statement::Insert { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Copy { .. }|Statement::Update { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Update { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Delete { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::CreateView { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::CreateTable { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::CreateVirtualTable { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::CreateIndex { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::AlterTable { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Drop { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::StartTransaction { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::SetTransaction { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Commit { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Rollback { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::CreateSchema { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Assert { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Deallocate { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Execute { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Prepare { .. } => {
            payloads = update_result(payloads, results);
        }
        Statement::Explain { .. } => {
            payloads = query_result(payloads, results);
        }
        Statement::Analyze { .. } => {
            payloads = query_result(payloads, results);
        }
    }
    payloads
}

fn update_result(mut payloads: Vec<Bytes>, results: QueryResult<Text>) -> Vec<Bytes> {
    // This query will emit two result sets.
    let mut result = results;

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

    payloads
}

fn query_result(mut payloads: Vec<Bytes>, results: QueryResult<Text>) -> Vec<Bytes>{
    // This query will emit more result sets.
    let mut result = results;

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
            let mut datas: Vec<(bool, Vec<u8>)> = Vec::new();
            for column_index in 0..columns_size {
                let v = row.as_ref(column_index).unwrap();
                let data = match v {
                    Value::Bytes(data) => (true, data.clone()),
                    Value::NULL => (false, Vec::new()),
                    _ => (true, Vec::new()),
                };
                datas.push(data);
            }

            global_sequence_id = global_sequence_id + 1;
            let mut text_result_set_row_packet = MySQLTextResultSetRowPacket::new(global_sequence_id, datas);
            let mut text_result_set_row_payload = MySQLPacketPayload::new();
            let text_result_set_row_payload = DatabasePacket::encode(&mut text_result_set_row_packet, &mut text_result_set_row_payload);

            payloads.push(text_result_set_row_payload.get_payload());
        }

        global_sequence_id = global_sequence_id + 1;
        let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
        let mut eof_payload = MySQLPacketPayload::new();
        let eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);

        payloads.push(eof_payload.get_payload());
    }

    payloads
}

pub fn bin_query(plan: &ExplainPlan<'_>) -> Option<Vec<Bytes>> {
    unimplemented!()
}