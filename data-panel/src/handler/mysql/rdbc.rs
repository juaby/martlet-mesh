use mysql::{QueryResult, Text, Binary, Conn, Error, Value};
use crate::{error};
use sqlparser::ast::Statement;
use mysql::prelude::Queryable;
use bytes::Bytes;
use crate::protocol::database::mysql::packet::{MySQLColumnDefinition41Packet, MySQLFieldCountPacket, MySQLPacketPayload, MySQLEOFPacket, MySQLOKPacket};
use crate::protocol::database::{DatabasePacket, PacketPayload};
use crate::protocol::database::mysql::packet::text::MySQLTextResultSetRowPacket;

pub enum TBProtocol {
    Text,
    Binary
}

pub struct ExplainPlanContext<'a> {
    sql: &'a str,
    statement: &'a Statement,
    protocol: TBProtocol
}

impl<'a> ExplainPlanContext<'a> {
    pub fn new(sql: &'a str,
               statement: &'a Statement,
               protocol: TBProtocol) -> Self {
        ExplainPlanContext {
            sql,
            statement,
            protocol
        }
    }

    pub fn get_sql(&self) -> &'a str {
        self.sql
    }

    pub fn get_statement(&self) -> &'a Statement {
        self.statement
    }
}

pub trait Executor {
    fn execute(&self) -> Option<Vec<Bytes>>;
}

pub struct PlanTask {

}

pub struct ExplainPlan<'a> {
    ctx: &'a ExplainPlanContext<'a>,
    tasks: Vec<PlanTask>,
}

impl<'a> ExplainPlan<'a> {
    pub fn new(ctx: &'a ExplainPlanContext<'a>) -> Self {
        ExplainPlan {
            ctx: ctx,
            tasks: vec![]
        }
    }

    pub fn gen(&self) {

    }

    pub fn text_query(&self) -> Option<Vec<Bytes>> {
        let sql = self.ctx.get_sql();

        let database_url = "mysql://root:root@localhost:8306/test";
        let mut conn = Conn::new(database_url).unwrap();

        // This query will emit more result sets.
        let results = conn.query_iter(sql).unwrap();

        let mut payloads = Vec::new();
        let statement = self.ctx.get_statement();
        match statement {
            Statement::Query(q) => {
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
            },
            Statement::SetVariable{
                local:_,
                variable:_,
                value:_,} => {

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
            }

            _ => {}
        }
        Some(payloads)
    }

    pub fn bin_query(&self) -> Option<Vec<Bytes>> {
        unimplemented!()
    }
}

impl<'a> Executor for ExplainPlan<'a> {
    fn execute(&self) -> Option<Vec<Bytes>> {
        match self.ctx.protocol {
            TBProtocol::Text => {self.text_query()},
            TBProtocol::Binary => {self.bin_query()},
        }
    }
}