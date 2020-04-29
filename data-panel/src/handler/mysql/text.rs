use crate::handler::mysql::CommandHandler;
use crate::protocol::database::mysql::packet::{MySQLPacketPayload, MySQLPacketHeader, MySQLFieldCountPacket, MySQLColumnDefinition41Packet, MySQLEOFPacket, MySQLOKPacket};
use crate::protocol::database::{DatabasePacket, PacketPayload};
use sqlparser::ast::Statement;
use sqlparser::ast::SetVariableValue::Ident;
use crate::parser;
use mysql::{Value};
use bytes::Bytes;

use crate::protocol::database::mysql::packet::text::{MySQLTextResultSetRowPacket, MySQLComQueryPacket};
use crate::handler::mysql::rdbc::{ExplainPlanContext, ExplainPlan, TBProtocol, Executor};

pub struct ComQueryHandler {}
impl CommandHandler<MySQLPacketPayload> for ComQueryHandler {
    fn handle(command_packet_header: Option<MySQLPacketHeader>, command_packet_payload: Option<MySQLPacketPayload>) -> Option<Vec<Bytes>> {
        // 1 解析報文
            // 2 解析SQL
            // 3 服務發現
            // 4 SQL重寫
            // 5 執行上下文
            // 6 執行SQL
        // 7 合并結果
        // 8 封裝報文
        let command_packet_header = command_packet_header.unwrap();
        let command_packet_type = command_packet_header.get_command_packet_type();
        let mut command_payload = command_packet_payload.unwrap();
        let mut query_packet = MySQLComQueryPacket::new(command_packet_type);
        let command_packet = DatabasePacket::decode(&mut query_packet, &command_packet_header, &mut command_payload);

        let command_sql = command_packet.get_sql();
        let cow_sql = String::from_utf8_lossy(command_sql.as_slice());
        let sql = cow_sql.to_string();
        println!("SQL = {}", sql);
        let mut statement = parser::mysql::parser(sql);
        let statement = statement.pop().unwrap();

        let x_query_context = ExplainPlanContext::new(cow_sql.as_ref(),
                                                      &statement, TBProtocol::Text);
        let plan = ExplainPlan::new(&x_query_context);

        plan.execute()
    }
}

pub struct SetVariableHandler {}
impl CommandHandler<MySQLPacketPayload> for SetVariableHandler {
    fn handle(command_packet_header: Option<MySQLPacketHeader>, command_packet: Option<MySQLPacketPayload>) -> Option<Vec<Bytes>> {
        unimplemented!()
    }
}