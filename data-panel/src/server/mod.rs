
use std::net::{SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};

use sqlparser::ast::Statement;
use sqlparser::ast::SetVariableValue::Ident;
use bytes::{Buf};

use futures::SinkExt;

use tokio::net::TcpStream;
use tokio::stream::StreamExt;

use mysql::prelude::*;
use mysql::{Conn, Value};

use crate::parser;
use crate::protocol::database::mysql::codec::{read_frame, write_frame};
use crate::protocol::database::mysql::packet::{MySQLHandshakeResponse41Packet, MySQLPacketPayload, MySQLOKPacket, MySQLComQueryPacket, MySQLFieldCountPacket, MySQLColumnDefinition41Packet, MySQLEOFPacket, MySQLTextResultSetRowPacket, MySQLPacket, MySQLHandshakePacket};
use crate::protocol::database::{DatabasePacket, CommandPacketType, PacketPayload};
use crate::protocol::database::mysql::constant::MySQLCommandPacketType;

use crate::session::{SessionManager, Session};

lazy_static! {
    static ref IO_CONTEXT_ID: AtomicU64 = AtomicU64::new(1);
    // static ref SESSION_MANAGER: SessionManager = SessionManager::new();
}

pub fn io_context_id() -> u64 {
    IO_CONTEXT_ID.fetch_add(1, Ordering::SeqCst)
}

pub fn start_session(mut session: Session) {
    session.start();
    // SESSION_MANAGER.add_and_start(session);
}

pub fn handle(socket: TcpStream) {
    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
    // to convert our stream of bytes, `socket`, into a `Stream` of lines
    // as well as convert our line based responses into a stream of bytes.

    let io_ctx_id = io_context_id();
    let io_ctx = IOContext::new(io_ctx_id, socket);
    let session = Session::new(io_ctx);

    start_session(session);
}

pub struct IOContext {
    id: u64,
    socket: TcpStream,
    client_addr: SocketAddr
}

impl IOContext {

    pub fn new(id: u64, socket: TcpStream) -> Self {
        let client_addr = socket.peer_addr().unwrap();
        IOContext {
            id: id,
            socket: socket,
            client_addr: client_addr
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn handshake() {

    }

    pub fn receive(&mut self, authorized: bool) {
        let (r, w) = self.socket.split();
        let mut stream = read_frame(r);
        let mut sink = write_frame(w);

        // self.handshake();
        let mut packet = MySQLHandshakePacket::new(100);
        let mut payload = MySQLPacketPayload::new();
        let payload = DatabasePacket::encode(&mut packet, &mut payload);

        if let Err(e) = sink.send(payload.as_bytes()).await {
            println!("error on sending Handshake Packet response; error = {:?}", e);
        }

        let mut authorized = authorized;
        // Here for every line we get back from the `Framed` decoder,
        // we parse the request, and if it's valid we generate a response
        // based on the values in the database.
        while let Some(result) = stream.next().await {
            match result {
                Ok(mut payload) => {
                    if !authorized {
                        let mut packet = MySQLHandshakeResponse41Packet::new();
                        let mut payload = MySQLPacketPayload::new_with_payload(payload);
                        let packet = DatabasePacket::decode(&mut packet, &mut payload);

                        let mut ok_packet = MySQLOKPacket::new(packet.get_sequence_id() + 1, 0, 0);
                        let mut ok_payload = MySQLPacketPayload::new();
                        let ok_payload = DatabasePacket::encode(&mut ok_packet, &mut ok_payload);
                        let ok_bytes = ok_payload.as_bytes();

                        if let Err(e) = sink.send(ok_bytes).await {
                            println!("error on sending response; error = {:?}", e);
                        }

                        authorized = true; //小鱼在水里活泼乱跳 闫圣哲 王茹玉 毛毛虫 人类 电脑
                    } else {
                        let _len = payload.get_uint_le(3);
                        let _sequence_id = payload.get_uint(1) as u32 & 0xff;
                        let command_packet_type = payload.get_uint(1) as u8;

                        let mut command_packet = MySQLComQueryPacket::new(command_packet_type);
                        let mut command_payload = MySQLPacketPayload::new_with_payload(payload);
                        let command_packet = DatabasePacket::decode(&mut command_packet, &mut command_payload);

                        match MySQLCommandPacketType::value_of(command_packet.get_command_type()) {
                            MySQLCommandPacketType::ComQuery => {
                                let database_url = "mysql://root:root@localhost:8306/test";
                                let mut conn = Conn::new(database_url).unwrap();
                                let command_sql = command_packet.get_sql();
                                let sql = String::from_utf8_lossy(command_sql.as_slice());
                                let sql = sql.to_string();
                                println!("SQL = {}", sql);
                                {
                                    let statement = if sql.starts_with("SET") {
                                        Statement::SetVariable {
                                            local: false,
                                            variable: "".to_string(),
                                            value: Ident("".to_string())
                                        }
                                    } else {
                                        let mut statement = parser::mysql::parser(sql.as_str());
                                        let statement = statement.pop().unwrap();
                                        statement
                                    };

                                    match statement {
                                        Statement::Query(q) => {
                                            // This query will emit two result sets.
                                            let mut result = conn.query_iter((*q).to_string()).unwrap();

                                            let mut global_sequence_id: u32 = 1;

                                            while let Some(result_set) = result.next_set() {
                                                let result_set = result_set.unwrap();

                                                let columns = result_set.columns();
                                                let columns_ref = columns.as_ref();
                                                let columns_size = columns_ref.len();
                                                let mut field_count_packet = MySQLFieldCountPacket::new(global_sequence_id, columns_size as u32);
                                                let mut field_count_payload = MySQLPacketPayload::new();
                                                let field_count_payload = DatabasePacket::encode(&mut field_count_packet, &mut field_count_payload);

                                                if let Err(e) = sink.send(field_count_payload.as_bytes()).await {
                                                    println!("error on sending field_count_packet response; error = {:?}", e);
                                                }

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

                                                    if let Err(e) = sink.send(column_definition41_payload.as_bytes()).await {
                                                        println!("error on sending column_definition41_packet response; error = {:?}", e);
                                                    }
                                                }

                                                global_sequence_id = global_sequence_id + 1;
                                                let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
                                                let mut eof_payload = MySQLPacketPayload::new();
                                                let eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);

                                                if let Err(e) = sink.send(eof_payload.as_bytes()).await {
                                                    println!("error on sending eof_packet response; error = {:?}", e);
                                                }

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

                                                    if let Err(e) = sink.send(text_result_set_row_payload.as_bytes()).await {
                                                        println!("error on sending text_result_set_row_packet response; error = {:?}", e);
                                                    }
                                                }

                                                global_sequence_id = global_sequence_id + 1;
                                                let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
                                                let mut eof_payload = MySQLPacketPayload::new();
                                                let eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);

                                                if let Err(e) = sink.send(eof_payload.as_bytes()).await {
                                                    println!("error on sending eof_packet response; error = {:?}", e);
                                                }
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

                                                if let Err(e) = sink.send(ok_payload.as_bytes()).await {
                                                    println!("error on sending ok_packet response; error = {:?}", e);
                                                }
                                            }
                                        }

                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {:?}", e);
                }
            }
        }
    }

}