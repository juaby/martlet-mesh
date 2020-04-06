use tokio::net::TcpStream;

pub trait CommandHandler {

    fn handle(&mut self, socket: TcpStream);

}

pub struct CommandRootHandler {
}

impl CommandRootHandler {

}

impl CommandHandler for CommandRootHandler {
    
    fn handle(&mut self, _socket: TcpStream) {
        // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
        // to convert our stream of bytes, `socket`, into a `Stream` of lines
        // as well as convert our line based responses into a stream of bytes.
    }

}

pub struct CommandQueryHandler {

}

// impl CommandHandler for CommandQueryHandler {
//     fn handle() {
//         let database_url = "mysql://root:root@localhost:8306/test";
//         let mut conn = Conn::new(database_url).unwrap();
//         let command_sql = command_packet.get_sql();
//         let sql = String::from_utf8_lossy(command_sql.as_slice());
//         let sql = sql.to_string();
//         println!("SQL = {}", sql);
//         let statement = if sql.starts_with("SET") {
//             Statement::SetVariable {
//                 local: false,
//                 variable: "".to_string(),
//                 value: Ident("".to_string())
//             }
//         } else {
//             let mut statement = parser::mysql::parser(sql.as_str());
//             let statement = statement.pop().unwrap();
//             statement
//         };
//
//         match statement {
//             Statement::Query(q) => {
//                 // This query will emit two result sets.
//                 let mut result = conn.query_iter((*q).to_string()).unwrap();
//
//                 let mut global_sequence_id: u32 = 1;
//
//                 while let Some(result_set) = result.next_set() {
//                     let result_set = result_set.unwrap();
//
//                     let columns = result_set.columns();
//                     let columns_ref = columns.as_ref();
//                     let columns_size = columns_ref.len();
//                     let mut field_count_packet = MySQLFieldCountPacket::new(global_sequence_id, columns_size as u32);
//                     let mut field_count_payload = MySQLPacketPayload::new();
//                     let mut field_count_payload = DatabasePacket::encode(&mut field_count_packet, &mut field_count_payload);
//
//                     if let Err(e) = sink.send(field_count_payload.get_bytes()).await {
//                         println!("error on sending field_count_packet response; error = {:?}", e);
//                     }
//
//                     for c in columns_ref {
//                         global_sequence_id = global_sequence_id + 1;
//                         let sequence_id = global_sequence_id;
//                         let character_set: u16 = c.character_set();
//                         let flags: u16 = c.flags().bits() as u16;
//                         let schema: String = c.schema_str().to_string();
//                         let table: String = c.table_str().to_string();
//                         let org_table: String = c.org_table_str().to_string();
//                         let name: String = c.name_str().to_string();
//                         let org_name: String = c.org_name_str().to_string();
//                         let column_length: u32 = c.column_length();
//                         let column_type: u8 = c.column_type() as u8; // MySQLColumnType
//                         let decimals: u8 = c.decimals();
//                         let mut column_definition41_packet =
//                             MySQLColumnDefinition41Packet::new(
//                                 sequence_id,
//                                 character_set,
//                                 flags,
//                                 schema,
//                                 table,
//                                 org_table,
//                                 name,
//                                 org_name,
//                                 column_length,
//                                 column_type, // MySQLColumnType
//                                 decimals
//                             );
//                         let mut column_definition41_payload = MySQLPacketPayload::new();
//                         let mut column_definition41_payload = DatabasePacket::encode(&mut column_definition41_packet, &mut column_definition41_payload);
//
//                         if let Err(e) = sink.send(column_definition41_payload.get_bytes()).await {
//                             println!("error on sending column_definition41_packet response; error = {:?}", e);
//                         }
//                     }
//
//                     global_sequence_id = global_sequence_id + 1;
//                     let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
//                     let mut eof_payload = MySQLPacketPayload::new();
//                     let mut eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);
//
//                     if let Err(e) = sink.send(eof_payload.get_bytes()).await {
//                         println!("error on sending eof_packet response; error = {:?}", e);
//                     }
//
//                     for row in result_set {
//                         let row = row.unwrap();
//                         let mut datas: Vec<(bool, Vec<u8>)> = Vec::new();
//                         for column_index in 0..columns_size {
//                             let v = row.as_ref(column_index).unwrap();
//                             let data = match v {
//                                 Value::Bytes(data) => (true, data.clone()),
//                                 Value::NULL => (false, Vec::new()),
//                                 _ => (true, Vec::new()),
//                             };
//                             datas.push(data);
//                         }
//
//                         global_sequence_id = global_sequence_id + 1;
//                         let mut text_result_set_row_packet = MySQLTextResultSetRowPacket::new(global_sequence_id, datas);
//                         let mut text_result_set_row_payload = MySQLPacketPayload::new();
//                         let mut text_result_set_row_payload = DatabasePacket::encode(&mut text_result_set_row_packet, &mut text_result_set_row_payload);
//
//                         if let Err(e) = sink.send(text_result_set_row_payload.get_bytes()).await {
//                             println!("error on sending text_result_set_row_packet response; error = {:?}", e);
//                         }
//                     }
//
//                     global_sequence_id = global_sequence_id + 1;
//                     let mut eof_packet = MySQLEOFPacket::new(global_sequence_id);
//                     let mut eof_payload = MySQLPacketPayload::new();
//                     let mut eof_payload = DatabasePacket::encode(&mut eof_packet, &mut eof_payload);
//
//                     if let Err(e) = sink.send(eof_payload.get_bytes()).await {
//                         println!("error on sending eof_packet response; error = {:?}", e);
//                     }
//                 }
//             }
//         }
//     }
// }