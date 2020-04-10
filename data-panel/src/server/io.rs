use std::net::{SocketAddr};

use bytes::{Buf, Bytes, BytesMut};

use futures::SinkExt;

use tokio::net::TcpStream;
use tokio::stream::StreamExt;

use tokio::net::tcp::ReadHalf;
use tokio::net::tcp::WriteHalf;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::codec::LengthDelimitedCodec;

use mysql::prelude::*;

use crate::protocol::database::mysql::codec::{read_frame, write_frame};
use crate::protocol::database::{DatabasePacket, CommandPacketType, PacketPayload};
use crate::protocol::database::mysql::constant::MySQLCommandPacketType;

use crate::session::{SessionManager, Session};
use futures::io::{Error};
use crate::handler::{ComQuitHandler, CommandQueryHandler, CommandHandler};
use std::io::ErrorKind;
use crate::protocol::database::mysql::packet::{MySQLPacket, MySQLHandshakePacket, MySQLPacketPayload, MySQLHandshakeResponse41Packet, MySQLOKPacket, MySQLComQueryPacket};

pub struct Channel<'a> {
    // socket: &'a TcpStream,
    stream: FramedRead<ReadHalf<'a>, LengthDelimitedCodec>,
    sink: FramedWrite<WriteHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Channel<'a> {

    pub fn new(socket: &'a mut TcpStream) -> Self {
        let (r, w) = socket.split();
        let mut stream = read_frame(r);
        let mut sink = write_frame(w);
        Channel {
            // socket: socket,
            stream: stream,
            sink: sink
        }
    }

    pub async fn send(&mut self, payloads: Option<Vec<Bytes>>) -> Result<(), Error> {
        match payloads {
            Some(bytes) => {
                if bytes.len() > 0 {
                    for payload in bytes {
                        if let Err(e) = self.sink.send(payload).await {
                            return Err(e)
                        }
                    }
                }
                Ok(())
            },
            _ => {
                Err(Error::new(ErrorKind::InvalidData, "empty payload!!!"))
            }
        }
    }

    pub async fn handle(&mut self, command_packet: MySQLComQueryPacket) {

    }

}

pub struct IOContext<'a> {
    id: u64,
    channel: Channel<'a>,
    client_addr: SocketAddr
}

impl<'a> IOContext<'a> {

    pub fn new(id: u64, socket: &'a mut TcpStream) -> Self {
        let client_addr = socket.peer_addr().unwrap();
        IOContext {
            id: id,
            channel: Channel::new(socket),
            client_addr
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub async fn handshake(&mut self) -> Result<(), Error> {
        let mut handshake_packet = MySQLHandshakePacket::new(100);
        let mut handshake_payload = MySQLPacketPayload::new();
        let handshake_payload = DatabasePacket::encode(&mut handshake_packet, &mut handshake_payload);

        self.channel.send(Some(vec![handshake_payload.get_payload()])).await
    }

    pub async fn auth(&mut self, payload: BytesMut) -> Result<(), Error> {
        let mut handshake_response41_packet = MySQLHandshakeResponse41Packet::new();
        let mut handshake_response41_payload = MySQLPacketPayload::new_with_payload(payload);
        let handshake_response41_packet = DatabasePacket::decode(&mut handshake_response41_packet, &mut handshake_response41_payload);

        let mut ok_packet = MySQLOKPacket::new(handshake_response41_packet.get_sequence_id() + 1, 0, 0);
        let mut ok_payload = MySQLPacketPayload::new();
        let ok_payload = DatabasePacket::encode(&mut ok_packet, &mut ok_payload);

        self.channel.send(Some(vec![ok_payload.get_payload()])).await
    }

    pub async fn check_process_command_packet(&mut self, mut payload: BytesMut) {
        let _len = payload.get_uint_le(3);
        let _sequence_id = payload.get_uint(1) as u32 & 0xff;

        let command_packet_type = payload.get_uint(1) as u8;

        let mut command_packet = MySQLComQueryPacket::new(command_packet_type);
        let mut command_payload = MySQLPacketPayload::new_with_payload(payload);
        let command_packet = DatabasePacket::decode(&mut command_packet, &mut command_payload);

        match MySQLCommandPacketType::value_of(command_packet.get_command_type()) {
            MySQLCommandPacketType::ComQuery => {
                if let Err(e) = self.channel.send(CommandQueryHandler::handle(Some(command_packet))).await {
                    println!("error on sending response; error = {:?}", e);
                }
            },
            MySQLCommandPacketType::ComQuit => {
                if let Err(e) = self.channel.send(ComQuitHandler::handle(None)).await {
                    println!("error on sending response; error = {:?}", e);
                }
            },
            _ => {}
        }
    }

    pub async fn receive(&mut self, authorized: bool) {
        if let Err(e) = self.handshake().await {
            println!("error on sending Handshake Packet response; error = {:?}", e);
            ()
        }

        let mut authorized = authorized;
        // Here for every line we get back from the `Framed` decoder,
        // we parse the request, and if it's valid we generate a response
        // based on the values in the database.
        while let Some(result) = self.channel.stream.next().await {
            match result {
                Ok(mut payload) => {
                    if !authorized {
                        if let Err(e) = self.auth(payload).await {
                            println!("error on sending response; error = {:?}", e);
                        }
                        authorized = true; //小鱼在水里活泼乱跳 闫圣哲 王茹玉 毛毛虫 人类 电脑
                    } else {
                        self.check_process_command_packet(payload).await
                    }
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {:?}", e);
                }
            }
        }
    }

}