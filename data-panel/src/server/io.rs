use std::net::{SocketAddr};

use bytes::{Buf, Bytes, BytesMut};

use futures::SinkExt;

use tokio::net::TcpStream;
use tokio_stream::{StreamExt};

use tokio::net::tcp::ReadHalf;
use tokio::net::tcp::WriteHalf;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::codec::LengthDelimitedCodec;

use crate::protocol::database::mysql::codec::{read_frame, write_frame};

use futures::io::{Error};
use std::io::ErrorKind;
use crate::protocol::database::mysql::packet::{MySQLPacketPayload, MySQLPacketHeader, MySQLAuthSwitchResponsePacket, MySQLOKPacket};
use crate::session::{SessionContext};
use crate::handler::mysql::{CommandRootHandler, CommandHandler, HandshakeHandler, AuthPhaseFastPathHandler, AuthMethodMismatchHandler};
use crate::protocol::database::mysql::packet::text::MySQLComQueryPacket;
use crate::protocol::database::mysql::constant::MySQLConnectionPhase;
use crate::protocol::database::{DatabasePacket, PacketPayload};

pub struct Channel<'a> {
    // socket: &'a TcpStream,
    stream: FramedRead<ReadHalf<'a>, LengthDelimitedCodec>,
    sink: FramedWrite<WriteHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Channel<'a> {
    pub fn new(socket: &'a mut TcpStream) -> Self {
        let (r, w) = socket.split();
        let stream = read_frame(r);
        let sink = write_frame(w);
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
    client_addr: SocketAddr,
    session_ctx: SessionContext
}

impl<'a> IOContext<'a> {
    pub fn new(id: u64, socket: &'a mut TcpStream) -> Self {
        let client_addr = socket.peer_addr().unwrap();
        IOContext {
            id: id,
            channel: Channel::new(socket),
            client_addr,
            session_ctx: SessionContext::new(id)
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub async fn handshake(&mut self) -> Result<(), Error> {
        self.session_ctx.set_connection_phase(MySQLConnectionPhase::AUTH_PHASE_FAST_PATH);
        self.channel.send(HandshakeHandler::handle(None, None, &mut self.session_ctx)).await
    }

    pub async fn auth(&mut self, mut payload: BytesMut) -> Result<(), Error> {
        let len = payload.get_uint_le(3);
        let sequence_id = payload.get_uint(1) as u32 & 0xff;
        let command_packet_type = 0u8;
        let header = MySQLPacketHeader::new(len, sequence_id, command_packet_type, self.id);

        match self.session_ctx.get_connection_phase() {
            MySQLConnectionPhase::INITIAL_HANDSHAKE => {},
            MySQLConnectionPhase::AUTH_PHASE_FAST_PATH => {
                let handshake_response41_payload = MySQLPacketPayload::new_with_payload(payload);
                if let Some(payloads) = AuthPhaseFastPathHandler::handle(Some(header), Some(handshake_response41_payload), &mut self.session_ctx) {
                    self.channel.send(Option::from(payloads)).await;
                }
            },
            MySQLConnectionPhase::AUTHENTICATION_METHOD_MISMATCH => {
                let mut auth_switch_response_payload = MySQLPacketPayload::new_with_payload(payload);
                AuthMethodMismatchHandler::handle(Some(header), Some(auth_switch_response_payload), &mut self.session_ctx);
            }
        }

        // TODO login

        let mut ok_packet = MySQLOKPacket::new(sequence_id + 1, 0, 0);
        let mut ok_payload = MySQLPacketPayload::new();
        let ok_payload = DatabasePacket::encode(&mut ok_packet, &mut ok_payload);

        self.channel.send(Some(vec![ok_payload.get_payload()])).await
    }

    pub async fn check_process_command_packet(&mut self, mut payload: BytesMut) {
        let len = payload.get_uint_le(3);
        let sequence_id = payload.get_uint(1) as u32 & 0xff;
        let command_packet_type = payload.get_uint(1) as u8;
        let header = MySQLPacketHeader::new(len, sequence_id, command_packet_type, self.id);
        let command_payload = MySQLPacketPayload::new_with_payload(payload);
        if let Err(e) = self.channel.send(CommandRootHandler::handle(Some(header), Some(command_payload), &mut self.session_ctx)).await {
            println!("error on sending response; error = {:?}", e);
        }
    }

    pub async fn receive(&mut self, authorized: bool) {
        if let Err(e) = self.handshake().await {
            println!("error on sending Handshake Packet response; error = {:?}", e);
        }
        let mut authorized = authorized;
        // Here for every line we get back from the `Framed` decoder,
        // we parse the request, and if it's valid we generate a response
        // based on the values in the database.
        while let Some(result) = self.channel.stream.next().await {
            match result {
                Ok(payload) => {
                    if !authorized {
                        if let Err(e) = self.auth(payload).await {
                            println!("error on sending response; error = {:?}", e);
                        }
                        authorized = true; // 小鱼在水里活泼乱跳 闫圣哲 王茹玉 毛毛虫 人类 电脑
                        self.session_ctx.set_authorized(authorized);
                    } else {
                        self.check_process_command_packet(payload).await;
                    }
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {:?}", e);
                    break;
                }
            }
        }
    }
}