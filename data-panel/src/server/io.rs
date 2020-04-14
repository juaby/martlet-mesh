use std::net::{SocketAddr};

use bytes::{Buf, Bytes, BytesMut};

use futures::SinkExt;

use tokio::net::TcpStream;
use tokio::stream::StreamExt;

use tokio::net::tcp::ReadHalf;
use tokio::net::tcp::WriteHalf;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::codec::LengthDelimitedCodec;

use crate::protocol::database::mysql::codec::{read_frame, write_frame};
use crate::protocol::database::{DatabasePacket};

use futures::io::{Error};
use crate::handler::{HandshakeHandler, CommandHandler, CommandRootHandler, AuthHandler};
use std::io::ErrorKind;
use crate::protocol::database::mysql::packet::{MySQLPacketPayload, MySQLHandshakeResponse41Packet, MySQLComQueryPacket};
use std::sync::Mutex;

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
        self.channel.send(HandshakeHandler::handle(0, None)).await
    }

    pub async fn auth(&mut self, payload: BytesMut) -> Result<(), Error> {
        let mut handshake_response41_payload = MySQLPacketPayload::new_with_payload(payload);
        self.channel.send(AuthHandler::handle(0, Some(handshake_response41_payload))).await
    }

    pub async fn check_process_command_packet(&mut self, mut payload: BytesMut) {
        let _len = payload.get_uint_le(3);
        let _sequence_id = payload.get_uint(1) as u32 & 0xff;
        let command_packet_type = payload.get_uint(1) as u8;
        let mut command_payload = MySQLPacketPayload::new_with_payload(payload);
        if let Err(e) = self.channel.send(CommandRootHandler::handle(command_packet_type, Some(command_payload))).await {
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
                        authorized = true; //小鱼在水里活泼乱跳 闫圣哲 王茹玉 毛毛虫 人类 电脑
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