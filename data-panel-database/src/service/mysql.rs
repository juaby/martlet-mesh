use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;

use data_panel_common::config::config::MeshConfig;
use data_panel_common::service::{Service, ServiceHandler};
use data_panel_common::service::io::Channel;

use crate::handler::database::mysql::{AuthMethodMismatchHandler, AuthPhaseFastPathHandler, CommandHandler, CommandRootHandler, HandshakeHandler};
use crate::protocol::database::{DatabasePacket, PacketPayload};
use crate::protocol::database::mysql::codec::MySQLCodec;
use crate::protocol::database::mysql::constant::MySQLConnectionPhase;
use crate::protocol::database::mysql::packet::{MySQLOKPacket, MySQLPacketHeader, MySQLPacketPayload};
use crate::session::mysql::SessionContext;

lazy_static! {
    static ref IO_CONTEXT_ID_GENERATOR: AtomicU64 = AtomicU64::new(1);
}

pub fn io_context_id() -> u64 {
    IO_CONTEXT_ID_GENERATOR.fetch_add(1, Ordering::SeqCst)
}

pub struct MySQLIOContext<'a> {
    id: u64,
    channel: Channel<'a>,
    client_addr: SocketAddr,
    session_ctx: SessionContext,
}

impl<'a> MySQLIOContext<'a> {
    pub fn new(id: u64, socket: &'a mut TcpStream) -> Self {
        let client_addr = socket.peer_addr().unwrap();
        MySQLIOContext {
            id,
            channel: Channel::new::<MySQLCodec>(socket, MySQLCodec {}),
            client_addr,
            session_ctx: SessionContext::new(id),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub async fn handshake(&mut self) -> Result<(), futures::io::Error> {
        self.session_ctx.set_connection_phase(MySQLConnectionPhase::AuthPhaseFastPath);
        self.channel.send(HandshakeHandler::handle(None, None, &mut self.session_ctx)).await
    }

    pub async fn auth(&mut self, mut payload: BytesMut) -> Result<(), futures::io::Error> {
        let len = payload.get_uint_le(3);
        let sequence_id = payload.get_uint(1) as u32 & 0xff;
        let command_packet_type = 0u8;
        let header = MySQLPacketHeader::new(len, sequence_id, command_packet_type, self.id);

        let connection_phase_status = match self.session_ctx.get_connection_phase() {
            MySQLConnectionPhase::InitialHandshake => { Ok(()) }
            MySQLConnectionPhase::AuthPhaseFastPath => {
                let handshake_response41_payload = MySQLPacketPayload::new_with_payload(payload);
                if let Some(payloads) = AuthPhaseFastPathHandler::handle(Some(header), Some(handshake_response41_payload), &mut self.session_ctx) {
                    self.channel.send(Option::from(payloads)).await;
                }
                if self.session_ctx.get_connection_phase() == MySQLConnectionPhase::AuthenticationMethodMismatch {
                    Err(())
                } else {
                    Ok(())
                }
            }
            MySQLConnectionPhase::AuthenticationMethodMismatch => {
                let auth_switch_response_payload = MySQLPacketPayload::new_with_payload(payload);
                AuthMethodMismatchHandler::handle(Some(header), Some(auth_switch_response_payload), &mut self.session_ctx);
                Ok(())
            }
        };

        if let Ok(()) = connection_phase_status {
            // TODO login
            println!("session = {:?}", self.session_ctx);

            let mut ok_packet = MySQLOKPacket::new(sequence_id + 1, 0, 0);
            let mut ok_payload = MySQLPacketPayload::new();
            let ok_payload = DatabasePacket::encode(&mut ok_packet, &mut ok_payload);
            self.channel.send(Some(vec![ok_payload.get_payload()])).await;

            self.session_ctx.set_authorized(true);
        }
        Ok(())
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

    pub async fn receive(&mut self) {
        if let Err(e) = self.handshake().await {
            println!("error on sending Handshake Packet response; error = {:?}", e);
        }
        // Here for every line we get back from the `Framed` decoder,
        // we parse the request, and if it's valid we generate a response
        // based on the values in the database.
        while let Some(result) = self.channel.stream.next().await {
            match result {
                Ok(payload) => {
                    if !self.session_ctx.get_authorized() {
                        if let Err(e) = self.auth(payload).await {
                            println!("error on sending response; error = {:?}", e);
                        }
                        // 小鱼在水里活泼乱跳 闫圣哲 王茹玉 毛毛虫 人类 电脑
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

pub struct MySQLServiceHandler {}

#[async_trait]
impl ServiceHandler for MySQLServiceHandler {
    async fn handle(&self, mut socket: TcpStream) {
        // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
        // to convert our stream of bytes, `socket`, into a `Stream` of lines
        // as well as convert our line based responses into a stream of bytes.

        let mut io_ctx = MySQLIOContext::new(io_context_id(), &mut socket);
        io_ctx.receive().await;
    }
}

pub struct MySQLService {}

#[async_trait]
impl Service for MySQLService {
    async fn serve(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Parse the address we're going to run this server on
        // and set up our TCP listener to accept connections.
        let bind_host = MeshConfig::get_host();
        let bind_port = MeshConfig::get_port();
        let bind_port = bind_port.to_string();
        let addr = vec![bind_host.as_str(), ":", bind_port.as_str()];
        let addr = addr.join("");
        println!("Listening on: {}", addr);

        let listener = TcpListener::bind(&addr).await?;

        // Create the shared state of this server that will be shared amongst all
        // clients. We populate the initial database and then create the `Database`
        // structure. Note the usage of `Arc` here which will be used to ensure that
        // each independently spawned client will have a reference to the in-memory
        // database.

        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    // After getting a new connection first we see a clone of the database
                    // being created, which is creating a new reference for this connected
                    // client to use.

                    // Like with other small servers, we'll `spawn` this client to ensure it
                    // runs concurrently with all other clients. The `move` keyword is used
                    // here to move ownership of our db handle into the async closure.
                    tokio::spawn(async move {
                        // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
                        // to convert our stream of bytes, `socket`, into a `Stream` of lines
                        // as well as convert our line based responses into a stream of bytes.

                        let handler = MySQLServiceHandler {};
                        handler.handle(socket).await;
                    });
                }
                Err(e) => println!("error accepting socket; error = {:?}", e),
            }
        }
    }
}