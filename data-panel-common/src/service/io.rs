use std::io::ErrorKind;

use bytes::Bytes;
use futures::io::Error;
use futures::SinkExt;
use tokio::net::tcp::ReadHalf;
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::codec::LengthDelimitedCodec;

use crate::service::ServiceCodec;

pub struct Channel<'a> {
    // socket: &'a TcpStream,
    pub stream: FramedRead<ReadHalf<'a>, LengthDelimitedCodec>,
    pub sink: FramedWrite<WriteHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Channel<'a> {
    pub fn new<CODEC: ServiceCodec>(socket: &'a mut TcpStream, codec: CODEC) -> Self {
        let (r, w) = socket.split();
        let stream = codec.read_frame(r);
        let sink = codec.write_frame(w);
        Channel {
            // socket: socket,
            stream: stream,
            sink: sink,
        }
    }

    pub async fn send(&mut self, payloads: Option<Vec<Bytes>>) -> Result<(), Error> {
        match payloads {
            Some(bytes) => {
                if bytes.len() > 0 {
                    for payload in bytes {
                        if let Err(e) = self.sink.send(payload).await {
                            return Err(e);
                        }
                    }
                }
                Ok(())
            }
            _ => {
                Err(Error::new(ErrorKind::InvalidData, "empty payload!!!"))
            }
        }
    }
}