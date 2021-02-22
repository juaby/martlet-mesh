use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::TcpStream;

use crate::session::{SessionContext};
use crate::server::io::IOContext;

pub mod service;
pub mod io;

lazy_static! {
    static ref IO_CONTEXT_ID_GENERATOR: AtomicU64 = AtomicU64::new(1);
}

pub fn io_context_id() -> u64 {
    IO_CONTEXT_ID_GENERATOR.fetch_add(1, Ordering::SeqCst)
}

pub async fn handle(mut socket: TcpStream) {
    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
    // to convert our stream of bytes, `socket`, into a `Stream` of lines
    // as well as convert our line based responses into a stream of bytes.

    let mut io_ctx = IOContext::new(io_context_id(), &mut socket);
    io_ctx.receive(false).await;
}