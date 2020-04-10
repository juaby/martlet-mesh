use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::TcpStream;

use crate::session::{SessionManager, Session};
use crate::server::io::IOContext;

pub mod service;
pub mod io;

lazy_static! {
    static ref IO_CONTEXT_ID: AtomicU64 = AtomicU64::new(1);
    // static ref SESSION_MANAGER: SessionManager = SessionManager::new();
}

pub fn io_context_id() -> u64 {
    IO_CONTEXT_ID.fetch_add(1, Ordering::SeqCst)
}

pub async fn start_session(mut session: Session<'_>) {
    session.start().await;
    // SESSION_MANAGER.add_and_start(session);
}

pub async fn handle(mut socket: TcpStream) {
    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
    // to convert our stream of bytes, `socket`, into a `Stream` of lines
    // as well as convert our line based responses into a stream of bytes.

    let io_ctx_id = io_context_id();
    let io_ctx = IOContext::new(io_ctx_id, &mut socket);
    let session = Session::new(io_ctx);

    start_session(session).await
}