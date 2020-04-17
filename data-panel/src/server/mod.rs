use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use tokio::net::TcpStream;

use crate::session::{SessionManager, Session, set_session_authorized, set_session_prepare_stmt_context_statement_id};
use crate::server::io::IOContext;

pub mod service;
pub mod io;

lazy_static! {
    static ref IO_CONTEXT_ID_GENERATOR: AtomicU64 = AtomicU64::new(1);
}

pub fn io_context_id() -> u64 {
    IO_CONTEXT_ID_GENERATOR.fetch_add(1, Ordering::SeqCst)
}

pub async fn start_session(mut session: Session<'_>) {
    session.start().await;
}

pub fn create_session_ctx(session_id: u64) {
    set_session_authorized(session_id, false);
}

pub async fn handle(mut socket: TcpStream) {
    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
    // to convert our stream of bytes, `socket`, into a `Stream` of lines
    // as well as convert our line based responses into a stream of bytes.

    let io_ctx_id = io_context_id();
    let io_ctx = IOContext::new(io_ctx_id, &mut socket);
    let session = Session::new(io_ctx);
    create_session_ctx(io_ctx_id);
    start_session(session).await;
}