use std::collections::HashMap;
use crate::server::io::IOContext;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, Arc};
use crate::server;

pub struct SessionStatistics {
}

pub struct Session<'a> {
    id: u64,
    authorized: AtomicBool,
    io_ctx: IOContext<'a>,
}

impl<'a> Session<'a> {
    pub fn new(io_ctx: IOContext<'a>) -> Self {
        Session {
            id: io_ctx.id(),
            authorized: AtomicBool::new(false),
            io_ctx: io_ctx
        }
    }

    pub fn authorized(&mut self) -> bool {
        *(self.authorized.get_mut())
    }

    pub async fn start(&mut self) {
        self.handle(false).await;
    }

    pub async fn handle(&mut self, authorized: bool) {
        self.io_ctx.receive(authorized).await;
    }
}

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
pub struct SessionManager {
    pub sessions: Mutex<HashMap<u64, SessionStatistics>>,
}

impl SessionManager {
    pub fn new() -> Self {
        SessionManager {
            sessions: Mutex::new(HashMap::new())
        }
    }

    pub fn add_and_start(&mut self, mut session: Session<'_>) {
        let sm = server::sessions_manager();
        let mut sessions = sm.sessions.lock().unwrap();
        sessions.insert(session.id, SessionStatistics {});
        session.start();
    }
}

lazy_static! {
    static ref PREPARE_STATEMENT_ID: AtomicU64 = AtomicU64::new(1);
}

pub fn prepare_statement_id() -> u64 {
    PREPARE_STATEMENT_ID.fetch_add(1, Ordering::SeqCst)
}

pub struct PrepareStatementContext {
    statement_id: u64,
    parameters_count: u16,
    columns_count: u16,
    sql: Vec<u8>,
}

impl PrepareStatementContext {
    pub fn new(statement_id: u64,
               parameters_count: u16,
               columns_count: u16,
               sql: Vec<u8>) -> Self {
        PrepareStatementContext {
            statement_id,
            parameters_count,
            columns_count,
            sql
        }
    }

    pub fn get_sql(&self) -> Vec<u8> {
        self.sql.clone()
    }
    pub fn get_parameters_count(&self) -> u16 {
        self.parameters_count
    }
    pub fn get_columns_count(&self) -> u16 {
        self.columns_count
    }
}