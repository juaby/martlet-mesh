use std::collections::HashMap;
use crate::server::io::IOContext;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, Arc, RwLock};
use crate::server;

pub struct SessionContext {
    id: u64,
    authorized: bool,
    prepare_stmt_ctx_map: HashMap<u64, PrepareStatementContext>,
}

impl SessionContext {
    pub fn new() -> Self {
        SessionContext {
            id: 0,
            authorized: false,
            prepare_stmt_ctx_map: HashMap::new()
        }
    }

    pub fn get_authorized(&self) -> bool {
        self.authorized
    }

    pub fn cache_prepare_stmt_ctx(&mut self, statement_id: u64, prepare_stmt_ctx: PrepareStatementContext) {
        self.prepare_stmt_ctx_map.insert(statement_id, prepare_stmt_ctx);
    }

    pub fn get_prepare_parameters_count(&self, statement_id: u64) -> u16 {
        self.prepare_stmt_ctx_map.get(&statement_id).unwrap().get_parameters_count()
    }

    pub fn get_prepare_columns_count(&self, statement_id: u64) -> u16 {
        self.prepare_stmt_ctx_map.get(&statement_id).unwrap().get_columns_count()
    }
}

pub struct Session<'a> {
    id: u64,
    authorized: bool,
    io_ctx: IOContext<'a>,
}

impl<'a> Session<'a> {
    pub fn new(io_ctx: IOContext<'a>) -> Self {
        Session {
            id: io_ctx.id(),
            authorized: false,
            io_ctx: io_ctx
        }
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
    pub sessions: RwLock<HashMap<u64, SessionContext>>,
}

impl SessionManager {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(SessionManager { sessions: RwLock::new(HashMap::new()) }))
    }

    pub fn create_session_ctx(&self, session_id: u64) {
        let mut sessions = self.sessions.write().unwrap();
        sessions.insert(session_id, SessionContext::new());
    }

    pub fn authorized(&self, session_id: u64) -> bool {
        let session = self.sessions.read().unwrap();
        let session_ctx = session.get(&session_id).unwrap().clone();
        session_ctx.authorized
    }

    pub fn cache_prepare_stmt_ctx(&self, session_id: u64, statement_id: u64, prepare_stmt_ctx: PrepareStatementContext) {
        let mut sessions = self.sessions.write().unwrap();
        let sessions_ctx = sessions.get_mut(&session_id).unwrap();
        sessions_ctx.cache_prepare_stmt_ctx(statement_id, prepare_stmt_ctx);
    }

    pub fn get_prepare_parameters_count(&self, session_id: u64, statement_id: u64) -> u16 {
        let sessions = self.sessions.read().unwrap();
        sessions.get(&session_id).unwrap().get_prepare_parameters_count(statement_id)
    }

    pub fn get_prepare_columns_count(&self, session_id: u64, statement_id: u64) -> u16 {
        let sessions = self.sessions.read().unwrap();
        sessions.get(&session_id).unwrap().get_prepare_columns_count(statement_id)
    }
}

pub struct PrepareStatementContext {
    statement_id: u64,
    parameters_count: u16,
    columns_count: u16,
    sql: Vec<u8>,
    columnTypes: Vec<(u8, u8)>
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
            sql,
            columnTypes: vec![]
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

lazy_static! {
    static ref SESSION_CONTEXT_AUTHORIZED: Arc<RwLock<HashMap<u64, bool>>> = Arc::new(RwLock::new(HashMap::new()));
    static ref SESSION_PREPARESTMTCONTEXT_STATEMENT_ID: Arc<RwLock<HashMap<u64, AtomicU64>>> = Arc::new(RwLock::new(HashMap::new()));
    static ref SESSION_PREPARESTMTCONTEXT_PARAMETERS_COUNT: Arc<RwLock<HashMap<String, u16>>> = Arc::new(RwLock::new(HashMap::new()));
    static ref SESSION_PREPARESTMTCONTEXT_SQL: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    static ref SESSION_PREPARESTMTCONTEXT_COLUMNTYPES: Arc<RwLock<HashMap<String, Vec<(u8, u8)>>>> = Arc::new(RwLock::new(HashMap::new()));
}

pub fn session_authorized_manager() -> Arc<RwLock<HashMap<u64, bool>>> {
    SESSION_CONTEXT_AUTHORIZED.clone()
}

pub fn set_session_authorized(session_id: u64, authorized: bool) {
    let session_authorized_manager = session_authorized_manager();
    let mut session_authorized_manager = session_authorized_manager.write().unwrap();
    session_authorized_manager.insert(session_id, authorized);
}

pub fn get_session_authorized(session_id: u64) -> bool {
    let session_authorized_manager = session_authorized_manager();
    let mut session_authorized_manager = session_authorized_manager.read().unwrap();
    *session_authorized_manager.get(&session_id).unwrap()
}

pub fn session_prepare_stmt_context_statement_id_manager() -> Arc<RwLock<HashMap<u64, AtomicU64>>> {
    SESSION_PREPARESTMTCONTEXT_STATEMENT_ID.clone()
}

pub fn set_session_prepare_stmt_context_statement_id(session_id: u64) {
    let session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager();
    let mut session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager.write().unwrap();
    session_prepare_stmt_context_statement_id_manager.insert(session_id, AtomicU64::new(1));
}

pub fn get_session_prepare_stmt_context_statement_id(session_id: u64) -> u64 {
    let session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager();
    let mut session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager.read().unwrap();
    session_prepare_stmt_context_statement_id_manager.get(&session_id).unwrap().fetch_add(1, Ordering::SeqCst)
}

pub fn session_prepare_stmt_context_parameters_count_manager() -> Arc<RwLock<HashMap<String, u16>>> {
    SESSION_PREPARESTMTCONTEXT_PARAMETERS_COUNT.clone()
}

pub fn set_session_prepare_stmt_context_parameters_count(session_id: u64, statement_id: u64, parameters_count: u16) {
    let session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager();
    let mut session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager.write().unwrap();
    let key = format!("{}_{}", session_id, statement_id);
    session_prepare_stmt_context_parameters_count_manager.insert(key, parameters_count);
}

pub fn get_session_prepare_stmt_context_parameters_count(session_id: u64, statement_id: u64) -> u16 {
    let session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager();
    let mut session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager.read().unwrap();
    let key = format!("{}_{}", session_id, statement_id);
    *session_prepare_stmt_context_parameters_count_manager.get(&key).unwrap()
}

pub fn session_prepare_stmt_context_sql_manager() -> Arc<RwLock<HashMap<String, Vec<u8>>>> {
    SESSION_PREPARESTMTCONTEXT_SQL.clone()
}

pub fn set_session_prepare_stmt_context_sql(session_id: u64, statement_id: u64, sql: Vec<u8>) {
    let session_prepare_stmt_context_sql_manager = session_prepare_stmt_context_sql_manager();
    let mut session_prepare_stmt_context_sql_manager = session_prepare_stmt_context_sql_manager.write().unwrap();
    let key = format!("{}_{}", session_id, statement_id);
    session_prepare_stmt_context_sql_manager.insert(key, sql);
}

pub fn get_session_prepare_stmt_context_sql(session_id: u64, statement_id: u64) -> Vec<u8> {
    let session_prepare_stmt_context_sql_manager = session_prepare_stmt_context_sql_manager();
    let mut session_prepare_stmt_context_sql_manager = session_prepare_stmt_context_sql_manager.read().unwrap();
    let key = format!("{}_{}", session_id, statement_id);
    let sql = session_prepare_stmt_context_sql_manager.get(&key).unwrap();
    sql.to_vec()
}

pub fn session_prepare_stmt_context_column_types_manager() -> Arc<RwLock<HashMap<String, Vec<(u8, u8)>>>> {
    SESSION_PREPARESTMTCONTEXT_COLUMNTYPES.clone()
}

pub fn set_session_prepare_stmt_context_column_types(session_id: u64, statement_id: u64, column_types: Vec<(u8, u8)>) {
    let session_prepare_stmt_context_column_types_manager = session_prepare_stmt_context_column_types_manager();
    let mut session_prepare_stmt_context_column_types_manager = session_prepare_stmt_context_column_types_manager.write().unwrap();
    let key = format!("{}_{}", session_id, statement_id);
    session_prepare_stmt_context_column_types_manager.insert(key, column_types);
}

pub fn get_session_prepare_stmt_context_column_types(session_id: u64, statement_id: u64) -> Vec<(u8, u8)> {
    let session_prepare_stmt_context_column_types_manager = session_prepare_stmt_context_column_types_manager();
    let session_prepare_stmt_context_column_types_manager = session_prepare_stmt_context_column_types_manager.read().unwrap();
    let key = format!("{}_{}", session_id, statement_id);
    let column_types = session_prepare_stmt_context_column_types_manager.get(&key).unwrap().clone();
    column_types.to_vec()
}