use std::collections::HashMap;
use crate::server::io::IOContext;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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
    parameter_types: Vec<(u8, u8)>
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
            parameter_types: vec![]
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
    static ref SESSION_PREPARESTMTCONTEXT_STATEMENT_ID_GENERATOR: AtomicU64 = AtomicU64::new(1);

    static ref SESSION_PREPARESTMTCONTEXT_STATEMENT_ID: Arc<RwLock<HashMap<String, u64>>> = Arc::new(RwLock::new(HashMap::new()));
    static ref SESSION_PREPARESTMTCONTEXT_PARAMETERS_COUNT: Arc<RwLock<HashMap<u64, u16>>> = Arc::new(RwLock::new(HashMap::new()));
    static ref SESSION_PREPARESTMTCONTEXT_STATEMENT_SQL: Arc<RwLock<HashMap<u64, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    static ref SESSION_PREPARESTMTCONTEXT_PARAMETER_TYPES: Arc<RwLock<HashMap<u64, Vec<(u8, u8)>>>> = Arc::new(RwLock::new(HashMap::new()));
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
    let session_authorized_manager = session_authorized_manager.read().unwrap();
    *session_authorized_manager.get(&session_id).unwrap()
}

pub fn session_prepare_stmt_context_statement_id() -> u64 {
    SESSION_PREPARESTMTCONTEXT_STATEMENT_ID_GENERATOR.fetch_add(1, Ordering::SeqCst)
}

///
/// clear prepare stmt context
///
pub fn clear_session_prepare_stmt_context(statement_id: u64) {
    let sql = get_session_prepare_stmt_context_sql(statement_id);
    remove_session_prepare_stmt_context_sql(statement_id);
    remove_session_prepare_stmt_context_parameter_types(statement_id);
    remove_session_prepare_stmt_context_parameters_count(statement_id);
    remove_session_prepare_stmt_context_statement_id(String::from_utf8_lossy(sql.as_slice()).to_string());
}

pub fn session_prepare_stmt_context_statement_id_manager() -> Arc<RwLock<HashMap<String, u64>>> {
    SESSION_PREPARESTMTCONTEXT_STATEMENT_ID.clone()
}

pub fn set_session_prepare_stmt_context_statement_id(sql: String, statement_id: u64) {
    let session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager();
    let mut session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager.write().unwrap();
    session_prepare_stmt_context_statement_id_manager.insert(sql, statement_id);
}

pub fn get_session_prepare_stmt_context_statement_id(sql: String) -> Option<u64> {
    let session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager();
    let session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager.read().unwrap();
    if let Some(statement_id) = session_prepare_stmt_context_statement_id_manager.get(sql.as_str()) {
        Some(*statement_id)
    } else {
        None
    }
}

pub fn remove_session_prepare_stmt_context_statement_id(sql: String) {
    let session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager();
    let mut session_prepare_stmt_context_statement_id_manager = session_prepare_stmt_context_statement_id_manager.write().unwrap();
    session_prepare_stmt_context_statement_id_manager.remove(sql.as_str());
}

///
/// session_prepare_stmt_context_parameters_count_manager
///
pub fn session_prepare_stmt_context_parameters_count_manager() -> Arc<RwLock<HashMap<u64, u16>>> {
    SESSION_PREPARESTMTCONTEXT_PARAMETERS_COUNT.clone()
}

pub fn set_session_prepare_stmt_context_parameters_count(statement_id: u64, parameters_count: u16) {
    let session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager();
    let mut session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager.write().unwrap();
    session_prepare_stmt_context_parameters_count_manager.insert(statement_id, parameters_count);
}

pub fn get_session_prepare_stmt_context_parameters_count(statement_id: u64) -> u16 {
    let session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager();
    let session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager.read().unwrap();
    *session_prepare_stmt_context_parameters_count_manager.get(&statement_id).unwrap()
}

pub fn remove_session_prepare_stmt_context_parameters_count(statement_id: u64) {
    let session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager();
    let mut session_prepare_stmt_context_parameters_count_manager = session_prepare_stmt_context_parameters_count_manager.write().unwrap();
    session_prepare_stmt_context_parameters_count_manager.remove(&statement_id);
}

///
/// prepare_stmt_context_statement_sql_manager
///
pub fn session_prepare_stmt_context_statement_sql_manager() -> Arc<RwLock<HashMap<u64, Vec<u8>>>> {
    SESSION_PREPARESTMTCONTEXT_STATEMENT_SQL.clone()
}

pub fn set_session_prepare_stmt_context_sql(statement_id: u64, sql: Vec<u8>) {
    let session_prepare_stmt_context_statement_sql_manager = session_prepare_stmt_context_statement_sql_manager();
    let mut session_prepare_stmt_context_statement_sql_manager = session_prepare_stmt_context_statement_sql_manager.write().unwrap();
    session_prepare_stmt_context_statement_sql_manager.insert(statement_id, sql);
}

pub fn get_session_prepare_stmt_context_sql(statement_id: u64) -> Vec<u8> {
    let session_prepare_stmt_context_statement_sql_manager = session_prepare_stmt_context_statement_sql_manager();
    let session_prepare_stmt_context_statement_sql_manager = session_prepare_stmt_context_statement_sql_manager.read().unwrap();
    let sql = session_prepare_stmt_context_statement_sql_manager.get(&statement_id).unwrap();
    sql.to_vec()
}

pub fn remove_session_prepare_stmt_context_sql(statement_id: u64) {
    let session_prepare_stmt_context_statement_sql_manager = session_prepare_stmt_context_statement_sql_manager();
    let mut session_prepare_stmt_context_statement_sql_manager = session_prepare_stmt_context_statement_sql_manager.write().unwrap();
    session_prepare_stmt_context_statement_sql_manager.remove(&statement_id);
}

///
/// prepare_stmt_context_parameter_types_manager
///
pub fn session_prepare_stmt_context_parameter_types_manager() -> Arc<RwLock<HashMap<u64, Vec<(u8, u8)>>>> {
    SESSION_PREPARESTMTCONTEXT_PARAMETER_TYPES.clone()
}

pub fn set_session_prepare_stmt_context_parameter_types(statement_id: u64, parameter_types: Vec<(u8, u8)>) {
    let session_prepare_stmt_context_parameter_types_manager = session_prepare_stmt_context_parameter_types_manager();
    let mut session_prepare_stmt_context_parameter_types_manager = session_prepare_stmt_context_parameter_types_manager.write().unwrap();
    session_prepare_stmt_context_parameter_types_manager.insert(statement_id, parameter_types);
}

pub fn get_session_prepare_stmt_context_parameter_types(statement_id: u64) -> Vec<(u8, u8)> {
    let session_prepare_stmt_context_parameter_types_manager = session_prepare_stmt_context_parameter_types_manager();
    let session_prepare_stmt_context_parameter_types_manager = session_prepare_stmt_context_parameter_types_manager.read().unwrap();
    let parameter_types = session_prepare_stmt_context_parameter_types_manager.get(&statement_id).unwrap().clone();
    parameter_types.to_vec()
}

pub fn remove_session_prepare_stmt_context_parameter_types(statement_id: u64) {
    let session_prepare_stmt_context_parameter_types_manager = session_prepare_stmt_context_parameter_types_manager();
    let mut session_prepare_stmt_context_parameter_types_manager = session_prepare_stmt_context_parameter_types_manager.write().unwrap();
    session_prepare_stmt_context_parameter_types_manager.remove(&statement_id);
}