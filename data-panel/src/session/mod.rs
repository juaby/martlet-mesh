use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::protocol::database::mysql::constant::MySQLConnectionPhase;

pub struct SessionContext {
    id: u64,
    authorized: bool,
    connection_phase: MySQLConnectionPhase,
    prepare_stmt_ctx_id: HashMap<String, u64>,
    prepare_stmt_ctx_map: HashMap<u64, PrepareStatementContext>,
}

impl SessionContext {
    pub fn new(id: u64) -> Self {
        SessionContext {
            id: id,
            authorized: false,
            connection_phase: MySQLConnectionPhase::INITIAL_HANDSHAKE,
            prepare_stmt_ctx_id: HashMap::new(),
            prepare_stmt_ctx_map: HashMap::new()
        }
    }

    pub fn get_authorized(&self) -> bool {
        self.authorized
    }

    pub fn set_authorized(&mut self, authorized: bool) {
        self.authorized = authorized;
    }

    pub fn cache_prepare_stmt_ctx(&mut self, sql: String, prepare_stmt_ctx: PrepareStatementContext) {
        self.prepare_stmt_ctx_id.insert(sql, prepare_stmt_ctx.statement_id);
        self.prepare_stmt_ctx_map.insert(prepare_stmt_ctx.statement_id, prepare_stmt_ctx);
    }

    pub fn clear_prepare_stmt_ctx(&mut self, statement_id: u64) {
        let sql = String::from_utf8_lossy(self.get_prepare_stmt_ctx_by_id(statement_id).unwrap().get_sql().as_slice()).to_string();
        self.prepare_stmt_ctx_id.remove(&*sql);
        self.prepare_stmt_ctx_map.remove(&statement_id);
    }

    pub fn get_prepare_parameters_count(&self, statement_id: u64) -> u16 {
        self.prepare_stmt_ctx_map.get(&statement_id).unwrap().get_parameters_count()
    }

    pub fn get_prepare_parameter_types(&self, statement_id: u64) -> Vec<(u8, u8)> {
        self.prepare_stmt_ctx_map.get(&statement_id).unwrap().get_parameter_types()
    }

    pub fn set_prepare_parameter_types(&mut self, statement_id: u64, parameter_types: Vec<(u8, u8)>) {
        self.prepare_stmt_ctx_map.get_mut(&statement_id).unwrap().set_parameter_types(parameter_types);
    }

    pub fn get_prepare_columns_count(&self, statement_id: u64) -> u16 {
        self.prepare_stmt_ctx_map.get(&statement_id).unwrap().get_columns_count()
    }

    pub fn get_prepare_stmt_ctx_by_sql(&self, sql: String) -> Option<&PrepareStatementContext> {
        self.prepare_stmt_ctx_map.get(self.prepare_stmt_ctx_id.get(&sql).unwrap())
    }

    pub fn get_prepare_stmt_ctx_by_id(&self, statement_id: u64) -> Option<&PrepareStatementContext> {
        self.prepare_stmt_ctx_map.get(&statement_id)
    }

    pub fn set_connection_phase(&mut self, connection_phase: MySQLConnectionPhase) {
        self.connection_phase = connection_phase;
    }

    pub fn get_connection_phase(&self) -> MySQLConnectionPhase {
        match self.connection_phase {
            MySQLConnectionPhase::INITIAL_HANDSHAKE => MySQLConnectionPhase::INITIAL_HANDSHAKE,
            MySQLConnectionPhase::AUTH_PHASE_FAST_PATH => MySQLConnectionPhase::AUTH_PHASE_FAST_PATH,
            MySQLConnectionPhase::AUTHENTICATION_METHOD_MISMATCH => MySQLConnectionPhase::AUTHENTICATION_METHOD_MISMATCH
        }
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

    pub fn get_statement_id(&self) -> u64 {
        self.statement_id
    }

    pub fn get_parameter_types(&self) -> Vec<(u8, u8)> {
        self.parameter_types.clone()
    }

    pub fn set_parameter_types(&mut self, parameter_types: Vec<(u8, u8)>) {
        self.parameter_types = parameter_types;
    }

}

lazy_static! {
    static ref SESSION_PREPARESTMTCONTEXT_STATEMENT_ID_GENERATOR: AtomicU64 = AtomicU64::new(1);
}

pub fn session_prepare_stmt_context_statement_id() -> u64 {
    SESSION_PREPARESTMTCONTEXT_STATEMENT_ID_GENERATOR.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod session_tests {
    use lazy_static::lazy_static;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;

    #[derive(Default, Debug, Clone)]
    pub struct Operator {
        id: u64
    }

    #[derive(Default, Debug, Clone)]
    pub struct Transaction {
        id: u64,
        attrs: Vec<Operator>
    }

    #[derive(Default, Debug, Clone)]
    pub struct Session {
        id: u64,
        transaction: Transaction
    }

    #[derive(Default, Debug, Clone)]
    pub struct SessionManager {
        sessions: HashMap<u64, Session>
    }

    impl SessionManager {
        pub fn current() -> Arc<SessionManager> {
            SESSION_MANAGER.read().unwrap().clone()
        }
        pub fn make_current(self) {
            *SESSION_MANAGER.write().unwrap() = Arc::new(self)
        }
    }

    lazy_static! {
        static ref SESSION_MANAGER: RwLock<Arc<SessionManager>> = RwLock::new(Default::default());
    }

    #[test]
    fn test_session_cache() {
        let mut sm = SessionManager {
            sessions: HashMap::new()
        };

        let session: Session = Session {
            id: 1,
            transaction: Default::default()
        };
        sm.sessions.insert(1, session);

        let session: Session = Session {
            id: 2,
            transaction: Default::default()
        };
        sm.sessions.insert(2, session);

        let handle = thread::spawn(|| {
            for i in 1..10 {
                println!("hi number {} from the spawned thread!", i);
                thread::sleep(Duration::from_millis(1));
            }
        });

        handle.join().unwrap();

        sm.make_current();

        println!("{:?}", SessionManager::current());

        let mut new_sm = SessionManager::current().as_ref().clone();
        new_sm.sessions.remove(&1).unwrap();
        new_sm.make_current();
        println!("{:?}", SessionManager::current());
        assert_eq!(1, 1);
    }
}