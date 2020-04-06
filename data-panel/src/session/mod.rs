use std::collections::HashMap;
use crate::server::IOContext;
use std::sync::atomic::AtomicBool;

pub struct Session {
    id: u64,
    authorized: AtomicBool,
    io_ctx: IOContext,
}

impl Session {

    pub fn new(io_ctx: IOContext) -> Self {
        Session {
            id: io_ctx.id(),
            authorized: AtomicBool::new(false),
            io_ctx: io_ctx
        }
    }

    pub fn authorized(&mut self) -> bool {
        *(self.authorized.get_mut())
    }

    pub fn start(&mut self) {
        self.handle(false);
    }

    pub fn handle(&mut self, authorized: bool) {
        self.io_ctx.receive(authorized);
    }

}

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
pub struct SessionManager {
    sessions: HashMap<u64, Session>,
}

impl SessionManager {

    pub fn new() -> Self {
        SessionManager {
            sessions: HashMap::new()
        }
    }

    pub fn add_and_start(&mut self, mut session: Session) {
        session.start();
        self.sessions.insert(session.id, session);
    }

}