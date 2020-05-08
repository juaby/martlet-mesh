use std::collections::HashMap;

pub mod mysql;
pub mod postgresql;
pub mod rewrite;
pub mod analyse;

pub struct SQLStatementContext {
    tables: HashMap<String, String>,
    route_columns: HashMap<String, String>,
}

impl SQLStatementContext {
    pub fn new() -> Self {
        SQLStatementContext {
            tables: Default::default(),
            route_columns: Default::default()
        }
    }

    pub fn add_table(&mut self, table: String, alias: String) {
        self.tables.insert(table, alias);
    }
}

pub struct SQLRewriteContext {

}