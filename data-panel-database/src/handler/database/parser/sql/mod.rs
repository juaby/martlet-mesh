use std::collections::HashMap;

pub mod mysql;
pub mod postgresql;

pub mod rewrite;
pub mod analyse;
pub mod route;

pub enum SQLStatementContext {
    Select(SelectStatementContext),
    Update(UpdateStatementContext),
    Delete(DeleteStatementContext),
    Default
}

impl SQLStatementContext {
    pub fn add_table(&mut self, table: String, alias: String) {
        match self {
            SQLStatementContext::Select(s) => {
                s.common_ctx.add_table(table, alias);
            },
            SQLStatementContext::Update(_) => {},
            SQLStatementContext::Delete(_) => {},
            SQLStatementContext::Default => {},
        }
    }
}

pub struct CommonStatementContext {
    tables: HashMap<String, String>,
}

impl CommonStatementContext {
    pub fn new() -> Self {
        CommonStatementContext {
            tables: Default::default(),
        }
    }

    pub fn add_table(&mut self, table: String, alias: String) {
        self.tables.insert(table, alias);
    }
}

pub struct SelectStatementContext {
    common_ctx: CommonStatementContext,
}

impl SelectStatementContext {
    pub fn new() -> Self {
        SelectStatementContext {
            common_ctx: CommonStatementContext::new()
        }
    }

    pub fn add_table(&mut self, table: String, alias: String) {
        self.common_ctx.tables.insert(table, alias);
    }
}

pub struct UpdateStatementContext {
    common_ctx: CommonStatementContext,
}

impl UpdateStatementContext {
    pub fn new() -> Self {
        UpdateStatementContext {
            common_ctx: CommonStatementContext::new()
        }
    }

    pub fn add_table(&mut self, table: String, alias: String) {
        self.common_ctx.tables.insert(table, alias);
    }
}

pub struct DeleteStatementContext {
    common_ctx: CommonStatementContext,
}

impl DeleteStatementContext {
    pub fn new() -> Self {
        DeleteStatementContext {
            common_ctx: CommonStatementContext::new()
        }
    }

    pub fn add_table(&mut self, table: String, alias: String) {
        self.common_ctx.tables.insert(table, alias);
    }
}

pub struct SQLRewriteContext {

}