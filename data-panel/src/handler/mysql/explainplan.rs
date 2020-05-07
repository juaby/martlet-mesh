use sqlparser::ast::Statement;
use bytes::Bytes;
use crate::handler::mysql::rdbc::{text_query, bin_query};

pub enum TBProtocol {
    Text,
    Binary
}

pub struct ExplainPlanContext<'a> {
    sql: &'a str,
    statement: &'a Statement,
    protocol: TBProtocol
}

impl<'a> ExplainPlanContext<'a> {
    pub fn new(sql: &'a str,
               statement: &'a Statement,
               protocol: TBProtocol) -> Self {
        ExplainPlanContext {
            sql,
            statement,
            protocol
        }
    }

    pub fn get_sql(&self) -> &'a str {
        self.sql
    }

    pub fn get_statement(&self) -> &'a Statement {
        self.statement
    }
}

pub trait Executor {
    fn execute(&self) -> Option<Vec<Bytes>>;
}

pub struct PlanTask {

}

pub struct ExplainPlan<'a> {
    ctx: &'a ExplainPlanContext<'a>,
    tasks: Vec<PlanTask>,
}

impl<'a> ExplainPlan<'a> {
    pub fn new(ctx: &'a ExplainPlanContext<'a>) -> Self {
        ExplainPlan {
            ctx: ctx,
            tasks: vec![]
        }
    }

    pub fn gen(&self) {

    }

    pub fn ctx(&self) -> &'a ExplainPlanContext<'a> {
        self.ctx
    }
}

impl<'a> Executor for ExplainPlan<'a> {
    fn execute(&self) -> Option<Vec<Bytes>> {
        match self.ctx.protocol {
            TBProtocol::Text => {text_query(&self)},
            TBProtocol::Binary => {bin_query(&self)},
        }
    }
}