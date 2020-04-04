use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;

pub fn parser(sql: &str) -> Vec<Statement> {
    let dialect = MySqlDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap();

    ast
}