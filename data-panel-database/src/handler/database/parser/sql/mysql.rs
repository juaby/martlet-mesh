use sqlparser::ast::{Ident, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

#[derive(Debug)]
pub struct MySQLDialect {}

impl Dialect for MySQLDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://dev.mysql.com/doc/refman/8.0/en/identifiers.html.
        // We don't yet support identifiers beginning with numbers, as that
        // makes it hard to distinguish numeric literals.
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ch == '_'
            || ch == '$'
            || ch == '@'
            || ch == '?'
            || ('\u{0080}'..='\u{ffff}').contains(&ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ('0'..='9').contains(&ch)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }
}

pub fn parser(sql: String) -> Vec<Statement> {
    let dialect = MySQLDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = if sql.to_uppercase().starts_with("XSET NAMES") {
        vec![Statement::SetVariable {
            local: false,
            hivevar: false,
            variable: Ident { value: "".to_string(), quote_style: None },
            value: vec![],
        }]
    } else {
        Parser::parse_sql(&dialect, &sql).unwrap()
    };

    ast
}