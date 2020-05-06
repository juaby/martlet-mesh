// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL Abstract Syntax Tree (AST) types

mod data_type;
mod ddl;
mod operator;
mod query;
mod value;

use sqlparser::ast::{SetVariableValue, ShowStatementFilter, TransactionIsolationLevel, TransactionAccessMode, TransactionMode, SqlOption, ObjectType, FileFormat, Function, Assignment, Statement, WindowFrameBound, WindowFrameUnits, WindowSpec, Expr, ObjectName};
use std::fmt;
use std::fmt::Write;
use std::collections::HashMap;

pub type SAResult = crate::common::Result<()>;

pub trait SQLAnalyse {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult;
}

struct DisplaySeparated<'a, T>
where
    T: SQLAnalyse,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> SQLAnalyse for DisplaySeparated<'a, T>
where
    T: SQLAnalyse,
{
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{}", delim)?;
            delim = self.sep;
            t.analyse(f, ctx)?;
        }
        Ok(())
    }
}

fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: SQLAnalyse,
{
    DisplaySeparated { slice, sep }
}

fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: SQLAnalyse,
{
    DisplaySeparated { slice, sep: ", " }
}

impl SQLAnalyse for ObjectName {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        display_separated(&self.0, ".").analyse(f, ctx)?;
        Ok(())
    }
}

impl SQLAnalyse for String {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        f.write_str(&self)?;
        Ok(())
    }
}

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
impl SQLAnalyse for Expr {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match self {
            Expr::Identifier(s) => {
                f.write_str(s)?;
            },
            Expr::Wildcard => {
                f.write_str("*")?;
            },
            Expr::QualifiedWildcard(q) => {
                display_separated(q, ".").analyse(f, ctx)?;
                write!(f, ".*")?;
            },
            Expr::CompoundIdentifier(s) => {
                display_separated(s, ".").analyse(f, ctx)?;
            },
            Expr::IsNull(ast) => {
                ast.analyse(f, ctx)?;
                write!(f, " IS NULL")?;
            },
            Expr::IsNotNull(ast) => {
                ast.analyse(f, ctx)?;
                write!(f, " IS NOT NULL")?;
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                expr.analyse(f, ctx)?;
                write!(
                    f,
                    " {}IN (",
                    if *negated { "NOT " } else { "" }
                )?;
                display_comma_separated(list).analyse(f, ctx)?;
                write!(
                    f,
                    ")"
                )?;
            },
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                expr.analyse(f, ctx)?;
                write!(
                    f,
                    " {}IN (",
                    if *negated { "NOT " } else { "" }
                )?;
                subquery.analyse(f, ctx)?;
                write!(
                    f,
                    ")"
                )?;
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                expr.analyse(f, ctx)?;
                write!(
                    f,
                    " {}BETWEEN ",
                    if *negated { "NOT " } else { "" }
                )?;
                low.analyse(f, ctx)?;
                write!(
                    f,
                    " AND "
                )?;
                high.analyse(f, ctx)?;
            },
            Expr::BinaryOp { left, op, right } => {
                left.analyse(f, ctx)?;
                write!(f, " ")?;
                op.analyse(f, ctx)?;
                write!(f, " ")?;
                right.analyse(f, ctx)?;
            },
            Expr::UnaryOp { op, expr } => {
                op.analyse(f, ctx)?;
                write!(f, " ")?;
                expr.analyse(f, ctx)?;
            },
            Expr::Cast { expr, data_type } => {
                write!(f, "CAST(")?;
                expr.analyse(f, ctx)?;
                write!(f, " AS ")?;
                data_type.analyse(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::Extract { field, expr } => {
                write!(f, "EXTRACT(")?;
                field.analyse(f, ctx)?;
                write!(f, " FROM ")?;
                expr.analyse(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::Collate { expr, collation } => {
                expr.analyse(f, ctx)?;
                write!(f, " COLLATE ")?;
                collation.analyse(f, ctx)?;
            },
            Expr::Nested(ast) => {
                write!(f, "(")?;
                ast.analyse(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::Value(v) => {
                v.analyse(f, ctx)?;
            },
            Expr::Function(fun) => {
                fun.analyse(f, ctx)?;
            },
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                f.write_str("CASE")?;
                if let Some(operand) = operand {
                    write!(f, " ")?;
                    operand.analyse(f, ctx)?;
                }
                for (c, r) in conditions.iter().zip(results) {
                    write!(f, " WHEN ")?;
                    c.analyse(f, ctx)?;
                    write!(f, " THEN ")?;
                    r.analyse(f, ctx)?;
                }

                if let Some(else_result) = else_result {
                    write!(f, " ELSE ")?;
                    else_result.analyse(f, ctx);
                }
                f.write_str(" END")?;
            }
            Expr::Exists(s) => {
                write!(f, "EXISTS (")?;
                s.analyse(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::Subquery(s) => {
                write!(f, "(")?;
                s.analyse(f, ctx)?;
                write!(f, ")")?;
            },
        };
        Ok(())
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
impl SQLAnalyse for WindowSpec {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            write!(
                f,
                "PARTITION BY "
            )?;
            display_comma_separated(&self.partition_by).analyse(f, ctx)?;
        }
        if !self.order_by.is_empty() {
            f.write_str(delim)?;
            delim = " ";
            write!(f, "ORDER BY ")?;
            display_comma_separated(&self.order_by).analyse(f, ctx)?;
        }
        if let Some(window_frame) = &self.window_frame {
            if let Some(end_bound) = &window_frame.end_bound {
                f.write_str(delim)?;
                window_frame.units.analyse(f, ctx)?;
                write!(
                    f,
                    " BETWEEN "
                )?;
                window_frame.start_bound.analyse(f, ctx)?;
                write!(
                    f,
                    " AND "
                )?;
                end_bound.analyse(f, ctx)?;
            } else {
                f.write_str(delim)?;
                window_frame.units.analyse(f, ctx)?;
                write!(f, " ")?;
                window_frame.start_bound.analyse(f, ctx)?;
            }
        }
        Ok(())
    }
}

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Note: The parser does not validate the specified bounds; the caller should
/// reject invalid bounds like `ROWS UNBOUNDED FOLLOWING` before execution.
impl SQLAnalyse for WindowFrameUnits {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })?;
        Ok(())
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
impl SQLAnalyse for WindowFrameBound {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => write!(f, "{} PRECEDING", n),
            WindowFrameBound::Following(Some(n)) => write!(f, "{} FOLLOWING", n),
        }?;
        Ok(())
    }
}

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
impl SQLAnalyse for Statement {
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match self {
            Statement::Query(s) => {
                s.analyse(f, ctx)?;
            },
            Statement::Insert {
                table_name,
                columns,
                source,
            } => {
                write!(f, "INSERT INTO ")?;
                table_name.analyse(f, ctx)?;
                write!(f, " ")?;
                if !columns.is_empty() {
                    write!(f, "(")?;
                    display_comma_separated(columns).analyse(f, ctx)?;
                    write!(f, ") ")?;
                }
                source.analyse(f, ctx)?;
            }
            Statement::Copy {
                table_name,
                columns,
                values,
            } => {
                write!(f, "COPY ")?;
                table_name.analyse(f, ctx)?;
                if !columns.is_empty() {
                    write!(f, " (")?;
                    display_comma_separated(columns).analyse(f, ctx)?;
                    write!(f, ")")?;
                }
                write!(f, " FROM stdin; ")?;
                if !values.is_empty() {
                    writeln!(f)?;
                    let mut delim = "";
                    for v in values {
                        write!(f, "{}", delim)?;
                        delim = "\t";
                        if let Some(v) = v {
                            write!(f, "{}", v)?;
                        } else {
                            write!(f, "\\N")?;
                        }
                    }
                }
                write!(f, "\n\\.")?;
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
            } => {
                write!(f, "UPDATE ")?;
                table_name.analyse(f, ctx)?;
                if !assignments.is_empty() {
                    write!(f, " SET ")?;
                    display_comma_separated(assignments).analyse(f, ctx)?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE ")?;
                    selection.analyse(f, ctx)?;
                }
            }
            Statement::Delete {
                table_name,
                selection,
            } => {
                write!(f, "DELETE FROM ")?;
                table_name.analyse(f, ctx)?;
                if let Some(selection) = selection {
                    write!(f, " WHERE ")?;
                    selection.analyse(f, ctx)?;
                }
            }
            Statement::CreateView {
                name,
                columns,
                query,
                materialized,
                with_options,
            } => {
                write!(f, "CREATE")?;
                if *materialized {
                    write!(f, " MATERIALIZED")?;
                }

                write!(f, " VIEW ")?;
                name.analyse(f, ctx)?;

                if !with_options.is_empty() {
                    write!(f, " WITH (")?;
                    display_comma_separated(with_options).analyse(f, ctx)?;
                    write!(f, ")")?;
                }

                if !columns.is_empty() {
                    write!(f, " (")?;
                    display_comma_separated(columns).analyse(f, ctx)?;
                    write!(f, ")")?;
                }

                write!(f, " AS ")?;
                query.analyse(f, ctx)?;
            }
            Statement::CreateTable {
                name,
                columns,
                constraints,
                with_options,
                external,
                file_format,
                location,
            } => {
                write!(
                    f,
                    "CREATE {}TABLE ",
                    if *external { "EXTERNAL " } else { "" }
                )?;
                name.analyse(f, ctx)?;
                write!(
                    f,
                    " ("
                )?;
                display_comma_separated(columns).analyse(f, ctx)?;
                if !constraints.is_empty() {
                    write!(f, ", ")?;
                    display_comma_separated(constraints).analyse(f, ctx)?;
                }
                write!(f, ")")?;

                if *external {
                    write!(
                        f,
                        " STORED AS "
                    )?;
                    file_format.as_ref().unwrap().analyse(f, ctx)?;
                    write!(
                        f,
                        " LOCATION '{}'",
                        location.as_ref().unwrap()
                    )?;
                }
                if !with_options.is_empty() {
                    write!(f, " WITH (")?;
                    display_comma_separated(with_options).analyse(f, ctx)?;
                    write!(f, ")")?;
                }
            }
            Statement::AlterTable { name, operation } => {
                write!(f, "ALTER TABLE ")?;
                name.analyse(f, ctx)?;
                write!(f, " ")?;
                operation.analyse(f, ctx)?;
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            } => {
                write!(
                    f,
                    "DROP "
                )?;
                object_type.analyse(f, ctx)?;
                write!(
                    f,
                    "{} ",
                    if *if_exists { " IF EXISTS" } else { "" }
                )?;
                display_comma_separated(names).analyse(f, ctx)?;
                write!(
                    f,
                    "{}",
                    if *cascade { " CASCADE" } else { "" }
                )?;
            },
            Statement::SetVariable {
                local,
                variable,
                value,
            } => {
                f.write_str("SET ")?;
                if *local {
                    f.write_str("LOCAL ")?;
                }
                write!(f, "{} = ", variable)?;
                value.analyse(f, ctx)?;
            }
            Statement::ShowVariable { variable } => {
                write!(f, "SHOW {}", variable)?;
            },
            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => {
                f.write_str("SHOW ")?;
                if *extended {
                    f.write_str("EXTENDED ")?;
                }
                if *full {
                    f.write_str("FULL ")?;
                }
                write!(f, "COLUMNS FROM ")?;
                table_name.analyse(f, ctx)?;
                if let Some(filter) = filter {
                    write!(f, " ")?;
                    filter.analyse(f, ctx)?;
                }
            }
            Statement::StartTransaction { modes } => {
                write!(f, "START TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " ")?;
                    display_comma_separated(modes).analyse(f, ctx)?;
                }
            }
            Statement::SetTransaction { modes } => {
                write!(f, "SET TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " ")?;
                    display_comma_separated(modes).analyse(f, ctx)?;
                }
            }
            Statement::Commit { chain } => {
                write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" },)?;
            }
            Statement::Rollback { chain } => {
                write!(f, "ROLLBACK{}", if *chain { " AND CHAIN" } else { "" },)?;
            }
        };
        Ok(())
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
impl SQLAnalyse for Assignment {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        write!(f, "{} = ", self.id)?;
        self.value.analyse(f, ctx)
    }
}

/// A function call
impl SQLAnalyse for Function {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        self.name.analyse(f, ctx)?;
        write!(
            f,
            "({}",
            if self.distinct { "DISTINCT " } else { "" }
        )?;
        display_comma_separated(&self.args).analyse(f, ctx)?;
        write!(
            f,
            ")"
        )?;
        if let Some(o) = &self.over {
            write!(f, " OVER (")?;
            o.analyse(f, ctx)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// External table's available file format
impl SQLAnalyse for FileFormat {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        use self::FileFormat::*;
        f.write_str(match self {
            TEXTFILE => "TEXTFILE",
            SEQUENCEFILE => "SEQUENCEFILE",
            ORC => "ORC",
            PARQUET => "PARQUET",
            AVRO => "AVRO",
            RCFILE => "RCFILE",
            JSONFILE => "TEXTFILE",
        })?;
        Ok(())
    }
}

impl SQLAnalyse for ObjectType {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
        })?;
        Ok(())
    }
}

impl SQLAnalyse for SqlOption {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        write!(f, "{} = ", self.name)?;
        self.value.analyse(f, ctx)?;
        Ok(())
    }
}

impl SQLAnalyse for TransactionMode {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => {
                access_mode.analyse(f, ctx)?
            },
            IsolationLevel(iso_level) => {
                write!(f, "ISOLATION LEVEL ")?;
                iso_level.analyse(f, ctx)?;
            },
        }
        Ok(())
    }
}

impl SQLAnalyse for TransactionAccessMode {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        use TransactionAccessMode::*;
        f.write_str(match self {
            ReadOnly => "READ ONLY",
            ReadWrite => "READ WRITE",
        })?;
        Ok(())
    }
}

impl SQLAnalyse for TransactionIsolationLevel {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
        })?;
        Ok(())
    }
}

impl SQLAnalyse for ShowStatementFilter {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => {
                write!(f, "LIKE '")?;
                value::escape_single_quote_string(pattern).analyse(f, ctx)?;
                write!(f, "'")?;
            },
            Where(expr) => {
                write!(f, "WHERE ")?;
                expr.analyse(f, ctx)?
            },
        }
        Ok(())
    }
}

impl SQLAnalyse for SetVariableValue {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        use SetVariableValue::*;
        match self {
            Ident(ident) => {
                f.write_str(ident)?;
            },
            Literal(literal) => {
                literal.analyse(f, ctx)?
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::mysql::parser;
    use crate::parser::sqlrewrite::SQLReWrite;
    use std::collections::HashMap;

    #[test]
    fn test_rewrite() {
        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";
        //let sql = "insert into test (a, b, c) values (1, 1, ?)";
        let mut ast = parser(sql);
        let stmt = ast.pop().unwrap();
        let mut resql = String::new();
        let mut ctx: HashMap<String, String> = HashMap::new();
        stmt.rewrite(&mut resql, &ctx).unwrap();
        assert_eq!(sql.to_uppercase(), resql.to_uppercase());
    }
}
