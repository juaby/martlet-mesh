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

use sqlparser::ast::{SetVariableValue, ShowStatementFilter, TransactionIsolationLevel, TransactionAccessMode, TransactionMode, SqlOption, ObjectType, FileFormat, Function, Assignment, Statement, WindowFrameBound, WindowFrameUnits, WindowSpec, Expr, ObjectName, Ident, ListAgg, ListAggOnOverflow};
use std::fmt::Write;
use std::collections::HashMap;

pub type SRWResult = crate::common::Result<()>;

pub trait SQLReWrite {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult;
}

struct DisplaySeparated<'a, T>
where
    T: SQLReWrite,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> SQLReWrite for DisplaySeparated<'a, T>
where
    T: SQLReWrite,
{
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{}", delim)?;
            delim = self.sep;
            t.rewrite(f, ctx)?;
        }
        Ok(())
    }
}

fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: SQLReWrite,
{
    DisplaySeparated { slice, sep }
}

fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: SQLReWrite,
{
    DisplaySeparated { slice, sep: ", " }
}

impl SQLReWrite for ObjectName {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        display_separated(&self.0, ".").rewrite(f, ctx)?;
        Ok(())
    }
}

impl SQLReWrite for String {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        f.write_str(&self)?;
        Ok(())
    }
}

impl SQLReWrite for Ident {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self.quote_style {
            Some(q) if q == '"' || q == '\'' || q == '`' => {
                write!(f, "{}{}{}", q, self.value, q);
            },
            Some(q) if q == '[' => {
                write!(f, "[{}]", self.value);
            },
            None => {
                f.write_str(&self.value);
            },
            _ => panic!("unexpected quote style"),
        }
        Ok(())
    }
}

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
impl SQLReWrite for Expr {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self {
            Expr::Identifier(s) => {
                s.rewrite(f, ctx)?;
            },
            Expr::Wildcard => {
                f.write_str("*")?;
            },
            Expr::QualifiedWildcard(q) => {
                display_separated(q, ".").rewrite(f, ctx)?;
                write!(f, ".*")?;
            },
            Expr::CompoundIdentifier(s) => {
                display_separated(s, ".").rewrite(f, ctx)?;
            },
            Expr::IsNull(ast) => {
                ast.rewrite(f, ctx)?;
                write!(f, " IS NULL")?;
            },
            Expr::IsNotNull(ast) => {
                ast.rewrite(f, ctx)?;
                write!(f, " IS NOT NULL")?;
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                expr.rewrite(f, ctx)?;
                write!(
                    f,
                    " {}IN (",
                    if *negated { "NOT " } else { "" }
                )?;
                display_comma_separated(list).rewrite(f, ctx)?;
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
                expr.rewrite(f, ctx)?;
                write!(
                    f,
                    " {}IN (",
                    if *negated { "NOT " } else { "" }
                )?;
                subquery.rewrite(f, ctx)?;
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
                expr.rewrite(f, ctx)?;
                write!(
                    f,
                    " {}BETWEEN ",
                    if *negated { "NOT " } else { "" }
                )?;
                low.rewrite(f, ctx)?;
                write!(
                    f,
                    " AND "
                )?;
                high.rewrite(f, ctx)?;
            },
            Expr::BinaryOp { left, op, right } => {
                left.rewrite(f, ctx)?;
                write!(f, " ")?;
                op.rewrite(f, ctx)?;
                write!(f, " ")?;
                right.rewrite(f, ctx)?;
            },
            Expr::UnaryOp { op, expr } => {
                op.rewrite(f, ctx)?;
                write!(f, " ")?;
                expr.rewrite(f, ctx)?;
            },
            Expr::Cast { expr, data_type } => {
                write!(f, "CAST(")?;
                expr.rewrite(f, ctx)?;
                write!(f, " AS ")?;
                data_type.rewrite(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::Extract { field, expr } => {
                write!(f, "EXTRACT(")?;
                field.rewrite(f, ctx)?;
                write!(f, " FROM ")?;
                expr.rewrite(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::Collate { expr, collation } => {
                expr.rewrite(f, ctx)?;
                write!(f, " COLLATE ")?;
                collation.rewrite(f, ctx)?;
            },
            Expr::Nested(ast) => {
                write!(f, "(")?;
                ast.rewrite(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::Value(v) => {
                v.rewrite(f, ctx)?;
            },
            Expr::TypedString { data_type, value } => {
                data_type.rewrite(f, ctx)?;
                write!(f, " '")?;
                &value::escape_single_quote_string(value).rewrite(f, ctx)?;
                write!(f, "'")?;
            }
            Expr::Function(fun) => {
                fun.rewrite(f, ctx)?;
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
                    operand.rewrite(f, ctx)?;
                }
                for (c, r) in conditions.iter().zip(results) {
                    write!(f, " WHEN ")?;
                    c.rewrite(f, ctx)?;
                    write!(f, " THEN ")?;
                    r.rewrite(f, ctx)?;
                }

                if let Some(else_result) = else_result {
                    write!(f, " ELSE ")?;
                    else_result.rewrite(f, ctx)?;
                }
                f.write_str(" END")?;
            }
            Expr::Exists(s) => {
                write!(f, "EXISTS (")?;
                s.rewrite(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::Subquery(s) => {
                write!(f, "(")?;
                s.rewrite(f, ctx)?;
                write!(f, ")")?;
            },
            Expr::ListAgg(listagg) => {
                listagg.rewrite(f, ctx)?;
            }
        };
        Ok(())
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
impl SQLReWrite for WindowSpec {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            write!(
                f,
                "PARTITION BY "
            )?;
            display_comma_separated(&self.partition_by).rewrite(f, ctx)?;
        }
        if !self.order_by.is_empty() {
            f.write_str(delim)?;
            delim = " ";
            write!(f, "ORDER BY ")?;
            display_comma_separated(&self.order_by).rewrite(f, ctx)?;
        }
        if let Some(window_frame) = &self.window_frame {
            if let Some(end_bound) = &window_frame.end_bound {
                f.write_str(delim)?;
                window_frame.units.rewrite(f, ctx)?;
                write!(
                    f,
                    " BETWEEN "
                )?;
                window_frame.start_bound.rewrite(f, ctx)?;
                write!(
                    f,
                    " AND "
                )?;
                end_bound.rewrite(f, ctx)?;
            } else {
                f.write_str(delim)?;
                window_frame.units.rewrite(f, ctx)?;
                write!(f, " ")?;
                window_frame.start_bound.rewrite(f, ctx)?;
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
impl SQLReWrite for WindowFrameUnits {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })?;
        Ok(())
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
impl SQLReWrite for WindowFrameBound {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
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
impl SQLReWrite for Statement {
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self {
            Statement::Query(s) => {
                s.rewrite(f, ctx)?;
            },
            Statement::Insert {
                table_name,
                columns,
                source,
            } => {
                write!(f, "INSERT INTO ")?;
                table_name.rewrite(f, ctx)?;
                write!(f, " ")?;
                if !columns.is_empty() {
                    write!(f, "(")?;
                    display_comma_separated(columns).rewrite(f, ctx)?;
                    write!(f, ") ")?;
                }
                source.rewrite(f, ctx)?;
            }
            Statement::Copy {
                table_name,
                columns,
                values,
            } => {
                write!(f, "COPY ")?;
                table_name.rewrite(f, ctx)?;
                if !columns.is_empty() {
                    write!(f, " (")?;
                    display_comma_separated(columns).rewrite(f, ctx)?;
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
                table_name.rewrite(f, ctx)?;
                if !assignments.is_empty() {
                    write!(f, " SET ")?;
                    display_comma_separated(assignments).rewrite(f, ctx)?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE ")?;
                    selection.rewrite(f, ctx)?;
                }
            }
            Statement::Delete {
                table_name,
                selection,
            } => {
                write!(f, "DELETE FROM ")?;
                table_name.rewrite(f, ctx)?;
                if let Some(selection) = selection {
                    write!(f, " WHERE ")?;
                    selection.rewrite(f, ctx)?;
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
                name.rewrite(f, ctx)?;

                if !with_options.is_empty() {
                    write!(f, " WITH (")?;
                    display_comma_separated(with_options).rewrite(f, ctx)?;
                    write!(f, ")")?;
                }

                if !columns.is_empty() {
                    write!(f, " (")?;
                    display_comma_separated(columns).rewrite(f, ctx)?;
                    write!(f, ")")?;
                }

                write!(f, " AS ")?;
                query.rewrite(f, ctx)?;
            }
            Statement::CreateTable {
                name,
                columns,
                constraints,
                with_options,
                if_not_exists,
                external,
                file_format,
                location,
                query,
                without_rowid,
            } => {
                // We want to allow the following options
                // Empty column list, allowed by PostgreSQL:
                //   `CREATE TABLE t ()`
                // No columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t AS SELECT a from t2`
                // Columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t (a INT) AS SELECT a from t2`
                write!(
                    f,
                    "CREATE {external}TABLE {if_not_exists}",
                    external = if *external { "EXTERNAL " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" }
                )?;
                name.rewrite(f, ctx)?;
                if !columns.is_empty() || !constraints.is_empty() {
                    write!(f, " (")?;
                    display_comma_separated(columns).rewrite(f, ctx)?;
                    if !columns.is_empty() && !constraints.is_empty() {
                        write!(f, ", ")?;
                    }
                    display_comma_separated(constraints).rewrite(f, ctx)?;
                    write!(f, ")")?;
                } else if query.is_none() {
                    // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
                    write!(f, " ()")?;
                }
                // Only for SQLite
                if *without_rowid {
                    write!(f, " WITHOUT ROWID")?;
                }

                if *external {
                    write!(f, " STORED AS ")?;
                    file_format.as_ref().unwrap().rewrite(f, ctx)?;
                    write!(
                        f,
                        " LOCATION '{}'",
                        location.as_ref().unwrap()
                    )?;
                }
                if !with_options.is_empty() {
                    write!(f, " WITH (")?;
                    display_comma_separated(with_options).rewrite(f, ctx)?;
                    write!(f, ")")?;
                }
                if let Some(query) = query {
                    write!(f, " AS ")?;
                    query.rewrite(f, ctx)?;
                }
            }
            Statement::CreateVirtualTable {
                name,
                if_not_exists,
                module_name,
                module_args,
            } => {
                write!(
                    f,
                    "CREATE VIRTUAL TABLE {if_not_exists}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" }
                )?;
                name.rewrite(f, ctx)?;
                write!(f, " USING ")?;
                module_name.rewrite(f, ctx)?;
                if !module_args.is_empty() {
                    write!(f, " (")?;
                    display_comma_separated(module_args).rewrite(f, ctx)?;
                    write!(f, ")")?;
                }
            }
            Statement::CreateIndex {
                name,
                table_name,
                columns,
                unique,
                if_not_exists,
            } => {
                write!(
                    f,
                    "CREATE{}INDEX{}",
                    if *unique { " UNIQUE " } else { " " },
                    if *if_not_exists {
                        " IF NOT EXISTS "
                    } else {
                        " "
                    }
                )?;
                name.rewrite(f, ctx)?;
                write!(f, " ON ")?;
                table_name.rewrite(f, ctx)?;
                write!(f, "(")?;
                display_separated(columns, ",").rewrite(f, ctx)?;
                write!(f, ");")?;
            }
            Statement::AlterTable { name, operation } => {
                write!(f, "ALTER TABLE ")?;
                name.rewrite(f, ctx)?;
                write!(f, " ")?;
                operation.rewrite(f, ctx)?;
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
                object_type.rewrite(f, ctx)?;
                write!(
                    f,
                    "{} ",
                    if *if_exists { " IF EXISTS" } else { "" }
                )?;
                display_comma_separated(names).rewrite(f, ctx)?;
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
                value.rewrite(f, ctx)?;
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
                table_name.rewrite(f, ctx)?;
                if let Some(filter) = filter {
                    write!(f, " ")?;
                    filter.rewrite(f, ctx)?;
                }
            }
            Statement::StartTransaction { modes } => {
                write!(f, "START TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " ")?;
                    display_comma_separated(modes).rewrite(f, ctx)?;
                }
            }
            Statement::SetTransaction { modes } => {
                write!(f, "SET TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " ")?;
                    display_comma_separated(modes).rewrite(f, ctx)?;
                }
            }
            Statement::Commit { chain } => {
                write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" },)?;
            }
            Statement::Rollback { chain } => {
                write!(f, "ROLLBACK{}", if *chain { " AND CHAIN" } else { "" },)?;
            }
            Statement::CreateSchema { schema_name } => {
                write!(f, "CREATE SCHEMA ")?;
                schema_name.rewrite(f, ctx)?;
            },
            Statement::Assert { condition, message } => {
                write!(f, "ASSERT ")?;
                condition.rewrite(f, ctx)?;

                if let Some(m) = message {
                    write!(f, " AS ")?;
                    m.rewrite(f, ctx)?;
                }
            }
        };
        Ok(())
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
impl SQLReWrite for Assignment {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        write!(f, "{} = ", self.id)?;
        self.value.rewrite(f, ctx)
    }
}

/// A function call
impl SQLReWrite for Function {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        self.name.rewrite(f, ctx)?;
        write!(
            f,
            "({}",
            if self.distinct { "DISTINCT " } else { "" }
        )?;
        display_comma_separated(&self.args).rewrite(f, ctx)?;
        write!(
            f,
            ")"
        )?;
        if let Some(o) = &self.over {
            write!(f, " OVER (")?;
            o.rewrite(f, ctx)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// External table's available file format
impl SQLReWrite for FileFormat {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
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

impl SQLReWrite for ListAgg {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        write!(
            f,
            "LISTAGG({}",
            if self.distinct { "DISTINCT " } else { "" },
        )?;
        self.expr.rewrite(f, ctx)?;
        if let Some(separator) = &self.separator {
            write!(f, ", ")?;
            separator.rewrite(f, ctx)?;
        }
        if let Some(on_overflow) = &self.on_overflow {
            on_overflow.rewrite(f, ctx)?;
        }
        write!(f, ")")?;
        if !self.within_group.is_empty() {
            write!(
                f,
                " WITHIN GROUP (ORDER BY "
            )?;
            display_comma_separated(&self.within_group).rewrite(f, ctx)?;
            write!(
                f,
                ")"
            )?;
        }
        Ok(())
    }
}

impl SQLReWrite for ListAggOnOverflow {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        write!(f, " ON OVERFLOW")?;
        match self {
            ListAggOnOverflow::Error => {
                write!(f, " ERROR")?;
            },
            ListAggOnOverflow::Truncate { filler, with_count } => {
                write!(f, " TRUNCATE")?;
                if let Some(filler) = filler {
                    write!(f, " ")?;
                    filler.rewrite(f, ctx)?;
                }
                if *with_count {
                    write!(f, " WITH")?;
                } else {
                    write!(f, " WITHOUT")?;
                }
                write!(f, " COUNT")?;
            }
        }
        Ok(())
    }
}

impl SQLReWrite for ObjectType {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::Index => "INDEX",
            ObjectType::Schema => "SCHEMA",
        })?;
        Ok(())
    }
}

impl SQLReWrite for SqlOption {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        write!(f, "{} = ", self.name)?;
        self.value.rewrite(f, ctx)?;
        Ok(())
    }
}

impl SQLReWrite for TransactionMode {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => {
                access_mode.rewrite(f, ctx)?
            },
            IsolationLevel(iso_level) => {
                write!(f, "ISOLATION LEVEL ")?;
                iso_level.rewrite(f, ctx)?;
            },
        }
        Ok(())
    }
}

impl SQLReWrite for TransactionAccessMode {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        use TransactionAccessMode::*;
        f.write_str(match self {
            ReadOnly => "READ ONLY",
            ReadWrite => "READ WRITE",
        })?;
        Ok(())
    }
}

impl SQLReWrite for TransactionIsolationLevel {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
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

impl SQLReWrite for ShowStatementFilter {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => {
                write!(f, "LIKE '")?;
                value::escape_single_quote_string(pattern).rewrite(f, ctx)?;
                write!(f, "'")?;
            },
            Where(expr) => {
                write!(f, "WHERE ")?;
                expr.rewrite(f, ctx)?
            },
        }
        Ok(())
    }
}

impl SQLReWrite for SetVariableValue {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        use SetVariableValue::*;
        match self {
            Ident(ident) => {
                ident.rewrite(f, ctx)?
            },
            Literal(literal) => {
                literal.rewrite(f, ctx)?
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::sql::mysql::parser;
    use crate::parser::sql::rewrite::SQLReWrite;
    use std::collections::HashMap;

    #[test]
    fn test_rewrite() {
        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";
        //let sql = "insert into test (a, b, c) values (1, 1, ?)";
        let mut ast = parser(sql.to_string());
        let stmt = ast.pop().unwrap();
        let mut resql = String::new();
        let mut ctx: HashMap<String, String> = HashMap::new();
        stmt.rewrite(&mut resql, &ctx).unwrap();
        assert_eq!(sql.to_uppercase(), resql.to_uppercase());
    }
}
