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

use sqlparser::ast::{AddDropSync, Assignment, Expr, FileFormat, Function, FunctionArg, HiveDistributionStyle, HiveFormat, HiveIOFormat, HiveRowFormat, Ident, ListAgg, ListAggOnOverflow, ObjectName, ObjectType, SetVariableValue, ShowStatementFilter, SqliteOnConflict, SqlOption, Statement, TransactionAccessMode, TransactionIsolationLevel, TransactionMode, UnaryOperator, WindowFrameBound, WindowFrameUnits, WindowSpec};
use sqlparser::tokenizer::{Token, Whitespace, Word};

// use std::fmt::Write;
use crate::handler::database::parser::sql::SQLStatementContext;

mod data_type;
mod ddl;
mod operator;
mod query;
mod value;

pub type SAResult = data_panel_common::common::Result<()>;

pub trait SQLAnalyse {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult;
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
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        let mut delim = "";
        for t in self.slice {
            // write!(f, "{}", delim)?;
            delim = self.sep;
            t.analyse(ctx)?;
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

impl SQLAnalyse for Ident {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // match self.quote_style {
        //     Some(q) if q == '"' || q == '\'' || q == '`' => write!(f, "{}{}{}", q, self.value, q)?,
        //     Some(q) if q == '[' => write!(f, "[{}]", self.value)?,
        //     None => f.write_str(&self.value)?,
        //     _ => panic!("unexpected quote style"),
        // }
        Ok(())
    }
}

impl SQLAnalyse for ObjectName {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        display_separated(&self.0, ".").analyse(ctx)?;
        Ok(())
    }
}

impl SQLAnalyse for String {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // f.write_str(&self)?;
        Ok(())
    }
}

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
impl SQLAnalyse for Expr {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match self {
            Expr::Identifier(s) => {
                s.analyse(ctx)?;
            }
            Expr::MapAccess { column, key } => {
                column.analyse(ctx)?;
                // write!(f, "[\"{}\"]", key)?;
            }
            Expr::Wildcard => {
                // f.write_str("*")?;
            }
            Expr::QualifiedWildcard(q) => {
                display_separated(q, ".").analyse(ctx)?;
                // write!(f, ".*")?;
            }
            Expr::CompoundIdentifier(s) => {
                display_separated(s, ".").analyse(ctx)?;
            }
            Expr::IsNull(ast) => {
                ast.analyse(ctx)?;
                // write!(f, " IS NULL")?;
            }
            Expr::IsNotNull(ast) => {
                ast.analyse(ctx)?;
                // write!(f, " IS NOT NULL")?;
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                expr.analyse(ctx)?;
                // write!(
                //     f,
                //     " {}IN (",
                //     if *negated { "NOT " } else { "" }
                // )?;
                display_comma_separated(list).analyse(ctx)?;
                // write!(
                //     f,
                //     ")"
                // )?;
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                expr.analyse(ctx)?;
                // write!(
                //     f,
                //     " {}IN (",
                //     if *negated { "NOT " } else { "" }
                // )?;
                subquery.analyse(ctx)?;
                // write!(
                //     f,
                //     ")"
                // )?;
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                expr.analyse(ctx)?;
                // write!(
                //     f,
                //     " {}BETWEEN ",
                //     if *negated { "NOT " } else { "" }
                // )?;
                low.analyse(ctx)?;
                // write!(
                //     f,
                //     " AND "
                // )?;
                high.analyse(ctx)?;
            }
            Expr::BinaryOp { left, op, right } => {
                left.analyse(ctx)?;
                // write!(f, " ")?;
                op.analyse(ctx)?;
                // write!(f, " ")?;
                right.analyse(ctx)?;
            }
            Expr::UnaryOp { op, expr } => {
                if op == &UnaryOperator::PGPostfixFactorial {
                    expr.analyse(ctx)?;
                    op.analyse(ctx)?;
                } else {
                    op.analyse(ctx)?;
                    // write!(f, " ")?;
                    expr.analyse(ctx)?;
                }
            }
            Expr::Cast { expr, data_type } => {
                // write!(f, "CAST(")?;
                expr.analyse(ctx)?;
                // write!(f, " AS ")?;
                data_type.analyse(ctx)?;
                // write!(f, ")")?;
            }
            Expr::Extract { field, expr } => {
                // write!(f, "EXTRACT(")?;
                field.analyse(ctx)?;
                // write!(f, " FROM ")?;
                expr.analyse(ctx)?;
                // write!(f, ")")?;
            }
            Expr::Collate { expr, collation } => {
                expr.analyse(ctx)?;
                // write!(f, " COLLATE ")?;
                collation.analyse(ctx)?;
            }
            Expr::Nested(ast) => {
                // write!(f, "(")?;
                ast.analyse(ctx)?;
                // write!(f, ")")?;
            }
            Expr::Value(v) => {
                v.analyse(ctx)?;
            }
            Expr::TypedString { data_type, value } => {
                data_type.analyse(ctx)?;
                // write!(f, " '")?;
                &value::escape_single_quote_string(value).analyse(ctx)?;
                // write!(f, "'")?;
            }
            Expr::Function(fun) => {
                fun.analyse(ctx)?;
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                // f.write_str("CASE")?;
                if let Some(operand) = operand {
                    // write!(f, " ")?;
                    operand.analyse(ctx)?;
                }
                for (c, r) in conditions.iter().zip(results) {
                    // write!(f, " WHEN ")?;
                    c.analyse(ctx)?;
                    // write!(f, " THEN ")?;
                    r.analyse(ctx)?;
                }

                if let Some(else_result) = else_result {
                    // write!(f, " ELSE ")?;
                    else_result.analyse(ctx)?;
                }
                // f.write_str(" END")?;
            }
            Expr::Exists(s) => {
                // write!(f, "EXISTS (")?;
                s.analyse(ctx)?;
                // write!(f, ")")?;
            }
            Expr::Subquery(s) => {
                // write!(f, "(")?;
                s.analyse(ctx)?;
                // write!(f, ")")?;
            }
            Expr::ListAgg(listagg) => {
                listagg.analyse(ctx)?;
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                // write!(f, "SUBSTRING(")?;
                expr.analyse(ctx)?;
                if let Some(from_part) = substring_from {
                    // write!(f, " FROM ")?;
                    from_part.analyse(ctx)?;
                }
                if let Some(from_part) = substring_for {
                    // write!(f, " FOR ")?;
                    from_part.analyse(ctx)?;
                }

                // write!(f, ")")?;
            }
        };
        Ok(())
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
impl SQLAnalyse for WindowSpec {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            // write!(
            //     f,
            //     "PARTITION BY "
            // )?;
            display_comma_separated(&self.partition_by).analyse(ctx)?;
        }
        if !self.order_by.is_empty() {
            // f.write_str(delim)?;
            delim = " ";
            // write!(f, "ORDER BY ")?;
            display_comma_separated(&self.order_by).analyse(ctx)?;
        }
        if let Some(window_frame) = &self.window_frame {
            if let Some(end_bound) = &window_frame.end_bound {
                // f.write_str(delim)?;
                window_frame.units.analyse(ctx)?;
                // write!(
                //     f,
                //     " BETWEEN "
                // )?;
                window_frame.start_bound.analyse(ctx)?;
                // write!(
                //     f,
                //     " AND "
                // )?;
                end_bound.analyse(ctx)?;
            } else {
                // f.write_str(delim)?;
                window_frame.units.analyse(ctx)?;
                // write!(f, " ")?;
                window_frame.start_bound.analyse(ctx)?;
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
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // f.write_str(match self {
        //     WindowFrameUnits::Rows => "ROWS",
        //     WindowFrameUnits::Range => "RANGE",
        //     WindowFrameUnits::Groups => "GROUPS",
        // })?;
        Ok(())
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
impl SQLAnalyse for WindowFrameBound {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // match self {
        //     WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
        //     WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
        //     WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
        //     WindowFrameBound::Preceding(Some(n)) => write!(f, "{} PRECEDING", n),
        //     WindowFrameBound::Following(Some(n)) => write!(f, "{} FOLLOWING", n),
        // }?;
        Ok(())
    }
}

impl SQLAnalyse for AddDropSync {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // match self {
        //     AddDropSync::SYNC => f.write_str("SYNC PARTITIONS"),
        //     AddDropSync::DROP => f.write_str("DROP PARTITIONS"),
        //     AddDropSync::ADD => f.write_str("ADD PARTITIONS"),
        // }?;
        Ok(())
    }
}

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
impl SQLAnalyse for Statement {
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match self {
            Statement::Explain {
                verbose,
                analyze,
                statement,
            } => {
                // write!(f, "EXPLAIN ")?;

                if *analyze {
                    // write!(f, "ANALYZE ")?;
                }

                if *verbose {
                    // write!(f, "VERBOSE ")?;
                }

                statement.analyse(ctx)?;
            }
            Statement::Query(s) => {
                s.analyse(ctx)?;
            }
            Statement::Directory {
                overwrite,
                local,
                path,
                file_format,
                source,
            } => {
                // write!(
                //     f,
                //     "INSERT{overwrite}{local} DIRECTORY '{path}'",
                //     overwrite = if *overwrite { " OVERWRITE" } else { "" },
                //     local = if *local { " LOCAL" } else { "" },
                //     path = path
                // )?;
                if let Some(ref ff) = file_format {
                    // write!(f, " STORED AS ")?;
                    ff.analyse(ctx)?;
                }
                source.analyse(ctx)?;
            }
            Statement::Msck {
                table_name,
                repair,
                partition_action,
            } => {
                // write!(
                //     f,
                //     "MSCK {repair}TABLE ",
                //     repair = if *repair { "REPAIR " } else { "" },
                // )?;
                table_name.analyse(ctx)?;
                if let Some(pa) = partition_action {
                    // write!(f, " ")?;
                    pa.analyse(ctx)?;
                }
            }
            Statement::Truncate {
                table_name,
                partitions,
            } => {
                // write!(f, "TRUNCATE TABLE ")?;
                table_name.analyse(ctx)?;
                if let Some(ref parts) = partitions {
                    if !parts.is_empty() {
                        // write!(f, " PARTITION (")?;
                        display_comma_separated(parts).analyse(ctx)?;
                        // write!(f, ")")?;
                    }
                }
            }
            Statement::Analyze { table_name, partitions, for_columns, columns, cache_metadata, noscan, compute_statistics } => {
                // write!(f, "ANALYZE TABLE ")?;
                table_name.analyse(ctx)?;

                if let Some(ref parts) = partitions {
                    if !parts.is_empty() {
                        // write!(f, " PARTITION (")?;
                        display_comma_separated(parts).analyse(ctx)?;
                        // write!(f, ")")?;
                    }
                }

                if *compute_statistics {
                    // write!(f, " COMPUTE STATISTICS")?;
                }
                if *noscan {
                    // write!(f, " NOSCAN")?;
                }
                if *cache_metadata {
                    // write!(f, " CACHE METADATA")?;
                }
                if *for_columns {
                    // write!(f, " FOR COLUMNS")?;
                    if !columns.is_empty() {
                        // write!(f, " ")?;
                        display_comma_separated(columns).analyse(ctx)?;
                    }
                }
            }
            Statement::Insert {
                or, table_name,
                columns,
                overwrite, source, partitioned, after_columns, table,
            } => {
                if let Some(action) = or {
                    // write!(f, "INSERT OR ")?;
                    action.analyse(ctx)?;
                    // write!(f, " INTO ")?;
                    table_name.analyse(ctx)?;
                    // write!(f, " ")?;
                } else {
                    // write!(
                    //     f,
                    //     "INSERT {act}{tbl} ",
                    //     act = if *overwrite { "OVERWRITE" } else { "INTO" },
                    //     tbl = if *table { " TABLE" } else { "" }
                    // )?;
                    table_name.analyse(ctx)?;
                    // write!(f, " ")?;
                }
                if !columns.is_empty() {
                    // write!(f, "(")?;
                    display_comma_separated(columns).analyse(ctx)?;
                    // write!(f, ") ")?;
                }
                if let Some(ref parts) = partitioned {
                    if !parts.is_empty() {
                        // write!(f, "PARTITION (")?;
                        display_comma_separated(parts).analyse(ctx)?;
                        // write!(f, ") ")?;
                    }
                }
                if !after_columns.is_empty() {
                    // write!(f, "(")?;
                    display_comma_separated(after_columns).analyse(ctx)?;
                    // write!(f, ") ")?;
                }
                source.analyse(ctx)?;
            }
            Statement::Copy {
                table_name,
                columns,
                values,
            } => {
                // write!(f, "COPY ")?;
                table_name.analyse(ctx)?;
                if !columns.is_empty() {
                    // write!(f, " (")?;
                    display_comma_separated(columns).analyse(ctx)?;
                    // write!(f, ")")?;
                }
                // write!(f, " FROM stdin; ")?;
                if !values.is_empty() {
                    // writeln!(f)?;
                    let mut delim = "";
                    for v in values {
                        // write!(f, "{}", delim)?;
                        delim = "\t";
                        if let Some(v) = v {
                            // write!(f, "{}", v)?;
                        } else {
                            // write!(f, "\\N")?;
                        }
                    }
                }
                // write!(f, "\n\\.")?;
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
                limit,
            } => {
                // write!(f, "UPDATE ")?;
                table_name.analyse(ctx)?;
                if !assignments.is_empty() {
                    // write!(f, " SET ")?;
                    display_comma_separated(assignments).analyse(ctx)?;
                }
                if let Some(selection) = selection {
                    // write!(f, " WHERE ")?;
                    selection.analyse(ctx)?;
                }
                if let Some(limit) = limit {
                    // write!(f, " LIMIT ")?;
                    limit.analyse(ctx)?;
                }
            }
            Statement::Delete {
                table_name,
                selection,
            } => {
                // write!(f, "DELETE FROM ")?;
                table_name.analyse(ctx)?;
                if let Some(selection) = selection {
                    // write!(f, " WHERE ")?;
                    selection.analyse(ctx)?;
                }
            }
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                location,
                managed_location,
            } => {
                // write!(f, "CREATE")?;
                if *if_not_exists {
                    // write!(f, " IF NOT EXISTS")?;
                }
                // write!(f, " ")?;
                db_name.analyse(ctx)?;
                if let Some(l) = location {
                    // write!(f, " LOCATION '{}'", l)?;
                }
                if let Some(ml) = managed_location {
                    // write!(f, " MANAGEDLOCATION '{}'", ml)?;
                }
            }
            Statement::CreateView {
                or_replace,
                name,
                columns,
                query,
                materialized,
                with_options,
            } => {
                // write!(
                //     f,
                //     "CREATE {or_replace}{materialized}VIEW ",
                //     or_replace = if *or_replace { "OR REPLACE " } else { "" },
                //     materialized = if *materialized { "MATERIALIZED " } else { "" }
                // )?;
                name.analyse(ctx)?;
                if !with_options.is_empty() {
                    // write!(f, " WITH (")?;
                    display_comma_separated(with_options).analyse(ctx)?;
                    // write!(f, ")")?;
                }
                if !columns.is_empty() {
                    // write!(f, " (")?;
                    display_comma_separated(columns).analyse(ctx)?;
                    // write!(f, ")")?;
                }
                // write!(f, " AS ")?;
                query.analyse(ctx)?;
            }
            Statement::CreateTable {
                temporary, name,
                columns,
                constraints,
                hive_distribution, hive_formats, table_properties, with_options,
                or_replace,
                if_not_exists,
                external,
                file_format,
                location,
                query,
                without_rowid, like,
            } => {
                // We want to allow the following options
                // Empty column list, allowed by PostgreSQL:
                //   `CREATE TABLE t ()`
                // No columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t AS SELECT a from t2`
                // Columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t (a INT) AS SELECT a from t2`
                // write!(
                //     f,
                //     "CREATE {or_replace}{external}TABLE {if_not_exists}",
                //     or_replace = if *or_replace { "OR REPLACE " } else { "" },
                //     external = if *external { "EXTERNAL " } else { "" },
                //     if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                // )?;
                name.analyse(ctx)?;
                if !columns.is_empty() || !constraints.is_empty() {
                    // write!(f, " (")?;
                    display_comma_separated(columns).analyse(ctx)?;
                    if !columns.is_empty() && !constraints.is_empty() {
                        // write!(f, ", ")?;
                    }
                    display_comma_separated(constraints).analyse(ctx)?;
                    // write!(f, ")")?;
                } else if query.is_none() {
                    // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
                    // write!(f, " ()")?;
                }
                // Only for SQLite
                if *without_rowid {
                    // write!(f, " WITHOUT ROWID")?;
                }

                // Only for Hive
                if let Some(l) = like {
                    // write!(f, " LIKE ")?;
                    l.analyse(ctx)?;
                }
                match hive_distribution {
                    HiveDistributionStyle::PARTITIONED { columns } => {
                        // write!(f, " PARTITIONED BY (")?;
                        display_comma_separated(&columns).analyse(ctx)?;
                        // write!(f, ")")?;
                    }
                    HiveDistributionStyle::CLUSTERED {
                        columns,
                        sorted_by,
                        num_buckets,
                    } => {
                        // write!(f, " CLUSTERED BY (")?;
                        display_comma_separated(&columns).analyse(ctx)?;
                        // write!(f, ")")?;
                        if !sorted_by.is_empty() {
                            // write!(f, " SORTED BY (")?;
                            display_comma_separated(&sorted_by).analyse(ctx)?;
                            // write!(f, ")")?;
                        }
                        if *num_buckets > 0 {
                            // write!(f, " INTO {} BUCKETS", num_buckets)?;
                        }
                    }
                    HiveDistributionStyle::SKEWED {
                        columns,
                        on,
                        stored_as_directories,
                    } => {
                        // write!(
                        //     f,
                        //     " SKEWED BY ("
                        // )?;
                        display_comma_separated(&columns).analyse(ctx)?;
                        // write!(
                        //     f,
                        //     ")) ON ("
                        // )?;
                        display_comma_separated(&on).analyse(ctx)?;
                        // write!(
                        //     f,
                        //     ")"
                        // )?;
                        if *stored_as_directories {
                            // write!(f, " STORED AS DIRECTORIES")?;
                        }
                    }
                    _ => (), // TODO
                }

                if let Some(HiveFormat {
                                row_format,
                                storage,
                                location,
                            }) = hive_formats
                {
                    match row_format {
                        Some(HiveRowFormat::SERDE { class }) => {
                            // write!(f, " ROW FORMAT SERDE '{}'", class)?
                        }
                        Some(HiveRowFormat::DELIMITED) => {
                            // write!(f, " ROW FORMAT DELIMITED")?
                        }
                        None => (),
                    }
                    match storage {
                        Some(HiveIOFormat::IOF {
                                 input_format,
                                 output_format,
                             }) => {
                            // write!(
                            //     f,
                            //     " STORED AS INPUTFORMAT "
                            // )?;
                            input_format.analyse(ctx)?;
                            // write!(
                            //     f,
                            //     " OUTPUTFORMAT "
                            // )?;
                            output_format.analyse(ctx)?;
                        }
                        Some(HiveIOFormat::FileFormat { format }) if !*external => {
                            // write!(f, " STORED AS ")?;
                            format.analyse(ctx)?;
                        }
                        _ => (),
                    }
                    if !*external {
                        if let Some(loc) = location {
                            // write!(f, " LOCATION '{}'", loc)?;
                        }
                    }
                }
                if *external {
                    // write!(
                    //     f,
                    //     " STORED AS "
                    // )?;
                    file_format.as_ref().unwrap().analyse(ctx)?;
                    // write!(
                    //     f,
                    //     " LOCATION '"
                    // )?;
                    location.as_ref().unwrap().analyse(ctx)?;
                    // write!(
                    //     f,
                    //     "'"
                    // )?;
                }
                if !table_properties.is_empty() {
                    // write!(
                    //     f,
                    //     " TBLPROPERTIES ("
                    // )?;
                    display_comma_separated(table_properties).analyse(ctx)?;
                    // write!(
                    //     f,
                    //     ")"
                    // )?;
                }
                if !with_options.is_empty() {
                    // write!(f, " WITH (")?;
                    display_comma_separated(with_options).analyse(ctx)?;
                    // write!(f, ")")?;
                }
                if let Some(query) = query {
                    // write!(f, " AS ")?;
                    query.analyse(ctx)?;
                }
            }
            Statement::CreateVirtualTable {
                name,
                if_not_exists,
                module_name,
                module_args,
            } => {
                // write!(
                //     f,
                //     "CREATE VIRTUAL TABLE {if_not_exists}",
                //     if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" }
                // )?;
                name.analyse(ctx)?;
                // write!(
                //     f,
                //     " USING "
                // )?;
                module_name.analyse(ctx)?;
                if !module_args.is_empty() {
                    // write!(f, " (")?;
                    display_comma_separated(module_args).analyse(ctx)?;
                    // write!(f, ")",)?;
                }
            }
            Statement::CreateIndex {
                name,
                table_name,
                columns,
                unique,
                if_not_exists,
            } => {
                // write!(
                //     f,
                //     "CREATE {unique}INDEX {if_not_exists}",
                //     unique = if *unique { "UNIQUE " } else { "" },
                //     if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" }
                // )?;
                name.analyse(ctx)?;
                // write!(
                //     f,
                //     " ON "
                // )?;
                table_name.analyse(ctx)?;
                // write!(
                //     f,
                //     "("
                // )?;
                display_separated(columns, ",").analyse(ctx)?;
                // write!(
                //     f,
                //     ")"
                // )?;
            }
            Statement::AlterTable { name, operation } => {
                // write!(f, "ALTER TABLE ")?;
                name.analyse(ctx)?;
                // write!(f, " ")?;
                operation.analyse(ctx)?;
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade, purge,
            } => {
                // write!(
                //     f,
                //     "DROP "
                // )?;
                object_type.analyse(ctx)?;
                // write!(
                //     f,
                //     "{} ",
                //     if *if_exists { " IF EXISTS" } else { "" }
                // )?;
                display_comma_separated(names).analyse(ctx)?;
                // write!(
                //     f,
                //     "{}{}",
                //     if *cascade { " CASCADE" } else { "" }
                //     if *purge { " PURGE" } else { "" }
                // )?;
            }
            Statement::SetVariable {
                local,
                hivevar, variable,
                value,
            } => {
                // f.write_str("SET ")?;
                if *local {
                    // f.write_str("LOCAL ")?;
                }
                // write!(
                //     f,
                //     "{hivevar}",
                //     hivevar = if *hivevar { "HIVEVAR:" } else { "" }
                // )?;
                variable.analyse(ctx)?;
                // write!(f, " = ")?;
                display_comma_separated(value).analyse(ctx)?;
            }
            Statement::ShowVariable { variable } => {
                // write!(f, "SHOW ")?;
                if !variable.is_empty() {
                    // write!(f, " ")?;
                    display_separated(variable, " ").analyse(ctx)?;
                }
            }
            Statement::UseDatabase { variable } => {
                // write!(f, "USE ")?;
                variable.analyse(ctx)?;
            }
            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => {
                // write!(
                //     f,
                //     "SHOW {extended}{full}COLUMNS FROM ",
                //     extended = if *extended { "EXTENDED " } else { "" },
                //     full = if *full { "FULL " } else { "" }
                // )?;
                table_name.analyse(ctx)?;
                if let Some(filter) = filter {
                    // write!(f, " ")?;
                    filter.analyse(ctx)?;
                }
            }
            Statement::StartTransaction { modes } => {
                // write!(f, "START TRANSACTION")?;
                if !modes.is_empty() {
                    // write!(f, " ")?;
                    display_comma_separated(modes).analyse(ctx)?;
                }
            }
            Statement::SetTransaction { session, modes } => {
                // write!(f, "SET{} TRANSACTION", if *session { " SESSION" } else { "" })?;
                if !modes.is_empty() {
                    // write!(f, " ")?;
                    display_comma_separated(modes).analyse(ctx)?;
                }
            }
            Statement::SetNames { variable } => {
                // write!(f, "SET NAMES ")?;
                variable.analyse(ctx)?;
            }
            Statement::Commit { chain } => {
                // write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" },)?;
            }
            Statement::Savepoint { variable } => {
                // write!(f, "SAVEPOINT ")?;
                variable.analyse(ctx)?;
            }
            Statement::Rollback { chain, savepoint } => {
                if let Some(savepoint) = savepoint {
                    // write!(f, "ROLLBACK TO SAVEPOINT ")?;
                    savepoint.analyse(ctx)?;
                } else {
                    // write!(f, "ROLLBACK{}", if *chain { " AND CHAIN" } else { "" },)?;
                }
            }
            Statement::Release { variable } => {
                // write!(f, "RELEASE SAVEPOINT ")?;
                variable.analyse(ctx)?;
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                // write!(
                //     f,
                //     "CREATE SCHEMA {if_not_exists}",
                //     if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                // )?;
                schema_name.analyse(ctx)?;
            }
            Statement::Assert { condition, message } => {
                // write!(f, "ASSERT ")?;
                condition.analyse(ctx)?;
                if let Some(m) = message {
                    // write!(f, " AS ")?;
                    m.analyse(ctx)?;
                }
            }
            Statement::Deallocate { name, prepare } => {
                // write!(
                //     f,
                //     "DEALLOCATE {prepare}",
                //     prepare = if *prepare { "PREPARE " } else { "" },
                // )?;
                name.analyse(ctx)?;
            }
            Statement::Execute { name, parameters } => {
                // write!(f, "EXECUTE ")?;
                name.analyse(ctx)?;
                if !parameters.is_empty() {
                    // write!(f, "(")?;
                    display_comma_separated(parameters).analyse(ctx)?;
                    // write!(f, "()",)?;
                }
            }
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => {
                // write!(f, "PREPARE ")?;
                name.analyse(ctx)?;
                // write!(f, " ")?;
                if !data_types.is_empty() {
                    // write!(f, "(")?;
                    display_comma_separated(data_types).analyse(ctx)?;
                    // write!(f, ") ")?;
                }
                // write!(f, "AS ")?;
                statement.analyse(ctx)?;
            }
        };
        Ok(())
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
impl SQLAnalyse for Assignment {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        self.id.analyse(ctx)?;
        // write!(f, " = ")?;
        self.value.analyse(ctx)
    }
}

impl SQLAnalyse for FunctionArg {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match self {
            FunctionArg::Named { name, arg } => {
                name.analyse(ctx)?;
                // write!(f, " => ")?;
                arg.analyse(ctx)?;
            }
            FunctionArg::Unnamed(unnamed_arg) => {
                unnamed_arg.analyse(ctx)?;
            }
        };
        Ok(())
    }
}

/// A function call
impl SQLAnalyse for Function {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        self.name.analyse(ctx)?;
        // write!(
        //     f,
        //     "({}",
        //     if self.distinct { "DISTINCT " } else { "" }
        // )?;
        display_comma_separated(&self.args).analyse(ctx)?;
        // write!(
        //     f,
        //     ")"
        // )?;
        if let Some(o) = &self.over {
            // write!(f, " OVER (")?;
            o.analyse(ctx)?;
            // write!(f, ")")?;
        }
        Ok(())
    }
}

/// External table's available file format
impl SQLAnalyse for FileFormat {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // use self::FileFormat::*;
        // f.write_str(match self {
        //     TEXTFILE => "TEXTFILE",
        //     SEQUENCEFILE => "SEQUENCEFILE",
        //     ORC => "ORC",
        //     PARQUET => "PARQUET",
        //     AVRO => "AVRO",
        //     RCFILE => "RCFILE",
        //     JSONFILE => "TEXTFILE",
        // })?;
        Ok(())
    }
}

impl SQLAnalyse for ListAgg {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // write!(
        //     f,
        //     "LISTAGG({}",
        //     if self.distinct { "DISTINCT " } else { "" },
        // )?;
        self.expr.analyse(ctx)?;
        if let Some(separator) = &self.separator {
            // write!(f, ", ")?;
            separator.analyse(ctx)?;
        }
        if let Some(on_overflow) = &self.on_overflow {
            on_overflow.analyse(ctx)?;
        }
        // write!(f, ")")?;
        if !self.within_group.is_empty() {
            // write!(
            //     f,
            //     " WITHIN GROUP (ORDER BY "
            // )?;
            display_comma_separated(&self.within_group).analyse(ctx)?;
            // write!(
            //     f,
            //     ")"
            // )?;
        }
        Ok(())
    }
}

impl SQLAnalyse for ListAggOnOverflow {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // write!(f, " ON OVERFLOW")?;
        match self {
            ListAggOnOverflow::Error => {
                // write!(f, " ERROR")?;
            }
            ListAggOnOverflow::Truncate { filler, with_count } => {
                // write!(f, " TRUNCATE")?;
                if let Some(filler) = filler {
                    // write!(f, " ")?;
                    filler.analyse(ctx)?;
                }
                if *with_count {
                    // write!(f, " WITH")?;
                } else {
                    // write!(f, " WITHOUT")?;
                }
                // write!(f, " COUNT")?;
            }
        }
        Ok(())
    }
}

impl SQLAnalyse for ObjectType {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // f.write_str(match self {
        //     ObjectType::Table => "TABLE",
        //     ObjectType::View => "VIEW",
        //     ObjectType::Index => "INDEX",
        //     ObjectType::Schema => "SCHEMA",
        // })?;
        Ok(())
    }
}

impl SQLAnalyse for SqlOption {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        self.name.analyse(ctx)?;
        // write!(f, " = ")?;
        self.value.analyse(ctx)?;
        Ok(())
    }
}

impl SQLAnalyse for TransactionMode {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => {
                access_mode.analyse(ctx)?
            }
            IsolationLevel(iso_level) => {
                // write!(f, "ISOLATION LEVEL ")?;
                iso_level.analyse(ctx)?;
            }
        }
        Ok(())
    }
}

impl SQLAnalyse for TransactionAccessMode {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // use TransactionAccessMode::*;
        // f.write_str(match self {
        //     ReadOnly => "READ ONLY",
        //     ReadWrite => "READ WRITE",
        // })?;
        Ok(())
    }
}

impl SQLAnalyse for TransactionIsolationLevel {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // use TransactionIsolationLevel::*;
        // f.write_str(match self {
        //     ReadUncommitted => "READ UNCOMMITTED",
        //     ReadCommitted => "READ COMMITTED",
        //     RepeatableRead => "REPEATABLE READ",
        //     Serializable => "SERIALIZABLE",
        // })?;
        Ok(())
    }
}

impl SQLAnalyse for ShowStatementFilter {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => {
                // write!(f, "LIKE '")?;
                value::escape_single_quote_string(pattern).analyse(ctx)?;
                // write!(f, "'")?;
            }
            Where(expr) => {
                // write!(f, "WHERE ")?;
                expr.analyse(ctx)?
            }
        }
        Ok(())
    }
}

impl SQLAnalyse for SetVariableValue {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        use SetVariableValue::*;
        match self {
            Ident(ident) => {
                ident.analyse(ctx)?;
            }
            Literal(literal) => {
                literal.analyse(ctx)?;
            }
        }
        Ok(())
    }
}

impl SQLAnalyse for SqliteOnConflict {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // use SqliteOnConflict::*;
        // match self {
        //     Rollback => write!(f, "ROLLBACK"),
        //     Abort => write!(f, "ABORT"),
        //     Fail => write!(f, "FAIL"),
        //     Ignore => write!(f, "IGNORE"),
        //     Replace => write!(f, "REPLACE"),
        // };
        Ok(())
    }
}

impl SQLAnalyse for Token {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match self {
            Token::EOF => {
                // f.write_str("EOF")?
            }
            Token::Word(ref w) => {
                w.analyse(ctx)?;
            }
            Token::Number(ref n, l) => {
                // write!(f, "{}{long}", n, long = if *l { "L" } else { "" })?,
            }
            Token::Char(ref c) => {
                // write!(f, "{}", c)?
            }
            Token::SingleQuotedString(ref s) => {
                // write!(f, "'{}'", s)?
            }
            Token::NationalStringLiteral(ref s) => {
                // write!(f, "N'{}'", s)?
            }
            Token::HexStringLiteral(ref s) => {
                // write!(f, "X'{}'", s)?
            }
            Token::Comma => {
                // f.write_str(",")?
            }
            Token::Whitespace(ws) => {
                ws.analyse(ctx)?;
            }
            Token::DoubleEq => {
                // f.write_str("==")?;
            }
            Token::Spaceship => {
                // f.write_str("<=>")?;
            }
            Token::Eq => {
                // f.write_str("=")?
            }
            Token::Neq => {
                // f.write_str("<>")?
            }
            Token::Lt => {
                // f.write_str("<")?
            }
            Token::Gt => {
                // f.write_str(">")?
            }
            Token::LtEq => {
                // f.write_str("<=")?
            }
            Token::GtEq => {
                // f.write_str(">=")?
            }
            Token::Plus => {
                // f.write_str("+")?
            }
            Token::Minus => {
                // f.write_str("-")?
            }
            Token::Mult => {
                // f.write_str("*")?
            }
            Token::Div => {
                // f.write_str("/")?
            }
            Token::StringConcat => {
                // f.write_str("||")?
            }
            Token::Mod => {
                // f.write_str("%")?
            }
            Token::LParen => {
                // f.write_str("(")?
            }
            Token::RParen => {
                // f.write_str(")")?
            }
            Token::Period => {
                // f.write_str(".")?
            }
            Token::Colon => {
                // f.write_str(":")?
            }
            Token::DoubleColon => {
                // f.write_str("::")?
            }
            Token::SemiColon => {
                // f.write_str(";")?
            }
            Token::Backslash => {
                // f.write_str("\\")?
            }
            Token::LBracket => {
                // f.write_str("[")?
            }
            Token::RBracket => {
                // f.write_str("]")?
            }
            Token::Ampersand => {
                // f.write_str("&")?
            }
            Token::Caret => {
                // f.write_str("^")?
            }
            Token::Pipe => {
                // f.write_str("|")?
            }
            Token::LBrace => {
                // f.write_str("{")?
            }
            Token::RBrace => {
                // f.write_str("}")?
            }
            Token::RArrow => {
                // f.write_str("=>")?
            }
            Token::Sharp => {
                // f.write_str("#")?
            }
            Token::ExclamationMark => {
                // f.write_str("!")?
            }
            Token::DoubleExclamationMark => {
                // f.write_str("!!")?
            }
            Token::Tilde => {
                // f.write_str("~")?
            }
            Token::AtSign => {
                // f.write_str("@")?
            }
            Token::ShiftLeft => {
                // f.write_str("<<")?
            }
            Token::ShiftRight => {
                // f.write_str(">>")?
            }
            Token::PGSquareRoot => {
                // f.write_str("|/")?
            }
            Token::PGCubeRoot => {
                // f.write_str("||/")?
            }
        };
        Ok(())
    }
}

impl SQLAnalyse for Word {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match self.quote_style {
            Some(s) if s == '"' || s == '[' || s == '`' => {
                // write!(f, "{}{}{}", s, self.value, matching_end_quote(s))?
            }
            None => {
                // f.write_str(&self.value)?
            }
            _ => panic!("Unexpected quote_style!"),
        };
        Ok(())
    }
}

fn matching_end_quote(ch: char) -> char {
    match ch {
        '"' => '"', // ANSI and most dialects
        '[' => ']', // MS SQL
        '`' => '`', // MySQL
        _ => panic!("unexpected quoting style!"),
    }
}

impl SQLAnalyse for Whitespace {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // match self {
        //     Whitespace::Space => f.write_str(" ")?,
        //     Whitespace::Newline => f.write_str("\n")?,
        //     Whitespace::Tab => f.write_str("\t")?,
        //     Whitespace::SingleLineComment { prefix, comment } => write!(f, "{}{}", prefix, comment)?,
        //     Whitespace::MultiLineComment(s) => write!(f, "/*{}*/", s)?,
        // };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::handler::database::parser::sql::{SelectStatementContext, SQLStatementContext};
    use crate::handler::database::parser::sql::analyse::SQLAnalyse;
    use crate::handler::database::parser::sql::mysql::parser;

    #[test]
    fn test_analyse() {
        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 as t, db.table_2 as t2, (select * from table_3 as t3) as t4 left join table_5 as t5 on t5.id = t1.id \
           WHERE a > b AND b < 100 and c = ? and d = 'are' \
           ORDER BY a DESC, b";
        // let sql = "insert into test (a, b, c) values (1, 1, ?)";
        let mut ast = parser(sql.to_string());
        let stmt = ast.pop().unwrap();
        let mut resql = String::new();
        let mut ctx = SQLStatementContext::Select(SelectStatementContext::new());
        stmt.analyse(&mut ctx).unwrap();
        ctx.add_table(String::from("table"), String::from("t1"));
        println!("{:?}", stmt.to_string());
        match ctx {
            SQLStatementContext::Select(mut s) => {
                println!("{:?}", s.common_ctx.tables);
            }
            SQLStatementContext::Update(_) => {}
            SQLStatementContext::Delete(_) => {}
            SQLStatementContext::Default => {}
        }
    }
}
