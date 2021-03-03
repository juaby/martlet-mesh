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

use sqlparser::ast::{Values, Fetch, OrderByExpr, JoinOperator, JoinConstraint, Join, TableAlias, TableFactor, TableWithJoins, SelectItem, Cte, Select, SetOperator, SetExpr, Query, With, Offset, OffsetRows, Top};

// use std::fmt::Write;

pub type SAResult = crate::common::Result<()>;

use crate::parser::sql::analyse::{display_comma_separated, SQLAnalyse};
use crate::parser::sql::SQLStatementContext;

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
impl SQLAnalyse for Query {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        if let Some(ref with) = self.with {
            with.analyse(ctx)?;
        }
        self.body.analyse(ctx)?;
        if !self.order_by.is_empty() {
            // write!(f, " ORDER BY ")?;
            display_comma_separated(&self.order_by).analyse(ctx)?;
        }
        if let Some(ref limit) = self.limit {
            // write!(f, " LIMIT ")?;
            limit.analyse(ctx)?;
        }
        if let Some(ref offset) = self.offset {
            // write!(f, " ")?;
            offset.analyse(ctx)?;
        }
        if let Some(ref fetch) = self.fetch {
            // write!(f, " ")?;
            fetch.analyse(ctx)?;
        }
        Ok(())
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
impl SQLAnalyse for SetExpr {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match self {
            SetExpr::Select(s) => {
                s.analyse(ctx)?;
            },
            SetExpr::Query(q) => {
                q.analyse(ctx)?;
            },
            SetExpr::Values(v) => {
                v.analyse(ctx)?;
            },
            SetExpr::Insert(v) => {
                v.analyse(ctx)?;
            },
            SetExpr::SetOperation {
                left,
                right,
                op,
                all,
            } => {
                let all_str = if *all { " ALL" } else { "" };
                left.analyse(ctx)?;
                // write!(f, " ")?;
                op.analyse(ctx)?;
                // write!(f, "{}", all_str)?;
                // write!(f, " ")?;
                right.analyse(ctx)?;
            }
        };
        Ok(())
    }
}

impl SQLAnalyse for SetOperator {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // f.write_str(match self {
        //     SetOperator::Union => "UNION",
        //     SetOperator::Except => "EXCEPT",
        //     SetOperator::Intersect => "INTERSECT",
        // })?;
        Ok(())
    }
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of an `SQLQuery`, or as an operand
/// to a set operation like `UNION`.
impl SQLAnalyse for Select {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // write!(f, "SELECT{}", if self.distinct { " DISTINCT" } else { "" })?;
        if let Some(ref top) = self.top {
            // write!(f, " ")?;
            top.analyse(ctx)?;
        }
        // write!(f, " ")?;
        display_comma_separated(&self.projection).analyse(ctx)?;
        if !self.from.is_empty() {
            // write!(f, " FROM ")?;
            display_comma_separated(&self.from).analyse(ctx)?;
        }
        if let Some(ref selection) = self.selection {
            // write!(f, " WHERE ")?;
            selection.analyse(ctx)?;
        }
        if !self.group_by.is_empty() {
            // write!(f, " GROUP BY ")?;
            display_comma_separated(&self.group_by).analyse(ctx)?;
        }
        if let Some(ref having) = self.having {
            // write!(f, " HAVING ")?;
            having.analyse(ctx)?;
        }
        Ok(())
    }
}

impl SQLAnalyse for With {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // write!(
        //     f,
        //     "WITH {}",
        //     if self.recursive { "RECURSIVE " } else { "" }
        // )?;
        display_comma_separated(&self.cte_tables).analyse(ctx)?;
        Ok(())
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
impl SQLAnalyse for Cte {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        self.alias.analyse(ctx)?;
        // write!(f, " AS (")?;
        self.query.analyse(ctx)?;
        // write!(f, ")")?;
        Ok(())
    }
}

/// One item of the comma-separated list following `SELECT`
impl SQLAnalyse for SelectItem {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match &self {
            SelectItem::UnnamedExpr(expr) => {
                expr.analyse(ctx)?;
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                expr.analyse(ctx)?;
                // write!(f, " AS ")?;
                alias.analyse(ctx)?;
            },
            SelectItem::QualifiedWildcard(prefix) => {
                prefix.analyse(ctx)?;
                // write!(f, ".*")?;
            },
            SelectItem::Wildcard => {
                // write!(f, "*")?;
            },
        }
        Ok(())
    }
}

impl SQLAnalyse for TableWithJoins {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        self.relation.analyse(ctx)?;
        for join in &self.joins {
            join.analyse(ctx)?;
        }
        Ok(())
    }
}

/// A table name or a parenthesized subquery with an optional alias
impl SQLAnalyse for TableFactor {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                name.analyse(ctx)?;
                if !args.is_empty() {
                    // write!(f, "(")?;
                    display_comma_separated(args).analyse(ctx)?;
                    // write!(f, ")")?;
                }
                if let Some(alias) = alias {
                    // write!(f, " AS ")?;
                    alias.analyse(ctx)?;
                }
                if !with_hints.is_empty() {
                    // write!(f, " WITH (")?;
                    display_comma_separated(with_hints).analyse(ctx)?;
                    // write!(f, ")")?;
                }
                Ok(())
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    // write!(f, "LATERAL ")?;
                }
                // write!(f, "(")?;
                subquery.analyse(ctx)?;
                // write!(f, ")")?;
                if let Some(alias) = alias {
                    // write!(f, " AS ")?;
                    alias.analyse(ctx)?;
                }
                Ok(())
            }
            TableFactor::TableFunction { expr, alias } => {
                // write!(f, "TABLE(")?;
                expr.analyse(ctx)?;
                // write!(f, ")")?;
                if let Some(alias) = alias {
                    // write!(f, " AS ")?;
                    alias.analyse(ctx)?;
                }
                Ok(())
            }
            TableFactor::NestedJoin(table_reference) => {
                // write!(f, "(")?;
                table_reference.analyse(ctx)?;
                // write!(f, ")")?;
                Ok(())
            },
        }
    }
}

impl SQLAnalyse for TableAlias {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        self.name.analyse(ctx)?;
        if !self.columns.is_empty() {
            // write!(f, " (")?;
            display_comma_separated(&self.columns).analyse(ctx)?;
            // write!(f, ")")?;
        }
        Ok(())
    }
}

impl SQLAnalyse for Join {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix<'a>(constraint: &'a JoinConstraint) -> impl SQLAnalyse + 'a {
            struct Suffix<'a>(&'a JoinConstraint);
            impl<'a> SQLAnalyse for Suffix<'a> {
                fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
                    match self.0 {
                        JoinConstraint::On(expr) => {
                            // write!(f, " ON ")?;
                            expr.analyse(ctx)?;
                            Ok(())
                        },
                        JoinConstraint::Using(attrs) => {
                            // write!(f, " USING(")?;
                            display_comma_separated(attrs).analyse(ctx)?;
                            // write!(f, ")")?;
                            Ok(())
                        }
                        _ => Ok(()),
                    }
                }
            }
            Suffix(constraint)
        }
        match &self.join_operator {
            JoinOperator::Inner(constraint) => {
                // write!(
                //     f,
                //     " {}JOIN ",
                //     prefix(constraint)
                // )?;
                self.relation.analyse(ctx)?;
                suffix(constraint).analyse(ctx)?;
            },
            JoinOperator::LeftOuter(constraint) => {
                // write!(
                //     f,
                //     " {}LEFT JOIN ",
                //     prefix(constraint)
                // )?;
                self.relation.analyse(ctx)?;
                suffix(constraint).analyse(ctx)?;
            },
            JoinOperator::RightOuter(constraint) => {
                // write!(
                //     f,
                //     " {}RIGHT JOIN ",
                //     prefix(constraint)
                // )?;
                self.relation.analyse(ctx)?;
                suffix(constraint).analyse(ctx)?;
            },
            JoinOperator::FullOuter(constraint) => {
                // write!(
                //     f,
                //     " {}FULL JOIN ",
                //     prefix(constraint)
                // )?;
                self.relation.analyse(ctx)?;
                suffix(constraint).analyse(ctx)?;
            },
            JoinOperator::CrossJoin => {
                // write!(f, " CROSS JOIN ")?;
                self.relation.analyse(ctx)?;
            },
            JoinOperator::CrossApply => {
                // write!(f, " CROSS APPLY ")?;
                self.relation.analyse(ctx)?;
            },
            JoinOperator::OuterApply => {
                // write!(f, " OUTER APPLY ")?;
                self.relation.analyse(ctx)?;
            },
        }
        Ok(())
    }
}

impl SQLAnalyse for OrderByExpr {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        self.expr.analyse(ctx)?;
        // match self.asc {
        //     Some(true) => write!(f, " ASC")?,
        //     Some(false) => write!(f, " DESC")?,
        //     None => (),
        // }
        // match self.nulls_first {
        //     Some(true) => write!(f, " NULLS FIRST")?,
        //     Some(false) => write!(f, " NULLS LAST")?,
        //     None => (),
        // }
        Ok(())
    }
}

impl SQLAnalyse for Offset {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // write!(f, "OFFSET ")?;
        self.value.analyse(ctx)?;
        self.rows.analyse(ctx)?;
        Ok(())
    }
}

impl SQLAnalyse for OffsetRows {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // match self {
        //     OffsetRows::None => write!(f, "")?,
        //     OffsetRows::Row => write!(f, " ROW")?,
        //     OffsetRows::Rows => write!(f, " ROWS")?,
        // }
        Ok(())
    }
}

impl SQLAnalyse for Fetch {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            // write!(f, "FETCH FIRST ")?;
            quantity.analyse(ctx)?;
            // write!(f, "{} ROWS {}",  percent, extension)?;
        } else {
            // write!(f, "FETCH FIRST ROWS {}", extension)?;
        }
        Ok(())
    }
}

impl SQLAnalyse for Top {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        let extension = if self.with_ties { " WITH TIES" } else { "" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            // write!(f, "TOP (")?;
            quantity.analyse(ctx)?;
            // write!(f, "){}{}", percent, extension)?;
        } else {
            // write!(f, "TOP{}", extension)?;
        }
        Ok(())
    }
}

impl SQLAnalyse for Values {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // write!(f, "VALUES ")?;
        let mut delim = "";
        for row in &self.0 {
            // write!(f, "{}", delim)?;
            delim = ", ";
            // write!(f, "(")?;
            display_comma_separated(row).analyse(ctx)?;
            // write!(f, ")")?;
        }
        Ok(())
    }
}
