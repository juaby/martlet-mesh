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

use sqlparser::ast::{Values, Fetch, OrderByExpr, JoinOperator, JoinConstraint, Join, TableAlias, TableFactor, TableWithJoins, SelectItem, Cte, Select, SetOperator, SetExpr, Query};

use std::fmt::Write;
use std::collections::HashMap;

pub type SAResult = crate::common::Result<()>;

use crate::parser::sql::analyse::{display_comma_separated, SQLAnalyse};
use crate::parser::sql::SQLStatementContext;

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
impl SQLAnalyse for Query {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        if !self.ctes.is_empty() {
            // write!(f, "WITH ")?;
            // display_comma_separated(&self.ctes).analyse(ctx)?;
            // write!(f, " ")?;
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
            // write!(f, " OFFSET ")?;
            offset.analyse(ctx)?;
            // write!(f, " ROWS")?;
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
        // write!(f, "SELECT")?;
        // write!(f, "{} ", if self.distinct { " DISTINCT" } else { "" })?;
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
                // write!(f, " AS {}", alias)?;
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
                // name.analyse(ctx)?; // TODO
                let table_name = &name.0[0];

                if !args.is_empty() {
                    // write!(f, "(")?;
                    display_comma_separated(args).analyse(ctx)?;
                    // write!(f, ")")?;
                }
                let mut alias_name = String::new();
                if let Some(alias) = alias {
                    // write!(f, " AS ")?;
                    alias.analyse(ctx)?;  // TODO
                    alias_name = alias.name.clone();
                }

                ctx.add_table(table_name.clone(), alias_name);

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
        // write!(f, "{}", self.name)?;
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
                // write!(f, " {}JOIN ", prefix(constraint))?;
                self.relation.analyse(ctx)?;
                suffix(constraint).analyse(ctx)?;
            },
            JoinOperator::LeftOuter(constraint) => {
                // write!(f, " {}LEFT JOIN ", prefix(constraint))?;
                self.relation.analyse(ctx)?;
                suffix(constraint).analyse(ctx)?;
            },
            JoinOperator::RightOuter(constraint) => {
                // write!(f, " {}RIGHT JOIN ", prefix(constraint))?;
                self.relation.analyse(ctx)?;
                suffix(constraint).analyse(ctx)?;
            },
            JoinOperator::FullOuter(constraint) => {
                // write!(f, " {}FULL JOIN ", prefix(constraint))?;
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
        match self.asc {
            Some(true) => {
                self.expr.analyse(ctx)?; // TODO
                // write!(f, " ASC")?;
            },
            Some(false) => {
                self.expr.analyse(ctx)?; // TODO
                // write!(f, " DESC")?;
            },
            None => {
                self.expr.analyse(ctx)?; // TODO
            },
        }
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

impl SQLAnalyse for Values {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // write!(f, "VALUES ")?;
        let mut delim = "";
        for row in &self.0 {
            // write!(f, "{}", delim)?;
            delim = ", ";
            // write!(f, "(")?;
            display_comma_separated(row).analyse(ctx)?; // TODO
            // write!(f, ")")?;
        }
        Ok(())
    }
}
