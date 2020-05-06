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

use std::fmt;
use std::fmt::Write;
use std::collections::HashMap;

pub type SAResult = crate::common::Result<()>;

use crate::parser::sqlanalyse::{display_comma_separated, SQLAnalyse};

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
impl SQLAnalyse for Query {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        if !self.ctes.is_empty() {
            write!(f, "WITH ")?;
            display_comma_separated(&self.ctes).analyse(f, ctx)?;
            write!(f, " ")?;
        }
        self.body.analyse(f, ctx)?;
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            display_comma_separated(&self.order_by).analyse(f, ctx)?;
        }
        if let Some(ref limit) = self.limit {
            write!(f, " LIMIT ")?;
            limit.analyse(f, ctx)?;
        }
        if let Some(ref offset) = self.offset {
            write!(f, " OFFSET ")?;
            offset.analyse(f, ctx)?;
            write!(f, " ROWS")?;
        }
        if let Some(ref fetch) = self.fetch {
            write!(f, " ")?;
            fetch.analyse(f, ctx)?;
        }
        Ok(())
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
impl SQLAnalyse for SetExpr {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match self {
            SetExpr::Select(s) => {
                s.analyse(f, ctx)?;
            },
            SetExpr::Query(q) => {
                q.analyse(f, ctx)?;
            },
            SetExpr::Values(v) => {
                v.analyse(f, ctx)?;
            },
            SetExpr::SetOperation {
                left,
                right,
                op,
                all,
            } => {
                let all_str = if *all { " ALL" } else { "" };
                left.analyse(f, ctx)?;
                write!(f, " ")?;
                op.analyse(f, ctx)?;
                write!(f, "{}", all_str)?;
                write!(f, " ")?;
                right.analyse(f, ctx)?;
            }
        };
        Ok(())
    }
}

impl SQLAnalyse for SetOperator {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        f.write_str(match self {
            SetOperator::Union => "UNION",
            SetOperator::Except => "EXCEPT",
            SetOperator::Intersect => "INTERSECT",
        })?;
        Ok(())
    }
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of an `SQLQuery`, or as an operand
/// to a set operation like `UNION`.
impl SQLAnalyse for Select {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        write!(
            f,
            "SELECT"
        )?;
        write!(
            f,
            "{} ",
            if self.distinct { " DISTINCT" } else { "" }
        )?;
        display_comma_separated(&self.projection).analyse(f, ctx)?;
        if !self.from.is_empty() {
            write!(f, " FROM ")?;
            display_comma_separated(&self.from).analyse(f, ctx)?;
        }
        if let Some(ref selection) = self.selection {
            write!(f, " WHERE ")?;
            selection.analyse(f, ctx)?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY ")?;
            display_comma_separated(&self.group_by).analyse(f, ctx)?;
        }
        if let Some(ref having) = self.having {
            write!(f, " HAVING ")?;
            having.analyse(f, ctx)?;
        }
        Ok(())
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
impl SQLAnalyse for Cte {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        self.alias.analyse(f, ctx)?;
        write!(f, " AS (")?;
        self.query.analyse(f, ctx)?;
        write!(f, ")")?;
        Ok(())
    }
}

/// One item of the comma-separated list following `SELECT`
impl SQLAnalyse for SelectItem {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match &self {
            SelectItem::UnnamedExpr(expr) => {
                expr.analyse(f, ctx)?;
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                expr.analyse(f, ctx)?;
                write!(f, " AS {}", alias)?;
            },
            SelectItem::QualifiedWildcard(prefix) => {
                prefix.analyse(f, ctx)?;
                write!(f, ".*")?;
            },
            SelectItem::Wildcard => {
                write!(f, "*")?;
            },
        }
        Ok(())
    }
}

impl SQLAnalyse for TableWithJoins {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        self.relation.analyse(f, ctx)?;
        for join in &self.joins {
            join.analyse(f, ctx)?;
        }
        Ok(())
    }
}

/// A table name or a parenthesized subquery with an optional alias
impl SQLAnalyse for TableFactor {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                name.analyse(f, ctx)?;
                if !args.is_empty() {
                    write!(f, "(")?;
                    display_comma_separated(args).analyse(f, ctx)?;
                    write!(f, ")")?;
                }
                if let Some(alias) = alias {
                    write!(f, " AS ")?;
                    alias.analyse(f, ctx)?;
                }
                if !with_hints.is_empty() {
                    write!(f, " WITH (")?;
                    display_comma_separated(with_hints).analyse(f, ctx)?;
                    write!(f, ")")?;
                }
                Ok(())
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "(")?;
                subquery.analyse(f, ctx)?;
                write!(f, ")")?;
                if let Some(alias) = alias {
                    write!(f, " AS ")?;
                    alias.analyse(f, ctx)?;
                }
                Ok(())
            }
            TableFactor::NestedJoin(table_reference) => {
                write!(f, "(")?;
                table_reference.analyse(f, ctx)?;
                write!(f, ")")?;
                Ok(())
            },
        }
    }
}

impl SQLAnalyse for TableAlias {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        write!(f, "{}", self.name)?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            display_comma_separated(&self.columns).analyse(f, ctx);
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl SQLAnalyse for Join {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix<'a>(constraint: &'a JoinConstraint) -> impl SQLAnalyse + 'a {
            struct Suffix<'a>(&'a JoinConstraint);
            impl<'a> SQLAnalyse for Suffix<'a> {
                fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
                    match self.0 {
                        JoinConstraint::On(expr) => {
                            write!(f, " ON ")?;
                            expr.analyse(f, ctx)?;
                            Ok(())
                        },
                        JoinConstraint::Using(attrs) => {
                            write!(f, " USING(")?;
                            display_comma_separated(attrs).analyse(f, ctx)?;
                            write!(f, ")")?;
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
                write!(
                    f,
                    " {}JOIN ",
                    prefix(constraint)
                )?;
                self.relation.analyse(f, ctx)?;
                suffix(constraint).analyse(f, ctx)?;
            },
            JoinOperator::LeftOuter(constraint) => {
                write!(
                    f,
                    " {}LEFT JOIN ",
                    prefix(constraint)
                )?;
                self.relation.analyse(f, ctx)?;
                suffix(constraint).analyse(f, ctx)?;
            },
            JoinOperator::RightOuter(constraint) => {
                write!(
                    f,
                    " {}RIGHT JOIN ",
                    prefix(constraint)
                )?;
                self.relation.analyse(f, ctx)?;
                suffix(constraint).analyse(f, ctx)?;
            },
            JoinOperator::FullOuter(constraint) => {
                write!(
                    f,
                    " {}FULL JOIN ",
                    prefix(constraint)
                )?;
                self.relation.analyse(f, ctx)?;
                suffix(constraint).analyse(f, ctx)?;
            },
            JoinOperator::CrossJoin => {
                write!(f, " CROSS JOIN ")?;
                self.relation.analyse(f, ctx)?;
            },
            JoinOperator::CrossApply => {
                write!(f, " CROSS APPLY ")?;
                self.relation.analyse(f, ctx)?;
            },
            JoinOperator::OuterApply => {
                write!(f, " OUTER APPLY ")?;
                self.relation.analyse(f, ctx)?;
            },
        }
        Ok(())
    }
}

impl SQLAnalyse for OrderByExpr {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match self.asc {
            Some(true) => {
                self.expr.analyse(f, ctx)?;
                write!(f, " ASC")?;
            },
            Some(false) => {
                self.expr.analyse(f, ctx)?;
                write!(f, " DESC")?;
            },
            None => {
                self.expr.analyse(f, ctx)?;
            },
        }
        Ok(())
    }
}

impl SQLAnalyse for Fetch {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(f, "FETCH FIRST ")?;
            quantity.analyse(f, ctx)?;
            write!(f, "{} ROWS {}",  percent, extension)?;
        } else {
            write!(f, "FETCH FIRST ROWS {}", extension)?;
        }
        Ok(())
    }
}

impl SQLAnalyse for Values {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        write!(f, "VALUES ")?;
        let mut delim = "";
        for row in &self.0 {
            write!(f, "{}", delim)?;
            delim = ", ";
            write!(f, "(")?;
            display_comma_separated(row).analyse(f, ctx)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}
