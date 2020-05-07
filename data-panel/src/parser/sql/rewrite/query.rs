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

pub type SRWResult = crate::common::Result<()>;

use crate::parser::sql::rewrite::{display_comma_separated, SQLReWrite};

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
impl SQLReWrite for Query {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        if !self.ctes.is_empty() {
            write!(f, "WITH ")?;
            display_comma_separated(&self.ctes).rewrite(f, ctx)?;
            write!(f, " ")?;
        }
        self.body.rewrite(f, ctx)?;
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            display_comma_separated(&self.order_by).rewrite(f, ctx)?;
        }
        if let Some(ref limit) = self.limit {
            write!(f, " LIMIT ")?;
            limit.rewrite(f, ctx)?;
        }
        if let Some(ref offset) = self.offset {
            write!(f, " OFFSET ")?;
            offset.rewrite(f, ctx)?;
            write!(f, " ROWS")?;
        }
        if let Some(ref fetch) = self.fetch {
            write!(f, " ")?;
            fetch.rewrite(f, ctx)?;
        }
        Ok(())
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
impl SQLReWrite for SetExpr {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self {
            SetExpr::Select(s) => {
                s.rewrite(f, ctx)?;
            },
            SetExpr::Query(q) => {
                q.rewrite(f, ctx)?;
            },
            SetExpr::Values(v) => {
                v.rewrite(f, ctx)?;
            },
            SetExpr::SetOperation {
                left,
                right,
                op,
                all,
            } => {
                let all_str = if *all { " ALL" } else { "" };
                left.rewrite(f, ctx)?;
                write!(f, " ")?;
                op.rewrite(f, ctx)?;
                write!(f, "{}", all_str)?;
                write!(f, " ")?;
                right.rewrite(f, ctx)?;
            }
        };
        Ok(())
    }
}

impl SQLReWrite for SetOperator {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
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
impl SQLReWrite for Select {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        write!(
            f,
            "SELECT"
        )?;
        write!(
            f,
            "{} ",
            if self.distinct { " DISTINCT" } else { "" }
        )?;
        display_comma_separated(&self.projection).rewrite(f, ctx)?;
        if !self.from.is_empty() {
            write!(f, " FROM ")?;
            display_comma_separated(&self.from).rewrite(f, ctx)?;
        }
        if let Some(ref selection) = self.selection {
            write!(f, " WHERE ")?;
            selection.rewrite(f, ctx)?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY ")?;
            display_comma_separated(&self.group_by).rewrite(f, ctx)?;
        }
        if let Some(ref having) = self.having {
            write!(f, " HAVING ")?;
            having.rewrite(f, ctx)?;
        }
        Ok(())
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
impl SQLReWrite for Cte {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        self.alias.rewrite(f, ctx)?;
        write!(f, " AS (")?;
        self.query.rewrite(f, ctx)?;
        write!(f, ")")?;
        Ok(())
    }
}

/// One item of the comma-separated list following `SELECT`
impl SQLReWrite for SelectItem {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match &self {
            SelectItem::UnnamedExpr(expr) => {
                expr.rewrite(f, ctx)?;
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                expr.rewrite(f, ctx)?;
                write!(f, " AS {}", alias)?;
            },
            SelectItem::QualifiedWildcard(prefix) => {
                prefix.rewrite(f, ctx)?;
                write!(f, ".*")?;
            },
            SelectItem::Wildcard => {
                write!(f, "*")?;
            },
        }
        Ok(())
    }
}

impl SQLReWrite for TableWithJoins {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        self.relation.rewrite(f, ctx)?;
        for join in &self.joins {
            join.rewrite(f, ctx)?;
        }
        Ok(())
    }
}

/// A table name or a parenthesized subquery with an optional alias
impl SQLReWrite for TableFactor {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                name.rewrite(f, ctx)?;
                if !args.is_empty() {
                    write!(f, "(")?;
                    display_comma_separated(args).rewrite(f, ctx)?;
                    write!(f, ")")?;
                }
                if let Some(alias) = alias {
                    write!(f, " AS ")?;
                    alias.rewrite(f, ctx)?;
                }
                if !with_hints.is_empty() {
                    write!(f, " WITH (")?;
                    display_comma_separated(with_hints).rewrite(f, ctx)?;
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
                subquery.rewrite(f, ctx)?;
                write!(f, ")")?;
                if let Some(alias) = alias {
                    write!(f, " AS ")?;
                    alias.rewrite(f, ctx)?;
                }
                Ok(())
            }
            TableFactor::NestedJoin(table_reference) => {
                write!(f, "(")?;
                table_reference.rewrite(f, ctx)?;
                write!(f, ")")?;
                Ok(())
            },
        }
    }
}

impl SQLReWrite for TableAlias {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        write!(f, "{}", self.name)?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            display_comma_separated(&self.columns).rewrite(f, ctx)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl SQLReWrite for Join {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix<'a>(constraint: &'a JoinConstraint) -> impl SQLReWrite + 'a {
            struct Suffix<'a>(&'a JoinConstraint);
            impl<'a> SQLReWrite for Suffix<'a> {
                fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
                    match self.0 {
                        JoinConstraint::On(expr) => {
                            write!(f, " ON ")?;
                            expr.rewrite(f, ctx)?;
                            Ok(())
                        },
                        JoinConstraint::Using(attrs) => {
                            write!(f, " USING(")?;
                            display_comma_separated(attrs).rewrite(f, ctx)?;
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
                self.relation.rewrite(f, ctx)?;
                suffix(constraint).rewrite(f, ctx)?;
            },
            JoinOperator::LeftOuter(constraint) => {
                write!(
                    f,
                    " {}LEFT JOIN ",
                    prefix(constraint)
                )?;
                self.relation.rewrite(f, ctx)?;
                suffix(constraint).rewrite(f, ctx)?;
            },
            JoinOperator::RightOuter(constraint) => {
                write!(
                    f,
                    " {}RIGHT JOIN ",
                    prefix(constraint)
                )?;
                self.relation.rewrite(f, ctx)?;
                suffix(constraint).rewrite(f, ctx)?;
            },
            JoinOperator::FullOuter(constraint) => {
                write!(
                    f,
                    " {}FULL JOIN ",
                    prefix(constraint)
                )?;
                self.relation.rewrite(f, ctx)?;
                suffix(constraint).rewrite(f, ctx)?;
            },
            JoinOperator::CrossJoin => {
                write!(f, " CROSS JOIN ")?;
                self.relation.rewrite(f, ctx)?;
            },
            JoinOperator::CrossApply => {
                write!(f, " CROSS APPLY ")?;
                self.relation.rewrite(f, ctx)?;
            },
            JoinOperator::OuterApply => {
                write!(f, " OUTER APPLY ")?;
                self.relation.rewrite(f, ctx)?;
            },
        }
        Ok(())
    }
}

impl SQLReWrite for OrderByExpr {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self.asc {
            Some(true) => {
                self.expr.rewrite(f, ctx)?;
                write!(f, " ASC")?;
            },
            Some(false) => {
                self.expr.rewrite(f, ctx)?;
                write!(f, " DESC")?;
            },
            None => {
                self.expr.rewrite(f, ctx)?;
            },
        }
        Ok(())
    }
}

impl SQLReWrite for Fetch {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(f, "FETCH FIRST ")?;
            quantity.rewrite(f, ctx)?;
            write!(f, "{} ROWS {}",  percent, extension)?;
        } else {
            write!(f, "FETCH FIRST ROWS {}", extension)?;
        }
        Ok(())
    }
}

impl SQLReWrite for Values {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        write!(f, "VALUES ")?;
        let mut delim = "";
        for row in &self.0 {
            write!(f, "{}", delim)?;
            delim = ", ";
            write!(f, "(")?;
            display_comma_separated(row).rewrite(f, ctx)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}
