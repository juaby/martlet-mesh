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

//! AST types specific to CREATE/ALTER variants of [Statement]
//! (commonly referred to as Data Definition Language, or DDL)
use sqlparser::ast::{ColumnOption, ColumnOptionDef, ColumnDef, TableConstraint, AlterTableOperation, Ident};
use crate::parser::sqlanalyse::{display_comma_separated, SQLAnalyse};

use std::fmt;
use std::fmt::Write;
use std::collections::HashMap;

pub type SAResult = crate::common::Result<()>;

/// An `ALTER TABLE` (`Statement::AlterTable`) operation
impl SQLAnalyse for AlterTableOperation {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match self {
            AlterTableOperation::AddConstraint(c) => {
                write!(f, "ADD ")?;
                c.analyse(f, ctx)?;
            },
            AlterTableOperation::DropConstraint { name } => {
                write!(f, "DROP CONSTRAINT {}", name)?;
            },
        };
        Ok(())
    }
}

/// A table-level constraint, specified in a `CREATE TABLE` or an
/// `ALTER TABLE ADD <constraint>` statement.
impl SQLAnalyse for TableConstraint {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        match self {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
            } => {
                display_constraint_name(name).analyse(f, ctx)?;
                write!(
                    f,
                    "{} (",
                    if *is_primary { "PRIMARY KEY" } else { "UNIQUE" }
                )?;
                display_comma_separated(columns).analyse(f, ctx)?;
                write!(
                    f,
                    ")"
                )?;
            },
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
            } => {
                display_constraint_name(name).analyse(f, ctx)?;
                write!(
                    f,
                    "FOREIGN KEY ("
                )?;
                display_comma_separated(columns).analyse(f, ctx)?;
                write!(
                    f,
                    ") REFERENCES "
                )?;
                foreign_table.analyse(f, ctx)?;
                write!(
                    f,
                    "("
                )?;
                display_comma_separated(referred_columns).analyse(f, ctx)?;
                write!(
                    f,
                    ")"
                )?;
            },
            TableConstraint::Check { name, expr } => {
                display_constraint_name(name).analyse(f, ctx)?;
                write!(f, "CHECK (")?;
                expr.analyse(f, ctx)?;
                write!(f, ")")?;
            }
        };
        Ok(())
    }
}

/// SQL column definition
impl SQLAnalyse for ColumnDef {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        write!(f, "{} ", self.name)?;
        self.data_type.analyse(f, ctx)?;
        for option in &self.options {
            write!(f, " ")?;
            option.analyse(f, ctx)?;
        }
        Ok(())
    }
}

/// An optionally-named `ColumnOption`: `[ CONSTRAINT <name> ] <column-option>`.
///
/// Note that implementations are substantially more permissive than the ANSI
/// specification on what order column options can be presented in, and whether
/// they are allowed to be named. The specification distinguishes between
/// constraints (NOT NULL, UNIQUE, PRIMARY KEY, and CHECK), which can be named
/// and can appear in any order, and other options (DEFAULT, GENERATED), which
/// cannot be named and must appear in a fixed order. PostgreSQL, however,
/// allows preceding any option with `CONSTRAINT <name>`, even those that are
/// not really constraints, like NULL and DEFAULT. MSSQL is less permissive,
/// allowing DEFAULT, UNIQUE, PRIMARY KEY and CHECK to be named, but not NULL or
/// NOT NULL constraints (the last of which is in violation of the spec).
///
/// For maximum flexibility, we don't distinguish between constraint and
/// non-constraint options, lumping them all together under the umbrella of
/// "column options," and we allow any column option to be named.
impl SQLAnalyse for ColumnOptionDef {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        display_constraint_name(&self.name).analyse(f, ctx)?;
        self.option.analyse(f, ctx)?;
        Ok(())
    }
}

/// `ColumnOption`s are modifiers that follow a column definition in a `CREATE
/// TABLE` statement.
impl SQLAnalyse for ColumnOption {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        use ColumnOption::*;
        match self {
            Null => {
                write!(f, "NULL")?;
            },
            NotNull => {
                write!(f, "NOT NULL")?;
            },
            Default(expr) => {
                write!(f, "DEFAULT ")?;
                expr.analyse(f, ctx)?;
            },
            Unique { is_primary } => {
                write!(f, "{}", if *is_primary { "PRIMARY KEY" } else { "UNIQUE" })?;
            }
            ForeignKey {
                foreign_table,
                referred_columns,
            } => {
                write!(
                    f,
                    "REFERENCES "
                )?;
                foreign_table.analyse(f, ctx)?;
                write!(
                    f,
                    " ("
                )?;
                display_comma_separated(referred_columns).analyse(f, ctx)?;
                write!(
                    f,
                    ")"
                )?;
            },
            Check(expr) => {
                write!(f, "CHECK (")?;
                expr.analyse(f, ctx)?;
                write!(f, ")")?;
            },
        };
        Ok(())
    }
}

fn display_constraint_name<'a>(name: &'a Option<Ident>) -> impl SQLAnalyse + 'a {
    struct ConstraintName<'a>(&'a Option<Ident>);
    impl<'a> SQLAnalyse for ConstraintName<'a> {
        fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
            if let Some(name) = self.0 {
                write!(f, "CONSTRAINT {} ", name)?;
            }
            Ok(())
        }
    }
    ConstraintName(name)
}
