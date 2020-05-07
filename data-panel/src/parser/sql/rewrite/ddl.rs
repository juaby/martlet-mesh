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
use crate::parser::sql::rewrite::{display_comma_separated, SQLReWrite};

use std::fmt::Write;
use std::collections::HashMap;

pub type SRWResult = crate::common::Result<()>;

/// An `ALTER TABLE` (`Statement::AlterTable`) operation
impl SQLReWrite for AlterTableOperation {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self {
            AlterTableOperation::AddConstraint(c) => {
                write!(f, "ADD ")?;
                c.rewrite(f, ctx)?;
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
impl SQLReWrite for TableConstraint {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
            } => {
                display_constraint_name(name).rewrite(f, ctx)?;
                write!(
                    f,
                    "{} (",
                    if *is_primary { "PRIMARY KEY" } else { "UNIQUE" }
                )?;
                display_comma_separated(columns).rewrite(f, ctx)?;
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
                display_constraint_name(name).rewrite(f, ctx)?;
                write!(
                    f,
                    "FOREIGN KEY ("
                )?;
                display_comma_separated(columns).rewrite(f, ctx)?;
                write!(
                    f,
                    ") REFERENCES "
                )?;
                foreign_table.rewrite(f, ctx)?;
                write!(
                    f,
                    "("
                )?;
                display_comma_separated(referred_columns).rewrite(f, ctx)?;
                write!(
                    f,
                    ")"
                )?;
            },
            TableConstraint::Check { name, expr } => {
                display_constraint_name(name).rewrite(f, ctx)?;
                write!(f, "CHECK (")?;
                expr.rewrite(f, ctx)?;
                write!(f, ")")?;
            }
        };
        Ok(())
    }
}

/// SQL column definition
impl SQLReWrite for ColumnDef {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        write!(f, "{} ", self.name)?;
        self.data_type.rewrite(f, ctx)?;
        for option in &self.options {
            write!(f, " ")?;
            option.rewrite(f, ctx)?;
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
impl SQLReWrite for ColumnOptionDef {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        display_constraint_name(&self.name).rewrite(f, ctx)?;
        self.option.rewrite(f, ctx)?;
        Ok(())
    }
}

/// `ColumnOption`s are modifiers that follow a column definition in a `CREATE
/// TABLE` statement.
impl SQLReWrite for ColumnOption {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
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
                expr.rewrite(f, ctx)?;
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
                foreign_table.rewrite(f, ctx)?;
                write!(
                    f,
                    " ("
                )?;
                display_comma_separated(referred_columns).rewrite(f, ctx)?;
                write!(
                    f,
                    ")"
                )?;
            },
            Check(expr) => {
                write!(f, "CHECK (")?;
                expr.rewrite(f, ctx)?;
                write!(f, ")")?;
            },
        };
        Ok(())
    }
}

fn display_constraint_name<'a>(name: &'a Option<Ident>) -> impl SQLReWrite + 'a {
    struct ConstraintName<'a>(&'a Option<Ident>);
    impl<'a> SQLReWrite for ConstraintName<'a> {
        fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
            if let Some(name) = self.0 {
                write!(f, "CONSTRAINT {} ", name)?;
            }
            Ok(())
        }
    }
    ConstraintName(name)
}
