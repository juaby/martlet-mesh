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

use sqlparser::ast::{BinaryOperator, UnaryOperator};

use std::fmt;
use std::fmt::Write;
use std::collections::HashMap;
use crate::parser::sqlanalyse::SQLAnalyse;

pub type SAResult = crate::common::Result<()>;

/// Unary operators
impl SQLAnalyse for UnaryOperator {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        f.write_str(match self {
            UnaryOperator::Plus => "+",
            UnaryOperator::Minus => "-",
            UnaryOperator::Not => "NOT",
        })?;
        Ok(())
    }
}

/// Binary operators
impl SQLAnalyse for BinaryOperator {
    fn analyse(&self, f: &mut String, ctx: &HashMap<String, String>) -> SAResult {
        f.write_str(match self {
            BinaryOperator::Plus => "+",
            BinaryOperator::Minus => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
            BinaryOperator::Modulus => "%",
            BinaryOperator::Gt => ">",
            BinaryOperator::Lt => "<",
            BinaryOperator::GtEq => ">=",
            BinaryOperator::LtEq => "<=",
            BinaryOperator::Eq => "=",
            BinaryOperator::NotEq => "<>",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Like => "LIKE",
            BinaryOperator::NotLike => "NOT LIKE",
        })?;
        Ok(())
    }
}