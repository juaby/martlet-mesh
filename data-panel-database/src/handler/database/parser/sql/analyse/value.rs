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

use sqlparser::ast::{DateTimeField, Value};

#[cfg(feature = "bigdecimal")]
use bigdecimal::BigDecimal;

// use std::fmt::Write;
use crate::handler::database::parser::sql::analyse::SQLAnalyse;
use crate::handler::database::parser::sql::SQLStatementContext;

pub type SAResult = data_panel_common::common::Result<()>;

/// Primitive SQL values such as number and string
impl SQLAnalyse for Value {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        match self {
            Value::Number(v, l) => {
                // write!(f, "{}{long}", v, long = if *l { "L" } else { "" })?;
            }
            Value::DoubleQuotedString(v) => {
                // write!(f, "\"{}\"", v)?;
            }
            Value::SingleQuotedString(v) => {
                // write!(f, "'")?;
                escape_single_quote_string(v).analyse(ctx)?;
                // write!(f, "'")?;
            }
            Value::NationalStringLiteral(v) => {
                // write!(f, "N'{}'", v)?;
            }
            Value::HexStringLiteral(v) => {
                // write!(f, "X'{}'", v)?;
            }
            Value::Boolean(v) => {
                // write!(f, "{}", v)?
            }
            Value::Interval {
                value,
                leading_field: Some(DateTimeField::Second),
                leading_precision: Some(leading_precision),
                last_field,
                fractional_seconds_precision: Some(fractional_seconds_precision),
            } => {
                // When the leading field is SECOND, the parser guarantees that
                // the last field is None.
                assert!(last_field.is_none());
                // write!(f, "INTERVAL '")?;
                escape_single_quote_string(value).analyse(ctx)?;
                // write!(
                //     f,
                //     "' SECOND ({}, {})",
                //     leading_precision,
                //     fractional_seconds_precision
                // )?;
            }
            Value::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => {
                // write!(f, "INTERVAL '")?;
                escape_single_quote_string(value).analyse(ctx)?;
                // write!(f, "' ")?;
                if let Some(leading_field) = leading_field {
                    leading_field.analyse(ctx)?;
                }
                if let Some(leading_precision) = leading_precision {
                    // write!(f, " ({})", leading_precision)?;
                }
                if let Some(last_field) = last_field {
                    // write!(f, " TO ")?;
                    last_field.analyse(ctx)?;
                }
                if let Some(fractional_seconds_precision) = fractional_seconds_precision {
                    // write!(f, " ({})", fractional_seconds_precision)?;
                }
            }
            Value::Null => {
                // write!(f, "NULL")?;
            }
        };
        Ok(())
    }
}

impl SQLAnalyse for DateTimeField {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        // f.write_str(match self {
        //     DateTimeField::Year => "YEAR",
        //     DateTimeField::Month => "MONTH",
        //     DateTimeField::Day => "DAY",
        //     DateTimeField::Hour => "HOUR",
        //     DateTimeField::Minute => "MINUTE",
        //     DateTimeField::Second => "SECOND",
        // })?;
        Ok(())
    }
}

pub struct EscapeSingleQuoteString<'a>(&'a str);

impl<'a> SQLAnalyse for EscapeSingleQuoteString<'a> {
    fn analyse(&self, ctx: &mut SQLStatementContext) -> SAResult {
        for c in self.0.chars() {
            if c == '\'' {
                // write!(f, "\'\'")?;
            } else {
                // write!(f, "{}", c)?;
            }
        }
        Ok(())
    }
}

pub fn escape_single_quote_string(s: &str) -> EscapeSingleQuoteString<'_> {
    EscapeSingleQuoteString(s)
}
