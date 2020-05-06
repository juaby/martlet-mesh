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

#[cfg(feature = "bigdecimal")]
use bigdecimal::BigDecimal;
use sqlparser::ast::{Value, DateTimeField};

use std::fmt;
use std::fmt::Write;
use std::collections::HashMap;
use crate::parser::sqlrewrite::SQLReWrite;

pub type SRWResult = crate::common::Result<()>;

/// Primitive SQL values such as number and string
impl SQLReWrite for Value {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        match self {
            Value::Number(v) => {
                write!(f, "{}", v)?;
            },
            Value::SingleQuotedString(v) => {
                write!(f, "'")?;
                escape_single_quote_string(v).rewrite(f, ctx)?;
                write!(f, "'")?;
            },
            Value::NationalStringLiteral(v) => {
                write!(f, "N'{}'", v)?;
            },
            Value::HexStringLiteral(v) => {
                write!(f, "X'{}'", v)?;
            },
            Value::Boolean(v) => {
                write!(f, "{}", v)?
            },
            Value::Date(v) => {
                write!(f, "DATE '")?;
                escape_single_quote_string(v).rewrite(f, ctx)?;
                write!(f, "'")?;
            },
            Value::Time(v) => {
                write!(f, "TIME '")?;
                escape_single_quote_string(v).rewrite(f, ctx)?;
                write!(f, "'")?;
            },
            Value::Timestamp(v) => {
                write!(f, "TIMESTAMP '")?;
                escape_single_quote_string(v).rewrite(f, ctx)?;
                write!(f, "'")?;
            },
            Value::Interval {
                value,
                leading_field: DateTimeField::Second,
                leading_precision: Some(leading_precision),
                last_field,
                fractional_seconds_precision: Some(fractional_seconds_precision),
            } => {
                // When the leading field is SECOND, the parser guarantees that
                // the last field is None.
                assert!(last_field.is_none());
                write!(
                    f,
                    "INTERVAL '"
                )?;
                escape_single_quote_string(value).rewrite(f, ctx)?;
                write!(
                    f,
                    "' SECOND ({}, {})",
                    leading_precision,
                    fractional_seconds_precision
                )?;
            }
            Value::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => {
                write!(
                    f,
                    "INTERVAL '"
                )?;
                escape_single_quote_string(value).rewrite(f, ctx)?;
                write!(
                    f,
                    "' "
                )?;
                leading_field.rewrite(f, ctx)?;
                if let Some(leading_precision) = leading_precision {
                    write!(f, " ({})", leading_precision)?;
                }
                if let Some(last_field) = last_field {
                    write!(f, " TO ")?;
                    last_field.rewrite(f, ctx)?;
                }
                if let Some(fractional_seconds_precision) = fractional_seconds_precision {
                    write!(f, " ({})", fractional_seconds_precision)?;
                }
            }
            Value::Null => {
                write!(f, "NULL")?;
            },
        };
        Ok(())
    }
}

impl SQLReWrite for DateTimeField {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        f.write_str(match self {
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Day => "DAY",
            DateTimeField::Hour => "HOUR",
            DateTimeField::Minute => "MINUTE",
            DateTimeField::Second => "SECOND",
        })?;
        Ok(())
    }
}

pub struct EscapeSingleQuoteString<'a>(&'a str);

impl<'a> SQLReWrite for EscapeSingleQuoteString<'a> {
    fn rewrite(&self, f: &mut String, ctx: &HashMap<String, String>) -> SRWResult {
        for c in self.0.chars() {
            if c == '\'' {
                write!(f, "\'\'")?;
            } else {
                write!(f, "{}", c)?;
            }
        }
        Ok(())
    }
}

pub fn escape_single_quote_string(s: &str) -> EscapeSingleQuoteString<'_> {
    EscapeSingleQuoteString(s)
}
