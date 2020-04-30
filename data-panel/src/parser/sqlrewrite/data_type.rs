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

use std::fmt;
use sqlparser::ast::DataType;

/// SQL data types
impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::Char(size) => format_type_with_optional_length(f, "char", size),
            DataType::Varchar(size) => {
                format_type_with_optional_length(f, "character varying", size)
            }
            DataType::Uuid => write!(f, "uuid"),
            DataType::Clob(size) => write!(f, "clob({})", size),
            DataType::Binary(size) => write!(f, "binary({})", size),
            DataType::Varbinary(size) => write!(f, "varbinary({})", size),
            DataType::Blob(size) => write!(f, "blob({})", size),
            DataType::Decimal(precision, scale) => {
                if let Some(scale) = scale {
                    write!(f, "numeric({},{})", precision.unwrap(), scale)
                } else {
                    format_type_with_optional_length(f, "numeric", precision)
                }
            }
            DataType::Float(size) => format_type_with_optional_length(f, "float", size),
            DataType::SmallInt => write!(f, "smallint"),
            DataType::Int => write!(f, "int"),
            DataType::BigInt => write!(f, "bigint"),
            DataType::Real => write!(f, "real"),
            DataType::Double => write!(f, "double"),
            DataType::Boolean => write!(f, "boolean"),
            DataType::Date => write!(f, "date"),
            DataType::Time => write!(f, "time"),
            DataType::Timestamp => write!(f, "timestamp"),
            DataType::Interval => write!(f, "interval"),
            DataType::Regclass => write!(f, "regclass"),
            DataType::Text => write!(f, "text"),
            DataType::Bytea => write!(f, "bytea"),
            DataType::Array(ty) => write!(f, "{}[]", ty),
            DataType::Custom(ty) => write!(f, "{}", ty),
        }
    }
}

fn format_type_with_optional_length(
    f: &mut fmt::Formatter,
    sql_type: &'static str,
    len: &Option<u64>,
) -> fmt::Result {
    write!(f, "{}", sql_type)?;
    if let Some(len) = len {
        write!(f, "({})", len)?;
    }
    Ok(())
}
