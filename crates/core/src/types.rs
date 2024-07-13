use serde_derive::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Display;

#[derive(Serialize, Deserialize, Hash, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableName(pub String);

impl Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Hash, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnName(pub String);

impl Display for ColumnName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Hash, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Constructor(pub String);

impl Display for Constructor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, PartialEq)]
pub struct Select {
    pub table: TableName,
    pub columns: SelectColumns,
    pub r#where: Expression,
}

#[derive(Debug, PartialEq)]
pub enum SelectColumns {
    SelectConstructor {
        constructor: Constructor,
        columns: Vec<ColumnName>,
    },
    SelectColumns {
        columns: Vec<ColumnName>,
    },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum InsertValue {
    Single {
        values: BTreeMap<ColumnName, ScalarValue>,
    },
    Multiple {
        #[serde(rename = "_type")]
        constructor: Constructor,
        values: BTreeMap<ColumnName, ScalarValue>,
    },
}

#[derive(Debug, PartialEq)]
pub struct Insert {
    pub table: TableName,
    pub key: i32,
    pub value: InsertValue,
}

#[derive(Debug, PartialEq)]
pub enum Function {
    And,
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Comparison(Comparison),
    Bool(bool),
    BinaryFunction {
        function: Function,
        expr_left: Box<Expression>,
        expr_right: Box<Expression>,
    },
}

#[derive(Debug, PartialEq)]
pub struct Comparison {
    pub column: ColumnName,
    pub value: ScalarValue,
}

pub fn equals(column: ColumnName, value: ScalarValue) -> Expression {
    Expression::Comparison(Comparison { column, value })
}

pub fn and(left: Expression, right: Expression) -> Expression {
    Expression::BinaryFunction {
        function: Function::And,
        expr_left: Box::new(left),
        expr_right: Box::new(right),
    }
}

pub fn bool_expr(bool: bool) -> Expression {
    Expression::Bool(bool)
}

#[derive(Debug, PartialEq)]
pub enum SelectError {
    TypeError(TypeError),
    TableNotFound(TableName),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum InsertError {
    #[error("{0}")]
    TypeError(TypeError),
    #[error("table not found: {0}")]
    TableNotFound(TableName),
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum TypeError {
    #[error("table not found: {0}")]
    TableNotFound(TableName),
    #[error("column {column_name:} not found in table {table_name:}")]
    ColumnNotFound {
        table_name: TableName,
        column_name: ColumnName,
    },
    #[error(
        "type mismatch in column {column_name:} in table {table_name:}: {left:?} vs {right:?}"
    )]
    ColumnMismatch {
        table_name: TableName,
        column_name: ColumnName,
        left: ScalarType,
        right: ScalarType,
    },
    #[error("missing column {column_name:} when inserting into table {table_name:}")]
    MissingColumnInInput {
        table_name: TableName,
        column_name: ColumnName,
    },
    #[error("expected type {expected_type:?} but found value {input_value:?}")]
    TypeMismatchInInput {
        expected_type: Type,
        input_value: ScalarValue,
    },
    #[error("unknown scalar type for value {value:?}")]
    UnknownScalarTypeForValue { value: ScalarValue },
    #[error("constructor not specified when inserting into table {table:}")]
    ConstructorNotSpecified { table: TableName },
    #[error("constructor specified when inserting into table {table:} but it is not required")]
    ConstructorSpecifiedButNotRequired { table: TableName },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ScalarType {
    String,
    Bool,
    Int,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
    Null,
    String(String),
    Bool(bool),
    Int(i32),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Type {
    Optional(Box<Type>),
    ScalarType(ScalarType),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: TableName,
    pub columns: Columns,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Columns {
    SingleConstructor(BTreeMap<ColumnName, ScalarType>),
    MultipleConstructors(BTreeMap<Constructor, BTreeMap<ColumnName, ScalarType>>),
}
