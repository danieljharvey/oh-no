use core::fmt;
use core::fmt::Display;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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

pub struct Select {
    pub table: TableName,
    pub columns: SelectColumns,
    pub r#where: Expression,
}

#[derive(Debug, PartialEq)]
pub enum SelectColumns {
    SelectConstructor {
        constructor: String,
        columns: Vec<ColumnName>,
    },
    SelectColumns {
        columns: Vec<ColumnName>,
    },
}

#[derive(Debug, PartialEq)]
pub struct Insert {
    pub table: TableName,
    pub key: i32,
    pub value: BTreeMap<ColumnName, Value>,
}

#[derive(Debug, PartialEq)]
pub enum Function {
    Equals,
    And,
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Column(ColumnName),
    Const(serde_json::Value),
    BinaryFunction {
        function: Function,
        expr_left: Box<Expression>,
        expr_right: Box<Expression>,
    },
}

pub fn equals(left: Expression, right: Expression) -> Expression {
    Expression::BinaryFunction {
        function: Function::Equals,
        expr_left: Box::new(left),
        expr_right: Box::new(right),
    }
}

pub fn and(left: Expression, right: Expression) -> Expression {
    Expression::BinaryFunction {
        function: Function::And,
        expr_left: Box::new(left),
        expr_right: Box::new(right),
    }
}

#[derive(Debug, PartialEq)]
pub enum SelectError {
    TypeError(TypeError),
    TableNotFound(TableName),
}

#[derive(Debug, PartialEq)]
pub enum TypeError {
    TableNotFound(TableName),
    ColumnNotFound {
        table_name: TableName,
        column_name: ColumnName,
    },
    ColumnMismatch {
        table_name: TableName,
        column_name: ColumnName,
        left: ScalarType,
        right: ScalarType,
    },
    MissingColumnInInput {
        table_name: TableName,
        column_name: ColumnName,
    },
    TypeMismatchInInput {
        expected_type: Type,
        input_value: Value,
    },
    UnknownScalarTypeForValue {
        value: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ScalarType {
    String,
    Bool,
    Int,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Type {
    Optional(Box<Type>),
    ScalarType(ScalarType),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Columns,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Columns {
    SingleConstructor(BTreeMap<ColumnName, ScalarType>),
    MultipleConstructors(BTreeMap<String, BTreeMap<ColumnName, ScalarType>>),
}

pub fn bool_expr(bool: bool) -> Expression {
    Expression::Const(serde_json::Value::Bool(bool))
}
