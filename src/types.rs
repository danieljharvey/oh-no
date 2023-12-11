use core::fmt;
use core::fmt::Display;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableName(pub String);

impl Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct Select {
    pub table: TableName,
    pub columns: SelectColumns,
    pub r#where: Expression,
}

pub enum SelectColumns {
    SelectConstructor{constructor:String,columns:Vec<String>},
    SelectColumns{columns:Vec<String>}
}

pub struct Insert {
    pub table: TableName,
    pub key: i32,
    pub value: Value,
}

#[derive(Debug, PartialEq)]
pub enum Function {
    Equals,
    And,
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Column(String),
    Const(serde_json::Value),
    BinaryFunction {
        function: Function,
        expr_left: Box<Expression>,
        expr_right: Box<Expression>,
    },
}

pub fn equals(left: Expression, right:Expression) -> Expression {
    Expression::BinaryFunction {
        function:Function::Equals,
        expr_left: Box::new(left),
        expr_right: Box::new(right)
    }
}


pub fn and(left: Expression, right:Expression) -> Expression {
    Expression::BinaryFunction {
        function:Function::And,
        expr_left: Box::new(left),
        expr_right: Box::new(right)
    }
}

#[derive(Debug, PartialEq)]
pub enum SelectError {
    TypeError(TypeError),
    TableNotFound(TableName),
}

#[derive(Debug, PartialEq)]
pub enum TypeError {
    ColumnNotFound {
        table_name: TableName,
        column_name: String,
    },
    ColumnMismatch {
        table_name: TableName,
        column_name: String,
        left: ScalarType,
        right: ScalarType,
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

#[derive(Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Columns,
}

#[derive(Serialize, Deserialize)]
pub enum Columns {
    SingleConstructor(BTreeMap<String, ScalarType>),
    MultipleConstructors(BTreeMap<String, BTreeMap<String, ScalarType>>),
}

pub fn bool_expr(bool: bool) -> Expression {
    Expression::Const(serde_json::Value::Bool(bool))
}
