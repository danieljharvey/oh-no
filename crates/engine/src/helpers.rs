use super::data::lookup_table;
use engine_core::typecheck_select;
use engine_core::{
    and, equals, ColumnName, Comparison, Expression, Function, ScalarValue, Select, SelectColumns,
    SelectError,
};
use rocksdb::DB;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

pub fn empty_where() -> Expression {
    Expression::Bool(true)
}

pub fn matches_prefix(prefix: &str, key: &[u8]) -> bool {
    let key_string = std::str::from_utf8(key).unwrap();
    let prefix_len = prefix.len();
    // only do check if key is longer than prefix
    if prefix_len < key_string.len() {
        let key_start = &key_string.get(0..prefix.len()).unwrap();
        key_start == &prefix
    } else {
        false
    }
}

pub fn add_constructor_to_expression(
    columns: SelectColumns,
    r#where: Expression,
) -> (Expression, Vec<ColumnName>) {
    match columns {
        SelectColumns::SelectColumns { columns } => (r#where, columns),
        SelectColumns::SelectConstructor {
            constructor,
            columns,
        } => (
            and(
                r#where,
                equals(
                    ColumnName("_type".to_string()),
                    ScalarValue::String(constructor.to_string()),
                ),
            ),
            columns,
        ),
    }
}

pub fn is_true(expression: &Expression) -> bool {
    matches!(expression, Expression::Bool(true))
}

fn bool_expr(bool: bool) -> Expression {
    Expression::Bool(bool)
}

pub fn to_serde_json(scalar_value: &ScalarValue) -> serde_json::Value {
    match scalar_value {
        ScalarValue::Int(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        ScalarValue::Bool(b) => serde_json::Value::Bool(*b),
        ScalarValue::String(s) => serde_json::Value::String(s.clone()),
        ScalarValue::Null => serde_json::Value::Null,
    }
}

// given a row and an expression, evaluate it
pub fn apply_expression(result: &serde_json::Value, expression: &Expression) -> Expression {
    match expression {
        Expression::Comparison(Comparison { column, value }) => {
            let json_object = result.as_object().unwrap();
            let column_value = json_object.get(&column.to_string()).unwrap();
            let json_value = to_serde_json(value);
            bool_expr(*column_value == json_value)
        }
        Expression::BinaryFunction {
            function,
            expr_left,
            expr_right,
        } => match function {
            Function::And => {
                if is_true(&apply_expression(result, expr_left)) {
                    apply_expression(result, expr_right)
                } else {
                    bool_expr(false)
                }
            }
        },
        Expression::Bool(bool) => Expression::Bool(*bool),
    }
}
