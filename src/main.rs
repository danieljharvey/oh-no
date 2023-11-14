use rocksdb::{Options, DB};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

fn main() {}

struct Select {
    table: String,
    columns: Vec<String>,
    r#where: Expression,
}

#[derive(Debug, PartialEq)]
enum Function {
    Equals,
    And,
}

#[derive(Debug, PartialEq)]
enum Expression {
    Column(String),
    Const(serde_json::Value),
    BinaryFunction {
        function: Function,
        expr_left: Box<Expression>,
        expr_right: Box<Expression>,
    },
}

#[derive(Serialize, Deserialize)]
enum ScalarType {
    String,
    Bool,
    Int,
}

#[derive(Serialize, Deserialize)]
struct Table {
    name: String,
    columns: BTreeMap<String, ScalarType>,
}

fn empty_where() -> Expression {
    Expression::Const(serde_json::Value::Bool(true))
}

struct Insert {
    table: String,
    key: i32,
    value: Value,
}

fn select(db: &DB, select: Select) -> Vec<(usize, Value)> {
    let prefix = format!("data_{}_", select.table);
    let iter = db.prefix_iterator(prefix);
    let mut results = vec![];
    for (index, item) in iter.enumerate() {
        let (_key, value) = item.unwrap();
        let val_string = std::str::from_utf8(&value).unwrap();
        let json = serde_json::Value::from_str(val_string).unwrap();

        if is_true(apply_expression(&json, &select.r#where)) {
            let json_object = json.as_object().unwrap();
            // collect only the columns we care about
            let mut output = serde_json::Map::new();

            // only the columns we like
            for column in &select.columns {
                output.insert(column.clone(), json_object.get(column).unwrap().clone());
            }

            let json_value = serde_json::Value::Object(output);

            results.push((index + 1, json_value));
        }
    }
    results
}

fn is_true(expression: Expression) -> bool {
    match expression {
        Expression::Const(serde_json::Value::Bool(true)) => true,
        _ => false,
    }
}

fn bool_expr(bool: bool) -> Expression {
    Expression::Const(serde_json::Value::Bool(bool))
}

// given a row and an expression, evaluate it
fn apply_expression(result: &serde_json::Value, expression: &Expression) -> Expression {
    match expression {
        Expression::Column(column_name) => {
            let json_object = result.as_object().unwrap();
            let inner = json_object.get(column_name).unwrap();
            Expression::Const(inner.clone())
        }
        Expression::BinaryFunction {
            function,
            expr_left,
            expr_right,
        } => match function {
            Function::Equals => {
                let left2 = apply_expression(result, expr_left);
                let right2 = apply_expression(result, expr_right);
                bool_expr(left2 == right2)
            }
            Function::And => {
                if is_true(apply_expression(result, expr_left)) {
                    apply_expression(result, expr_right)
                } else {
                    bool_expr(false)
                }
            }
        },
        Expression::Const(value) => Expression::Const(value.clone()),
    }
}

fn insert(db: &DB, insert: Insert) {
    let key = format!("data_{}_{}", insert.table, insert.key);
    let _ = db.put(key, serde_json::to_string(&insert.value).unwrap());
}

fn insert_table(db: &DB, table: Table) {
    let key = format!("table_{}", table.name);
    let _ = db.put(key, serde_json::to_string(&table).unwrap());
}

fn lookup_table(db: &DB, table_name: String) {
    todo!("implement me!")
}

#[test]
fn test_get_users() {
    let path = format!("./test_storage{}", rand::random::<i32>());
    {
        let db = DB::open_default(path.clone()).unwrap();
        insert_test_data(&db);

        let expected = vec![
            (1, serde_json::from_str("{\"name\":\"Egg\"}").unwrap()),
            (2, serde_json::from_str("{\"name\":\"Horse\"}").unwrap()),
            (3, serde_json::from_str("{\"name\":\"Log\"}").unwrap()),
        ];

        assert_eq!(
            select(
                &db,
                Select {
                    table: "user".to_string(),
                    columns: vec!["name".to_string()],
                    r#where: empty_where()
                }
            ),
            expected
        );
    }
    let _ = DB::destroy(&Options::default(), path);
}

fn insert_test_data(db: &DB) {
    insert(
        &db,
        Insert {
            table: "user".to_string(),
            key: 1,
            value: serde_json::from_str("{\"age\":27,\"nice\":false,\"name\":\"Egg\"}").unwrap(),
        },
    );
    insert(
        &db,
        Insert {
            table: "user".to_string(),
            key: 2,
            value: serde_json::from_str("{\"age\":100,\"nice\":true,\"name\":\"Horse\"}").unwrap(),
        },
    );
    insert(
        &db,
        Insert {
            table: "user".to_string(),
            key: 3,
            value: serde_json::from_str("{\"age\":46,\"nice\":false,\"name\":\"Log\"}").unwrap(),
        },
    );
}

#[test]
fn test_get_users_where() {
    let path = format!("./test_storage{}", rand::random::<i32>());
    {
        let db = DB::open_default(path.clone()).unwrap();
        insert_test_data(&db);

        let expected = vec![(2, serde_json::from_str("{\"name\":\"Horse\"}").unwrap())];

        assert_eq!(
            select(
                &db,
                Select {
                    table: "user".to_string(),
                    columns: vec!["name".to_string()],
                    r#where: Expression::BinaryFunction {
                        function: Function::And,
                        expr_left: Box::new(Expression::Column("nice".to_string())),
                        expr_right: Box::new(Expression::BinaryFunction {
                            function: Function::Equals,
                            expr_left: Box::new(Expression::Column("age".to_string())),
                            expr_right: Box::new(Expression::Const(Value::Number(
                                serde_json::Number::from(100)
                            )))
                        })
                    }
                }
            ),
            expected
        );
    }
    let _ = DB::destroy(&Options::default(), path);
}
