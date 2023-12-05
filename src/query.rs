use super::data::lookup_table;
use super::typecheck::typecheck_select;
use super::types::{Expression, Function, Select, SelectError};
use rocksdb::DB;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

pub fn empty_where() -> Expression {
    Expression::Const(serde_json::Value::Bool(true))
}

fn matches_prefix(prefix: &String, key: &[u8]) -> bool {
    println!("prefix {}", prefix);
    let key_string = std::str::from_utf8(&key).unwrap();
    let key_start = &key_string.get(0..prefix.len().into()).unwrap();
    key_start == &prefix.as_str()
}

pub fn select(db: &DB, select: Select) -> Result<Vec<(usize, Value)>, SelectError> {
    let table = match lookup_table(&db, &select.table) {
        Some(table) => Ok(table),
        None => Err(SelectError::TableNotFound(select.table.clone())),
    }?;

    let mut tables = BTreeMap::new();
    tables.insert(select.table.clone(), table);

    typecheck_select(&tables, &select).map_err(SelectError::TypeError)?;

    let prefix = format!("data_{}_", select.table);
    let iter = db.prefix_iterator(prefix.clone());
    let mut results = vec![];
    for (index, item) in iter.enumerate() {
        let (key, value) = item.unwrap();

        // prefix_iterator chucks in things we don't want, filter them out
        if !matches_prefix(&prefix, &key) {
            continue;
        }

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
    Ok(results)
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

#[cfg(test)]
mod testing {
    use super::super::data::{insert, insert_table};
    use super::super::types::{
        Columns, Expression, Function, Insert, ScalarType, Select, SelectError, Table, TableName,
        TypeError,
    };
    use super::{empty_where, select};
    use rocksdb::{Options, DB};
    use serde_json::Value;
    use std::collections::BTreeMap;

    fn insert_test_data(db: &DB) {
        insert_user_data(db);
        // insert_pet_data(db); // this breaks users tests
    }

    fn insert_pet_data(db: &DB) {
        let mut pet_columns = BTreeMap::new();
        pet_columns.insert("age".to_string(), ScalarType::Int);
        pet_columns.insert("name".to_string(), ScalarType::String);

        let mut constructors = BTreeMap::new();
        constructors.insert("cat".to_string(), pet_columns.clone());

        pet_columns.insert("likes_stick".to_string(), ScalarType::Bool);
        constructors.insert("dog".to_string(), pet_columns);

        insert_table(
            &db,
            Table {
                name: "pet".to_string(),
                columns: Columns::MultipleConstructors(constructors),
            },
        );
        /*
                insert(
                    &db,
                    Insert {
                        table: TableName("user".to_string()),
                        key: 1,
                        value: serde_json::from_str("{\"age\":27,\"nice\":false,\"name\":\"Egg\"}")
                            .unwrap(),
                    },
                );
                insert(
                    &db,
                    Insert {
                        table: TableName("user".to_string()),
                        key: 2,
                        value: serde_json::from_str("{\"age\":100,\"nice\":true,\"name\":\"Horse\"}")
                            .unwrap(),
                    },
                );
                insert(
                    &db,
                    Insert {
                        table: TableName("user".to_string()),
                        key: 3,
                        value: serde_json::from_str("{\"age\":46,\"nice\":false,\"name\":\"Log\"}")
                            .unwrap(),
                    },
                );
        */
    }

    fn insert_user_data(db: &DB) {
        let mut user_columns = BTreeMap::new();
        user_columns.insert("age".to_string(), ScalarType::Int);
        user_columns.insert("nice".to_string(), ScalarType::Bool);
        user_columns.insert("name".to_string(), ScalarType::String);

        insert_table(
            &db,
            Table {
                name: "user".to_string(),
                columns: Columns::SingleConstructor(user_columns),
            },
        );

        insert(
            &db,
            Insert {
                table: TableName("user".to_string()),
                key: 1,
                value: serde_json::from_str("{\"age\":27,\"nice\":false,\"name\":\"Egg\"}")
                    .unwrap(),
            },
        );
        insert(
            &db,
            Insert {
                table: TableName("user".to_string()),
                key: 2,
                value: serde_json::from_str("{\"age\":100,\"nice\":true,\"name\":\"Horse\"}")
                    .unwrap(),
            },
        );
        insert(
            &db,
            Insert {
                table: TableName("user".to_string()),
                key: 3,
                value: serde_json::from_str("{\"age\":46,\"nice\":false,\"name\":\"Log\"}")
                    .unwrap(),
            },
        );
    }

    #[test]
    fn test_missing_table() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db);

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("missing".to_string()),
                        columns: vec!["name".to_string()],
                        r#where: empty_where()
                    }
                ),
                Err(SelectError::TableNotFound(TableName("missing".to_string())))
            )
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn test_missing_column() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db);

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("user".to_string()),
                        columns: vec!["missing".to_string()],
                        r#where: empty_where()
                    }
                ),
                Err(SelectError::TypeError(TypeError::ColumnNotFound {
                    column_name: "missing".to_string(),
                    table_name: TableName("user".to_string())
                }))
            )
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn test_missing_column_in_where() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db);

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("user".to_string()),
                        columns: vec![],
                        r#where: Expression::Column("missing".to_string())
                    }
                ),
                Err(SelectError::TypeError(TypeError::ColumnNotFound {
                    column_name: "missing".to_string(),
                    table_name: TableName("user".to_string())
                }))
            )
        }
        let _ = DB::destroy(&Options::default(), path);
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
                        table: TableName("user".to_string()),
                        columns: vec!["name".to_string()],
                        r#where: empty_where()
                    }
                ),
                Ok(expected)
            );
        }
        let _ = DB::destroy(&Options::default(), path);
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
                        table: TableName("user".to_string()),
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
                Ok(expected)
            );
        }
        let _ = DB::destroy(&Options::default(), path);
    }
}
