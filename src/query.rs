use super::data::lookup_table;
use super::typecheck::select::typecheck_select;
use super::types::{and, equals, Expression, Function, Select, SelectColumns, SelectError};
use rocksdb::DB;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

pub fn empty_where() -> Expression {
    Expression::Const(serde_json::Value::Bool(true))
}

fn matches_prefix(prefix: &str, key: &[u8]) -> bool {
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

fn add_constructor_to_expression(
    columns: SelectColumns,
    r#where: Expression,
) -> (Expression, Vec<String>) {
    match columns {
        SelectColumns::SelectColumns { columns } => (r#where, columns),
        SelectColumns::SelectConstructor {
            constructor,
            columns,
        } => (
            and(
                r#where,
                equals(
                    Expression::Column("_type".to_string()),
                    Expression::Const(serde_json::Value::String(constructor.clone())),
                ),
            ),
            columns,
        ),
    }
}

pub fn select(db: &DB, select: Select) -> Result<Vec<(usize, Value)>, SelectError> {
    let table = match lookup_table(db, &select.table) {
        Some(table) => Ok(table),
        None => Err(SelectError::TableNotFound(select.table.clone())),
    }?;

    let mut tables = BTreeMap::new();
    tables.insert(select.table.clone(), table);

    typecheck_select(&tables, &select).map_err(SelectError::TypeError)?;

    let prefix = format!("data_{}_", select.table);
    let iter = db.prefix_iterator(prefix.clone());
    let mut results = vec![];

    // if we are using a constructor to match, add it to where clause
    let (expression, columns) = add_constructor_to_expression(select.columns, select.r#where);

    for (index, item) in iter.enumerate() {
        let (key, value) = item.unwrap();

        // prefix_iterator chucks in things we don't want, filter them out
        if !matches_prefix(&prefix, &key) {
            continue;
        }

        let val_string = std::str::from_utf8(&value).unwrap();
        let json = serde_json::Value::from_str(val_string).unwrap();

        if is_true(apply_expression(&json, &expression)) {
            let json_object = json.as_object().unwrap();
            // collect only the columns we care about
            let mut output = serde_json::Map::new();

            // only the columns we like
            for column in &columns {
                // if we can't find the value, return `null`
                // the typechecker should have worked out if this should happen or not
                let item = json_object.get(column).cloned().unwrap_or(Value::Null);
                output.insert(column.clone(), item);
            }

            let json_value = serde_json::Value::Object(output);

            results.push((index + 1, json_value));
        }
    }
    Ok(results)
}

fn is_true(expression: Expression) -> bool {
    matches!(expression, Expression::Const(serde_json::Value::Bool(true)))
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
        and, equals, Columns, Expression, Insert, ScalarType, Select, SelectColumns, SelectError,
        Table, TableName, TypeError,
    };
    use super::{empty_where, select};
    use rocksdb::{Options, DB};
    use serde_json::Value;
    use std::collections::BTreeMap;

    fn insert_test_data(db: &DB) {
        insert_user_data(db);
        insert_pet_data(db); // this breaks users tests
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
        insert(
            &db,
            Insert {
                table: TableName("pet".to_string()),
                key: 1,
                value: serde_json::from_str("{\"_type\":\"cat\",\"age\":27,\"name\":\"Mr Cat\"}")
                    .unwrap(),
            },
        );

        insert(
            &db,
            Insert {
                table: TableName("pet".to_string()),
                key: 2,
                value: serde_json::from_str(
                    "{\"_type\":\"dog\",\"age\":21,\"name\":\"Mr Dog\",\"likes_stick\":true}",
                )
                .unwrap(),
            },
        );
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
                        columns: SelectColumns::SelectColumns {
                            columns: vec!["name".to_string()]
                        },
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
                        columns: SelectColumns::SelectColumns {
                            columns: vec!["missing".to_string()]
                        },
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
                        columns: SelectColumns::SelectColumns { columns: vec![] },
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
                        columns: SelectColumns::SelectColumns {
                            columns: vec!["name".to_string()]
                        },
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
                        columns: SelectColumns::SelectColumns {
                            columns: vec!["name".to_string()]
                        },
                        r#where: and(
                            Expression::Column("nice".to_string()),
                            equals(
                                Expression::Column("age".to_string()),
                                Expression::Const(Value::Number(serde_json::Number::from(100)))
                            )
                        )
                    }
                ),
                Ok(expected)
            );
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn test_get_cats() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db);

            let expected = vec![(
                1,
                serde_json::from_str("{\"age\":27,\"name\":\"Mr Cat\"}").unwrap(),
            )];

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("pet".to_string()),
                        columns: SelectColumns::SelectConstructor {
                            constructor: "cat".to_string(),
                            columns: vec!["age".to_string(), "name".to_string()]
                        },
                        r#where: empty_where()
                    }
                ),
                Ok(expected)
            );
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn test_get_pets() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db);

            let expected = vec![
                (
                    1,
                    serde_json::from_str("{\"age\":27,\"name\":\"Mr Cat\"}").unwrap(),
                ),
                (
                    2,
                    serde_json::from_str("{\"age\":21,\"name\":\"Mr Dog\"}").unwrap(),
                ),
            ];

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("pet".to_string()),
                        columns: SelectColumns::SelectColumns {
                            columns: vec!["age".to_string(), "name".to_string()]
                        },
                        r#where: empty_where()
                    }
                ),
                Ok(expected)
            );
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn test_get_pets_with_nullable() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db);

            let expected = vec![
                (
                    1,
                    serde_json::from_str("{\"age\":27,\"name\":\"Mr Cat\", \"likes_stick\":null}")
                        .unwrap(),
                ),
                (
                    2,
                    serde_json::from_str("{\"age\":21,\"name\":\"Mr Dog\", \"likes_stick\": true}")
                        .unwrap(),
                ),
            ];

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("pet".to_string()),
                        columns: SelectColumns::SelectColumns {
                            columns: vec![
                                "age".to_string(),
                                "name".to_string(),
                                "likes_stick".to_string()
                            ]
                        },
                        r#where: empty_where()
                    }
                ),
                Ok(expected)
            );
        }
        let _ = DB::destroy(&Options::default(), path);
    }
}
