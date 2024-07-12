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

        if is_true(&apply_expression(&json, &expression)) {
            let json_object = json.as_object().unwrap();
            // collect only the columns we care about
            let mut output = serde_json::Map::new();

            // only the columns we like
            for column in &columns {
                // if we can't find the value, return `null`
                // the typechecker should have worked out if this should happen or not
                let item = json_object
                    .get(&column.to_string())
                    .cloned()
                    .unwrap_or(Value::Null);
                output.insert(column.to_string(), item);
            }

            let json_value = serde_json::Value::Object(output);

            results.push((index + 1, json_value));
        }
    }
    Ok(results)
}

fn is_true(expression: &Expression) -> bool {
    matches!(expression, Expression::Bool(true))
}

fn bool_expr(bool: bool) -> Expression {
    Expression::Bool(bool)
}

fn to_serde_json(scalar_value: &ScalarValue) -> serde_json::Value {
    match scalar_value {
        ScalarValue::Int(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        ScalarValue::Bool(b) => serde_json::Value::Bool(*b),
        ScalarValue::String(s) => serde_json::Value::String(s.clone()),
    }
}

// given a row and an expression, evaluate it
fn apply_expression(result: &serde_json::Value, expression: &Expression) -> Expression {
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

#[cfg(test)]
mod testing {
    use crate::data::insert_table;
    use crate::query::select;
    use engine_core::{
        and, empty_where, equals, ColumnName, Columns, Constructor, Insert, InsertValue,
        ScalarType, ScalarValue, Select, SelectColumns, SelectError, Table, TableName, TypeError,
    };
    use rocksdb::{Options, DB};
    use serde_json::Value;
    use std::collections::BTreeMap;

    fn insert_test_data(db: &DB) -> anyhow::Result<()> {
        let _ = insert_user_data(db);
        insert_pet_data(db)
    }

    fn insert_pet_data(db: &DB) -> anyhow::Result<()> {
        let mut pet_columns = BTreeMap::new();
        pet_columns.insert(ColumnName("age".to_string()), ScalarType::Int);
        pet_columns.insert(ColumnName("name".to_string()), ScalarType::String);

        let mut constructors = BTreeMap::new();
        constructors.insert(Constructor("cat".to_string()), pet_columns.clone());

        pet_columns.insert(ColumnName("likes_stick".to_string()), ScalarType::Bool);

        constructors.insert(Constructor("dog".to_string()), pet_columns);

        insert_table(
            &db,
            &Table {
                name: TableName("pet".to_string()),
                columns: Columns::MultipleConstructors(constructors),
            },
        );

        let mut cat_row = BTreeMap::new();
        cat_row.insert(ColumnName("age".to_string()), Value::Number(27.into()));
        cat_row.insert(
            ColumnName("name".to_string()),
            Value::String("Mr Cat".into()),
        );

        let _ = crate::insert::insert(
            &db,
            &Insert {
                table: TableName("pet".to_string()),
                key: 1,
                value: InsertValue::Multiple {
                    constructor: Constructor("cat".into()),
                    values: cat_row,
                },
            },
        )?;

        let mut dog_row = BTreeMap::new();
        dog_row.insert(ColumnName("age".to_string()), Value::Number(21.into()));
        dog_row.insert(
            ColumnName("name".to_string()),
            Value::String("Mr Dog".into()),
        );
        dog_row.insert(ColumnName("likes_stick".to_string()), Value::Bool(true));

        let _ = crate::insert::insert(
            &db,
            &Insert {
                table: TableName("pet".to_string()),
                key: 2,
                value: InsertValue::Multiple {
                    constructor: Constructor("dog".into()),
                    values: dog_row,
                },
            },
        )?;

        Ok(())
    }

    fn insert_user_data(db: &DB) -> anyhow::Result<()> {
        let mut user_columns = BTreeMap::new();
        user_columns.insert(ColumnName("age".to_string()), ScalarType::Int);
        user_columns.insert(ColumnName("nice".to_string()), ScalarType::Bool);
        user_columns.insert(ColumnName("name".to_string()), ScalarType::String);

        insert_table(
            &db,
            &Table {
                name: TableName("user".to_string()),
                columns: Columns::SingleConstructor(user_columns),
            },
        );

        let mut user_row_1 = BTreeMap::new();
        user_row_1.insert(ColumnName("age".to_string()), Value::Number(27.into()));
        user_row_1.insert(ColumnName("nice".to_string()), Value::Bool(false));
        user_row_1.insert(ColumnName("name".to_string()), Value::String("Egg".into()));

        let _ = crate::insert::insert(
            &db,
            &Insert {
                table: TableName("user".to_string()),
                key: 1,
                value: InsertValue::Single { values: user_row_1 },
            },
        )?;

        let mut user_row_2 = BTreeMap::new();
        user_row_2.insert(ColumnName("age".to_string()), Value::Number(100.into()));
        user_row_2.insert(ColumnName("nice".to_string()), Value::Bool(true));
        user_row_2.insert(
            ColumnName("name".to_string()),
            Value::String("Horse".into()),
        );

        let _ = crate::insert::insert(
            &db,
            &Insert {
                table: TableName("user".to_string()),
                key: 2,
                value: InsertValue::Single { values: user_row_2 },
            },
        )?;

        let mut user_row_3 = BTreeMap::new();
        user_row_3.insert(ColumnName("age".to_string()), Value::Number(46.into()));
        user_row_3.insert(ColumnName("nice".to_string()), Value::Bool(false));
        user_row_3.insert(ColumnName("name".to_string()), Value::String("Log".into()));

        let _ = crate::insert::insert(
            &db,
            &Insert {
                table: TableName("user".to_string()),
                key: 3,
                value: InsertValue::Single { values: user_row_3 },
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_missing_table() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db).expect("insert test data failure");

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("missing".to_string()),
                        columns: SelectColumns::SelectColumns {
                            columns: vec![ColumnName("name".to_string())]
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
            insert_test_data(&db).expect("insert test data failure");

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("user".to_string()),
                        columns: SelectColumns::SelectColumns {
                            columns: vec![ColumnName("missing".to_string())]
                        },
                        r#where: empty_where()
                    }
                ),
                Err(SelectError::TypeError(TypeError::ColumnNotFound {
                    column_name: ColumnName("missing".to_string()),
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
            insert_test_data(&db).expect("insert test data failure");

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("user".to_string()),
                        columns: SelectColumns::SelectColumns { columns: vec![] },
                        r#where: equals(ColumnName("missing".to_string()), ScalarValue::Bool(true))
                    }
                ),
                Err(SelectError::TypeError(TypeError::ColumnNotFound {
                    column_name: ColumnName("missing".to_string()),
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
            insert_test_data(&db).expect("insert test data failure");

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
                            columns: vec![ColumnName("name".to_string())]
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
            insert_test_data(&db).expect("insert test data failure");

            let expected = vec![(2, serde_json::from_str("{\"name\":\"Horse\"}").unwrap())];

            assert_eq!(
                select(
                    &db,
                    Select {
                        table: TableName("user".to_string()),
                        columns: SelectColumns::SelectColumns {
                            columns: vec![ColumnName("name".to_string())]
                        },
                        r#where: and(
                            equals(ColumnName("nice".to_string()), ScalarValue::Bool(true)),
                            equals(ColumnName("age".to_string()), ScalarValue::Int(100))
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
            insert_test_data(&db).expect("insert test data failure");

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
                            constructor: Constructor("cat".to_string()),
                            columns: vec![
                                ColumnName("age".to_string()),
                                ColumnName("name".to_string())
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

    #[test]
    fn test_get_pets() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db).expect("insert test data failure");

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
                            columns: vec![
                                ColumnName("age".to_string()),
                                ColumnName("name".to_string())
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

    #[test]
    fn test_get_pets_with_nullable() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db).expect("insert test data failure");

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
                                ColumnName("age".to_string()),
                                ColumnName("name".to_string()),
                                ColumnName("likes_stick".to_string())
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
