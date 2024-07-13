use super::data::lookup_table;
use super::helpers::{add_constructor_to_expression, apply_expression, is_true, matches_prefix};
use engine_core::typecheck_select;
use engine_core::{Select, SelectError};
use rocksdb::DB;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

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

#[cfg(test)]
mod testing {
    use super::select;
    use crate::data::insert_table;
    use engine_core::{
        ColumnName, Constructor, Insert, InsertValue, ScalarValue, SelectError, TableName,
        TypeError,
    };
    use rocksdb::{Options, DB};
    use std::collections::BTreeMap;

    fn insert_test_data(db: &DB) -> anyhow::Result<()> {
        let _ = insert_user_data(db);
        insert_pet_data(db)
    }

    fn insert_pet_data(db: &DB) -> anyhow::Result<()> {
        let (_,table_sql) = engine_core::parse_table("type pet { Cat { age: Int, name: String }, Dog { age: Int, name: String, likes_stick: Bool } }").expect("parse_table");

        insert_table(db, &table_sql);

        let mut cat_row = BTreeMap::new();
        cat_row.insert(ColumnName("age".to_string()), ScalarValue::Int(27));
        cat_row.insert(
            ColumnName("name".to_string()),
            ScalarValue::String("Mr Cat".into()),
        );

        let _ = crate::insert::insert(
            db,
            &Insert {
                table: TableName("pet".to_string()),
                key: 1,
                value: InsertValue::Multiple {
                    constructor: Constructor("Cat".into()),
                    values: cat_row,
                },
            },
        )?;

        let mut dog_row = BTreeMap::new();
        dog_row.insert(ColumnName("age".to_string()), ScalarValue::Int(21));
        dog_row.insert(
            ColumnName("name".to_string()),
            ScalarValue::String("Mr Dog".into()),
        );
        dog_row.insert(
            ColumnName("likes_stick".to_string()),
            ScalarValue::Bool(true),
        );

        let _ = crate::insert::insert(
            db,
            &Insert {
                table: TableName("pet".to_string()),
                key: 2,
                value: InsertValue::Multiple {
                    constructor: Constructor("Dog".into()),
                    values: dog_row,
                },
            },
        )?;

        Ok(())
    }

    fn insert_user_data(db: &DB) -> anyhow::Result<()> {
        let (_, table_sql) =
            engine_core::parse_table("type user { age: Int, nice: Bool, name: String }")
                .expect("parse_table");

        insert_table(db, &table_sql);

        // todo: parser
        // insert into user [
        //   {age: 27, nice: false, name: "Egg"},
        //   {age: 100, nice: true, name: "Horse"}
        // ]
        let mut user_row_1 = BTreeMap::new();
        user_row_1.insert(ColumnName("age".to_string()), ScalarValue::Int(27));
        user_row_1.insert(ColumnName("nice".to_string()), ScalarValue::Bool(false));
        user_row_1.insert(
            ColumnName("name".to_string()),
            ScalarValue::String("Egg".into()),
        );

        let _ = crate::insert::insert(
            db,
            &Insert {
                table: TableName("user".to_string()),
                key: 1,
                value: InsertValue::Single { values: user_row_1 },
            },
        )?;

        let mut user_row_2 = BTreeMap::new();
        user_row_2.insert(ColumnName("age".to_string()), ScalarValue::Int(100));
        user_row_2.insert(ColumnName("nice".to_string()), ScalarValue::Bool(true));
        user_row_2.insert(
            ColumnName("name".to_string()),
            ScalarValue::String("Horse".into()),
        );

        let _ = crate::insert::insert(
            db,
            &Insert {
                table: TableName("user".to_string()),
                key: 2,
                value: InsertValue::Single { values: user_row_2 },
            },
        )?;

        let mut user_row_3 = BTreeMap::new();
        user_row_3.insert(ColumnName("age".to_string()), ScalarValue::Int(46));
        user_row_3.insert(ColumnName("nice".to_string()), ScalarValue::Bool(false));
        user_row_3.insert(
            ColumnName("name".to_string()),
            ScalarValue::String("Log".into()),
        );

        let _ = crate::insert::insert(
            db,
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

            let (_, select_sql) =
                engine_core::parse_select("select name from missing").expect("parse_select");

            assert_eq!(
                select(&db, select_sql),
                Err(SelectError::TableNotFound(TableName("missing".to_string())))
            );
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn test_missing_column() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db).expect("insert test data failure");

            let (_, select_sql) =
                engine_core::parse_select("select missing from user").expect("parse_select");

            assert_eq!(
                select(&db, select_sql),
                Err(SelectError::TypeError(TypeError::ColumnNotFound {
                    column_name: ColumnName("missing".to_string()),
                    table_name: TableName("user".to_string())
                }))
            );
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn test_missing_column_in_where() {
        let path = format!("./test_storage{}", rand::random::<i32>());
        {
            let db = DB::open_default(path.clone()).unwrap();
            insert_test_data(&db).expect("insert test data failure");

            let (_, select_sql) =
                engine_core::parse_select("select name from user where missing = true")
                    .expect("parse_select");

            assert_eq!(
                select(&db, select_sql),
                Err(SelectError::TypeError(TypeError::ColumnNotFound {
                    column_name: ColumnName("missing".to_string()),
                    table_name: TableName("user".to_string())
                }))
            );
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

            let (_, select_sql) =
                engine_core::parse_select("select name from user").expect("parse_select");

            assert_eq!(select(&db, select_sql), Ok(expected));
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

            let (_, select_sql) =
                engine_core::parse_select("select name from user where nice = true && age = 100")
                    .expect("parse_select");

            assert_eq!(select(&db, select_sql), Ok(expected));
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

            let (_, select_sql) = engine_core::parse_select("select Cat { age, name } from pet")
                .expect("parse_select");

            assert_eq!(select(&db, select_sql), Ok(expected));
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

            let (_, select_sql) =
                engine_core::parse_select("select age, name from pet").expect("parse_select");

            assert_eq!(select(&db, select_sql), Ok(expected));
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

            let (_, select_sql) =
                engine_core::parse_select("select age, name, likes_stick from pet")
                    .expect("parse select");

            assert_eq!(select(&db, select_sql), Ok(expected));
        }
        let _ = DB::destroy(&Options::default(), path);
    }
}
