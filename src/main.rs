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

#[derive(Debug,PartialEq)]
enum SelectError {
    TypeError(TypeError),
    TableNotFound(String)
}

#[derive(Debug,PartialEq)]
enum TypeError {
     ColumnNotFound{table_name:String,column_name:String}
}

// does this query even make sense?
fn typecheck_select(tables: &BTreeMap<String,Table>,select:&Select)->Result<Vec<(String,ScalarType)>,TypeError> {
    // this should already be there
    let table = tables.get(&select.table).unwrap();

    let typed_columns:Vec<(String,ScalarType)> = select.columns.iter().try_fold(Vec::new(),|mut acc, column| {
        let res = typecheck_column(table,column)?;
        acc.push(res);
            Ok(acc)
    })?;

    typecheck_expression(table,&select.r#where)?;

    Ok(typed_columns)
}

fn typecheck_column(table:&Table, column_name: &String) -> Result<(String,ScalarType),TypeError> {
    match table.columns.get(column_name) {
        Some(scalar_type) => Ok((column_name.clone(),scalar_type.clone())),
        None => Err(TypeError::ColumnNotFound { table_name: table.name.clone(), column_name:column_name.to_string() })
    }
}

// we don't 'learn' anything, just explode or don't
fn typecheck_expression(table:&Table, expression: &Expression) -> Result<(),TypeError> {
    match expression {
        Expression::Column(column_name) => {
            match table.columns.get(column_name) {
                Some(_) => Ok(()),
                None => Err(TypeError::ColumnNotFound{column_name:column_name.clone(),table_name:table.name.clone()})
            }
        },
        Expression::BinaryFunction {expr_left,expr_right,..} => {
            typecheck_expression(table,expr_left)?;
            typecheck_expression(table,expr_right)?;
            Ok(())
        },
        Expression::Const(_) => Ok(())
    }
}


#[derive(Clone,Serialize, Deserialize)]
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

fn matches_prefix(prefix: &String, key: &[u8]) -> bool {
        let key_string = std::str::from_utf8(&key).unwrap();
        let key_start = &key_string.get(0..prefix.len().into()).unwrap();
        key_start == &prefix.as_str()

}

fn select(db: &DB, select: Select) -> Result<Vec<(usize, Value)>,SelectError> {
    let table = match lookup_table(&db,&select.table) {
        Some(table) => Ok(table),
            None => Err(SelectError::TableNotFound(select.table.clone()))
    }?;

    let mut tables = BTreeMap::new();
    tables.insert(select.table.clone(),table);

    typecheck_select(&tables,&select).map_err(SelectError::TypeError)?;

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

fn insert(db: &DB, insert: Insert) {
    let key = format!("data_{}_{}", insert.table, insert.key);
    let _ = db.put(key, serde_json::to_string(&insert.value).unwrap());
}

fn insert_table(db: &DB, table: Table) {
    let key = format!("table_{}", table.name);
    let _ = db.put(key, serde_json::to_string(&table).unwrap());
}

fn lookup_table(db: &DB, table_name: &String) -> Option<Table> {
    let key = format!("table_{}", table_name);
    let raw = db.get(key).unwrap()?;
    let json = std::str::from_utf8(&raw).ok()?;
    serde_json::from_str(&json).ok()?
}


fn insert_test_data(db: &DB) {
    let mut columns = BTreeMap::new();
    columns.insert("age".to_string(),ScalarType::Int);
    columns.insert("nice".to_string(),ScalarType::Bool);
    columns.insert("name".to_string(),ScalarType::String);

    insert_table(&db,
        Table {
            name: "user".to_string(),
            columns
        });

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
fn test_missing_table() {
    let path = format!("./test_storage{}", rand::random::<i32>());
    {
        let db = DB::open_default(path.clone()).unwrap();
        insert_test_data(&db);

        assert_eq!(select(
            &db,
            Select {
                table: "missing".to_string(),
                columns: vec!["name".to_string()],
                r#where: empty_where()
            }
        ),
        Err(SelectError::TableNotFound("missing".to_string())))
    }
    let _ = DB::destroy(&Options::default(), path);
}

#[test]
fn test_missing_column() {
    let path = format!("./test_storage{}", rand::random::<i32>());
    {
        let db = DB::open_default(path.clone()).unwrap();
        insert_test_data(&db);

        assert_eq!(select(
            &db,
            Select {
                table: "user".to_string(),
                columns: vec!["missing".to_string()],
                r#where: empty_where()
            }
        ),
        Err(SelectError::TypeError(TypeError::ColumnNotFound{
            column_name:"missing".to_string(),
        table_name:"user".to_string()

        })))
    }
    let _ = DB::destroy(&Options::default(), path);
}


#[test]
fn test_missing_column_in_where() {
    let path = format!("./test_storage{}", rand::random::<i32>());
    {
        let db = DB::open_default(path.clone()).unwrap();
        insert_test_data(&db);

        assert_eq!(select(
            &db,
            Select {
                table: "user".to_string(),
                columns: vec![],
                r#where:

                        Expression::Column("missing".to_string())


            }
        ),
        Err(SelectError::TypeError(TypeError::ColumnNotFound{
            column_name:"missing".to_string(),
        table_name:"user".to_string()

        })))
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
                    table: "user".to_string(),
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
            Ok(expected)
        );
    }
    let _ = DB::destroy(&Options::default(), path);
}
