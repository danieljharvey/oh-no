//! functions for smashing stuff into `RocksDB`
use engine_core::{Insert, InsertValue, Table, TableName};
use rocksdb::DB;
use serde_json::Value;

/// turn an `InsertValue` into `RocksDB` row
fn insert_value_to_json(insert_value: &InsertValue) -> Value {
    match insert_value {
        InsertValue::Single { values } => serde_json::to_value(values).unwrap(),
        InsertValue::Multiple {
            constructor,
            values,
        } => {
            let mut json = serde_json::to_value(values).unwrap();
            let object = json.as_object_mut().unwrap();
            object.insert("_type".to_string(), Value::String(constructor.0.clone()));
            Value::Object(object.clone())
        }
    }
}

pub fn insert(db: &DB, insert: &Insert) -> i32 {
    let key = format!("data_{}_{}", insert.table, insert.key);
    let value = serde_json::to_string(&insert_value_to_json(&insert.value)).unwrap();
    let _ = db.put(key, value);
    1
}

pub fn insert_table(db: &DB, table: &Table) -> i32 {
    let key = format!("table_{}", table.name);
    let _ = db.put(key, serde_json::to_string(&table).unwrap());
    1
}

pub fn lookup_table(db: &DB, table_name: &TableName) -> Option<Table> {
    let key = format!("table_{table_name}");
    let raw = db.get(key).unwrap()?;
    let json = std::str::from_utf8(&raw).ok()?;
    serde_json::from_str(json).ok()?
}
