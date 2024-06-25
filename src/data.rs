use super::types::{Insert, InsertValue, Table, TableName};
use rocksdb::DB;
use serde_json::Value;

// functions for smashing stuff into RocksDB

pub fn insert(db: &DB, insert: &Insert) {
    let key = format!("data_{}_{}", insert.table, insert.key);
    let value = serde_json::to_string(&insert.value).unwrap();
    println!("insert {value}");
    let _ = db.put(key, value);
}

pub fn insert_table(db: &DB, table: &Table) {
    let key = format!("table_{}", table.name);
    let _ = db.put(key, serde_json::to_string(&table).unwrap());
}

pub fn lookup_table(db: &DB, table_name: &TableName) -> Option<Table> {
    let key = format!("table_{table_name}");
    let raw = db.get(key).unwrap()?;
    let json = std::str::from_utf8(&raw).ok()?;
    serde_json::from_str(json).ok()?
}
