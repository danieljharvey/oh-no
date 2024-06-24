use super::types::{Insert, Table, TableName};
use rocksdb::DB;

// functions for smashing stuff into RocksDB

pub fn insert(db: &DB, insert: Insert) {
    let key = format!("data_{}_{}", insert.table, insert.key);
    let _ = db.put(key, serde_json::to_string(&insert.value).unwrap());
}

pub fn insert_table(db: &DB, table: Table) {
    let key = format!("table_{}", table.name);
    let _ = db.put(key, serde_json::to_string(&table).unwrap());
}

pub fn lookup_table(db: &DB, table_name: &TableName) -> Option<Table> {
    let key = format!("table_{}", table_name);
    let raw = db.get(key).unwrap()?;
    let json = std::str::from_utf8(&raw).ok()?;
    serde_json::from_str(json).ok()?
}
