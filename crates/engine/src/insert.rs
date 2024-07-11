use engine_core::{Insert, InsertError};
use rocksdb::DB;
use std::collections::BTreeMap;

pub fn insert(db: &DB, insert: &Insert) -> Result<i32, InsertError> {
    let table = match crate::data::lookup_table(db, &insert.table) {
        Some(table) => Ok(table),
        None => Err(InsertError::TableNotFound(insert.table.clone())),
    }?;

    let mut tables = BTreeMap::new();
    tables.insert(insert.table.clone(), table);

    engine_core::typecheck_insert(&tables, insert).map_err(InsertError::TypeError)?;
    Ok(crate::data::insert(db, insert))
}
