use super::super::types::{Columns, Insert, ScalarType, Table, TableName, TypeError};
use std::collections::BTreeMap;

fn get_table<'a>(
    tables: &'a BTreeMap<TableName, Table>,
    table_name: &'a TableName,
) -> Result<&'a Table, TypeError> {
    tables
        .get(table_name)
        .ok_or(TypeError::TableNotFound(table_name.clone()))
}

// is this insert allowed?
pub fn typecheck_insert(
    tables: &BTreeMap<TableName, Table>,
    insert: &Insert,
) -> Result<(), TypeError> {
    let table = get_table(tables, &insert.table)?;

    match &table.columns {
        Columns::SingleConstructor(columns) => {
            let mut typecheck_columns = BTreeMap::new();

            for (column_name, _) in columns {
                let column_type = crate::typecheck::column::typecheck_column(table, &column_name);
                typecheck_columns.insert(column_name, column_type);
            }

            match insert.value.as_object() {
                Some(map) => {}
                None => panic!("no an object"),
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

mod tests {

    use super::{typecheck_insert, Insert, TableName, TypeError};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn table_doesnt_exist() {
        let tables = BTreeMap::new();

        let insert = Insert {
            table: TableName("Horses".to_string()),
            key: 100,
            value: ().into(),
        };

        assert_eq!(
            typecheck_insert(&tables, &insert),
            Err(TypeError::TableNotFound(TableName("Horses".to_string())))
        );
    }
    /*
    #[test]
    fn column_is_missing() {
        let mut columns = BTreeMap::new();
        columns.insert("age".to_string(), ScalarType::Int);

        let table = Table {
            name: "Horses".to_string(),
            columns: Columns::SingleConstructor(columns),
        };

        let mut tables = BTreeMap::new();
        tables.insert(TableName("Horses".to_string()), table);

        let insert_value = json!({});

        let insert = Insert {
            table: TableName("Horses".to_string()),
            key: 100,
            value: insert_value,
        };

        assert_eq!(
            typecheck_insert(&tables, &insert),
            Err(TypeError::MissingColumnInInput {
                table_name: TableName("Horses".to_string()),
                column_name: "age".to_string()
            })
        )
    }
    */
}
