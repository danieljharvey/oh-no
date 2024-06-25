use crate::typecheck::{column::typecheck_column, scalar::typecheck_scalar};
use crate::types::{Columns, Insert, Table, TableName, TypeError};
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

            for column_name in columns.keys() {
                let (_, column_type) = typecheck_column(table, column_name)?;
                let value = insert.value.get(column_name).ok_or_else(|| {
                    TypeError::MissingColumnInInput {
                        column_name: column_name.clone(),
                        table_name: insert.table.clone(),
                    }
                })?;
                typecheck_scalar(value, &column_type)?;
                typecheck_columns.insert(column_name, column_type);
            }

            Ok(())
        }
        Columns::MultipleConstructors(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::{typecheck_insert, BTreeMap};
    use crate::types::{
        ColumnName, Columns, Insert, ScalarType, Table, TableName, Type, TypeError,
    };
    use serde_json::Value;

    #[test]
    fn table_doesnt_exist() {
        let tables = BTreeMap::new();

        let insert = Insert {
            table: TableName("Horses".to_string()),
            key: 100,
            value: BTreeMap::new(),
        };

        assert_eq!(
            typecheck_insert(&tables, &insert),
            Err(TypeError::TableNotFound(TableName("Horses".to_string())))
        );
    }

    #[test]
    fn column_is_missing() {
        let mut columns = BTreeMap::new();
        columns.insert(ColumnName("age".to_string()), ScalarType::Int);

        let table = Table {
            name: "Horses".to_string(),
            columns: Columns::SingleConstructor(columns),
        };

        let mut tables = BTreeMap::new();
        tables.insert(TableName("Horses".to_string()), table);

        let insert = Insert {
            table: TableName("Horses".to_string()),
            key: 100,
            value: BTreeMap::new(),
        };

        assert_eq!(
            typecheck_insert(&tables, &insert),
            Err(TypeError::MissingColumnInInput {
                table_name: TableName("Horses".to_string()),
                column_name: ColumnName("age".to_string())
            })
        )
    }

    #[test]
    fn value_has_wrong_type() {
        let mut columns = BTreeMap::new();
        columns.insert(ColumnName("age".to_string()), ScalarType::Int);

        let table = Table {
            name: "Horses".to_string(),
            columns: Columns::SingleConstructor(columns),
        };

        let mut tables = BTreeMap::new();
        tables.insert(TableName("Horses".to_string()), table);

        let mut insert_value = BTreeMap::new();

        insert_value.insert(
            ColumnName("age".to_string()),
            Value::String("dog".to_string()),
        );

        let insert = Insert {
            table: TableName("Horses".to_string()),
            key: 100,
            value: insert_value,
        };

        assert_eq!(
            typecheck_insert(&tables, &insert),
            Err(TypeError::TypeMismatchInInput {
                expected_type: Type::ScalarType(ScalarType::Int),
                input_value: Value::String("dog".to_string())
            })
        )
    }
}
