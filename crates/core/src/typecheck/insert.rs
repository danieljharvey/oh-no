use crate::typecheck::{column::typecheck_column, scalar::typecheck_scalar};
use crate::types::{
    ColumnName, Columns, Insert, InsertValue, ScalarType, Table, TableName, TypeError,
};
use serde_json::Value;
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

    match (&insert.value, &table.columns) {
        (InsertValue::Single { values }, Columns::SingleConstructor(columns)) => {
            check_values_against_column(table, columns, values)
        }
        (
            InsertValue::Multiple {
                constructor,
                values,
            },
            Columns::MultipleConstructors(constructors),
        ) => {
            let columns = constructors.get(constructor).unwrap();
            check_values_against_column(table, columns, values)
        }
        (InsertValue::Single { .. }, Columns::MultipleConstructors(_)) => {
            Err(TypeError::ConstructorNotSpecified {
                table: table.name.clone(),
            })
        }
        (InsertValue::Multiple { .. }, Columns::SingleConstructor(_)) => {
            Err(TypeError::ConstructorSpecifiedButNotRequired {
                table: table.name.clone(),
            })
        }
    }
}

fn check_values_against_column(
    table: &Table,
    columns: &BTreeMap<ColumnName, ScalarType>,
    values: &BTreeMap<ColumnName, Value>,
) -> Result<(), TypeError> {
    for column_name in columns.keys() {
        let (_, column_type) = typecheck_column(table, column_name)?;
        let value = values
            .get(column_name)
            .ok_or_else(|| TypeError::MissingColumnInInput {
                column_name: column_name.clone(),
                table_name: table.name.clone(),
            })?;
        typecheck_scalar(value, &column_type)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{typecheck_insert, BTreeMap};
    use crate::types::{
        ColumnName, Columns, Constructor, Insert, InsertValue, ScalarType, Table, TableName, Type,
        TypeError,
    };
    use serde_json::Value;

    #[test]
    fn table_doesnt_exist() {
        let tables = BTreeMap::new();

        let insert = Insert {
            table: TableName("Horses".to_string()),
            key: 100,
            value: InsertValue::Single {
                values: BTreeMap::new(),
            },
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
            name: TableName("Horses".to_string()),
            columns: Columns::SingleConstructor(columns),
        };

        let mut tables = BTreeMap::new();
        tables.insert(TableName("Horses".to_string()), table);

        let insert = Insert {
            table: TableName("Horses".to_string()),
            key: 100,
            value: InsertValue::Single {
                values: BTreeMap::new(),
            },
        };

        assert_eq!(
            typecheck_insert(&tables, &insert),
            Err(TypeError::MissingColumnInInput {
                table_name: TableName("Horses".to_string()),
                column_name: ColumnName("age".to_string())
            })
        );
    }

    #[test]
    fn value_has_wrong_type() {
        let mut columns = BTreeMap::new();
        columns.insert(ColumnName("age".to_string()), ScalarType::Int);

        let table = Table {
            name: TableName("Horses".to_string()),
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
            value: InsertValue::Single {
                values: insert_value,
            },
        };

        assert_eq!(
            typecheck_insert(&tables, &insert),
            Err(TypeError::TypeMismatchInInput {
                expected_type: Type::ScalarType(ScalarType::Int),
                input_value: Value::String("dog".to_string())
            })
        );
    }

    #[test]
    fn multi_constructor_column_is_missing() {
        let mut age_columns = BTreeMap::new();
        age_columns.insert(ColumnName("age".to_string()), ScalarType::Int);

        let mut name_columns = BTreeMap::new();
        name_columns.insert(ColumnName("name".to_string()), ScalarType::String);

        let mut constructors = BTreeMap::new();
        constructors.insert(Constructor("Age".to_string()), age_columns);
        constructors.insert(Constructor("Name".to_string()), name_columns);

        let table = Table {
            name: TableName("Horses".to_string()),
            columns: Columns::MultipleConstructors(constructors),
        };

        let mut tables = BTreeMap::new();
        tables.insert(TableName("Horses".to_string()), table);

        let insert = Insert {
            table: TableName("Horses".to_string()),
            key: 100,
            value: InsertValue::Multiple {
                constructor: Constructor("Age".to_string()),
                values: BTreeMap::new(),
            },
        };

        assert_eq!(
            typecheck_insert(&tables, &insert),
            Err(TypeError::MissingColumnInInput {
                table_name: TableName("Horses".to_string()),
                column_name: ColumnName("age".to_string())
            })
        );
    }
}
