use super::super::types::{ColumnName, Columns, Table, TableName, Type, TypeError};

pub fn typecheck_column(
    table: &Table,
    column_name: &ColumnName,
) -> Result<(ColumnName, Type), TypeError> {
    match &table.columns {
        Columns::SingleConstructor(columns) => match columns.get(column_name) {
            Some(scalar_type) => Ok((column_name.clone(), Type::ScalarType(scalar_type.clone()))),
            None => Err(TypeError::ColumnNotFound {
                table_name: TableName(table.name.clone()),
                column_name: column_name.clone(),
            }),
        },
        Columns::MultipleConstructors(constructors) => {
            let mut matches: Vec<_> = constructors
                .iter()
                .filter_map(|(_constructor_name, columns)| columns.get(column_name))
                .collect();

            if let Some(first) = matches.pop() {
                // compare the rest to the first
                for this_match in &matches {
                    if this_match == &first {
                        Ok(())
                    } else {
                        // throw error, different types
                        Err(TypeError::ColumnMismatch {
                            column_name: ColumnName("age".to_string()),
                            table_name: TableName(table.name.clone()),
                            left: first.clone(),
                            right: (*this_match).clone(),
                        })
                    }?;
                }
                // how many constructors contain this column?
                if (matches.len() + 1) < constructors.len() {
                    // not all of them - it's Option<first>
                    Ok((
                        column_name.clone(),
                        Type::Optional(Box::new(Type::ScalarType(first.clone()))),
                    ))
                } else {
                    // it's first
                    Ok((column_name.clone(), Type::ScalarType(first.clone())))
                }
            } else {
                // no matches at all is a type error
                Err(TypeError::ColumnNotFound {
                    table_name: TableName(table.name.clone()),
                    column_name: column_name.clone(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::typecheck_column;
    use crate::types::{ColumnName, Columns, ScalarType, Table, TableName, Type, TypeError};
    use std::collections::BTreeMap;

    #[test]
    fn single_column_is_non_null() {
        let mut columns = BTreeMap::new();
        columns.insert(ColumnName("age".to_string()), ScalarType::Int);

        let table = Table {
            name: "User".to_string(),
            columns: Columns::SingleConstructor(columns),
        };

        assert_eq!(
            typecheck_column(&table, &ColumnName("age".to_string())),
            Ok((
                ColumnName("age".to_string()),
                Type::ScalarType(ScalarType::Int)
            ))
        );
    }

    #[test]
    fn multiple_columns_found_null() {
        let columns = BTreeMap::new();

        let mut constructors = BTreeMap::new();
        constructors.insert("User".to_string(), columns.clone());
        constructors.insert("Admin".to_string(), columns);

        let table = Table {
            name: "User".to_string(),
            columns: Columns::MultipleConstructors(constructors),
        };

        assert_eq!(
            typecheck_column(&table, &ColumnName("age".to_string())),
            Err(TypeError::ColumnNotFound {
                table_name: TableName(table.name.clone()),
                column_name: ColumnName("age".to_string())
            })
        );
    }

    #[test]
    fn multiple_columns_non_null_in_all() {
        let mut columns = BTreeMap::new();
        columns.insert(ColumnName("age".to_string()), ScalarType::Int);

        let mut constructors = BTreeMap::new();
        constructors.insert("User".to_string(), columns.clone());
        constructors.insert("Admin".to_string(), columns);

        let table = Table {
            name: "User".to_string(),
            columns: Columns::MultipleConstructors(constructors),
        };

        assert_eq!(
            typecheck_column(&table, &ColumnName("age".to_string())),
            Ok((
                ColumnName("age".to_string()),
                Type::ScalarType(ScalarType::Int)
            ))
        );
    }

    #[test]
    fn mismatched_columns() {
        let mut user_columns = BTreeMap::new();
        user_columns.insert(ColumnName("age".to_string()), ScalarType::Int);

        let mut admin_columns = BTreeMap::new();
        admin_columns.insert(ColumnName("age".to_string()), ScalarType::String);

        let mut constructors = BTreeMap::new();
        constructors.insert("User".to_string(), user_columns);
        constructors.insert("Admin".to_string(), admin_columns);

        let table = Table {
            name: "User".to_string(),
            columns: Columns::MultipleConstructors(constructors),
        };

        assert_eq!(
            typecheck_column(&table, &ColumnName("age".to_string())),
            Err(TypeError::ColumnMismatch {
                column_name: ColumnName("age".to_string()),
                table_name: TableName(table.name.clone()),
                left: ScalarType::Int,
                right: ScalarType::String
            })
        );
    }
}
