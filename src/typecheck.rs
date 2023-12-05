use super::types::{Columns, Expression, Select, Table, TableName, Type, TypeError};
use std::collections::BTreeMap;

pub fn empty_where() -> Expression {
    Expression::Const(serde_json::Value::Bool(true))
}

// does this query even make sense?
pub fn typecheck_select(
    tables: &BTreeMap<TableName, Table>,
    select: &Select,
) -> Result<Vec<(String, Type)>, TypeError> {
    // this should already be there
    let table = tables.get(&select.table).unwrap();

    let typed_columns: Vec<(String, Type)> =
        select
            .columns
            .iter()
            .try_fold(Vec::new(), |mut acc, column| {
                let res = typecheck_column(table, column)?;
                acc.push(res);
                Ok(acc)
            })?;

    typecheck_expression(table, &select.r#where)?;

    Ok(typed_columns)
}

fn typecheck_column(table: &Table, column_name: &String) -> Result<(String, Type), TypeError> {
    match &table.columns {
        Columns::SingleConstructor(columns) => match columns.get(column_name) {
            Some(scalar_type) => Ok((column_name.clone(), Type::ScalarType(scalar_type.clone()))),
            None => Err(TypeError::ColumnNotFound {
                table_name: TableName(table.name.clone()),
                column_name: column_name.to_string(),
            }),
        },
        Columns::MultipleConstructors(constructors) => {
            let mut matches: Vec<_> = constructors
                .iter()
                .filter_map(|(_constructor_name, columns)| columns.get(column_name))
                .collect();

            if let Some(first) = matches.pop() {
                // compare the rest to the first
                let _ = for this_match in matches.iter() {
                    if this_match != &first {
                        // throw error, different types
                        Err(TypeError::ColumnMismatch {
                            column_name: "age".to_string(),
                            table_name: TableName(table.name.clone()),
                            left: first.clone(),
                            right: this_match.clone().clone(),
                        })
                    } else {
                        Ok(())
                    }?;
                };
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
                    column_name: column_name.to_string(),
                })
            }
        }
    }
}

// we don't 'learn' anything, just explode or don't
fn typecheck_expression(table: &Table, expression: &Expression) -> Result<(), TypeError> {
    match expression {
        Expression::Column(column_name) => match &table.columns {
            Columns::SingleConstructor(columns) => match columns.get(column_name) {
                Some(_) => Ok(()),
                None => Err(TypeError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: TableName(table.name.clone()),
                }),
            },
            Columns::MultipleConstructors(_) => todo!("multiple constructors"),
        },
        Expression::BinaryFunction {
            expr_left,
            expr_right,
            ..
        } => {
            typecheck_expression(table, expr_left)?;
            typecheck_expression(table, expr_right)?;
            Ok(())
        }
        Expression::Const(_) => Ok(()),
    }
}

#[cfg(tests)]
mod tests {
    use super::typecheck_column;
    use crate::types::{Columns, ScalarType, Table, TableName, Type, TypeError};
    use std::collections::BTreeMap;

    #[test]
    fn single_column_is_non_null() {
        let mut columns = BTreeMap::new();
        columns.insert("age".to_string(), ScalarType::Int);

        let table = Table {
            name: "User".to_string(),
            columns: Columns::SingleConstructor(columns),
        };

        assert_eq!(
            typecheck_column(&table, &"age".to_string()),
            Ok(("age".to_string(), Type::ScalarType(ScalarType::Int)))
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
            typecheck_column(&table, &"age".to_string()),
            Err(TypeError::ColumnNotFound {
                table_name: TableName(table.name.clone()),
                column_name: "age".to_string()
            })
        );
    }

    #[test]
    fn multiple_columns_non_null_in_all() {
        let mut columns = BTreeMap::new();
        columns.insert("age".to_string(), ScalarType::Int);

        let mut constructors = BTreeMap::new();
        constructors.insert("User".to_string(), columns.clone());
        constructors.insert("Admin".to_string(), columns);

        let table = Table {
            name: "User".to_string(),
            columns: Columns::MultipleConstructors(constructors),
        };

        assert_eq!(
            typecheck_column(&table, &"age".to_string()),
            Ok(("age".to_string(), Type::ScalarType(ScalarType::Int)))
        );
    }

    #[test]
    fn mismatched_columns() {
        let mut user_columns = BTreeMap::new();
        user_columns.insert("age".to_string(), ScalarType::Int);

        let mut admin_columns = BTreeMap::new();
        admin_columns.insert("age".to_string(), ScalarType::String);

        let mut constructors = BTreeMap::new();
        constructors.insert("User".to_string(), user_columns);
        constructors.insert("Admin".to_string(), admin_columns);

        let table = Table {
            name: "User".to_string(),
            columns: Columns::MultipleConstructors(constructors),
        };

        assert_eq!(
            typecheck_column(&table, &"age".to_string()),
            Err(TypeError::ColumnMismatch {
                column_name: "age".to_string(),
                table_name: TableName(table.name.clone()),
                left: ScalarType::Int,
                right: ScalarType::String
            })
        );
    }
}
