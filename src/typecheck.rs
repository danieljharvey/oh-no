use super::types::{Columns, Expression, ScalarType, Select, Table, TableName, TypeError};
use std::collections::BTreeMap;

pub fn empty_where() -> Expression {
    Expression::Const(serde_json::Value::Bool(true))
}

// does this query even make sense?
pub fn typecheck_select(
    tables: &BTreeMap<TableName, Table>,
    select: &Select,
) -> Result<Vec<(String, ScalarType)>, TypeError> {
    // this should already be there
    let table = tables.get(&select.table).unwrap();

    let typed_columns: Vec<(String, ScalarType)> =
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

fn typecheck_column(
    table: &Table,
    column_name: &String,
) -> Result<(String, ScalarType), TypeError> {
    match &table.columns {
        Columns::SingleConstructor(columns) => match columns.get(column_name) {
            Some(scalar_type) => Ok((column_name.clone(), scalar_type.clone())),
            None => Err(TypeError::ColumnNotFound {
                table_name: TableName(table.name.clone()),
                column_name: column_name.to_string(),
            }),
        },
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
