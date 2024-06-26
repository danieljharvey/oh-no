use super::column::typecheck_column;
use crate::types::{
    ColumnName, Columns, Expression, Select, SelectColumns, Table, TableName, Type, TypeError,
};
use std::collections::BTreeMap;

pub fn empty_where() -> Expression {
    Expression::Const(serde_json::Value::Bool(true))
}

// does this query even make sense?
pub fn typecheck_select(
    tables: &BTreeMap<TableName, Table>,
    select: &Select,
) -> Result<Vec<(ColumnName, Type)>, TypeError> {
    // this should already be there
    let table = tables.get(&select.table).unwrap();

    let select_columns = match &select.columns {
        SelectColumns::SelectColumns { columns }
        | SelectColumns::SelectConstructor { columns, .. } => columns,
    };

    let typed_columns: Vec<(ColumnName, Type)> =
        select_columns
            .iter()
            .try_fold(Vec::new(), |mut acc, column| {
                let res = typecheck_column(table, column)?;
                acc.push(res);
                Ok(acc)
            })?;

    typecheck_expression(table, &select.r#where)?;

    Ok(typed_columns)
}

// we don't 'learn' anything, just explode or don't
fn typecheck_expression(table: &Table, expression: &Expression) -> Result<(), TypeError> {
    match expression {
        Expression::Column(column_name) => match &table.columns {
            Columns::SingleConstructor(columns) => match columns.get(column_name) {
                Some(_) => Ok(()),
                None => Err(TypeError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table.name.clone(),
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
