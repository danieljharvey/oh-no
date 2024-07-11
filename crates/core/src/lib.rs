mod typecheck;
mod types;
pub use typecheck::{
    insert::typecheck_insert,
    select::{empty_where, typecheck_select},
};
pub use types::{
    and, bool_expr, equals, ColumnName, Columns, Constructor, Expression, Function, Insert,
    InsertError, InsertValue, ScalarType, Select, SelectColumns, SelectError, Table, TableName,
    TypeError,
};
