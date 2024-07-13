use super::identifiers::{column_name, constructor, table_name, ws};
use crate::types::{ColumnName, Columns, ScalarType, Table};
use std::collections::BTreeMap;

use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::{map},
    sequence::{delimited, pair, preceded},
    IResult,
};

pub fn parse_table(input: &str) -> IResult<&str, Table> {
    map(
        pair(preceded(ws(tag("type")), table_name), columns),
        |(table_name, columns)| Table {
            name: table_name,
            columns,
        },
    )(input)
}

fn columns(input: &str) -> IResult<&str, Columns> {
    let single = map(single_constructor, Columns::SingleConstructor);
    let multiple = map(
        delimited(
            ws(tag("{")),
            nom::multi::separated_list1(ws(tag(",")), pair(constructor, single_constructor)),
            ws(tag("}")),
        ),
        |constructors| {
            let mut all_constructors = BTreeMap::new();
            for (constructor, columns) in constructors {
                all_constructors.insert(constructor, columns);
            }
            Columns::MultipleConstructors(all_constructors)
        },
    );
    alt((single, multiple))(input)
}

fn scalar_type(input: &str) -> IResult<&str, ScalarType> {
    let bool = map(ws(tag("Bool")), |_| ScalarType::Bool);
    let int = map(ws(tag("Int")), |_| ScalarType::Int);
    let string = map(ws(tag("String")), |_| ScalarType::String);

    alt((bool, alt((int, string))))(input)
}

fn single_constructor(input: &str) -> IResult<&str, BTreeMap<ColumnName, ScalarType>> {
    let parse_pair = pair(column_name, preceded(ws(tag(":")), scalar_type));

    map(
        delimited(
            ws(tag("{")),
            nom::multi::separated_list1(ws(tag(",")), parse_pair),
            ws(tag("}")),
        ),
        |pairs| {
            let mut btree = BTreeMap::new();
            for (key, value) in pairs {
                btree.insert(key, value);
            }
            btree
        },
    )(input)
}

#[cfg(test)]
mod tests {
    use super::parse_table;
    use crate::{ColumnName, Columns, Constructor, ScalarType, Table, TableName};
    use std::collections::BTreeMap;

    #[test]
    fn test_single_constructor_table() {
        let mut columns = BTreeMap::new();
        columns.insert(ColumnName("id".to_string()), ScalarType::Int);
        columns.insert(ColumnName("name".to_string()), ScalarType::String);
        columns.insert(ColumnName("likes_dogs".to_string()), ScalarType::Bool);

        assert_eq!(
            parse_table("type user { id: Int, name: String, likes_dogs: Bool }"),
            Ok((
                "",
                Table {
                    name: TableName("user".to_string()),
                    columns: Columns::SingleConstructor(columns)
                }
            ))
        );
    }

    #[test]
    fn test_multiple_constructor_table() {
        let mut rgb_columns = BTreeMap::new();
        rgb_columns.insert(ColumnName("red".to_string()), ScalarType::Int);
        rgb_columns.insert(ColumnName("green".to_string()), ScalarType::Int);
        rgb_columns.insert(ColumnName("blue".to_string()), ScalarType::Int);

        let mut greyscale_columns = BTreeMap::new();
        greyscale_columns.insert(ColumnName("value".to_string()), ScalarType::Int);

        let mut constructors = BTreeMap::new();
        constructors.insert(Constructor("RGB".to_string()), rgb_columns);
        constructors.insert(Constructor("Greyscale".to_string()), greyscale_columns);

        assert_eq!(
            parse_table(
                "type color { RGB { red: Int, green: Int, blue: Int }, Greyscale { value: Int } }"
            ),
            Ok((
                "",
                Table {
                    name: TableName("color".to_string()),
                    columns: Columns::MultipleConstructors(constructors)
                }
            ))
        );
    }
}
