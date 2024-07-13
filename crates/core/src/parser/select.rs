use super::expression::expression;
use super::identifiers::{column_name, constructor, table_name, ws};
use crate::empty_where;
use crate::types::{
    ColumnName, Comparison, Constructor, Expression, Function, ScalarValue, Select, SelectColumns,
    TableName,
};

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::i32,
    character::complete::{alpha1, alphanumeric1, multispace0},
    combinator::recognize,
    combinator::{map, opt},
    error::ParseError,
    multi::{many0, many0_count},
    sequence::{pair, preceded, terminated},
    IResult,
};

fn select_columns(input: &str) -> IResult<&str, SelectColumns> {
    alt((select_just_columns, select_constructor))(input)
}

// `name,age,title`
fn select_just_columns(input: &str) -> IResult<&str, SelectColumns> {
    map(
        nom::multi::separated_list1(ws(tag(",")), column_name),
        |columns| SelectColumns::SelectColumns { columns },
    )(input)
}

// `RGB{red,green,blue}`
fn select_constructor(input: &str) -> IResult<&str, SelectColumns> {
    map(
        pair(
            constructor,
            preceded(
                ws(tag("{")),
                terminated(
                    nom::multi::separated_list1(ws(tag(",")), column_name),
                    ws(tag("}")),
                ),
            ),
        ),
        |(constructor, columns)| SelectColumns::SelectConstructor {
            constructor,
            columns,
        },
    )(input)
}

#[test]
fn test_select_columns() {
    assert_eq!(
        select_columns("RGB{ red ,  green ,  blue }"),
        Ok((
            "",
            SelectColumns::SelectConstructor {
                constructor: Constructor("RGB".to_string()),
                columns: vec![
                    ColumnName("red".to_string()),
                    ColumnName("green".to_string()),
                    ColumnName("blue".to_string())
                ]
            }
        ))
    );

    assert_eq!(
        select_columns("horse  ,    course,eggs"),
        Ok((
            "",
            SelectColumns::SelectColumns {
                columns: vec![
                    ColumnName("horse".to_string()),
                    ColumnName("course".to_string()),
                    ColumnName("eggs".to_string())
                ]
            }
        ))
    );
}

pub fn select(input: &str) -> IResult<&str, Select> {
    map(
        pair(
            preceded(ws(tag("select ")), select_columns),
            pair(preceded(ws(tag("from")), table_name), r#where),
        ),
        |(select_columns, (table_name, expression))| Select {
            table: table_name,
            columns: select_columns,
            r#where: expression,
        },
    )(input)
}

fn r#where(input: &str) -> IResult<&str, Expression> {
    map(
        opt(preceded(ws(tag("where")), expression)),
        |maybe_exp| match maybe_exp {
            Some(expression) => expression,
            None => empty_where(),
        },
    )(input)
}

#[test]
fn test_select() {
    assert_eq!(
        select("select id, name from users"),
        Ok((
            "",
            Select {
                table: TableName("users".to_string()),
                columns: SelectColumns::SelectColumns {
                    columns: vec![ColumnName("id".to_string()), ColumnName("name".to_string())]
                },
                r#where: empty_where()
            }
        ))
    );
    assert_eq!(
        select("select id, name from users where user_id=100"),
        Ok((
            "",
            Select {
                table: TableName("users".to_string()),
                columns: SelectColumns::SelectColumns {
                    columns: vec![ColumnName("id".to_string()), ColumnName("name".to_string())]
                },
                r#where: Expression::Comparison(Comparison {
                    column: ColumnName("user_id".to_string()),
                    value: ScalarValue::Int(100)
                })
            }
        ))
    )
}
