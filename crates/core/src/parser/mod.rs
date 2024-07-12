use crate::empty_where;
use crate::types::{ColumnName, Constructor, Select, SelectColumns, TableName};

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{alpha1, alphanumeric1},
    combinator::map,
    combinator::recognize,
    error::ParseError,
    multi::many0_count,
    sequence::{pair, preceded, terminated},
    IResult,
};

// parse at least one uppercase char
fn uppercase_char<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    take_while1(move |c| chars.contains(c))(i)
}

// parse at least one lowercase char
fn lowercase_char<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let chars = "abcdefghijklmnopqrstuvwxyz";
    take_while1(move |c| chars.contains(c))(i)
}

pub fn table_name(input: &str) -> IResult<&str, TableName> {
    map(
        recognize(pair(alpha1, many0_count(alt((alphanumeric1, tag("_")))))),
        |ident: &str| TableName(ident.to_string()),
    )(input)
}

#[test]
fn test_table_name() {
    assert_eq!(matches!(table_name("123horse"), Err(_)), true);

    assert_eq!(
        table_name("horse"),
        Ok(("", TableName("horse".to_string())))
    );
}

pub fn constructor(input: &str) -> IResult<&str, Constructor> {
    map(
        recognize(pair(
            uppercase_char,
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
        |ident: &str| Constructor(ident.to_string()),
    )(input)
}

#[test]
fn test_constructor() {
    assert_eq!(matches!(constructor("horse"), Err(_)), true);

    assert_eq!(
        constructor("Horse"),
        Ok(("", Constructor("Horse".to_string())))
    );
}

pub fn column_name(input: &str) -> IResult<&str, ColumnName> {
    map(
        recognize(pair(
            lowercase_char,
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
        |ident: &str| ColumnName(ident.to_string()),
    )(input)
}

#[test]
fn test_column_name() {
    assert_eq!(matches!(column_name("Horse"), Err(_)), true);

    assert_eq!(
        column_name("horse"),
        Ok(("", ColumnName("horse".to_string())))
    );
}

pub fn select_columns(input: &str) -> IResult<&str, SelectColumns> {
    alt((select_just_columns, select_constructor))(input)
}
// `name,age,title`
pub fn select_just_columns(input: &str) -> IResult<&str, SelectColumns> {
    map(
        nom::multi::separated_list1(tag(","), column_name),
        |columns| SelectColumns::SelectColumns { columns },
    )(input)
}

// `RGB{red,green,blue`
pub fn select_constructor(input: &str) -> IResult<&str, SelectColumns> {
    map(
        pair(
            constructor,
            preceded(
                tag("{"),
                terminated(nom::multi::separated_list1(tag(","), column_name), tag("}")),
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
        select_columns("RGB{red,green,blue}"),
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
        select_columns("horse,course,eggs"),
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

fn select(input: &str) -> IResult<&str, Select> {
    map(
        pair(
            preceded(tag("select "), select_columns),
            preceded(tag(" from "), table_name),
        ),
        |(select_columns, table_name)| Select {
            table: table_name,
            columns: select_columns,
            r#where: empty_where(),
        },
    )(input)
}

#[test]
fn test_select() {
    assert_eq!(
        select("select id,name from users"),
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
    )
}
