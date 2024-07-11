use crate::types::{ColumnName, Constructor, TableName};

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{alpha1, alphanumeric1},
    combinator::map,
    combinator::recognize,
    error::ParseError,
    multi::many0_count,
    sequence::pair,
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
