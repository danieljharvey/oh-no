use crate::types::{ColumnName, Constructor, TableName};

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{alpha1, alphanumeric1, multispace0},
    combinator::map,
    combinator::recognize,
    error::ParseError,
    multi::many0_count,
    sequence::{pair, preceded},
    IResult,
};

/// A combinator that takes a parser `inner` and produces a parser that also consumes both leading and
/// trailing whitespace, returning the output of `inner`.
pub fn ws<'a, F, O, E: ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    preceded(multispace0, inner)
}

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
        ws(recognize(pair(
            alpha1,
            many0_count(alt((alphanumeric1, tag("_")))),
        ))),
        |ident: &str| TableName(ident.to_string()),
    )(input)
}

#[test]
fn test_table_name() {
    assert!(table_name("123horse").is_err());

    assert_eq!(
        table_name("horse"),
        Ok(("", TableName("horse".to_string())))
    );

    assert_eq!(
        table_name(" horse"),
        Ok(("", TableName("horse".to_string())))
    );
}

pub fn constructor(input: &str) -> IResult<&str, Constructor> {
    map(
        ws(recognize(pair(
            uppercase_char,
            many0_count(alt((alphanumeric1, tag("_")))),
        ))),
        |ident: &str| Constructor(ident.to_string()),
    )(input)
}

#[test]
fn test_constructor() {
    assert!(constructor("horse").is_err());

    assert_eq!(
        constructor(" Horse"),
        Ok(("", Constructor("Horse".to_string())))
    );

    assert_eq!(
        constructor("Horse"),
        Ok(("", Constructor("Horse".to_string())))
    );
}

pub fn column_name(input: &str) -> IResult<&str, ColumnName> {
    map(
        ws(recognize(pair(
            lowercase_char,
            many0_count(alt((alphanumeric1, tag("_")))),
        ))),
        |ident: &str| ColumnName(ident.to_string()),
    )(input)
}

#[test]
fn test_column_name() {
    assert!(column_name("Horse").is_err());

    assert_eq!(
        column_name(" horse"),
        Ok(("", ColumnName("horse".to_string())))
    );

    assert_eq!(
        column_name("horse"),
        Ok(("", ColumnName("horse".to_string())))
    );
}
