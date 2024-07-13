use crate::empty_where;
use crate::types::{
    ColumnName, Comparison, Constructor, Expression, Function, ScalarValue, Select, SelectColumns,
    TableName,
};

use super::identifiers::{column_name, ws};

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

fn bool(input: &str) -> IResult<&str, bool> {
    alt((
        map(ws(tag("true")), |_| true),
        map(ws(tag("false")), |_| false),
    ))(input)
}

fn scalar_value(input: &str) -> IResult<&str, ScalarValue> {
    let parse_bool = map(bool, ScalarValue::Bool);
    let parse_int = map(ws(i32), ScalarValue::Int);
    let parse_string = map(
        ws(preceded(
            tag("\""),
            terminated(recognize(many0(alphanumeric1)), tag("\"")),
        )),
        |str: &str| ScalarValue::String(str.to_string()),
    );
    alt((parse_bool, alt((parse_int, parse_string))))(input)
}

#[test]
fn test_scalar_value() {
    assert_eq!(scalar_value("  false"), Ok(("", ScalarValue::Bool(false))));
    assert_eq!(scalar_value("   true"), Ok(("", ScalarValue::Bool(true))));
    assert_eq!(scalar_value("  100"), Ok(("", ScalarValue::Int(100))));
    assert_eq!(
        scalar_value("     \"dog\""),
        Ok(("", ScalarValue::String("dog".to_string())))
    );
}

fn comparison(input: &str) -> IResult<&str, Comparison> {
    map(
        pair(column_name, preceded(ws(tag("=")), scalar_value)),
        |(column, value)| Comparison { column, value },
    )(input)
}

#[test]
fn test_comparison() {
    assert_eq!(
        comparison("alive =   true"),
        Ok((
            "",
            Comparison {
                column: ColumnName("alive".to_string()),
                value: ScalarValue::Bool(true)
            }
        ))
    );
}

fn function(input: &str) -> IResult<&str, Function> {
    map(ws(tag("&&")), |_| Function::And)(input)
}

#[test]
fn test_function() {
    assert_eq!(function("  &&"), Ok(("", Function::And)));
}

fn non_recursive_expression(input: &str) -> IResult<&str, Expression> {
    let parse_bool = map(bool, Expression::Bool);
    let parse_comparison = map(comparison, Expression::Comparison);
    alt((parse_bool, parse_comparison))(input)
}

pub fn expression(input: &str) -> IResult<&str, Expression> {
    let parse_bool = map(bool, Expression::Bool);
    let parse_comparison = map(comparison, Expression::Comparison);
    let parse_binary = map(
        pair(
            non_recursive_expression,
            pair(function, non_recursive_expression),
        ),
        |(expr_left, (function, expr_right))| Expression::BinaryFunction {
            function,
            expr_left: Box::new(expr_left),
            expr_right: Box::new(expr_right),
        },
    );

    alt((parse_binary, alt((parse_bool, parse_comparison))))(input)
}

#[test]
fn test_expression() {
    assert_eq!(expression("true"), Ok(("", Expression::Bool(true))));
    assert_eq!(expression("false"), Ok(("", Expression::Bool(false))));
    assert_eq!(
        expression("alive =  true"),
        Ok((
            "",
            Expression::Comparison(Comparison {
                column: ColumnName("alive".to_string()),
                value: ScalarValue::Bool(true)
            })
        ))
    );
    assert_eq!(
        expression("  alive = true   &&  dog =   100"),
        Ok((
            "",
            Expression::BinaryFunction {
                function: Function::And,
                expr_left: Box::new(Expression::Comparison(Comparison {
                    column: ColumnName("alive".to_string()),
                    value: ScalarValue::Bool(true)
                })),
                expr_right: Box::new(Expression::Comparison(Comparison {
                    column: ColumnName("dog".to_string()),
                    value: ScalarValue::Int(100)
                }))
            }
        ))
    );
}
