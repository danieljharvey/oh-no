use crate::types::{ScalarType, Type, TypeError};
use serde_json::Value;

pub fn typecheck_scalar(value: &Value, expected_type: &Type) -> Result<(), TypeError> {
    match (value, expected_type) {
        (Value::String(_), Type::ScalarType(ScalarType::String))
        | (Value::Bool(_), Type::ScalarType(ScalarType::Bool))
        | (Value::Null, Type::Optional(_)) => Ok(()),
        (Value::Number(num), Type::ScalarType(ScalarType::Int)) => {
            if num.as_i64().is_some() {
                Ok(())
            } else {
                Err(TypeError::UnknownScalarTypeForValue {
                    value: value.clone(),
                })
            }
        }
        _ => Err(TypeError::TypeMismatchInInput {
            expected_type: expected_type.clone(),
            input_value: value.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{ScalarType, Type};
    use serde_json::Value;

    #[test]
    fn int_is_int() {
        super::typecheck_scalar(&Value::Number(1.into()), &Type::ScalarType(ScalarType::Int))
            .expect("should be Right");
    }

    #[test]
    fn maybe_int_accepts_null() {
        super::typecheck_scalar(
            &Value::Null,
            &Type::Optional(Box::new(Type::ScalarType(ScalarType::Int))),
        )
        .expect("should be Right");
    }
}
