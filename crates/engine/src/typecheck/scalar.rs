use crate::types::{ScalarType, Type, TypeError};
use serde_json::Value;

pub fn typecheck_scalar(value: &Value, expected_type: &Type) -> Result<(), TypeError> {
    match (value, expected_type) {
        (Value::Null, Type::Optional(_)) => Ok(()),
        (Value::Number(num), _)
            if num.as_i64().is_some() && *inner_scalar_type(expected_type) == ScalarType::Int =>
        {
            Ok(())
        }
        (Value::String(_), _) if *inner_scalar_type(expected_type) == ScalarType::String => Ok(()),
        (Value::Bool(_), _) if *inner_scalar_type(expected_type) == ScalarType::Bool => Ok(()),
        _ => Err(TypeError::TypeMismatchInInput {
            expected_type: expected_type.clone(),
            input_value: value.clone(),
        }),
    }
}

fn inner_scalar_type(expected_type: &Type) -> &ScalarType {
    match expected_type {
        Type::Optional(ty) => inner_scalar_type(ty),
        Type::ScalarType(st) => st,
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
