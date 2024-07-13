use crate::types::{ScalarType, ScalarValue, Type, TypeError};

pub fn typecheck_scalar(value: &ScalarValue, expected_type: &Type) -> Result<(), TypeError> {
    match (value, expected_type) {
        (ScalarValue::Null, Type::Optional(_)) => Ok(()),
        (ScalarValue::Int(_), _) if *inner_scalar_type(expected_type) == ScalarType::Int => Ok(()),
        (ScalarValue::String(_), _) if *inner_scalar_type(expected_type) == ScalarType::String => {
            Ok(())
        }
        (ScalarValue::Bool(_), _) if *inner_scalar_type(expected_type) == ScalarType::Bool => {
            Ok(())
        }
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
    use crate::types::{ScalarType, ScalarValue, Type};

    #[test]
    fn int_is_int() {
        super::typecheck_scalar(&ScalarValue::Int(1), &Type::ScalarType(ScalarType::Int))
            .expect("should be Right");
    }

    #[test]
    fn maybe_int_accepts_null() {
        super::typecheck_scalar(
            &ScalarValue::Null,
            &Type::Optional(Box::new(Type::ScalarType(ScalarType::Int))),
        )
        .expect("should be Right");
    }
}
