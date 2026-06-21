use std::{collections::BTreeMap, error::Error, fmt};

use crate::{
    ids::{Generation, WorkloadClassId},
    instance::InstanceValues,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadClassVersionRef {
    pub class_id: WorkloadClassId,
    pub version: Generation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadWorkloadClassVersionRequest {
    pub reference: WorkloadClassVersionRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateWorkloadClassVersionRequest {
    pub workload_class_version: WorkloadClassVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadClassVersion {
    pub reference: WorkloadClassVersionRef,
    pub template_generation: Generation,
    pub default_values: InstanceValues,
    pub value_schema: WorkloadValueSchema,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadValueSchema {
    pub fields: BTreeMap<String, WorkloadValueFieldRule>,
    pub allow_extra: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadValueFieldRule {
    pub required: bool,
    pub default: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueSchemaError {
    MissingRequiredField { field: String },
    UnknownField { field: String },
}

impl WorkloadClassVersionRef {
    pub fn new(class_id: WorkloadClassId, version: Generation) -> Self {
        Self { class_id, version }
    }
}

impl CreateWorkloadClassVersionRequest {
    pub fn new(workload_class_version: WorkloadClassVersion) -> Self {
        Self {
            workload_class_version,
        }
    }
}

impl LoadWorkloadClassVersionRequest {
    pub fn new(reference: WorkloadClassVersionRef) -> Self {
        Self { reference }
    }
}

impl WorkloadValueSchema {
    pub fn new(allow_extra: bool) -> Self {
        Self {
            fields: BTreeMap::new(),
            allow_extra,
        }
    }

    pub fn with_field(mut self, name: impl Into<String>, rule: WorkloadValueFieldRule) -> Self {
        self.fields.insert(name.into(), rule);
        self
    }

    pub fn validate_values(
        &self,
        values: &InstanceValues,
    ) -> Result<InstanceValues, ValueSchemaError> {
        if !self.allow_extra {
            for field in values.keys() {
                if !self.fields.contains_key(field) {
                    return Err(ValueSchemaError::UnknownField {
                        field: field.clone(),
                    });
                }
            }
        }

        let mut validated = values.clone();
        for (field, rule) in &self.fields {
            if validated.contains_key(field) {
                continue;
            }

            if let Some(default) = &rule.default {
                validated.insert(field.clone(), default.clone());
            } else if rule.required {
                return Err(ValueSchemaError::MissingRequiredField {
                    field: field.clone(),
                });
            }
        }

        Ok(validated)
    }
}

impl Default for WorkloadValueSchema {
    fn default() -> Self {
        Self::new(false)
    }
}

impl WorkloadValueFieldRule {
    pub fn required() -> Self {
        Self {
            required: true,
            default: None,
        }
    }

    pub fn optional() -> Self {
        Self {
            required: false,
            default: None,
        }
    }

    pub fn optional_with_default(default: impl Into<String>) -> Self {
        Self {
            required: false,
            default: Some(default.into()),
        }
    }
}

impl fmt::Display for ValueSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRequiredField { field } => {
                write!(f, "missing required instance value {field:?}")
            }
            Self::UnknownField { field } => write!(f, "unknown instance value {field:?}"),
        }
    }
}

impl Error for ValueSchemaError {}

#[cfg(test)]
mod tests {
    use super::{ValueSchemaError, WorkloadValueFieldRule, WorkloadValueSchema};
    use crate::instance::InstanceValues;

    #[test]
    fn value_schema_applies_defaults() {
        let schema = WorkloadValueSchema::new(false)
            .with_field("tenant", WorkloadValueFieldRule::required())
            .with_field(
                "image",
                WorkloadValueFieldRule::optional_with_default("example/app:1"),
            );

        let validated = schema
            .validate_values(&values([("tenant", "acme")]))
            .expect("values are valid");

        assert_eq!(
            validated,
            values([("image", "example/app:1"), ("tenant", "acme")])
        );
    }

    #[test]
    fn value_schema_rejects_missing_required_fields() {
        let schema = WorkloadValueSchema::new(false)
            .with_field("tenant", WorkloadValueFieldRule::required());

        let error = schema
            .validate_values(&InstanceValues::new())
            .expect_err("missing required field is rejected");

        assert_eq!(
            error,
            ValueSchemaError::MissingRequiredField {
                field: "tenant".to_owned()
            }
        );
    }

    #[test]
    fn value_schema_rejects_unknown_fields_when_extra_values_are_disallowed() {
        let schema = WorkloadValueSchema::new(false)
            .with_field("tenant", WorkloadValueFieldRule::required());

        let error = schema
            .validate_values(&values([("extra", "value"), ("tenant", "acme")]))
            .expect_err("unknown field is rejected");

        assert_eq!(
            error,
            ValueSchemaError::UnknownField {
                field: "extra".to_owned()
            }
        );
    }

    #[test]
    fn value_schema_allows_unknown_fields_when_extra_values_are_enabled() {
        let schema = WorkloadValueSchema::new(true).with_field(
            "image",
            WorkloadValueFieldRule::optional_with_default("example/app:1"),
        );

        let validated = schema
            .validate_values(&values([("tenant", "acme")]))
            .expect("extra value is allowed");

        assert_eq!(
            validated,
            values([("image", "example/app:1"), ("tenant", "acme")])
        );
    }

    #[test]
    fn value_schema_output_order_is_deterministic() {
        let schema = WorkloadValueSchema::new(true)
            .with_field("zeta", WorkloadValueFieldRule::optional())
            .with_field("alpha", WorkloadValueFieldRule::optional_with_default("a"))
            .with_field("middle", WorkloadValueFieldRule::optional_with_default("m"));

        let validated = schema
            .validate_values(&values([("zeta", "z"), ("tenant", "acme")]))
            .expect("values are valid");
        let keys = validated.keys().cloned().collect::<Vec<_>>();

        assert_eq!(keys, vec!["alpha", "middle", "tenant", "zeta"]);
    }

    fn values<const N: usize>(pairs: [(&str, &str); N]) -> InstanceValues {
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }
}
