use std::{error::Error, fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonEmptyString(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptyStringError {
    field: &'static str,
}

macro_rules! string_newtype {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, EmptyStringError> {
                Ok(Self(
                    NonEmptyString::new(stringify!($name), value)?.into_inner(),
                ))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = EmptyStringError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

impl NonEmptyString {
    pub fn new(field: &'static str, value: impl Into<String>) -> Result<Self, EmptyStringError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(EmptyStringError { field });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl EmptyStringError {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for EmptyStringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} must not be empty", self.field)
    }
}

impl Error for EmptyStringError {}

string_newtype!(IdempotencyKey);
string_newtype!(InstanceId);
string_newtype!(WorkloadClassId);
string_newtype!(RouteBindingId);
string_newtype!(MaterializationId);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackendGeneration(u64);

impl Generation {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl BackendGeneration {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for BackendGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{Generation, IdempotencyKey};

    #[test]
    fn idempotency_key_rejects_blank_values() {
        let error = IdempotencyKey::new("  ").expect_err("blank keys are invalid");

        assert_eq!(error.field(), "IdempotencyKey");
    }

    #[test]
    fn generation_advances_by_one() {
        assert_eq!(Generation::new(41).next(), Generation::new(42));
    }
}
