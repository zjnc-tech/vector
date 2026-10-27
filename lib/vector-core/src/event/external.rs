//! Checked, owned field conversions used by `ExternalSourceEvent`.
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
};

use super::{ObjectMap, Value};

/// The reason an external field cannot be represented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeErrorKind {
    IntegerOverflow,
    NonFiniteFloat,
}

/// A location within an external event, ordered from root to leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    Field(&'static str),
    Index(usize),
    Key(String),
}

/// A checked conversion failure, including its field and container location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeError {
    pub kind: EncodeErrorKind,
    pub path: Vec<PathSegment>,
}

impl EncodeError {
    fn new(kind: EncodeErrorKind) -> Self {
        Self {
            kind,
            path: Vec::new(),
        }
    }

    /// Attach the outer struct field without allocating on the success path.
    pub fn at_field(mut self, field: &'static str) -> Self {
        self.path.insert(0, PathSegment::Field(field));
        self
    }

    fn at(mut self, segment: PathSegment) -> Self {
        self.path.insert(0, segment);
        self
    }
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in &self.path {
            match segment {
                PathSegment::Field(name) => write!(f, "{name}")?,
                PathSegment::Index(index) => write!(f, "[{index}]")?,
                PathSegment::Key(key) => write!(f, "[{key:?}]")?,
            }
        }
        write!(
            f,
            ": {}",
            match self.kind {
                EncodeErrorKind::IntegerOverflow => "integer is outside the i64 range",
                EncodeErrorKind::NonFiniteFloat => "float must be finite",
            }
        )
    }
}

impl std::error::Error for EncodeError {}

mod sealed {
    pub trait Value {}
    pub trait Scalar {}
}

/// Supported external values. Sealed to keep the first version's type contract.
pub trait IntoExternalValue: sealed::Value {
    fn into_external_value(self) -> Result<Value, EncodeError>;
}

/// Supported container elements; containers themselves are not scalars.
pub trait ExternalScalar: IntoExternalValue + sealed::Scalar {}

macro_rules! integer {
    ($($ty:ty),* $(,)?) => {$(
        impl sealed::Value for $ty {}
        impl sealed::Scalar for $ty {}
        impl ExternalScalar for $ty {}
        impl IntoExternalValue for $ty {
            fn into_external_value(self) -> Result<Value, EncodeError> {
                i64::try_from(self).map(Value::Integer)
                    .map_err(|_| EncodeError::new(EncodeErrorKind::IntegerOverflow))
            }
        }
    )*};
}
integer!(i8, i16, i32, i64, u8, u16, u32, u64, isize, usize);

macro_rules! float {
    ($($ty:ty),* $(,)?) => {$(
        impl sealed::Value for $ty {}
        impl sealed::Scalar for $ty {}
        impl ExternalScalar for $ty {}
        impl IntoExternalValue for $ty {
            fn into_external_value(self) -> Result<Value, EncodeError> {
                if !self.is_finite() {
                    return Err(EncodeError::new(EncodeErrorKind::NonFiniteFloat));
                }
                Ok(Value::Float(ordered_float::NotNan::new(f64::from(self))
                    .expect("finite value is not NaN")))
            }
        }
    )*};
}
float!(f32, f64);

impl sealed::Value for String {}
impl sealed::Scalar for String {}
impl ExternalScalar for String {}
impl IntoExternalValue for String {
    fn into_external_value(self) -> Result<Value, EncodeError> {
        Ok(Value::Bytes(bytes::Bytes::from(self)))
    }
}

impl<T: ExternalScalar> sealed::Value for Vec<T> {}
impl<T: ExternalScalar> IntoExternalValue for Vec<T> {
    fn into_external_value(self) -> Result<Value, EncodeError> {
        self.into_iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .into_external_value()
                    .map_err(|error| error.at(PathSegment::Index(index)))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array)
    }
}

fn map_value<T: ExternalScalar>(
    items: impl IntoIterator<Item = (String, T)>,
) -> Result<Value, EncodeError> {
    items
        .into_iter()
        .map(|(key, value)| match value.into_external_value() {
            Ok(value) => Ok((key.into(), value)),
            Err(error) => Err(error.at(PathSegment::Key(key))),
        })
        .collect::<Result<ObjectMap, _>>()
        .map(Value::Object)
}

impl<T: ExternalScalar> sealed::Value for HashMap<String, T> {}
impl<T: ExternalScalar> IntoExternalValue for HashMap<String, T> {
    fn into_external_value(self) -> Result<Value, EncodeError> {
        map_value(self)
    }
}

impl<T: ExternalScalar> sealed::Value for BTreeMap<String, T> {}
impl<T: ExternalScalar> IntoExternalValue for BTreeMap<String, T> {
    fn into_external_value(self) -> Result<Value, EncodeError> {
        map_value(self)
    }
}
