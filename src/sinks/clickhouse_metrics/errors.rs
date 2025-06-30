//! error for the `Clickhouse_metrics` sink.
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClickhouseMetricsError {
    #[error("auth can not empty")]
    EmptyAuth,
    #[error("bearer not supported")]
    BearerUnsupported,
    #[error("event must be metrics datatype")]
    NotMetrics,
    #[error("event timestamp can not be empty")]
    EmptyTimestamp,
    #[error("unsupported metric value type {0}")]
    UnsupportedMetricValueType(String),
    #[error("unsupported metric kind {0}")]
    UnsupportedMetricKind(String),
    #[error("clickhouse error")]
    ClickhouseError(#[from] clickhouse::error::Error),
}
