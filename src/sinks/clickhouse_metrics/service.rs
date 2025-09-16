//! Service implementation for the `Clickhouse_metrics` sink.

use crate::sinks::prelude::*;
use clickhouse::{Client, Row};
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use tower::Service;
use vector_lib::event::{MetricKind, MetricValue};
use vector_lib::stream::DriverResponse;
use vector_lib::{finalization::EventStatus, request_metadata::GroupedCountByteSize};

use crate::event::{Event, EventFinalizers, Finalizable};

use time::OffsetDateTime;

use super::errors::ClickhouseMetricsError;

#[derive(Clone)]
pub struct KeyPartitioner {
    database: Template,
    table: Template,
}

impl KeyPartitioner {
    pub const fn new(database: Template, table: Template) -> Self {
        Self { database, table }
    }

    fn render(template: &Template, item: &Event, field: &'static str) -> Option<String> {
        template
            .render_string(item)
            .map_err(|error| {
                emit!(TemplateRenderingError {
                    error,
                    field: Some(field),
                    drop_event: true,
                })
            })
            .ok()
    }
}

impl Partitioner for KeyPartitioner {
    type Item = Event;
    type Key = Option<PartitionKey>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let database = Self::render(&self.database, item, "database_key")?;
        let table = Self::render(&self.table, item, "table_key")?;
        Some(PartitionKey {
            database: database,
            table,
        })
    }
}

/// clickhosue metrics batch request
#[derive(Clone)]
pub struct ClickhouseMetricsBatchRequest {
    pub key: PartitionKey,
    pub events: Vec<Event>,
    pub metadata: RequestMetadata,
}

impl MetaDescriptive for ClickhouseMetricsBatchRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

impl Finalizable for ClickhouseMetricsBatchRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.events.take_finalizers()
    }
}

pub struct ClickhouseService {
    client: Client,
}

impl ClickhouseService {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// PartitionKey used to partition events by (database, table) pair.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct PartitionKey {
    pub database: String,
    pub table: String,
}

#[derive(Debug)]
pub struct ClickhouseResponse {
    events_byte_size: GroupedCountByteSize,
}

impl crate::sinks::util::sink::Response for ClickhouseResponse {
    fn is_successful(&self) -> bool {
        true
    }

    fn is_transient(&self) -> bool {
        true
    }
}

impl DriverResponse for ClickhouseResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }
    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }

    /// Return the number of bytes that were sent in the request that returned this response.
    // TODO, remove the default implementation once all sinks have
    // implemented this function.
    fn bytes_sent(&self) -> Option<usize> {
        None
    }
}

impl Service<ClickhouseMetricsBatchRequest> for ClickhouseService {
    type Response = ClickhouseResponse;
    type Error = ClickhouseMetricsError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ClickhouseMetricsBatchRequest) -> Self::Future {
        let client = self.client.clone();
        let table = req.key.table.clone();
        let json_size = req.metadata.into_events_estimated_json_encoded_byte_size();
        let rows: Result<Vec<MetricsRow>, ClickhouseMetricsError> =
            req.events.into_iter().map(|req| req.try_into()).collect();
        Box::pin(async move {
            let mut insert = client.insert(table.as_str())?;
            let rows = rows?;
            for row in rows {
                insert.write(&row).await?;
            }
            insert.end().await?;
            Ok(ClickhouseResponse {
                events_byte_size: json_size,
            })
        })
    }
}

#[derive(Row, serde::Serialize, Debug)]
struct MetricsRow {
    #[serde(with = "clickhouse::serde::time::datetime")]
    timestamp: OffsetDateTime,
    name: String,
    tags: Vec<(String, String)>,
    val: f64,
}

impl MetricsRow {
    fn new(
        timestamp: impl Into<OffsetDateTime>,
        name: String,
        tags: Vec<(String, String)>,
        val: f64,
    ) -> Self {
        Self {
            timestamp: timestamp.into(),
            name,
            tags,
            val,
        }
    }
}

impl TryFrom<Event> for MetricsRow {
    type Error = ClickhouseMetricsError;
    fn try_from(event: Event) -> Result<Self, Self::Error> {
        let metric = event
            .try_into_metric()
            .ok_or_else(|| ClickhouseMetricsError::NotMetrics)?;

        let name = metric.name();
        let timestamp = metric
            .timestamp()
            .ok_or_else(|| ClickhouseMetricsError::EmptyTimestamp)?;
        let timestamp: time::OffsetDateTime = std::time::SystemTime::from(timestamp).into();

        if metric.kind() == MetricKind::Absolute {
            let tags = match metric.tags() {
                Some(metric_tags) => metric_tags
                    .iter_all()
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            v.map_or_else(|| "".to_string(), |v| v.to_string()),
                        )
                    })
                    .collect(),
                None => vec![],
            };

            let val = match metric.value() {
                MetricValue::Counter { value } => Ok(value.to_owned()),
                MetricValue::Gauge { value } => Ok(value.to_owned()),
                MetricValue::Set { values } => Ok(values.len() as f64),
                MetricValue::Distribution { samples, .. } => {
                    // 使用样本平均值
                    if samples.is_empty() {
                        Ok(0.0)
                    } else {
                        let sum: f64 = samples.iter().map(|s| s.value).sum();
                        Ok(sum / samples.len() as f64)
                    }
                }
                MetricValue::AggregatedHistogram { count, sum, .. } => {
                    // 用总和 / 总数来取平均
                    if *count == 0 {
                        Ok(0.0)
                    } else {
                        Ok(*sum / *count as f64)
                    }
                }
                MetricValue::AggregatedSummary { count, sum, .. } => {
                    // 同样用平均值
                    if *count == 0 {
                        Ok(0.0)
                    } else {
                        Ok(*sum / *count as f64)
                    }
                }
                _ => Err(ClickhouseMetricsError::UnsupportedMetricValueType(
                    metric.value().to_string(),
                )),
            }?;
            Ok(MetricsRow::new(timestamp, name.to_string(), tags, val))
        } else {
            Err(ClickhouseMetricsError::UnsupportedMetricKind(
                "Incremental".to_string(),
            ))
        }
    }
}
