use crate::sinks::{prelude::*, util::http::HttpRequest};
// use crate::sinks::{prelude::*};
use super::config::Format;
use super::request_builder::ClickhouseRequestBuilder;
use futures::stream;

/// PartitionKey used to partition events by (database, table) pair.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(super) struct PartitionKey {
    pub database: String,
    pub table: String,
    pub format: Format,
}

impl Partitioner for KeyPartitioner {
    type Item = Event;
    type Key = Option<PartitionKey>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let database = Self::render(&self.database, item, "database_key")?;
        let mut table = "blackhole".to_string();
        match item {
            Event::Log(log) => match log.get("table") {
                Some(Value::Bytes(b)) => {
                    if let Ok(s) = std::str::from_utf8(b) {
                        table = s.to_string();
                    }
                }
                _ => {}
            },
            _ => {}
        }
        Some(PartitionKey {
            database,
            table: table,
            format: self.format,
        })
    }
}

/// KeyPartitioner that partitions events by (database, table) pair.
struct KeyPartitioner {
    database: Template,
    format: Format,
}

impl KeyPartitioner {
    const fn new(database: Template, format: Format) -> Self {
        Self { database, format }
    }

    fn render(template: &Template, item: &Event, field: &'static str) -> Option<String> {
        template
            .render_string(item)
            .map_err(|error| {
                emit!(TemplateRenderingError {
                    error,
                    field: Some(field),
                    drop_event: true,
                });
            })
            .ok()
    }
}

pub struct ClickhouseAnyTableSink<S> {
    batch_settings: BatcherSettings,
    service: S,
    database: Template,
    format: Format,
    request_builder: ClickhouseRequestBuilder,
}

impl<S> ClickhouseAnyTableSink<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    pub const fn new(
        batch_settings: BatcherSettings,
        service: S,
        database: Template,
        format: Format,
        request_builder: ClickhouseRequestBuilder,
    ) -> Self {
        Self {
            batch_settings,
            service,
            database,
            format,
            request_builder,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batch_settings = self.batch_settings;

        // // Transform events to extract content field before partitioning and batching
        // let transformed_input = input.map(|mut event| {
        //     let logevent = event.as_mut_log();
        //     if let Some(Value::Object(content)) = logevent.remove("contents") {
        //         for (k, v) in content {
        //             logevent.insert(k.as_str(), v);
        //         }
        //     }
        //     event
        // });

        let transformed_input = input.flat_map(|mut event| {
            let mut out = Vec::new();
            let log = event.as_mut_log();

            // 保存原始事件中的table字段值
            let original_table = log.get("table").and_then(|value| {
                if let Value::Bytes(bytes) = value {
                    std::str::from_utf8(bytes).ok().map(|s| s.to_string())
                } else {
                    None
                }
            });

            match log.remove("contents") {
                Some(Value::Array(items)) => {
                    for item in items {
                        if let Value::Object(obj) = item {
                            let mut new_event = event.clone();
                            let new_log = new_event.as_mut_log();

                            // 确保新事件包含原始事件的table字段
                            if let Some(ref table) = original_table {
                                new_log.insert("table", table.clone());
                            }

                            for (k, v) in obj {
                                new_log.insert(k.as_str(), v);
                            }

                            out.push(new_event);
                        }
                    }
                }
                Some(other) => {
                    log.insert("contents", other);
                    out.push(event);
                }
                None => {
                    out.push(event);
                }
            }

            stream::iter(out)
        });

        transformed_input
            .batched_partitioned(KeyPartitioner::new(self.database, self.format), || {
                batch_settings.as_byte_size_config()
            })
            .filter_map(|(key, batch)| async move {
                match key {
                    None => None,
                    Some(key) => {
                        if key.table == "blackhole" {
                            None
                        } else {
                            Some((key, batch))
                        }
                    }
                }
            })
            .request_builder(
                default_request_builder_concurrency_limit(),
                self.request_builder,
            )
            .filter_map(|request| async {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for ClickhouseAnyTableSink<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
