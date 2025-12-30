use crate::sinks::{prelude::*, util::http::HttpRequest};
// use crate::sinks::{prelude::*};
use super::config::Format;
use super::request_builder::ClickhouseRequestBuilder;

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

        // Transform events to extract content field before partitioning and batching
        let transformed_input = input.map(|mut event| {
            let logevent = event.as_mut_log();
            if let Some(Value::Object(content)) = logevent.remove("content") {
                for (k, v) in content {
                    logevent.insert(k.as_str(), v);
                }
            }

            // If the event is a log and has a "content" field, extract its value to the root level
            // if let Event::Log(ref mut log_value) = event {
            //     if let Some(content) = log_value.get("content") {
            //         for item in content.into_iter(false) { //这里报错了cannot move out of `*content` which is behind a shared reference
            //             if let IterItem::KeyValue(k,v) = item {
            //                 log_value.insert(k.as_str(), v.clone());
            //             }
            //         }
            //         log_value.remove("content");
            //     }
            // }
            event
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
