//! Implementation of the `clickhouse` sink.

use crate::{
    http::Auth,
    sinks::{clickhouse_metrics::service::ClickhouseService, prelude::*},
};

use std::num::NonZeroUsize;

use super::errors::ClickhouseMetricsError;
use super::service::{ClickhouseMetricsBatchRequest, KeyPartitioner};
use clickhouse::Client;
use http::Uri;

pub struct ClickhouseMetricsSink {
    client: Client,
    batch_settings: BatcherSettings,
    table: Template,
    database: Template,
}

impl ClickhouseMetricsSink {
    pub fn new(
        endpoint: Uri,
        auth: Auth,
        database: Template,
        table: Template,
        batch_settings: BatcherSettings,
    ) -> crate::Result<Self> {
        let (user, passwd) = match auth {
            Auth::Basic { user, password } => Ok((user, password)),
            Auth::Bearer {
                token: _,
                token_file: _,
            } => Err(Box::new(ClickhouseMetricsError::BearerUnsupported)),
        }?;

        let ck_client = Client::default()
            .with_url(endpoint.to_string())
            .with_user(user)
            .with_password(passwd)
            .with_database(database.clone());

        Ok(Self {
            client: ck_client,
            batch_settings,
            table,
            database,
        })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batch_settings = self.batch_settings;

        let service = ClickhouseService::new(self.client);

        let svc = ServiceBuilder::new().concurrency_limit(4).buffer(1024).service(service);

        input
            .batched_partitioned(KeyPartitioner::new(self.database, self.table), || {
                batch_settings.as_byte_size_config()
            })
            .filter_map(async move |(key, events)| {
                let key = key?;

                let metadata = RequestMetadataBuilder::from_events(&events).with_request_size(
                    NonZeroUsize::new(events.estimated_json_encoded_size_of().get())?,
                );

                Some(ClickhouseMetricsBatchRequest {
                    key,
                    events,
                    metadata,
                })
            })
            .into_driver(svc)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for ClickhouseMetricsSink {
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
