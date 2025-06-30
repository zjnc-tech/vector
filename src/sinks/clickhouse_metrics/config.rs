//! Configuration for the `Clickhouse_metrics` sink.

use super::errors::ClickhouseMetricsError;
use super::sink::ClickhouseMetricsSink;
use crate::{
    http::{Auth, MaybeAuth},
    sinks::{
        prelude::*,
        util::{RealtimeSizeBasedDefaultBatchSettings, UriSerde},
    },
};

/// Configuration for the `clickhouse_metrics` sink.
#[configurable_component(sink(
    "clickhouse_metrics",
    "Deliver metrics data to a ClickHouse database."
))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseMetricsConfig {
    /// The endpoint of the ClickHouse server.
    #[serde(alias = "host")]
    #[configurable(metadata(docs::examples = "http://localhost:8123"))]
    pub endpoint: UriSerde,

    /// The table that data is inserted into.
    #[configurable(metadata(docs::examples = "mytable"))]
    pub table: Template,

    /// The database that contains the table that data is inserted into.
    #[configurable(metadata(docs::examples = "mydatabase"))]
    pub database: Option<Template>,

    /// Sets `input_format_skip_unknown_fields`, allowing ClickHouse to discard fields not present in the table schema.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub skip_unknown_fields: Option<bool>,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    pub auth: Option<Auth>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl_generate_config_from_default!(ClickhouseMetricsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "clickhouse_metrics")]
impl SinkConfig for ClickhouseMetricsConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = self.endpoint.with_default_parts().uri;

        let auth = self
            .auth
            .choose_one(&self.endpoint.auth)?
            .ok_or_else(|| ClickhouseMetricsError::EmptyAuth)?;

        let batch_settings = self.batch.into_batcher_settings()?;

        let database = self.database.clone().unwrap_or_else(|| {
            "default"
                .try_into()
                .expect("'default' should be a valid template")
        });

        let sink = ClickhouseMetricsSink::new(
            endpoint,
            auth,
            database,
            self.table.clone(),
            batch_settings,
        )?;

        Ok((
            VectorSink::from_event_streamsink(sink),
            Box::pin(healthcheck()),
        ))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

pub async fn healthcheck() -> crate::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<ClickhouseMetricsConfig>();
    }
}
