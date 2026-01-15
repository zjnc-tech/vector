#[allow(
    improper_ctypes,
    unused_imports,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    clippy::all,
    clippy::trivially_copy_pass_by_ref,
    clippy::missing_const_for_fn
)]
mod ffi;

use chrono::Utc;
use std::time::Duration;

use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::metric::{Metric, MetricKind, MetricTags, MetricValue},
};
use vector_lib::configurable::configurable_component;

use super::lldp::ffi::LldpError;

/// Configuration for the `lldp` source.
#[configurable_component(source("lldp", "Collect lldp data."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct LldpMetricsConfig {
    /// interface data of lldp.
    #[serde(default = "default_interface_scrape_interval")]
    pub interface_scrape_secs: u64,

    /// link data of lldp.
    #[serde(default = "default_link_scrape_interval")]
    pub link_scrape_secs: u64,
}

const fn default_interface_scrape_interval() -> u64 {
    30
}

const fn default_link_scrape_interval() -> u64 {
    60
}

#[derive(Clone)]
pub struct Config {
    pub node_name: String,
    pub cluster: String,
}

impl_generate_config_from_default!(LldpMetricsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "lldp")]
impl SourceConfig for LldpMetricsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let interface_scrape_secs = self.interface_scrape_secs;
        let link_scrape_secs = self.link_scrape_secs;
        let mut interface_out = cx.out.clone();
        let mut link_out = cx.out;
        let shutdown = cx.shutdown.clone();

        Ok(Box::pin(async move {
            let config = Config {
                node_name: std::env::var("NODE_NAME").unwrap_or_else(|_| "unknown-node".into()),
                cluster: std::env::var("CLUSTER_NAME").unwrap_or_else(|_| "unknown-cluster".into()),
            };

            let mut interface_interval =
                tokio::time::interval(Duration::from_secs(interface_scrape_secs));
            let mut link_interval = tokio::time::interval(Duration::from_secs(link_scrape_secs));

            loop {
                tokio::select! {
                    _ = interface_interval.tick() => {
                        match ffi::get_lldp_interfaces_async().await {
                            Ok(interfaces) => {
                                let interfaces_metrics = map_interfaces_to_metrics(interfaces, &config);
                                if interface_out.send_batch(interfaces_metrics).await.is_err() {
                                    warn!("Failed to send LLDP interface batch");
                                }
                            }
                            Err(e) => {
                                // 如果是库不可用错误，记录一次后退出循环
                                if let LldpError::LibraryNotAvailable(_) = e {
                                    error!("LLDP library not available: {}", e);
                                    break;
                                } else {
                                    warn!("LLDP interface error: {}", e);
                                }
                            }
                        }
                    }

                    _ = link_interval.tick() => {
                        match ffi::get_lldp_neighbors_async().await {
                            Ok(neighbors) => {
                                let (interfaces, links) = map_neighbors_to_interface_and_link(neighbors, &config);
                                if link_out.send_batch(interfaces).await.is_err() {
                                    warn!("Failed to send LLDP interface batch");
                                }
                                if link_out.send_batch(links).await.is_err() {
                                    warn!("Failed to send LLDP link batch");
                                }
                            }
                            Err(e) => {
                                // 如果是库不可用错误，记录一次后退出循环
                                if let LldpError::LibraryNotAvailable(_) = e {
                                    error!("LLDP library not available: {}", e);
                                    break;
                                } else {
                                    warn!("LLDP link error: {}", e);
                                }
                            }
                        }
                    }

                    _ = shutdown.clone() => {
                        info!("Shutting down LLDP source");
                        break;
                    }
                }
            }

            Ok(())
        }))
    }
    fn outputs(&self, _: vector_lib::config::LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

pub fn map_interfaces_to_metrics(
    interfaces: Vec<ffi::LldpInterface>,
    config: &Config,
) -> Vec<Metric> {
    let now = Utc::now();
    let mut metrics = Vec::new();

    for interface in interfaces {
        let mut tags = MetricTags::default();
        tags.insert("cluster".to_string(), config.cluster.clone());
        tags.insert("device".to_string(), interface.device_name.clone());
        tags.insert("port".to_string(), interface.name.clone());
        tags.insert("protocol".to_string(), "interface".to_string());
        tags.insert("source".to_string(), "lldp".to_string());

        metrics.push(
            Metric::new(
                "interface",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 1.0 },
            )
            .with_timestamp(Some(now))
            .with_tags(Some(tags)),
        );
    }

    metrics
}

pub fn map_neighbors_to_interface_and_link(
    neighbors: Vec<ffi::LldpNeighbor>,
    config: &Config,
) -> (Vec<Metric>, Vec<Metric>) {
    let mut interface_metrics = Vec::new();
    let mut link_metrics = Vec::new();

    for neighbor in neighbors {
        let now = Utc::now();

        // switch interface
        let mut switch_tags = MetricTags::default();
        switch_tags.insert("cluster".to_string(), config.cluster.clone());
        switch_tags.insert("device".to_string(), neighbor.remote_device.clone());
        switch_tags.insert("port".to_string(), neighbor.remote_port.clone());
        switch_tags.insert("type".to_string(), "0");
        switch_tags.insert("protocol".to_string(), "interface".to_string());
        switch_tags.insert("source".to_string(), "lldp-collector".to_string());

        interface_metrics.push(
            Metric::new(
                "interface",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 1.0 },
            )
            .with_timestamp(Some(now))
            .with_tags(Some(switch_tags)),
        );

        // link
        let mut link_tags = MetricTags::default();
        link_tags.insert("cluster".to_string(), config.cluster.clone());
        link_tags.insert("local_device".to_string(), neighbor.local_device.clone());
        link_tags.insert("local_port".to_string(), neighbor.local_interface.clone());
        link_tags.insert("remote_device".to_string(), neighbor.remote_device.clone());
        link_tags.insert("remote_port".to_string(), neighbor.remote_port.clone());
        link_tags.insert("level".to_string(), "0");
        link_tags.insert("protocol".to_string(), "lldp".to_string());
        link_tags.insert("source".to_string(), "lldp-collector".to_string());

        link_metrics.push(
            Metric::new(
                "link",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 1.0 },
            )
            .with_timestamp(Some(now))
            .with_tags(Some(link_tags)),
        );
    }

    (interface_metrics, link_metrics)
}