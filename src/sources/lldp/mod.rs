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
use std::env;
use std::time::Duration;

use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::{LogEvent, Value},
};
use vector_lib::config::LogNamespace;
use vector_lib::configurable::configurable_component;
use vector_lib::lookup::owned_value_path;
use vector_lib::{config::DataType, schema};
use vrl::value::Kind;

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
                                let interfaces_logs = map_interfaces_to_logs(interfaces, &config);
                                if interface_out.send_batch(interfaces_logs).await.is_err() {
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
                                let (interfaces_logs, links_logs) = map_neighbors_to_interface_and_link_logs(neighbors, &config);
                                if interface_out.send_batch(interfaces_logs).await.is_err() {
                                    warn!("Failed to send LLDP interface batch");
                                }
                                if link_out.send_batch(links_logs).await.is_err() {
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

    fn outputs(&self, _: LogNamespace) -> Vec<SourceOutput> {
        let definition = schema::Definition::empty_legacy_namespace()
            .with_event_field(
                &owned_value_path!("timestamp"),
                Kind::timestamp(),
                Some("Time when the event was observed"),
            )
            .with_event_field(
                &owned_value_path!("cluster"),
                Kind::bytes(),
                Some("Cluster identifier"),
            )
            .with_event_field(
                &owned_value_path!("device"),
                Kind::bytes(),
                Some("Device name"),
            )
            .with_event_field(
                &owned_value_path!("interface"),
                Kind::bytes(),
                Some("Interface name"),
            )
            .with_event_field(
                &owned_value_path!("interface_normalized"),
                Kind::bytes(),
                Some("Normalized interface name"),
            )
            .with_event_field(
                &owned_value_path!("out_band_ip"),
                Kind::bytes(),
                Some("Out-of-band IP address"),
            )
            .with_event_field(
                &owned_value_path!("type"),
                Kind::bytes(), // 修改为bytes以匹配实际实现
                Some("Device type (leaf/spine/node)"),
            )
            .with_event_field(
                &owned_value_path!("from_device"),
                Kind::bytes(),
                Some("Source device in connection"),
            )
            .with_event_field(
                &owned_value_path!("from_interface"),
                Kind::bytes(),
                Some("Source interface in connection"),
            )
            .with_event_field(
                &owned_value_path!("from_interface_normalized"),
                Kind::bytes(),
                Some("Normalized source interface name"),
            )
            .with_event_field(
                &owned_value_path!("to_device"),
                Kind::bytes(),
                Some("Destination device in connection"),
            )
            .with_event_field(
                &owned_value_path!("to_interface"),
                Kind::bytes(),
                Some("Destination interface in connection"),
            )
            .with_event_field(
                &owned_value_path!("to_interface_normalized"),
                Kind::bytes(),
                Some("Normalized destination interface name"),
            )
            .with_event_field(
                &owned_value_path!("log_type"),
                Kind::bytes(),
                Some("Type of log: interface or link"),
            )
            .with_event_field(
                &owned_value_path!("level"),
                Kind::bytes(), // 修改为bytes以匹配实际实现
                Some("Connection level"),
            );

        vec![SourceOutput::new_maybe_logs(DataType::Log, definition)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

fn normalize_port_name(port: &str) -> String {
    // 直接返回原始端口名称，不做任何处理
    port.to_string()
}

pub fn map_interfaces_to_logs(
    interfaces: Vec<ffi::LldpInterface>,
    config: &Config,
) -> Vec<LogEvent> {
    let now = Utc::now();
    let mut logs = Vec::new();

    for interface in interfaces {
        let normalized_port = normalize_port_name(&interface.name);
        let mut log = LogEvent::default();
        log.insert("timestamp", Value::Timestamp(now));
        log.insert("cluster", Value::from(config.cluster.clone()));
        log.insert("device", Value::from(interface.device_name.clone()));
        log.insert("interface", Value::from(interface.name.clone()));
        log.insert("interface_normalized", Value::from(normalized_port));

        let node_ip = env::var("VECTOR_SELF_POD_HOSTIP").unwrap_or_else(|_| "unknown".to_string());

        // 设置默认的IP和类型值
        (log).insert("out_band_ip", Value::from(node_ip));
        log.insert("type", Value::Integer(0)); // 默认类型为node
        log.insert("log_type", Value::from("interface".to_string()));

        logs.push(log);
    }

    logs
}

pub fn map_neighbors_to_interface_and_link_logs(
    neighbors: Vec<ffi::LldpNeighbor>,
    config: &Config,
) -> (Vec<LogEvent>, Vec<LogEvent>) {
    let mut interface_logs = Vec::new();
    let mut link_logs = Vec::new();

    for neighbor in neighbors {
        let now = Utc::now();

        // switch interface log
        let mut interface_log = LogEvent::default();
        let normalized_port = normalize_port_name(&neighbor.remote_port);
        interface_log.insert("timestamp", Value::Timestamp(now));
        interface_log.insert("cluster", Value::from(config.cluster.clone()));
        interface_log.insert("device", Value::from(neighbor.remote_device.clone()));
        interface_log.insert("interface", Value::from(neighbor.remote_port.clone()));
        interface_log.insert("interface_normalized", Value::from(normalized_port));
        interface_log.insert("log_type", Value::from("interface".to_string()));

        let node_ip = env::var("VECTOR_SELF_POD_HOSTIP").unwrap_or_else(|_| "unknown".to_string());

        interface_log.insert("out_band_ip", Value::from(node_ip));
        interface_log.insert("type", Value::Integer(0)); // 默认类型为node

        interface_logs.push(interface_log);

        // link log
        let mut link_log = LogEvent::default();
        let from_interface_normalized = normalize_port_name(&neighbor.local_interface);
        let to_interface_normalized = normalize_port_name(&neighbor.remote_port);

        link_log.insert("timestamp", Value::Timestamp(now));
        link_log.insert("cluster", Value::from(config.cluster.clone()));
        link_log.insert("level", Value::Integer(0));
        link_log.insert("from_device", Value::from(neighbor.local_device.clone()));
        link_log.insert(
            "from_interface",
            Value::from(neighbor.local_interface.clone()),
        );
        (link_log).insert(
            "from_interface_normalized",
            Value::from(from_interface_normalized),
        );
        link_log.insert("to_device", Value::from(neighbor.remote_device.clone()));
        link_log.insert("to_interface", Value::from(neighbor.remote_port.clone()));
        link_log.insert(
            "to_interface_normalized",
            Value::from(to_interface_normalized),
        );
        link_log.insert("log_type", Value::from("link".to_string()));

        link_logs.push(link_log);
    }

    (interface_logs, link_logs)
}
