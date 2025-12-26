//! SNMP source for collecting network topology information.
//!
//! This source collects LLDP information from network devices using SNMP
//! to build a topology of nodes, leaf switches, and spine switches.

use chrono::Utc;
use std::time::Duration;
use std::collections::HashMap;
use regex::Regex;
use tracing::{debug, warn};

use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::metric::{Metric, MetricKind, MetricTags, MetricValue},
};
use vector_lib::configurable::configurable_component;

/// Configuration for the `snmp` source.
#[configurable_component(source(
    "snmp",
    "Collect LLDP neighbors from switches via SNMP"
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SnmpSwitchLldpConfig {
    /// Switch IPs
    pub targets: Vec<String>,

    /// SNMP v3 user
    pub user: String,

    /// Auth protocol: MD5 / SHA
    pub auth_protocol: String,

    /// Auth password
    pub auth_password: String,

    /// Scrape interval seconds
    #[serde(default = "default_interval")]
    pub scrape_interval_secs: u64,
}

const fn default_interval() -> u64 {
    60
}

#[derive(Debug, Clone)]
struct LldpNeighbor {
    local_device: String,
    local_port: String,
    remote_device: String,
    remote_port: String,
}
#[derive(Debug, Clone, Default)]
struct PartialNeighbor {
    local_port: Option<String>,
    remote_device: Option<String>,
    remote_port: Option<String>,
}

impl_generate_config_from_default!(SnmpSwitchLldpConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "snmp_switch_lldp")]
impl SourceConfig for SnmpSwitchLldpConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let interval = self.scrape_interval_secs;
        let targets = self.targets.clone();
        let shutdown = cx.shutdown.clone();
        let mut out = cx.out;

        let user = self.user.clone();
        let auth_protocol = self.auth_protocol.clone();
        let auth_password = self.auth_password.clone();

        Ok(Box::pin(async move {
            let mut ticker =
                tokio::time::interval(Duration::from_secs(interval));

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        for target in &targets {
                            match collect_lldp_from_switch(
                                target,
                                &user,
                                &auth_protocol,
                                &auth_password,
                            ).await {
                                Ok(neighbors) => {
                                    let metrics = neighbors_to_metrics(neighbors);
                                    if out.send_batch(metrics).await.is_err() {
                                        warn!("failed to send LLDP metrics");
                                    }
                                }
                                Err(e) => {
                                    warn!("SNMP LLDP scrape failed for {}: {}", target, e);
                                }
                            }
                        }
                    }

                    _ = shutdown.clone() => {
                        info!("snmp_switch_lldp source shutdown");
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

const LLDP_REM_SYS_NAME: &str = "1.0.8802.1.1.2.1.4.1.1.9";
const LLDP_REM_PORT_ID: &str = "1.0.8802.1.1.2.1.4.1.1.7";
// const LLDP_REM_CHASSIS_ID: &str = "1.0.8802.1.1.2.1.4.1.1.5";

async fn collect_lldp_from_switch(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
) -> Result<Vec<LldpNeighbor>, String> {

    let local_device = get_local_device(
        target, user, auth_protocol, auth_password
    ).await?;

    let mut neighbors: HashMap<String, PartialNeighbor> = HashMap::new();

    // local_port
    snmpwalk_fill(
        target, user, auth_protocol, auth_password,
        "1.0.8802.1.1.2.1.3.7.1.4",
        |idx, val| {
            neighbors.entry(idx).or_default().local_port = Some(
                extract_port_from_desc(&val)
            );
        },
    ).await?;

    // remote_device
    snmpwalk_fill(
        target, user, auth_protocol, auth_password,
        LLDP_REM_SYS_NAME,
        |idx, val| {
            neighbors.entry(idx).or_default().remote_device = Some(val);
        },
    ).await?;

    // remote_port
    snmpwalk_fill(
        target, user, auth_protocol, auth_password,
        LLDP_REM_PORT_ID,
        |idx, val| {
            neighbors.entry(idx).or_default().remote_port = Some(val);
        },
    ).await?;

    Ok(neighbors.into_iter().filter_map(|(_, n)| {
        Some(LldpNeighbor {
            local_device: local_device.clone(),
            local_port: n.local_port?,
            remote_device: n.remote_device?,
            remote_port: n.remote_port?,
        })
    }).collect())
}

async fn snmpwalk_fill<F>(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    oid: &str,
    mut f: F,
) -> Result<(), String>
where
    F: FnMut(String, String),
{
    let cmd_args = [
        "-v3", "-l", "AuthNoPriv",
        "-u", user,
        "-a", auth_protocol,
        "-A", auth_password,
        target,
        oid,
    ];

    debug!("Executing snmpwalk command: snmpwalk {}", cmd_args.join(" "));

    let out = tokio::process::Command::new("snmpwalk")
        .args(&cmd_args)
        .output()
        .await
        .map_err(|e| {
            let err_msg = e.to_string();
            debug!("snmpwalk command failed: {}", err_msg);
            err_msg
        })?;

    let output = String::from_utf8_lossy(&out.stdout);
    debug!("snmpwalk output (first 500 chars): {}", output.chars().take(500).collect::<String>().replace("\n", " "));

    if !out.stderr.is_empty() {
        let stderr_str = String::from_utf8_lossy(&out.stderr);
        debug!("snmpwalk stderr: {}", stderr_str);
    }

    let mut count = 0;
    for line in output.lines() {
        if let Some((oid_part, val)) = line.split_once(" = STRING: ") {
            let idx = extract_lldp_index(oid_part)?;
            let value = val.trim_matches('"').to_string();
            debug!("Parsed LLDP entry - index: {}, value: {}", idx, value);
            f(idx, value);
            count += 1;
        }
    }
    debug!("Parsed {} LLDP entries from output", count);
    Ok(())
}



fn extract_port_from_desc(desc: &str) -> String {
    static PORT_RE: once_cell::sync::Lazy<Regex> =
        once_cell::sync::Lazy::new(|| {
            Regex::new(r"^([A-Za-z]+[A-Za-z0-9/]+)").unwrap()
        });

    let s = desc.trim().trim_matches('"');

    if let Some(cap) = PORT_RE.captures(s) {
        cap.get(1).unwrap().as_str().to_string()
    } else {
        s.to_string()
    }
}

fn extract_lldp_index(oid: &str) -> Result<String, String> {
    // Example OID: 1.0.8802.1.1.2.1.4.1.1.9.1001.1.1
    // We want to extract the last 3 parts: 1001.1.1 (last 3 segments)
    let parts: Vec<&str> = oid.split('.').collect();
    if parts.len() < 3 {
        return Err("bad oid".into());
    }
    Ok(parts[parts.len()-3..].join("."))
}

async fn get_local_device(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
) -> Result<String, String> {
    let cmd_args = [
        "-v3", "-l", "AuthNoPriv",
        "-u", user,
        "-a", auth_protocol,
        "-A", auth_password,
        target,
        "1.3.6.1.2.1.1.5.0",
    ];
    
    debug!("Executing snmpget command: snmpget {}", cmd_args.join(" "));
    
    let out = tokio::process::Command::new("snmpget")
        .args(&cmd_args)
        .output()
        .await
        .map_err(|e| {
            let err_msg = e.to_string();
            debug!("snmpget command failed: {}", err_msg);
            err_msg
        })?;

    let s = String::from_utf8_lossy(&out.stdout);
    debug!("snmpget output: {}", s);
    
    if !out.stderr.is_empty() {
        let stderr_str = String::from_utf8_lossy(&out.stderr);
        debug!("snmpget stderr: {}", stderr_str);
    }
    
    let output = s.trim();
    
    // 尝试解析 "= STRING:" 格式，例如: "iso.3.6.1.2.1.1.5.0 = STRING: \"value\""
    if let Some(pos) = output.find("= STRING:") {
        let value_part = &output[pos + 9..]; // 9 is the length of "= STRING:"
        let trimmed = value_part.trim();
        debug!("Found = STRING: format, extracted value: {}", trimmed);
        
        // 检查是否有引号包围
        if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() > 1 {
            return Ok(trimmed[1..trimmed.len()-1].to_string());
        } else {
            return Ok(trimmed.to_string());
        }
    }
    
    // 备用解析方法：查找 "STRING:" 后的内容
    if let Some(pos) = output.find("STRING:") {
        let value_part = &output[pos + 7..]; // 7 is the length of "STRING:"
        let trimmed = value_part.trim();
        debug!("Found STRING: format, extracted value: {}", trimmed);
        
        // 检查是否有引号包围
        if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() > 1 {
            return Ok(trimmed[1..trimmed.len()-1].to_string());
        } else {
            return Ok(trimmed.to_string());
        }
    }
    
    debug!("Failed to parse sysName from output: {}", output);
    // 如果以上都失败，返回错误
    Err("parse sysName failed".into())
}

fn neighbors_to_metrics(
    neighbors: Vec<LldpNeighbor>,
) -> Vec<Metric> {
    let ts = Utc::now();

    neighbors
        .into_iter()
        .map(|n| {
            let mut tags = MetricTags::default();

            tags.insert("local_device".to_string(), n.local_device);
            tags.insert("local_port".to_string(), n.local_port);
            tags.insert("remote_device".to_string(), n.remote_device);
            tags.insert("remote_port".to_string(), n.remote_port);
            tags.insert("source".to_string(), "snmp".to_string());
            tags.insert("protocol".to_string(), "lldp".to_string());

            Metric::new(
                "lldp_link",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 1.0 },
            )
                .with_tags(Some(tags))
                .with_timestamp(Some(ts))
        })
        .collect()
}

// Default implementation for config generation
impl Default for SnmpSwitchLldpConfig {
    fn default() -> Self {
        Self {
            targets: vec!["127.0.0.1".to_string()],
            user: "snmp_user".to_string(),
            auth_protocol: "MD5".to_string(),
            auth_password: "password".to_string(),
            scrape_interval_secs: default_interval(),
        }
    }
}