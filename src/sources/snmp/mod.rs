//! SNMP source for collecting network topology information.
//!
//! This source collects LLDP information from network devices using SNMP
//! to build a topology of nodes, leaf switches, and spine switches.

use chrono::Utc;
use std::{collections::HashMap, time::Duration};

// use regex::Regex;
use tracing::{info, warn};

use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::metric::{Metric, MetricKind, MetricTags, MetricValue},
};
use vector_lib::configurable::configurable_component;

/// Configuration for the `snmp_switch_lldp` source.
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
    local_device: Option<String>,
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
            let mut ticker = tokio::time::interval(Duration::from_secs(interval));

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

// LLDP OIDs aligned with Go logic
const LLDP_LOCAL_SYS_PORT: &str = "1.0.8802.1.1.2.1.4.1.1.8";
const LLDP_REM_SYS_NAME:  &str = "1.0.8802.1.1.2.1.4.1.1.9";
const LLDP_REM_PORT_ID:   &str = "1.0.8802.1.1.2.1.4.1.1.7";

async fn collect_lldp_from_switch(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
) -> Result<Vec<LldpNeighbor>, String> {
    // warn!("get local_device");
    // let local_device = get_local_device(target, user, auth_protocol, auth_password).await?;

    let mut neighbors: HashMap<String, PartialNeighbor> = HashMap::new();

    warn!("get neighbors");
    // local system port
    snmpwalk_fill(
        target,
        user,
        auth_protocol,
        auth_password,
        LLDP_LOCAL_SYS_PORT,
        |idx, val| {
            if let Ok((dev, port)) = split_local_sys_port(&val) {
                let entry = neighbors.entry(idx).or_default();
                entry.local_device = Some(dev);
                entry.local_port = Some(port);
            }
        },
    )
        .await?;

    // remote system name
    snmpwalk_fill(
        target,
        user,
        auth_protocol,
        auth_password,
        LLDP_REM_SYS_NAME,
        |idx, val| {
            neighbors.entry(idx).or_default().remote_device = Some(val);
        },
    )
        .await?;

    // remote port id
    snmpwalk_fill(
        target,
        user,
        auth_protocol,
        auth_password,
        LLDP_REM_PORT_ID,
        |idx, val| {
            neighbors.entry(idx).or_default().remote_port = Some(val);
        },
    )
        .await?;

    Ok(neighbors
        .into_values()
        .filter_map(|n| {
            // filter RemoteSystemName contains Spine
            // if let Some(ref remote) = n.remote_device {
            //     if !remote.contains("Spine") && !remote.contains("spine") {
            //         return None;
            //     }
            // } else {
            //     return None;
            // }
            Some(LldpNeighbor {
                local_device: n.local_device?,
                local_port: n.local_port?,
                remote_device: n.remote_device?,
                remote_port: n.remote_port?,
            })
        })
        .collect())
}

async fn snmpwalk_fill<F>(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    base_oid: &str,
    mut f: F,
) -> Result<(), String>
where
    F: FnMut(String, String),
{
    let cmd_args = [
        "-v3",
        "-l",
        "AuthNoPriv",
        "-u",
        user,
        "-a",
        auth_protocol,
        "-A",
        auth_password,
        target,
        base_oid,
    ];

    warn!("Executing snmpwalk: snmpwalk {}", cmd_args.join(" "));

    let out = tokio::process::Command::new("snmpwalk")
        .args(&cmd_args)
        .output()
        .await
        .map_err(|e| e.to_string())?;

    if !out.stderr.is_empty() {
        warn!("snmpwalk stderr: {}", String::from_utf8_lossy(&out.stderr));
    }

    let output = String::from_utf8_lossy(&out.stdout);
    let mut count = 0;

    for line in output.lines() {
        if let Some((oid_part, val)) = line.split_once(" = ") {
            let idx = extract_lldp_index(oid_part, base_oid)?;
            let value = val
                .trim_start_matches("STRING:")
                .trim()
                .trim_matches('"')
                .to_string();
            f(idx, value);
            count += 1;
        }
    }

    warn!("Parsed {} entries for base_oid {}", count, base_oid);
    Ok(())
}

fn split_local_sys_port(val: &str) -> Result<(String, String), String> {
    let s = val.trim().trim_matches('"');
    let mut parts = s.split('_');
    let dev = parts
        .next()
        .ok_or_else(|| format!("invalid local sys/port value: {}", s))?;
    let port = parts
        .next()
        .ok_or_else(|| format!("invalid local sys/port value: {}", s))?;
    Ok((dev.to_string(), port.to_string()))
}

fn extract_lldp_index(oid: &str, base_oid: &str) -> Result<String, String> {
    let oid = normalize_oid(oid);
    if !oid.starts_with(base_oid) {
        return Err(format!("oid {} does not start with base {}", oid, base_oid));
    }
    Ok(oid[base_oid.len()..].trim_start_matches('.').to_string())
}

fn normalize_oid(oid: &str) -> String {
    if let Some(rest) = oid.strip_prefix("iso.") {
        format!("1.{}", rest)
    } else {
        oid.to_string()
    }
}

// async fn get_local_device(
//     target: &str,
//     user: &str,
//     auth_protocol: &str,
//     auth_password: &str,
// ) -> Result<String, String> {
//     let cmd_args = [
//         "-v3",
//         "-l",
//         "AuthNoPriv",
//         "-u",
//         user,
//         "-a",
//         auth_protocol,
//         "-A",
//         auth_password,
//         target,
//         "1.3.6.1.2.1.1.5.0",
//     ];
//
//     warn!("Executing snmpget: snmpget {}", cmd_args.join(" "));
//
//     let out = tokio::process::Command::new("snmpget")
//         .args(&cmd_args)
//         .output()
//         .await
//         .map_err(|e| e.to_string())?;
//
//     if !out.stderr.is_empty() {
//         warn!("snmpget stderr: {}", String::from_utf8_lossy(&out.stderr));
//     }
//
//     let output = String::from_utf8_lossy(&out.stdout);
//
//     if let Some(pos) = output.find("STRING:") {
//         let v = output[pos + 7..].trim().trim_matches('"');
//         return Ok(v.to_string());
//     }
//
//     Err(format!("failed to parse sysName from output: {}", output))
// }

fn neighbors_to_metrics(neighbors: Vec<LldpNeighbor>) -> Vec<Metric> {
    let ts = Utc::now();

    neighbors
        .into_iter()
        .map(|n| {
            let mut tags = MetricTags::default();
            tags.insert("local_device".into(), n.local_device);
            tags.insert("local_port".into(), n.local_port);
            tags.insert("remote_device".into(), n.remote_device);
            tags.insert("remote_port".into(), n.remote_port);
            tags.insert("source".into(), "snmp");
            tags.insert("protocol".into(), "lldp");

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