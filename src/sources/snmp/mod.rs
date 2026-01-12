//! SNMP source for collecting LLDP topology information
use chrono::Utc;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::metric::{Metric, MetricKind, MetricTags, MetricValue},
};
use vector_lib::configurable::configurable_component;

/// 表示单个集群的配置
#[configurable_component]
#[derive(Clone, Debug)]
pub struct SnmpClusterConfig {
    /// 集群名称
    #[configurable(description = "Name of the cluster")]
    pub name: String,

    /// 该集群中的目标交换机列表
    #[configurable(description = "List of switch management IPs or hostnames for this cluster")]
    pub targets: Vec<String>,
}

/// Configuration for the `snmp_lldp` source.
#[configurable_component(source("snmp_lldp", "Collect LLDP neighbors from switches via SNMP"))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SnmpSwitchLldpConfig {
    #[configurable(description = "List of cluster configurations")]
    pub clusters: Vec<SnmpClusterConfig>,
    #[configurable(description = "SNMP v3 authentication username")]
    pub user: String,
    #[configurable(description = "SNMP v3 authentication protocol (MD5 or SHA)")]
    pub auth_protocol: String,
    #[configurable(description = "SNMP v3 authentication password")]
    pub auth_password: String,
    #[configurable(description = "Scrape interval in seconds")]
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

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct InterfaceInfo {
    device: String,
    port: String,
}

impl_generate_config_from_default!(SnmpSwitchLldpConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "snmp_lldp")]
impl SourceConfig for SnmpSwitchLldpConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let interval = self.scrape_interval_secs;
        let clusters = self.clusters.clone();
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
                        for cluster in &clusters {
                            for target in &cluster.targets {
                                match collect_lldp_from_switch(target, &user, &auth_protocol, &auth_password, &cluster.name).await {
                                    Ok(neighbors) => {
                                        let metrics = neighbors_to_metrics(neighbors, &cluster.name);
                                        if out.send_batch(metrics).await.is_err() {
                                            error!("failed to send LLDP metrics");
                                        }
                                    }
                                    Err(e) => {
                                        error!("SNMP LLDP scrape failed for target {} in cluster {}: {}", target, cluster.name, e);
                                    }
                                }
                            }
                        }
                    }
                    _ = shutdown.clone() => {
                        info!("snmp_lldp source shutdown");
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

// ----------------- OIDs -----------------
const SYS_NAME: &str = "1.3.6.1.2.1.1.5.0";
const LLDP_LOC_PORT_ID: &str = "1.0.8802.1.1.2.1.3.7.1.3";
const LLDP_REM_SYS_NAME: &str = "1.0.8802.1.1.2.1.4.1.1.9";
const LLDP_REM_PORT_ID: &str = "1.0.8802.1.1.2.1.4.1.1.7";

// ----------------- Collect LLDP -----------------
async fn collect_lldp_from_switch(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    cluster_name: &str,
) -> Result<Vec<LldpNeighbor>, String> {
    debug!(
        "Starting LLDP scrape for {} in cluster {}",
        target, cluster_name
    );

    // 1. local device name
    let local_device = snmp_get(target, user, auth_protocol, auth_password, SYS_NAME).await?;

    // 2. LLDP local port table（index -> port name）
    let lldp_loc_ports =
        snmpwalk_kv(target, user, auth_protocol, auth_password, LLDP_LOC_PORT_ID).await?;

    // 3. LLDP remote table（index -> remote device / remote port）
    let rem_sys = snmpwalk_kv(
        target,
        user,
        auth_protocol,
        auth_password,
        LLDP_REM_SYS_NAME,
    )
    .await?;
    let rem_port =
        snmpwalk_kv(target, user, auth_protocol, auth_password, LLDP_REM_PORT_ID).await?;

    debug!("local_device: {}", local_device);
    debug!("lldp_loc_ports: {:?}", lldp_loc_ports);
    debug!("rem_sys: {:?}", rem_sys);
    debug!("rem_port: {:?}", rem_port);

    let mut neighbors = Vec::new();

    for (lldp_idx, remote_device) in &rem_sys {
        debug!("Processing lldp_idx: {}", lldp_idx);

        // 1. remote port
        let remote_port_name = match rem_port.get(lldp_idx) {
            Some(v) => v.clone(),
            None => {
                error!("lldp_idx {} not found in rem_port", lldp_idx);
                continue;
            }
        };

        // 2. 解析 lldp_idx，取倒数第二段作为本地端口号
        let parts: Vec<&str> = lldp_idx.split('.').collect();
        let local_port_num_str = match parts.get(parts.len().saturating_sub(2)) {
            Some(v) => *v,
            None => {
                error!("lldp_idx {} invalid format", lldp_idx);
                continue;
            }
        };

        // 3. 查本地端口名
        let local_port_raw = match lldp_loc_ports.get(local_port_num_str) {
            Some(v) => v,
            None => {
                error!(
                    "local_port_num {} not found in lldp_loc_ports",
                    local_port_num_str
                );
                continue;
            }
        };

        let local_port_name = normalize_port_name(local_port_raw);
        let remote_port_name = normalize_port_name(&remote_port_name);

        neighbors.push(LldpNeighbor {
            local_device: local_device.clone(),
            local_port: local_port_name,
            remote_device: remote_device.clone(),
            remote_port: remote_port_name,
        });
    }

    Ok(neighbors)
}

// ----------------- SNMP Helpers -----------------
async fn snmp_get(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    oid: &str,
) -> Result<String, String> {
    let out = tokio::process::Command::new("snmpget")
        .args([
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
            oid,
        ])
        .output()
        .await
        .map_err(|e| e.to_string())?;
    let s = String::from_utf8_lossy(&out.stdout);
    parse_snmp_value(&s)
}

async fn snmpwalk_kv(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    base_oid: &str,
) -> Result<HashMap<String, String>, String> {
    let out = tokio::process::Command::new("snmpwalk")
        .args([
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
        ])
        .output()
        .await
        .map_err(|e| e.to_string())?;
    let s = String::from_utf8_lossy(&out.stdout);

    let mut map = HashMap::new();
    for line in s.lines() {
        if let Some((oid, val)) = line.split_once(" = ") {
            let idx = normalize_oid(oid)
                .trim_start_matches(base_oid)
                .trim_start_matches('.')
                .to_string();
            let value = val
                .trim_start_matches("STRING:")
                .trim()
                .trim_matches('"')
                .to_string();
            map.insert(idx, value);
        }
    }
    Ok(map)
}

fn parse_snmp_value(out: &str) -> Result<String, String> {
    if let Some(pos) = out.find("STRING:") {
        Ok(out[pos + 7..].trim().trim_matches('"').to_string())
    } else {
        Err(format!("invalid snmp output: {}", out))
    }
}

fn normalize_oid(oid: &str) -> String {
    oid.strip_prefix("iso.")
        .map(|v| format!("1.{}", v))
        .unwrap_or_else(|| oid.to_string())
}

fn normalize_port_name(port: &str) -> String {
    // 提取末尾连续数字
    let digits: String = port
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect();

    if digits.is_empty() {
        port.to_string()
    } else {
        format!("Ethernet{}", digits)
    }
}

fn device_role(name: &str) -> &'static str {
    let n = name.to_ascii_uppercase();
    if n.contains("RASW") {
        "leaf"
    } else if n.contains("RDSW") {
        "spine"
    } else {
        "node"
    }
}

fn neighbors_to_metrics(neighbors: Vec<LldpNeighbor>, cluster_name: &str) -> Vec<Metric> {
    let ts = Utc::now();
    let mut metrics = Vec::new();

    let mut interfaces: HashSet<InterfaceInfo> = HashSet::new();
    for n in &neighbors {
        interfaces.insert(InterfaceInfo {
            device: n.local_device.clone(),
            port: n.local_port.clone(),
        });
    }

    // -------- interface --------
    for iface in interfaces {
        let role = device_role(&iface.device);

        let mut tags = MetricTags::default();
        tags.insert("device".into(), iface.device.clone());
        tags.insert("port".into(), iface.port);
        tags.insert("cluster".into(), cluster_name.to_string());
        tags.insert("source".into(), "snmp-collector");
        tags.insert("protocol".into(), "interface");

        match role {
            "leaf" => {
                tags.insert("type".into(), "1");
            }
            "spine" => {
                tags.insert("type".into(), "2");
            }
            _ => {}
        }

        metrics.push(
            Metric::new(
                "interface",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 1.0 },
            )
            .with_tags(Some(tags))
            .with_timestamp(Some(ts)),
        );
    }

    // -------- link --------
    for n in neighbors {
        let local_role = device_role(&n.local_device);
        let remote_role = device_role(&n.remote_device);

        let level = match (local_role, remote_role) {
            ("leaf", "spine") => Some("1"),
            ("leaf", "node") => Some("2"),
            ("spine", "leaf") => Some("3"),
            _ => None,
        };

        // level 3：直接丢弃
        if level == Some("3") {
            continue;
        }

        // 是否需要反转
        let (local_device, local_port, remote_device, remote_port) = if level == Some("2") {
            (n.remote_device, n.remote_port, n.local_device, n.local_port)
        } else {
            (n.local_device, n.local_port, n.remote_device, n.remote_port)
        };

        let mut tags = MetricTags::default();
        tags.insert("local_device".into(), local_device);
        tags.insert("local_port".into(), local_port);
        tags.insert("remote_device".into(), remote_device);
        tags.insert("remote_port".into(), remote_port);
        tags.insert("cluster".into(), cluster_name.to_string());
        tags.insert("source".into(), "snmp-collector");
        tags.insert("protocol".into(), "lldp");

        if let Some(lv) = level {
            tags.insert("level".into(), lv);
        }

        metrics.push(
            Metric::new(
                "link",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 1.0 },
            )
            .with_tags(Some(tags))
            .with_timestamp(Some(ts)),
        );
    }

    metrics
}
impl Default for SnmpSwitchLldpConfig {
    fn default() -> Self {
        Self {
            clusters: vec![SnmpClusterConfig {
                name: "default".to_string(),
                targets: vec!["127.0.0.1".to_string()],
            }],
            user: "snmp_user".to_string(),
            auth_protocol: "MD5".to_string(),
            auth_password: "password".to_string(),
            scrape_interval_secs: default_interval(),
        }
    }
}
