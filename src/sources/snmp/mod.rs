//! SNMP source for collecting LLDP topology information
use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::metric::{Metric, MetricKind, MetricTags, MetricValue},
};
use chrono::Utc;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
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

    #[configurable(description = "SNMP v3 username")]
    pub user: String,

    #[configurable(description = "SNMP v3 authentication protocol (MD5 or SHA)")]
    pub auth_protocol: String,

    #[configurable(description = "SNMP v3 authentication password")]
    pub auth_password: String,

    #[configurable(description = "SNMP v3 security level: noAuthNoPriv | authNoPriv | authPriv")]
    pub security_level: String,

    #[configurable(description = "SNMP v3 privacy protocol (AES/DES), required for authPriv")]
    #[serde(default)]
    pub priv_protocol: Option<String>,

    #[configurable(description = "SNMP v3 privacy password, required for authPriv")]
    #[serde(default)]
    pub priv_password: Option<String>,

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

#[derive(Clone, Debug)]
struct Cluster {
    name: String,
    targets: Vec<Target>,
}

#[derive(Clone, Debug)]
struct Target {
    ip: String,
    vendor: Vendor,
}

#[derive(Clone, Debug)]
enum Vendor {
    CNIT,
    H3C,
    Huawei,
    Unknown,
}

impl_generate_config_from_default!(SnmpSwitchLldpConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "snmp_lldp")]
impl SourceConfig for SnmpSwitchLldpConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let interval = self.scrape_interval_secs;
        let clusters = self.init_clusters().await;
        let shutdown = cx.shutdown.clone();
        let mut out = cx.out;

        let user = self.user.clone();
        let auth_protocol = self.auth_protocol.clone();
        let auth_password = self.auth_password.clone();
        let security_level = self.security_level.clone();
        let priv_protocol = self.priv_protocol.clone();
        let priv_password = self.priv_password.clone();

        Ok(Box::pin(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        for cluster in &clusters {
                            for target in &cluster.targets {
                                match collect_lldp_from_switch(
                                    target,
                                    &user,
                                    &auth_protocol,
                                    &auth_password,
                                    &security_level,
                                    &priv_protocol,
                                    &priv_password,
                                    &cluster.name,
                                ).await {
                                    Ok(neighbors) => {
                                        let metrics = neighbors_to_metrics(
                                            neighbors,
                                            &cluster.name,
                                            &target.ip,
                                        );
                                        if out.send_batch(metrics).await.is_err() {
                                            error!("failed to send LLDP metrics");
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "SNMP LLDP scrape failed for target {} in cluster {}: {}",
                                            target.ip, cluster.name, e
                                        );
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

impl SnmpSwitchLldpConfig {
    async fn init_clusters(&self) -> Vec<Cluster> {
        let mut out = Vec::new();

        for cluster in &self.clusters {
            let mut targets = Vec::new();

            for ip in &cluster.targets {
                let sys_descr = snmp_get(
                    ip,
                    &self.user,
                    &self.auth_protocol,
                    &self.auth_password,
                    &self.security_level,
                    &self.priv_protocol,
                    &self.priv_password,
                    "1.3.6.1.2.1.1.1.0",
                )
                    .await
                    .unwrap_or_default();

                let vendor = detect_vendor(&sys_descr);

                info!(
                    "init target={} sysDescr='{}' vendor={:?}",
                    ip, sys_descr, vendor
                );

                targets.push(Target {
                    ip: ip.clone(),
                    vendor,
                });
            }

            out.push(Cluster {
                name: cluster.name.clone(),
                targets,
            });
        }

        out
    }
}

fn detect_vendor(sys_descr: &str) -> Vendor {
    let s = sys_descr.to_ascii_lowercase();

    if s.contains("cnit") {
        Vendor::CNIT
    } else if s.contains("h3c") {
        Vendor::H3C
    } else if s.contains("huawei") {
        Vendor::Huawei
    } else {
        Vendor::Unknown
    }
}

struct LldpOidSet {
    loc_port: &'static str,
    rem_sys: &'static str,
    rem_port: &'static str,
}

fn lldp_oids(_: &Vendor) -> LldpOidSet {
    LldpOidSet {
        loc_port: "1.0.8802.1.1.2.1.3.7.1.3",
        rem_sys: "1.0.8802.1.1.2.1.4.1.1.9",
        rem_port: "1.0.8802.1.1.2.1.4.1.1.7",
    }
}

const SYS_NAME: &str = "1.3.6.1.2.1.1.5.0";

// ----------------- Collect LLDP -----------------
async fn collect_lldp_from_switch(
    target: &Target,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    security_level: &str,
    priv_protocol: &Option<String>,
    priv_password: &Option<String>,
    cluster_name: &str,
) -> Result<Vec<LldpNeighbor>, String> {
    debug!(
        "Starting LLDP scrape for {} in cluster {}",
        target.ip, cluster_name
    );

    let oids = lldp_oids(&target.vendor);

    let local_device = snmp_get(
        &target.ip,
        user,
        auth_protocol,
        auth_password,
        security_level,
        priv_protocol,
        priv_password,
        SYS_NAME,
    )
        .await?;

    let lldp_loc_ports = snmpwalk_kv(
        &target.ip,
        user,
        auth_protocol,
        auth_password,
        security_level,
        priv_protocol,
        priv_password,
        oids.loc_port,
    )
        .await?;

    let rem_sys = snmpwalk_kv(
        &target.ip,
        user,
        auth_protocol,
        auth_password,
        security_level,
        priv_protocol,
        priv_password,
        oids.rem_sys,
    )
        .await?;

    let rem_port = snmpwalk_kv(
        &target.ip,
        user,
        auth_protocol,
        auth_password,
        security_level,
        priv_protocol,
        priv_password,
        oids.rem_port,
    )
        .await?;

    let mut neighbors = Vec::new();

    for (lldp_idx, remote_device) in &rem_sys {
        let remote_port_name = match rem_port.get(lldp_idx) {
            Some(v) => v.clone(),
            None => continue,
        };

        let parts: Vec<&str> = lldp_idx.split('.').collect();
        let local_port_num_str = match parts.get(parts.len().saturating_sub(2)) {
            Some(v) => *v,
            None => continue,
        };

        let local_port_raw = match lldp_loc_ports.get(local_port_num_str) {
            Some(v) => v,
            None => continue,
        };

        neighbors.push(LldpNeighbor {
            local_device: local_device.clone(),
            local_port: normalize_port_name(local_port_raw),
            remote_device: remote_device.clone(),
            remote_port: normalize_port_name(&remote_port_name),
        });
    }

    Ok(neighbors)
}

// ----------------- SNMP helpers -----------------
fn build_snmpv3_args(
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    security_level: &str,
    priv_protocol: &Option<String>,
    priv_password: &Option<String>,
) -> Result<Vec<String>, String> {
    let mut args = vec![
        "-v3".into(),
        "-l".into(),
        security_level.into(),
        "-u".into(),
        user.into(),
    ];

    match security_level {
        "noAuthNoPriv" => {}
        "authNoPriv" => {
            args.extend([
                "-a".into(),
                auth_protocol.into(),
                "-A".into(),
                auth_password.into(),
            ]);
        }
        "authPriv" => {
            let proto = priv_protocol.as_ref().ok_or("priv_protocol required for authPriv")?;
            let pass = priv_password.as_ref().ok_or("priv_password required for authPriv")?;

            args.extend([
                "-a".into(),
                auth_protocol.into(),
                "-A".into(),
                auth_password.into(),
                "-x".into(),
                proto.clone(),
                "-X".into(),
                pass.clone(),
            ]);
        }
        _ => return Err(format!("invalid security_level: {}", security_level)),
    }

    Ok(args)
}

async fn snmp_get(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    security_level: &str,
    priv_protocol: &Option<String>,
    priv_password: &Option<String>,
    oid: &str,
) -> Result<String, String> {
    let mut args = build_snmpv3_args(
        user,
        auth_protocol,
        auth_password,
        security_level,
        priv_protocol,
        priv_password,
    )?;
    args.push(target.into());
    args.push(oid.into());

    let out = tokio::process::Command::new("snmpget")
        .args(args)
        .output()
        .await
        .map_err(|e| e.to_string())?;

    parse_snmp_value(&String::from_utf8_lossy(&out.stdout))
}

async fn snmpwalk_kv(
    target: &str,
    user: &str,
    auth_protocol: &str,
    auth_password: &str,
    security_level: &str,
    priv_protocol: &Option<String>,
    priv_password: &Option<String>,
    base_oid: &str,
) -> Result<HashMap<String, String>, String> {
    let mut args = build_snmpv3_args(
        user,
        auth_protocol,
        auth_password,
        security_level,
        priv_protocol,
        priv_password,
    )?;
    args.push(target.into());
    args.push(base_oid.into());

    let out = tokio::process::Command::new("snmpwalk")
        .args(args)
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
    if n.contains("RASW") || n.contains("LEAF") {
        "leaf"
    } else if n.contains("RDSW") || n.contains("SPINE") {
        "spine"
    } else {
        "node"
    }
}

fn neighbors_to_metrics(
    neighbors: Vec<LldpNeighbor>,
    cluster_name: &str,
    target_ip: &str,
) -> Vec<Metric> {
    let ts = Utc::now();
    let mut metrics = Vec::new();

    let mut interfaces = HashSet::new();
    for n in &neighbors {
        interfaces.insert(InterfaceInfo {
            device: n.local_device.clone(),
            port: n.local_port.clone(),
        });
    }

    for iface in interfaces {
        let role = device_role(&iface.device);

        let mut tags = MetricTags::default();
        tags.insert("device".into(), iface.device);
        tags.insert("port".into(), iface.port);
        tags.insert("cluster".into(), cluster_name.to_string());
        tags.insert("source".into(), "snmp-collector");
        tags.insert("protocol".into(), "interface");
        tags.insert("ip".into(), target_ip.to_string());

        match role {
            "leaf" => tags.insert("type".into(), "1"),
            "spine" => tags.insert("type".into(), "2"),
            _ => {}
        };

        metrics.push(
            Metric::new("interface", MetricKind::Absolute, MetricValue::Gauge { value: 1.0 })
                .with_tags(Some(tags))
                .with_timestamp(Some(ts)),
        );
    }

    for n in neighbors {
        let local_role = device_role(&n.local_device);
        let remote_role = device_role(&n.remote_device);

        let level = match (local_role, remote_role) {
            ("leaf", "spine") => Some("1"),
            ("leaf", "node") => Some("2"),
            ("spine", "leaf") => Some("3"),
            _ => None,
        };

        if level == Some("3") {
            continue;
        }

        let (ld, lp, rd, rp) = if level == Some("2") {
            (n.remote_device, n.remote_port, n.local_device, n.local_port)
        } else {
            (n.local_device, n.local_port, n.remote_device, n.remote_port)
        };

        let mut tags = MetricTags::default();
        tags.insert("local_device".into(), ld);
        tags.insert("local_port".into(), lp);
        tags.insert("remote_device".into(), rd);
        tags.insert("remote_port".into(), rp);
        tags.insert("cluster".into(), cluster_name.to_string());
        tags.insert("source".into(), "snmp-collector");
        tags.insert("protocol".into(), "lldp");

        if let Some(lv) = level {
            tags.insert("level".into(), lv);
        }

        metrics.push(
            Metric::new("link", MetricKind::Absolute, MetricValue::Gauge { value: 1.0 })
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
            security_level: "authNoPriv".to_string(),
            priv_protocol: None,
            priv_password: None,
            scrape_interval_secs: default_interval(),
        }
    }
}