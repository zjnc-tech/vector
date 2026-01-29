//! SNMP source for collecting LLDP topology information
use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::{LogEvent, Value},
};
use chrono::Utc;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use vector_lib::configurable::configurable_component;
use vector_lib::{config::DataType, schema};
use vector_lib::config::LogNamespace;
use vector_lib::lookup::owned_value_path;
use vrl::value::Kind;

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
                                        let logs = neighbors_to_logs(
                                            neighbors,
                                            &cluster.name,
                                            &target.ip,
                                        );
                                        if out.send_batch(logs).await.is_err() {
                                            error!("failed to send LLDP logs");
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

    fn outputs(&self, _: LogNamespace) -> Vec<SourceOutput> {
        let definition = schema::Definition::empty_legacy_namespace()
            .with_event_field(&owned_value_path!("timestamp"), Kind::timestamp(), Some("Time when the event was observed"))
            .with_event_field(&owned_value_path!("cluster"), Kind::bytes(), Some("Cluster identifier"))
            .with_event_field(&owned_value_path!("device"), Kind::bytes(), Some("Device name"))
            .with_event_field(&owned_value_path!("port"), Kind::bytes(), Some("Device port"))
            .with_event_field(&owned_value_path!("out_band_ip"), Kind::bytes(), Some("Out-of-band IP address"))
            .with_event_field(&owned_value_path!("type"), Kind::integer(), Some("Device type (0=node, 1=leaf, 2=spine)"))
            .with_event_field(&owned_value_path!("from_device"), Kind::bytes(), Some("Source device in connection"))
            .with_event_field(&owned_value_path!("from_port"), Kind::bytes(), Some("Source port in connection"))
            .with_event_field(&owned_value_path!("to_device"), Kind::bytes(), Some("Destination device in connection"))
            .with_event_field(&owned_value_path!("to_port"), Kind::bytes(), Some("Destination port in connection"))
            .with_event_field(&owned_value_path!("log_type"), Kind::bytes(), Some("Type of log: interface or link"));

        vec![SourceOutput::new_maybe_logs(DataType::Log, definition)]
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

fn neighbors_to_logs(
    neighbors: Vec<LldpNeighbor>,
    cluster_name: &str,
    target_ip: &str,
) -> Vec<LogEvent> {
    let ts = Utc::now();
    let mut logs = Vec::new();

    let mut interfaces = HashSet::new();
    for n in &neighbors {
        interfaces.insert(InterfaceInfo {
            device: n.local_device.clone(),
            port: n.local_port.clone(),
        });
    }

    // 创建interface日志 - 存储设备的本地name和port
    for iface in interfaces {
        let role = device_role(&iface.device);

        let mut log = LogEvent::default();
        log.insert("timestamp", Value::Timestamp(ts));  // 使用Value::Timestamp以确保兼容性
        log.insert("cluster", Value::from(cluster_name.to_string()));
        log.insert("device", Value::from(iface.device));
        log.insert("port", Value::from(iface.port));
        log.insert("out_band_ip", Value::from(target_ip.to_string()));
        
        // let device_type = match role {
        //     "leaf" => 1i64,
        //     "spine" => 2i64,
        //     _ => 0i64,
        // };
        log.insert("type", Value::from(role));
        log.insert("log_type", Value::from("interface".to_string())); // 标识这是interface日志
        
        logs.push(log);
    }

    // 创建link日志 - 存储连接关系，包含from-name、from-port和remote-name、remote-port字段
    for n in neighbors {
        let mut log = LogEvent::default();
        log.insert("timestamp", Value::Timestamp(ts));  // 使用Value::Timestamp以确保兼容性
        log.insert("cluster", Value::from(cluster_name.to_string()));
        log.insert("from_device", Value::from(n.local_device));      // from-name
        log.insert("from_port", Value::from(n.local_port));          // from-port
        log.insert("to_device", Value::from(n.remote_device));       // remote-name
        log.insert("to_port", Value::from(n.remote_port));           // remote-port
        log.insert("log_type", Value::from("link".to_string()));     // 标识这是link日志
        
        logs.push(log);
    }

    logs
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