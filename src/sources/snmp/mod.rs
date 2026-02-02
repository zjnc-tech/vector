//! SNMP source for collecting LLDP topology information
use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::{LogEvent, Value},
};
use chrono::Utc;
use std::{collections::HashMap, time::Duration};
use vector_lib::config::LogNamespace;
use vector_lib::configurable::configurable_component;
use vector_lib::lookup::owned_value_path;
use vector_lib::{config::DataType, schema};
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

    /// SNMP v3用户名
    #[configurable(description = "SNMP v3 username for this cluster")]
    pub user: String,

    /// SNMP v3认证协议 (MD5 or SHA)
    #[configurable(description = "SNMP v3 authentication protocol (MD5 or SHA)")]
    pub auth_protocol: String,

    /// SNMP v3认证密码
    #[configurable(description = "SNMP v3 authentication password")]
    pub auth_password: String,

    /// SNMP v3安全级别: noAuthNoPriv | authNoPriv | authPriv
    #[configurable(description = "SNMP v3 security level: noAuthNoPriv | authNoPriv | authPriv")]
    pub security_level: String,

    /// SNMP v3隐私协议 (AES/DES), 仅authPriv时需要
    #[serde(default)]
    #[configurable(description = "SNMP v3 privacy protocol (AES/DES), required for authPriv")]
    pub priv_protocol: Option<String>,

    /// SNMP v3隐私密码, 仅authPriv时需要
    #[serde(default)]
    #[configurable(description = "SNMP v3 privacy password, required for authPriv")]
    pub priv_password: Option<String>,

    /// 采集间隔时间(秒)
    #[serde(default = "default_interval")]
    #[configurable(description = "Scrape interval in seconds for this cluster")]
    pub scrape_interval_secs: u64,
}

/// Configuration for the `snmp_lldp` source.
#[configurable_component(source("snmp_lldp", "Collect LLDP neighbors from switches via SNMP"))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SnmpSwitchLldpConfig {
    #[configurable(description = "List of cluster configurations")]
    pub clusters: Vec<SnmpClusterConfig>,
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

#[derive(Debug, Clone)]
struct LocalInterface {
    device: String,
    port: String,
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
        let shutdown = cx.shutdown.clone();
        let out = cx.out;
        let clusters = self.clusters.clone();

        Ok(Box::pin(async move {
            let mut cluster_handles = Vec::new();

            // 为每个集群启动一个独立的任务
            for cluster_config in clusters {
                let mut out_clone = out.clone();  // 需要可变引用以发送批次
                let shutdown_clone = shutdown.clone();

                let handle = tokio::spawn(async move {
                    let mut cluster_shutdown = shutdown_clone;
                    
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(cluster_config.scrape_interval_secs)) => {
                                let cluster_logs = collect_cluster_data_with_config(
                                    &cluster_config,
                                ).await;
                                
                                if let Err(e) = out_clone.send_batch(cluster_logs).await {
                                    error!("failed to send cluster {} logs: {}", cluster_config.name, e);
                                }
                            }
                            _ = &mut cluster_shutdown => {
                                info!("Cluster {} shutdown", cluster_config.name);
                                break;
                            }
                        }
                    }
                });

                cluster_handles.push(handle);
            }

            // 等待所有集群任务完成
            for handle in cluster_handles {
                let _ = handle.await;
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
                Kind::bytes(),
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
                Kind::bytes(),
                Some("Connection level"),
            );

        vec![SourceOutput::new_maybe_logs(DataType::Log, definition)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

// 新增函数：使用集群特定配置采集数据
async fn collect_cluster_data_with_config(cluster_config: &SnmpClusterConfig) -> Vec<LogEvent> {
    let mut all_logs = Vec::new();

    // 初始化集群中的目标
    let targets = init_targets(&cluster_config.targets, cluster_config).await;

    // 并发采集集群中每个目标的数据
    let mut tasks = Vec::new();
    for target in &targets {
        let task = tokio::spawn(collect_single_target_with_config(
            target.clone(),
            cluster_config.user.clone(),
            cluster_config.auth_protocol.clone(),
            cluster_config.auth_password.clone(),
            cluster_config.security_level.clone(),
            cluster_config.priv_protocol.clone(),
            cluster_config.priv_password.clone(),
            cluster_config.name.clone(),
        ));
        tasks.push(task);
    }

    // 等待所有任务完成并收集结果
    for task in tasks {
        match task.await {
            Ok(Ok(logs)) => all_logs.extend(logs),
            Ok(Err(e)) => error!("Failed to collect data from target: {}", e),
            Err(e) => error!("Task failed: {}", e),
        }
    }

    all_logs
}

// 初始化目标
async fn init_targets(targets: &[String], cluster_config: &SnmpClusterConfig) -> Vec<Target> {
    let mut result = Vec::new();

    for ip in targets {
        let sys_descr = snmp_get(
            ip,
            &cluster_config.user,
            &cluster_config.auth_protocol,
            &cluster_config.auth_password,
            &cluster_config.security_level,
            &cluster_config.priv_protocol,
            &cluster_config.priv_password,
            "1.3.6.1.2.1.1.1.0",
        )
        .await
        .unwrap_or_default();

        let vendor = detect_vendor(&sys_descr);

        info!(
            "init target={} sysDescr='{}' vendor={:?}",
            ip, sys_descr, vendor
        );

        result.push(Target {
            ip: ip.clone(),
            vendor,
        });
    }

    result
}

// 修改函数签名以接受配置参数
async fn collect_single_target_with_config(
    target: Target,
    user: String,
    auth_protocol: String,
    auth_password: String,
    security_level: String,
    priv_protocol: Option<String>,
    priv_password: Option<String>,
    cluster_name: String,
) -> Result<Vec<LogEvent>, String> {
    match collect_lldp_from_switch(
        &target,
        &user,
        &auth_protocol,
        &auth_password,
        &security_level,
        &priv_protocol,
        &priv_password,
        &cluster_name,
    )
    .await
    {
        Ok((interfaces, neighbors)) => {
            let logs = neighbors_to_logs(interfaces, neighbors, &cluster_name, &target.ip);
            Ok(logs)
        }
        Err(e) => {
            error!(
                "SNMP LLDP scrape failed for target {} in cluster {}: {}",
                target.ip, cluster_name, e
            );
            Err(e)
        }
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
) -> Result<(Vec<LocalInterface>, Vec<LldpNeighbor>), String> {
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

    // 获取本地所有端口信息
    let local_ports = snmpwalk_kv(
        &target.ip,
        user,
        auth_protocol,
        auth_password,
        security_level,
        priv_protocol,
        priv_password,
        "1.3.6.1.2.1.2.2.1.2", // ifDescr OID - 获取所有接口描述
    )
    .await?;

    // 获取LLDP本地端口信息
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

    let mut interfaces = Vec::new();
    let mut neighbors = Vec::new();

    // 收集所有本地接口
    for (_, port_desc) in &local_ports {
        interfaces.push(LocalInterface {
            device: local_device.clone(),
            port: port_desc.to_string(),
        });
    }

    // 收集LLDP邻居信息（有对端连接的端口）
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
            local_port: local_port_raw.clone(),
            remote_device: remote_device.clone(),
            remote_port: normalize_port_name(&remote_port_name),
        });
    }

    Ok((interfaces, neighbors))
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
            let proto = priv_protocol
                .as_ref()
                .ok_or("priv_protocol required for authPriv")?;
            let pass = priv_password
                .as_ref()
                .ok_or("priv_password required for authPriv")?;

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

            // 处理各种前缀，如 "Hex-STRING:", "STRING:"
            let value = process_snmp_value(val);
            map.insert(idx, value);
        }
    }

    Ok(map)
}

// 处理SNMP值的函数
fn process_snmp_value(val: &str) -> String {
    let trimmed = val.trim();

    // 处理可能的前缀，然后清理引号
    let without_prefix = if let Some(stripped) = trimmed.strip_prefix("STRING:") {
        stripped.trim()
    } else if let Some(stripped) = trimmed.strip_prefix("Hex-STRING:") {
        stripped.trim()
    } else {
        trimmed
    };

    // 清理引号
    without_prefix.trim_matches('"').to_string()
}

fn parse_snmp_value(out: &str) -> Result<String, String> {
    if let Some(pos) = out.find("STRING:") {
        Ok(process_snmp_value(&out[pos + 7..]))
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
    // 直接返回原始端口名称，不做任何处理
    port.to_string()
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
    interfaces: Vec<LocalInterface>,
    neighbors: Vec<LldpNeighbor>,
    cluster_name: &str,
    target_ip: &str,
) -> Vec<LogEvent> {
    let ts = Utc::now();
    let mut logs = Vec::new();

    // 创建interface日志 - 存储设备的本地name和port
    for iface in interfaces {
        let role = device_role(&iface.device);
        let normalized_port = normalize_port_name(&iface.port);

        let mut log = LogEvent::default();
        log.insert("timestamp", Value::Timestamp(ts)); // 使用Value::Timestamp以确保兼容性
        log.insert("cluster", Value::from(cluster_name.to_string()));
        log.insert("device", Value::from(iface.device));
        log.insert("interface", Value::from(iface.port));
        log.insert("interface_normalized", Value::from(normalized_port));
        log.insert("out_band_ip", Value::from(target_ip.to_string()));
        log.insert("type", Value::from(role));
        log.insert("log_type", Value::from("interface".to_string())); // 标识这是interface日志

        logs.push(log);
    }

    // 创建link日志 - 存储连接关系，包含from-name、from-port和remote-name、remote-port字段
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

        let from_interface_normalized = normalize_port_name(&local_port);
        let to_interface_normalized = normalize_port_name(&remote_port);

        let mut log = LogEvent::default();
        log.insert("timestamp", Value::Timestamp(ts)); // 使用Value::Timestamp以确保兼容性
        log.insert("cluster", Value::from(cluster_name.to_string()));
        log.insert("level", Value::from(level));
        log.insert("from_device", Value::from(local_device)); // from-name
        log.insert("from_interface", Value::from(local_port)); // from-port
        log.insert(
            "from_interface_normalized",
            Value::from(from_interface_normalized),
        );
        log.insert("to_device", Value::from(remote_device)); // remote-name
        log.insert("to_interface", Value::from(remote_port)); // remote-port
        log.insert(
            "to_interface_normalized",
            Value::from(to_interface_normalized),
        );
        log.insert("log_type", Value::from("link".to_string())); // 标识这是link日志

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
                user: "snmp_user".to_string(),
                auth_protocol: "MD5".to_string(),
                auth_password: "password".to_string(),
                security_level: "authNoPriv".to_string(),
                priv_protocol: None,
                priv_password: None,
                scrape_interval_secs: default_interval(),
            }],
        }
    }
}
