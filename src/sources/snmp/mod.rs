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

/// SNMP 协议版本，决定底层命令走 v1/v2c 还是 v3 那套认证参数
#[configurable_component]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SnmpVersion {
    /// SNMPv1，使用 community 字符串认证
    V1,

    /// SNMPv2c，使用 community 字符串认证
    V2c,

    /// SNMPv3，使用用户名与安全级别认证
    V3,
}

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

    /// SNMP 协议版本: v1 | v2c | v3，默认 v3
    #[serde(default = "default_version")]
    #[configurable(derived)]
    pub version: SnmpVersion,

    /// SNMP v1/v2c community 字符串, 仅 v1/v2c 时需要
    #[serde(default)]
    #[configurable(description = "SNMP v1/v2c community string, required for v1/v2c")]
    pub community: Option<String>,

    /// SNMP v3用户名, 仅 v3 时需要
    #[serde(default)]
    #[configurable(description = "SNMP v3 username for this cluster, required for v3")]
    pub user: Option<String>,

    /// SNMP v3认证协议 (MD5 or SHA)
    #[serde(default)]
    #[configurable(description = "SNMP v3 authentication protocol (MD5 or SHA)")]
    pub auth_protocol: Option<String>,

    /// SNMP v3认证密码
    #[serde(default)]
    #[configurable(description = "SNMP v3 authentication password")]
    pub auth_password: Option<String>,

    /// SNMP v3安全级别: noAuthNoPriv | authNoPriv | authPriv, 仅 v3 时需要
    #[serde(default)]
    #[configurable(description = "SNMP v3 security level: noAuthNoPriv | authNoPriv | authPriv")]
    pub security_level: Option<String>,

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

// 默认保持 v3，避免已有配置在不感知 version 的情况下语义发生变化
const fn default_version() -> SnmpVersion {
    SnmpVersion::V3
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
        // 先把认证参数收敛成传输配置，版本与参数不匹配时直接在构建阶段失败
        let clusters = self
            .clusters
            .iter()
            .map(|cluster_config| {
                SnmpTransport::from_cluster(cluster_config)
                    .map(|transport| (cluster_config.clone(), transport))
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(Box::pin(async move {
            let mut cluster_handles = Vec::new();

            // 为每个集群启动一个独立的任务
            for (cluster_config, transport) in clusters {
                let mut out_clone = out.clone(); // 需要可变引用以发送批次
                let shutdown_clone = shutdown.clone();

                let handle = tokio::spawn(async move {
                    let mut cluster_shutdown = shutdown_clone;

                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(cluster_config.scrape_interval_secs)) => {
                                let cluster_logs = collect_cluster_data_with_config(
                                    &cluster_config,
                                    &transport,
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
async fn collect_cluster_data_with_config(
    cluster_config: &SnmpClusterConfig,
    transport: &SnmpTransport,
) -> Vec<LogEvent> {
    let mut all_logs = Vec::new();

    // 初始化集群中的目标
    let targets = init_targets(&cluster_config.targets, transport).await;

    // 并发采集集群中每个目标的数据
    let mut tasks = Vec::new();
    for target in &targets {
        let task = tokio::spawn(collect_single_target_with_config(
            target.clone(),
            transport.clone(),
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
async fn init_targets(targets: &[String], transport: &SnmpTransport) -> Vec<Target> {
    let mut result = Vec::new();

    for ip in targets {
        let sys_descr = snmp_get(transport, ip, "1.3.6.1.2.1.1.1.0")
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
    transport: SnmpTransport,
    cluster_name: String,
) -> Result<Vec<LogEvent>, String> {
    match collect_lldp_from_switch(&target, &transport, &cluster_name).await {
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
    transport: &SnmpTransport,
    cluster_name: &str,
) -> Result<(Vec<LocalInterface>, Vec<LldpNeighbor>), String> {
    debug!(
        "Starting LLDP scrape for {} in cluster {}",
        target.ip, cluster_name
    );

    let oids = lldp_oids(&target.vendor);

    let local_device = snmp_get(transport, &target.ip, SYS_NAME).await?;

    // 获取本地所有端口信息
    let local_ports = snmpwalk_kv(
        transport,
        &target.ip,
        "1.3.6.1.2.1.2.2.1.2", // ifDescr OID - 获取所有接口描述
    )
    .await?;

    // 获取LLDP本地端口信息
    let lldp_loc_ports = snmpwalk_kv(transport, &target.ip, oids.loc_port).await?;

    let rem_sys = snmpwalk_kv(transport, &target.ip, oids.rem_sys).await?;

    let rem_port = snmpwalk_kv(transport, &target.ip, oids.rem_port).await?;

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
/// 一次 SNMP 请求所需的传输参数，已从集群配置中收敛并校验完成
#[derive(Clone, Debug)]
struct SnmpTransport {
    version: SnmpVersion,
    community: Option<String>,
    user: Option<String>,
    auth_protocol: Option<String>,
    auth_password: Option<String>,
    security_level: Option<String>,
    priv_protocol: Option<String>,
    priv_password: Option<String>,
}

impl SnmpTransport {
    /// 从集群配置中收敛认证参数，并在启动采集前完成一次参数合法性校验
    fn from_cluster(cluster_config: &SnmpClusterConfig) -> Result<Self, String> {
        let transport = Self {
            version: cluster_config.version,
            community: cluster_config.community.clone(),
            user: cluster_config.user.clone(),
            auth_protocol: cluster_config.auth_protocol.clone(),
            auth_password: cluster_config.auth_password.clone(),
            security_level: cluster_config.security_level.clone(),
            priv_protocol: cluster_config.priv_protocol.clone(),
            priv_password: cluster_config.priv_password.clone(),
        };

        // 复用 build_args 做前置校验，避免同样的版本分支写两遍
        transport.build_args()?;

        Ok(transport)
    }

    /// 按版本拼接底层命令的参数，v1/v2c 与 v3 走完全不同的两套
    fn build_args(&self) -> Result<Vec<String>, String> {
        let mut args = vec![match self.version {
            SnmpVersion::V1 => "-v1".to_string(),
            SnmpVersion::V2c => "-v2c".to_string(),
            SnmpVersion::V3 => "-v3".to_string(),
        }];

        if self.version != SnmpVersion::V3 {
            args.extend([
                "-c".into(),
                self.required(self.community.as_deref(), "community")?,
            ]);
            return Ok(args);
        }

        let user = self.required(self.user.as_deref(), "user")?;
        let security_level = self.required(self.security_level.as_deref(), "security_level")?;

        args.extend(["-l".into(), security_level.clone(), "-u".into(), user]);

        match security_level.as_str() {
            "noAuthNoPriv" => {}
            "authNoPriv" => {
                args.extend([
                    "-a".into(),
                    self.required(self.auth_protocol.as_deref(), "auth_protocol")?,
                    "-A".into(),
                    self.required(self.auth_password.as_deref(), "auth_password")?,
                ]);
            }
            "authPriv" => {
                args.extend([
                    "-a".into(),
                    self.required(self.auth_protocol.as_deref(), "auth_protocol")?,
                    "-A".into(),
                    self.required(self.auth_password.as_deref(), "auth_password")?,
                    "-x".into(),
                    self.required(self.priv_protocol.as_deref(), "priv_protocol")?,
                    "-X".into(),
                    self.required(self.priv_password.as_deref(), "priv_password")?,
                ]);
            }
            other => return Err(format!("invalid security_level: {}", other)),
        }

        Ok(args)
    }

    /// 取出必填参数，缺失或为空时报出字段名，方便定位配置问题
    fn required(&self, value: Option<&str>, field: &str) -> Result<String, String> {
        value
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("SNMP {:?} requires `{}` to be set", self.version, field))
    }
}

/// 执行底层 snmp 命令；命令自身报错时只记录退出码与 stderr，仍然把 stdout 交给调用方解析，
/// 保持与之前一致的「单个 OID 取不到不影响其余数据采集」行为
async fn run_snmp(program: &str, args: &[String]) -> Result<String, String> {
    let out = tokio::process::Command::new(program)
        .args(args)
        .output()
        .await
        .map_err(|e| format!("failed to run {}: {}", program, e))?;

    if !out.status.success() {
        // 参数尾部固定是 target 与 oid，日志只输出这两个，避免把密码带上
        let target = args
            .get(args.len().saturating_sub(2))
            .map(String::as_str)
            .unwrap_or("unknown");
        let stderr = String::from_utf8_lossy(&out.stderr);
        warn!(
            "{} query for {} failed with {}: {}",
            program,
            target,
            out.status,
            stderr.trim()
        );
    }

    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

async fn snmp_get(transport: &SnmpTransport, target: &str, oid: &str) -> Result<String, String> {
    let mut args = transport.build_args()?;
    args.push(target.into());
    args.push(oid.into());

    debug!(
        "running snmpget version={:?} target={} oid={}",
        transport.version, target, oid
    );

    let out = run_snmp("snmpget", &args).await?;

    parse_snmp_value(&out)
}

async fn snmpwalk_kv(
    transport: &SnmpTransport,
    target: &str,
    base_oid: &str,
) -> Result<HashMap<String, String>, String> {
    let mut args = transport.build_args()?;
    args.push(target.into());
    args.push(base_oid.into());

    debug!(
        "running snmpwalk version={:?} target={} oid={}",
        transport.version, target, base_oid
    );

    let out = run_snmp("snmpwalk", &args).await?;

    let mut map = HashMap::new();

    for line in out.lines() {
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

    debug!("snmpwalk {} returned {} entries", base_oid, map.len());

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
    if n.contains("RASW") || n.contains("EHSW ") || n.contains("LEAF") {
        "leaf"
    } else if n.contains("RDSW") || n.contains("EDSW ") || n.contains("SPINE") {
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
        // 如果本端设备名以"O"开头或以"RMSW"开头，则舍弃数据
        if n.local_device.starts_with('O')
            || n.local_device.starts_with('o')
            || n.local_device.starts_with("RMSW")
            || n.local_device.starts_with("rmsw")
        {
            continue;
        }

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
                version: default_version(),
                community: None,
                user: Some("snmp_user".to_string()),
                auth_protocol: Some("MD5".to_string()),
                auth_password: Some("password".to_string()),
                security_level: Some("authNoPriv".to_string()),
                priv_protocol: None,
                priv_password: None,
                scrape_interval_secs: default_interval(),
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{default_interval, SnmpClusterConfig, SnmpTransport, SnmpVersion};

    /// 构造一个只指定版本的集群配置，其余认证参数由测试用例按需填充
    fn cluster(version: SnmpVersion) -> SnmpClusterConfig {
        SnmpClusterConfig {
            name: "test".to_string(),
            targets: vec!["127.0.0.1".to_string()],
            version,
            community: None,
            user: None,
            auth_protocol: None,
            auth_password: None,
            security_level: None,
            priv_protocol: None,
            priv_password: None,
            scrape_interval_secs: default_interval(),
        }
    }

    /// 构造一个完整的 v3 集群配置
    fn v3_cluster(security_level: &str) -> SnmpClusterConfig {
        let mut config = cluster(SnmpVersion::V3);
        config.user = Some("snmp_user".to_string());
        config.security_level = Some(security_level.to_string());
        config.auth_protocol = Some("MD5".to_string());
        config.auth_password = Some("snmppwv3".to_string());
        config
    }

    #[test]
    fn v2c_builds_community_command() {
        let mut config = cluster(SnmpVersion::V2c);
        config.community = Some("public".to_string());

        let transport = SnmpTransport::from_cluster(&config).unwrap();
        assert_eq!(
            transport.build_args().unwrap(),
            vec!["-v2c", "-c", "public"]
        );
    }

    #[test]
    fn v1_builds_community_command() {
        let mut config = cluster(SnmpVersion::V1);
        config.community = Some("public".to_string());

        let transport = SnmpTransport::from_cluster(&config).unwrap();
        assert_eq!(transport.build_args().unwrap(), vec!["-v1", "-c", "public"]);
    }

    #[test]
    fn v3_builds_usm_command() {
        let config = v3_cluster("authNoPriv");

        let transport = SnmpTransport::from_cluster(&config).unwrap();
        assert_eq!(
            transport.build_args().unwrap(),
            vec![
                "-v3",
                "-l",
                "authNoPriv",
                "-u",
                "snmp_user",
                "-a",
                "MD5",
                "-A",
                "snmppwv3"
            ]
        );
    }

    #[test]
    fn v2c_requires_community() {
        let config = cluster(SnmpVersion::V2c);

        assert!(SnmpTransport::from_cluster(&config).is_err());
    }

    #[test]
    fn v3_requires_user_and_security_level() {
        let config = cluster(SnmpVersion::V3);

        assert!(SnmpTransport::from_cluster(&config).is_err());
    }

    #[test]
    fn auth_priv_requires_priv_params() {
        let config = v3_cluster("authPriv");

        assert!(SnmpTransport::from_cluster(&config).is_err());
    }

    #[test]
    fn unknown_security_level_is_rejected() {
        let config = v3_cluster("noSuchLevel");

        let error = SnmpTransport::from_cluster(&config).err().unwrap();
        assert!(error.contains("invalid security_level"), "got: {}", error);
    }
}
