// //! SNMP source for collecting network topology information.
// //!
// //! This source collects LLDP information from network devices using SNMP
// //! to build a topology of nodes, leaf switches, and spine switches.

// use chrono::Utc;
// use regex::Regex;
// // use std::collections::HashMap;
// use std::time::Duration;

// use crate::{
//     config::{SourceConfig, SourceContext, SourceOutput},
//     event::metric::{Metric, MetricKind, MetricTags, MetricValue},
// };
// use vector_lib::configurable::configurable_component;

// /// Configuration for the `snmp` source.
// #[configurable_component(source("snmp", "Collect LLDP neighbors from switches via SNMP"))]
// #[derive(Clone, Debug)]
// #[serde(deny_unknown_fields)]
// pub struct SnmpSwitchLldpConfig {
//     /// Switch IPs
//     pub targets: Vec<String>,

//     /// SNMP v3 user
//     pub user: String,

//     /// Auth protocol: MD5 / SHA
//     pub auth_protocol: String,

//     /// Auth password
//     pub auth_password: String,

//     /// Scrape interval seconds
//     #[serde(default = "default_interval")]
//     pub scrape_interval_secs: u64,
// }

// const fn default_interval() -> u64 {
//     60
// }

// #[derive(Debug, Clone)]
// struct LldpNeighbor {
//     local_device: String,
//     local_port: String,
//     remote_device: String,
//     remote_port: String,
// }
// #[derive(Debug, Clone, Default)]
// struct PartialNeighbor {
//     local_port: Option<String>,
//     remote_device: Option<String>,
//     remote_port: Option<String>,
// }

// impl_generate_config_from_default!(SnmpSwitchLldpConfig);

// #[async_trait::async_trait]
// #[typetag::serde(name = "snmp_switch_lldp")]
// impl SourceConfig for SnmpSwitchLldpConfig {
//     async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
//         let interval = self.scrape_interval_secs;
//         let targets = self.targets.clone();
//         let shutdown = cx.shutdown.clone();
//         let mut out = cx.out;

//         let user = self.user.clone();
//         let auth_protocol = self.auth_protocol.clone();
//         let auth_password = self.auth_password.clone();

//         Ok(Box::pin(async move {
//             let mut ticker = tokio::time::interval(Duration::from_secs(interval));

//             loop {
//                 tokio::select! {
//                     _ = ticker.tick() => {
//                         for target in &targets {
//                             match collect_lldp_from_switch(
//                                 target,
//                                 &user,
//                                 &auth_protocol,
//                                 &auth_password,
//                             ).await {
//                                 Ok(neighbors) => {
//                                     let metrics = neighbors_to_metrics(target, neighbors);
//                                     if out.send_batch(metrics).await.is_err() {
//                                         warn!("failed to send LLDP metrics");
//                                     }
//                                 }
//                                 Err(e) => {
//                                     warn!("SNMP LLDP scrape failed for {}: {}", target, e);
//                                 }
//                             }
//                         }
//                     }

//                     _ = shutdown.clone() => {
//                         info!("snmp_switch_lldp source shutdown");
//                         break;
//                     }
//                 }
//             }

//             Ok(())
//         }))
//     }

//     fn outputs(&self, _: vector_lib::config::LogNamespace) -> Vec<SourceOutput> {
//         vec![SourceOutput::new_metrics()]
//     }

//     fn can_acknowledge(&self) -> bool {
//         false
//     }
// }

// const LLDP_REM_SYS_NAME: &str = "1.0.8802.1.1.2.1.4.1.1.9";
// const LLDP_REM_PORT_ID: &str = "1.0.8802.1.1.2.1.4.1.1.7";
// const LLDP_REM_CHASSIS_ID: &str = "1.0.8802.1.1.2.1.4.1.1.5";

// async fn collect_lldp_from_switch(
//     target: &str,
//     user: &str,
//     auth_protocol: &str,
//     auth_password: &str,
// ) -> Result<Vec<LldpNeighbor>, String> {
//     let local_device = get_local_device(target, user, auth_protocol, auth_password).await?;

//     let mut neighbors: std::collections::HashMap<String, PartialNeighbor> =
//         std::collections::HashMap::new();

//     // local_port
//     snmpwalk_fill(
//         target,
//         user,
//         auth_protocol,
//         auth_password,
//         "1.0.8802.1.1.2.1.3.7.1.4",
//         |idx, val| {
//             neighbors.entry(idx).or_default().local_port = Some(extract_port_from_desc(&val));
//         },
//     )
//     .await?;

//     // remote_device
//     snmpwalk_fill(
//         target,
//         user,
//         auth_protocol,
//         auth_password,
//         LLDP_REM_SYS_NAME,
//         |idx, val| {
//             neighbors.entry(idx).or_default().remote_device = Some(val);
//         },
//     )
//     .await?;

//     // remote_port
//     snmpwalk_fill(
//         target,
//         user,
//         auth_protocol,
//         auth_password,
//         LLDP_REM_PORT_ID,
//         |idx, val| {
//             neighbors.entry(idx).or_default().remote_port = Some(val);
//         },
//     )
//     .await?;

//     Ok(neighbors
//         .into_iter()
//         .filter_map(|(_, n)| {
//             Some(LldpNeighbor {
//                 local_device: local_device.clone(),
//                 local_port: n.local_port?,
//                 remote_device: n.remote_device?,
//                 remote_port: n.remote_port?,
//             })
//         })
//         .collect())
// }

// async fn snmpwalk_fill<F>(
//     target: &str,
//     user: &str,
//     auth_protocol: &str,
//     auth_password: &str,
//     oid: &str,
//     mut f: F,
// ) -> Result<(), String>
// where
//     F: FnMut(String, String),
// {
//     let out = tokio::process::Command::new("snmpwalk")
//         .args([
//             "-v3",
//             "-l",
//             "AuthNoPriv",
//             "-u",
//             user,
//             "-a",
//             auth_protocol,
//             "-A",
//             auth_password,
//             target,
//             oid,
//         ])
//         .output()
//         .await
//         .map_err(|e| e.to_string())?;

//     for line in String::from_utf8_lossy(&out.stdout).lines() {
//         if let Some((oid, val)) = line.split_once(" = STRING: ") {
//             let idx = extract_lldp_index(oid)?;
//             f(idx, val.trim_matches('"').to_string());
//         }
//     }
//     Ok(())
// }

// fn extract_port_from_desc(desc: &str) -> String {
//     static PORT_RE: once_cell::sync::Lazy<Regex> =
//         once_cell::sync::Lazy::new(|| Regex::new(r"^([A-Za-z]+[A-Za-z0-9/]+)").unwrap());

//     let s = desc.trim().trim_matches('"');

//     if let Some(cap) = PORT_RE.captures(s) {
//         cap.get(1).unwrap().as_str().to_string()
//     } else {
//         s.to_string()
//     }
// }

// fn extract_lldp_index(oid: &str) -> Result<String, String> {
//     let parts: Vec<&str> = oid.split('.').collect();
//     if parts.len() < 3 {
//         return Err("bad oid".into());
//     }
//     Ok(parts[parts.len() - 3..].join("."))
// }

// async fn get_local_device(
//     target: &str,
//     user: &str,
//     auth_protocol: &str,
//     auth_password: &str,
// ) -> Result<String, String> {
//     let out = tokio::process::Command::new("snmpget")
//         .args([
//             "-v3",
//             "-l",
//             "AuthNoPriv",
//             "-u",
//             user,
//             "-a",
//             auth_protocol,
//             "-A",
//             auth_password,
//             target,
//             "1.3.6.1.2.1.1.5.0",
//         ])
//         .output()
//         .await
//         .map_err(|e| e.to_string())?;

//     let s = String::from_utf8_lossy(&out.stdout);
//     s.split("STRING:")
//         .nth(1)
//         .map(|v| v.trim().trim_matches('"').to_string())
//         .ok_or("parse sysName failed".into())
// }

// fn neighbors_to_metrics(target: &str, neighbors: Vec<LldpNeighbor>) -> Vec<Metric> {
//     let ts = Utc::now();

//     neighbors
//         .into_iter()
//         .map(|n| {
//             let mut tags = MetricTags::default();

//             tags.insert("local_device".into(), n.local_device.into());
//             tags.insert("local_port".into(), n.local_port.into());
//             tags.insert("remote_device".into(), n.remote_device.into());
//             tags.insert("remote_port".into(), n.remote_port.into());
//             tags.insert("protocol".into(), "lldp".into());

//             Metric::new(
//                 "snmp_lldp_link",
//                 MetricKind::Absolute,
//                 MetricValue::Gauge { value: 1.0 },
//             )
//             .with_tags(Some(tags))
//             .with_timestamp(Some(ts))
//         })
//         .collect()
// }

// // Default implementation for config generation
// impl Default for SnmpSwitchLldpConfig {
//     fn default() -> Self {
//         Self {
//             targets: vec!["127.0.0.1".to_string()],
//             user: "snmp_user".to_string(),
//             auth_protocol: "MD5".to_string(),
//             auth_password: "password".to_string(),
//             scrape_interval_secs: default_interval(),
//         }
//     }
// }
