use k8s_openapi::api::core::v1::{Node, Pod, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::reflector::{self, store::Store, ObjectRef};
use kube::runtime::WatchStreamExt;
use kube::{api::Api, runtime::watcher, Client};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use vector_lib::configurable::Configurable;

use crate::kubernetes::{custom_reflector, meta_cache::MetaCache as K8sMetaCache};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// ============================================================================
// 配置结构
// ============================================================================

/// Kubernetes service discovery configuration.
#[derive(Clone, Debug, Serialize, Deserialize, Configurable)]
pub struct KubernetesSdConfig {
    /// The Kubernetes resource type to discover.
    pub role: KubernetesRole,

    /// List of namespaces to discover resources in. If empty, discovers in all namespaces.
    #[serde(default)]
    pub namespaces: Vec<String>,

    /// Label selector to filter resources.
    #[serde(default)]
    pub label_selector: Option<String>,

    /// Field selector to filter resources.
    #[serde(default)]
    pub field_selector: Option<String>,

    /// Configuration for metadata labels to add to metrics.
    #[serde(default)]
    pub metadata_labels: MetadataLabelsConfig,

    /// Maximum number of target batches to keep in flight while sending.
    ///
    /// This limits how many discovered kubelet targets can be processed concurrently before
    /// backpressure is applied to the scrape loop.
    #[serde(default = "default_send_batch_buffer")]
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::human_name = "Send Batch Buffer"))]
    pub send_batch_buffer: NonZeroUsize,

    /// Maximum number of scrape requests to keep in flight.
    ///
    /// This limits how many targets are scraped concurrently before backpressure is applied to
    /// the scrape loop.
    #[serde(default = "default_scrape_concurrency")]
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::human_name = "Scrape Concurrency"))]
    pub scrape_concurrency: NonZeroUsize,

    /// job name to distinguish resources.
    #[serde(default)]
    // ✅ 必填字段，不使用 Option
    pub job_name: String,

    /// URL scheme for scraping endpoints. Can be "http" or "https".
    #[serde(default = "default_scheme")]
    pub scheme: String,

    /// Port name or number to scrape.
    #[serde(default)]
    pub port: Option<String>,

    /// Metric path to scrape. Defaults to "/metrics".
    #[serde(default = "default_metrics_path")]
    pub metrics_path: String,
}

fn default_scheme() -> String {
    "http".to_string()
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

fn default_send_batch_buffer() -> NonZeroUsize {
    NonZeroUsize::new(4).expect("static")
}

fn default_scrape_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(32).expect("static")
}

/// The Kubernetes resource type to use for service discovery.
#[derive(Clone, Debug, Serialize, Deserialize, Configurable)]
pub enum KubernetesRole {
    /// Discover from Kubernetes Endpoints (includes Service metadata).
    #[serde(rename = "endpoints")]
    Endpoints,

    /// Discover from Kubernetes Nodes (scrapes metrics from node IP:port).
    #[serde(rename = "node")]
    Node,
}

/// Configuration for Pod metadata labels to add to scraped metrics.
#[derive(Clone, Debug, Serialize, Deserialize, Configurable)]
pub struct MetadataLabelsConfig {
    /// Whether to add the namespace as a metric label.
    #[serde(default = "default_true")]
    pub namespace: bool,

    /// Whether to add the Pod name as a metric label.
    #[serde(default = "default_true")]
    pub pod_name: bool,

    /// Whether to add the node name as a metric label.
    #[serde(default = "default_true")]
    pub node_name: bool,

    /// Whether to add the node IP as a metric label.
    #[serde(default = "default_true")]
    pub host_ip: bool,

    /// Whether to add the pod IP as a metric label.
    #[serde(default = "default_true")]
    pub pod_ip: bool,

    /// Pod labels to include.
    #[serde(default)]
    pub pod_labels: LabelSelector,

    /// Pod annotations to include.
    #[serde(default)]
    pub pod_annotations: LabelSelector,

    /// Service labels to include.
    #[serde(default)]
    pub service_labels: LabelSelector,

    /// Service annotations to include.
    #[serde(default)]
    pub service_annotations: LabelSelector,

    /// Node labels to include.
    #[serde(default)]
    pub node_labels: LabelSelector,

    /// Prefix to add to all Kubernetes metadata labels.
    #[serde(default = "default_label_prefix")]
    pub label_prefix: String,
}

/// Selector for which labels/annotations to include
#[derive(Clone, Debug, Serialize, Deserialize, Configurable)]
#[serde(untagged)]
pub enum LabelSelector {
    All(String),
    Specific(Vec<String>),
}

impl Default for LabelSelector {
    fn default() -> Self {
        LabelSelector::Specific(vec![])
    }
}

impl LabelSelector {
    pub fn is_all(&self) -> bool {
        matches!(self, LabelSelector::All(s) if s == "*")
    }

    pub fn is_none(&self) -> bool {
        matches!(self, LabelSelector::Specific(v) if v.is_empty())
    }

    pub fn keys(&self) -> Option<&Vec<String>> {
        match self {
            LabelSelector::Specific(keys) if !keys.is_empty() => Some(keys),
            _ => None,
        }
    }
}

impl Default for MetadataLabelsConfig {
    fn default() -> Self {
        Self {
            pod_labels: LabelSelector::Specific(vec![]),
            pod_annotations: LabelSelector::Specific(vec![]),
            namespace: true,
            pod_name: true,
            node_name: true,
            host_ip: true,
            pod_ip: true,
            service_labels: LabelSelector::Specific(vec![]),
            service_annotations: LabelSelector::Specific(vec![]),
            node_labels: LabelSelector::Specific(vec![]),
            label_prefix: "".to_string(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_label_prefix() -> String {
    "".to_string()
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct DiscoveredTarget {
    pub url: String,
    pub job_name: String,
    pub node_metadata: Option<Arc<NodeMetadata>>,
    pub service_metadata: Option<Arc<ServiceMetadata>>,
    pub pod_metadata: Option<Arc<PodMetadata>>,
}

#[derive(Clone, Debug)]
pub struct PodMetadata {
    pub name: String,
    pub namespace: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub pod_ip: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NodeMetadata {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub node_ip: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ServiceMetadata {
    pub name: String,
    pub namespace: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

impl DiscoveredTarget {
    pub fn node_name(&self) -> Option<&str> {
        self.node_metadata.as_deref().map(|m| m.name.as_str())
    }
    pub fn pod_name(&self) -> Option<&str> {
        self.pod_metadata.as_deref().map(|m| m.name.as_str())
    }

    pub fn service_name(&self) -> Option<&str> {
        self.service_metadata.as_deref().map(|m| m.name.as_str())
    }
    pub fn service_namespace(&self) -> Option<&str> {
        self.service_metadata
            .as_deref()
            .map(|m| m.namespace.as_str())
    }
    pub fn pod_namespace(&self) -> Option<&str> {
        self.pod_metadata.as_deref().map(|m| m.namespace.as_str())
    }
}

// ============================================================================
// Kubernetes 服务发现实现
// ============================================================================

pub struct K8sServiceDiscovery {
    client: Client,
    config: KubernetesSdConfig,
    targets: Arc<RwLock<Vec<DiscoveredTarget>>>,
    // ✅ 只需要 shutdown_tx 来停止 Endpoints/Node 发现的 Watch
    shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>,

    node_store: Store<Node>,
    // endpoint_store: Store<Endpoints>,
    endpoint_slice_store: Store<EndpointSlice>,
    pod_store: Store<Pod>,
    service_store: Store<Service>,
}

impl Clone for K8sServiceDiscovery {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            targets: Arc::clone(&self.targets),
            shutdown_tx: Arc::clone(&self.shutdown_tx),
            node_store: self.node_store.clone(),
            // endpoint_store: self.endpoint_store.clone(),
            endpoint_slice_store: self.endpoint_slice_store.clone(),
            pod_store: self.pod_store.clone(),
            service_store: self.service_store.clone(),
        }
    }
}

impl K8sServiceDiscovery {
    pub async fn new(config: KubernetesSdConfig) -> Result<Self> {
        let client = Client::try_default().await?;
        let targets = Arc::new(RwLock::new(Vec::new()));
        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);

        let mut watcher_cfg = watcher::Config::default();
        if let Some(ref sel) = config.field_selector {
            watcher_cfg = watcher_cfg.fields(sel);
        }
        if let Some(ref sel) = config.label_selector {
            watcher_cfg = watcher_cfg.labels(sel);
        }

        // 添加node_store
        let node_store_w = reflector::store::Writer::<Node>::default();
        let node_store = node_store_w.as_reader();
        // 如果当前配置 role 是 Node，则使用用户传入的 watcher_cfg（保留 label/field selector），
        // 否则使用默认 watcher 配置以获取集群中所有 Node（避免被 selector 限制）。
        let node_watcher_cfg = if matches!(config.role, KubernetesRole::Node) {
            watcher_cfg.clone()
        } else {
            watcher::Config::default()
        };
        let node_reflector_stream = watcher(Api::<Node>::all(client.clone()), node_watcher_cfg)
            .backoff(watcher::DefaultBackoff::default());
        // 后台任务：持续同步所有 Node，支持延迟删除（60 s）
        tokio::spawn(custom_reflector(
            node_store_w,
            K8sMetaCache::new(),
            node_reflector_stream,
            Duration::from_secs(60),
        ));

        let ep_slice_store_w = reflector::store::Writer::<EndpointSlice>::default();
        let endpoint_slice_store = ep_slice_store_w.as_reader();
        if matches!(config.role, KubernetesRole::Endpoints) {
            tokio::spawn(custom_reflector(
                ep_slice_store_w,
                K8sMetaCache::new(),
                watcher(Api::<EndpointSlice>::all(client.clone()), watcher_cfg)
                    .backoff(watcher::DefaultBackoff::default()),
                Duration::from_secs(30),
            ));
        }

        // Pod
        let pod_store_w = reflector::store::Writer::<Pod>::default();
        let pod_store = pod_store_w.as_reader();
        if matches!(config.role, KubernetesRole::Endpoints) {
            tokio::spawn(custom_reflector(
                pod_store_w,
                K8sMetaCache::new(),
                watcher(Api::<Pod>::all(client.clone()), watcher::Config::default())
                    .backoff(watcher::DefaultBackoff::default()),
                Duration::from_secs(60),
            ));
        }

        // Service
        let svc_store_w = reflector::store::Writer::<Service>::default();
        let service_store = svc_store_w.as_reader();
        if matches!(config.role, KubernetesRole::Endpoints) {
            tokio::spawn(custom_reflector(
                svc_store_w,
                K8sMetaCache::new(),
                watcher(
                    Api::<Service>::all(client.clone()),
                    watcher::Config::default(),
                )
                .backoff(watcher::DefaultBackoff::default()),
                Duration::from_secs(60),
            ));
        }

        let discovery = Self {
            client,
            config,
            targets,
            shutdown_tx: Arc::new(shutdown_tx),
            node_store,
            // endpoint_store,
            endpoint_slice_store,
            pod_store,
            service_store,
        };

        // ✅ 初始发现
        let initial_targets = discovery.discover_targets_internal().await?;
        *discovery.targets.write().await = initial_targets;

        // ✅ 启动 Endpoints/Node 发现的 Watch（这些仍然是每个 source 独立的）
        match discovery.config.role {
            // KubernetesRole::Endpoints => {
            //     discovery.start_endpoints_poll_task();
            // }
            KubernetesRole::Endpoints => {
                discovery.start_endpoint_slices_poll_task();
            }
            KubernetesRole::Node => {
                discovery.start_nodes_poll_task();
            }
        }

        Ok(discovery)
    }

    pub async fn get_targets(&self) -> Vec<DiscoveredTarget> {
        self.targets.read().await.clone()
    }

    async fn discover_targets_internal(&self) -> Result<Vec<DiscoveredTarget>> {
        match self.config.role {
            // KubernetesRole::Endpoints => self.discover_from_endpoints(),
            KubernetesRole::Endpoints => self.discover_from_endpoint_slices(),
            KubernetesRole::Node => self.discover_from_nodes().await,
        }
    }

    // // ============================================================================
    // // Endpoints 轮询更新
    // // ============================================================================

    // fn start_endpoints_poll_task(&self) {
    //     let discovery = self.clone();
    //     let mut shutdown_rx = self.shutdown_tx.subscribe();
    //     tokio::spawn(async move {
    //         // 每 30s 重新扫一次 store 快照，与 endpoint_store 的延迟删除窗口匹配
    //         let mut interval = tokio::time::interval(Duration::from_secs(5));
    //         loop {
    //             tokio::select! {
    //                 _ = shutdown_rx.changed() => {
    //                     info!("Endpoints watch task shutting down");
    //                     break;
    //                 }
    //                 _ = interval.tick() => {
    //                     match discovery.discover_from_endpoints() {
    //                         Ok(new_targets) => {
    //                             *discovery.targets.write().await = new_targets;
    //                         }
    //                         Err(e) => {
    //                             error!("Failed to refresh endpoints targets: {:?}", e);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     });
    // }

    // fn discover_from_endpoints(&self) -> Result<Vec<DiscoveredTarget>> {
    //     let mut all_targets = Vec::new();

    //     // store.state() 返回 Arc<Endpoints> 的快照，纯内存，不 async
    //     let all_endpoints = self.endpoint_store.state();
    //     let allowed_ns: Option<&[String]> = if self.config.namespaces.is_empty() {
    //         None  // 空 = 全部 namespace
    //     } else {
    //         Some(&self.config.namespaces)
    //     };

    //     for ep in all_endpoints {
    //         // namespace 过滤
    //         let ep_ns = match ep.metadata.namespace.as_deref() {
    //             Some(ns) => ns,
    //             None => continue,
    //         };
    //         if let Some(allowed) = allowed_ns {
    //             if !allowed.iter().any(|n| n == ep_ns) {
    //                 continue;
    //             }
    //         }

    //         let targets = self.discover_from_endpoints_internal(&ep)?;
    //         all_targets.extend(targets);
    //     }

    //     Ok(all_targets)
    // }

    // fn discover_from_endpoints_internal(
    //     &self,
    //     endpoints: &Endpoints,
    // ) -> Result<Vec<DiscoveredTarget>> {
    //     let mut targets = Vec::new();
    //     let endpoints_name = match endpoints.metadata.name.as_deref() {
    //         Some(n) => n,
    //         None => return Ok(targets),
    //     };
    //     let namespace = match endpoints.metadata.namespace.as_deref() {
    //         Some(ns) => ns,
    //         None => return Ok(targets),
    //     };
    //     // ─── Service 元数据（从 store，同步）───
    //     let service_metadata = self.service_store
    //         .get(&ObjectRef::<Service>::new(endpoints_name).within(namespace))
    //         .map(|s| Arc::new(ServiceMetadata {
    //             name: s.metadata.name.clone().unwrap_or_default(),
    //             namespace: s.metadata.namespace.clone().unwrap_or_default(),
    //             labels: s.metadata.labels.clone().unwrap_or_default(),
    //             annotations: s.metadata.annotations.clone().unwrap_or_default(),
    //         }));

    //     let Some(subsets) = &endpoints.subsets else { return Ok(targets) };
    //     for subset in subsets {
    //         let Some(addresses) = &subset.addresses else { continue };
    //         let Some(ports) = &subset.ports else { continue };

    //         for address in addresses {
    //             // ─── Pod / Node 元数据（从 store，同步）───
    //             let (pod_metadata, node_metadata) = if let Some(r) = &address.target_ref {
    //                 if r.kind.as_deref() == Some("Pod") {
    //                     if let Some(pod_name) = &r.name {
    //                         let pod = self.pod_store
    //                             .get(&ObjectRef::<Pod>::new(pod_name).within(namespace));

    //                         let pod_meta = pod.as_ref().map(|p| Arc::new(PodMetadata {
    //                             name: p.metadata.name.clone().unwrap_or_default(),
    //                             namespace: p.metadata.namespace.clone().unwrap_or_default(),
    //                             labels: p.metadata.labels.clone().unwrap_or_default(),
    //                             annotations: p.metadata.annotations.clone().unwrap_or_default(),
    //                             pod_ip: p.status.as_ref().and_then(|s| s.pod_ip.clone()),
    //                         }));

    //                         // node_name 来自 Pod.spec.node_name
    //                         let node_meta = pod.as_ref()
    //                             .and_then(|p| p.spec.as_ref()?.node_name.as_ref().map(|n| n.clone()))
    //                             .and_then(|node_name| {
    //                                 self.node_store.get(&ObjectRef::<Node>::new(&node_name))
    //                                     .map(|n| {
    //                                         let node_ip = n.status.as_ref()
    //                                             .and_then(|s| s.addresses.as_ref())
    //                                             .and_then(|a| a.iter().find(|a| a.type_ == "InternalIP"))
    //                                             .map(|a| a.address.clone());
    //                                         Arc::new(NodeMetadata {
    //                                             name: node_name,
    //                                             labels: n.metadata.labels.clone().unwrap_or_default(),
    //                                             node_ip,
    //                                         })
    //                                     })
    //                             });

    //                         (pod_meta, node_meta)
    //                     } else { (None, None) }
    //                 } else { (None, None) }
    //             } else { (None, None) };

    //             for port in ports {
    //                  if let Some(ref port_filter) = self.config.port {
    //                     let port_matches = if let Ok(port_num) = port_filter.parse::<i32>() {
    //                         port.port == port_num
    //                     } else {
    //                         port.name.as_deref() == Some(port_filter.as_str())
    //                     };

    //                     if !port_matches {
    //                         continue;
    //                     }
    //                 }
    //                 // port 过滤
    //                 let url = format!(
    //                     "{}://{}:{}{}",
    //                     self.config.scheme,
    //                     address.ip,
    //                     port.port,
    //                     self.config.metrics_path
    //                 );

    //                 targets.push(DiscoveredTarget {
    //                     url,
    //                     job_name: self.config.job_name.clone(),
    //                     pod_metadata: pod_metadata.clone(),
    //                     node_metadata: node_metadata.clone(),
    //                     service_metadata: service_metadata.clone(),
    //                 });
    //             }
    //         }
    //     }
    //     Ok(targets)
    // }
    fn start_endpoint_slices_poll_task(&self) {
        let discovery = self.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("EndpointSlice watch task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        match discovery.discover_from_endpoint_slices() {
                            Ok(new_targets) => {
                                *discovery.targets.write().await = new_targets;
                            }
                            Err(e) => {
                                error!("Failed to refresh endpoint slice targets: {:?}", e);
                            }
                        }
                    }
                }
            }
        });
    }

    fn discover_from_endpoint_slices(&self) -> Result<Vec<DiscoveredTarget>> {
        let mut all_targets = Vec::new();

        let all_slices = self.endpoint_slice_store.state();
        let allowed_ns: Option<&[String]> = if self.config.namespaces.is_empty() {
            None
        } else {
            Some(&self.config.namespaces)
        };

        for slice in all_slices {
            let slice_ns = match slice.metadata.namespace.as_deref() {
                Some(ns) => ns,
                None => continue,
            };
            if let Some(allowed) = allowed_ns {
                if !allowed.iter().any(|n| n == slice_ns) {
                    continue;
                }
            }
            let targets = self.discover_from_endpoint_slice_internal(&slice)?;
            all_targets.extend(targets);
        }

        Ok(all_targets)
    }

    fn discover_from_endpoint_slice_internal(
        &self,
        slice: &EndpointSlice,
    ) -> Result<Vec<DiscoveredTarget>> {
        let mut targets = Vec::new();

        let namespace = match slice.metadata.namespace.as_deref() {
            Some(ns) => ns,
            None => return Ok(targets),
        };

        // Service 名称存放在标签 "kubernetes.io/service-name" 中
        let service_name = match slice
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
        {
            Some(name) => name.as_str(),
            None => return Ok(targets),
        };

        let service_metadata = self
            .service_store
            .get(&ObjectRef::<Service>::new(service_name).within(namespace))
            .map(|s| {
                Arc::new(ServiceMetadata {
                    name: s.metadata.name.clone().unwrap_or_default(),
                    namespace: s.metadata.namespace.clone().unwrap_or_default(),
                    labels: s.metadata.labels.clone().unwrap_or_default(),
                    annotations: s.metadata.annotations.clone().unwrap_or_default(),
                })
            });

        // EndpointSlice 的 ports 在顶层
        let ports = match &slice.ports {
            Some(p) if !p.is_empty() => p,
            _ => return Ok(targets),
        };

        for endpoint in &slice.endpoints {
            // 只抓 ready 状态的 endpoint
            let ready = endpoint
                .conditions
                .as_ref()
                .and_then(|c| c.ready)
                .unwrap_or(true);
            if !ready {
                continue;
            }

            let pod_metadata = endpoint.target_ref.as_ref().and_then(|r| {
                if r.kind.as_deref() != Some("Pod") {
                    return None;
                }
                let pod_name = r.name.as_deref()?;
                self.pod_store
                    .get(&ObjectRef::<Pod>::new(pod_name).within(namespace))
                    .map(|p| {
                        Arc::new(PodMetadata {
                            name: p.metadata.name.clone().unwrap_or_default(),
                            namespace: p.metadata.namespace.clone().unwrap_or_default(),
                            labels: p.metadata.labels.clone().unwrap_or_default(),
                            annotations: p.metadata.annotations.clone().unwrap_or_default(),
                            pod_ip: p.status.as_ref().and_then(|s| s.pod_ip.clone()),
                        })
                    })
            });

            // EndpointSlice 直接带 node_name，无需再通过 Pod 二次查询
            let node_metadata = endpoint.node_name.as_deref().and_then(|node_name| {
                self.node_store
                    .get(&ObjectRef::<Node>::new(node_name))
                    .map(|n| {
                        let node_ip = n
                            .status
                            .as_ref()
                            .and_then(|s| s.addresses.as_ref())
                            .and_then(|a| a.iter().find(|a| a.type_ == "InternalIP"))
                            .map(|a| a.address.clone());
                        Arc::new(NodeMetadata {
                            name: node_name.to_string(),
                            labels: n.metadata.labels.clone().unwrap_or_default(),
                            node_ip,
                        })
                    })
            });

            for ip in &endpoint.addresses {
                for port in ports {
                    let port_num = match port.port {
                        Some(p) => p,
                        None => continue,
                    };

                    if let Some(ref port_filter) = self.config.port {
                        let port_matches = if let Ok(n) = port_filter.parse::<i32>() {
                            port_num == n
                        } else {
                            port.name.as_deref() == Some(port_filter.as_str())
                        };
                        if !port_matches {
                            continue;
                        }
                    }

                    let url = format!(
                        "{}://{}:{}{}",
                        self.config.scheme, ip, port_num, self.config.metrics_path
                    );

                    targets.push(DiscoveredTarget {
                        url,
                        job_name: self.config.job_name.clone(),
                        pod_metadata: pod_metadata.clone(),
                        node_metadata: node_metadata.clone(),
                        service_metadata: service_metadata.clone(),
                    });
                }
            }
        }

        Ok(targets)
    }

    // ============================================================================
    // Node 轮询更新
    // ============================================================================

    fn start_nodes_poll_task(&self) {
        let discovery = self.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("Nodes watch task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        match discovery.discover_from_nodes().await {
                            Ok(new_targets) => {
                                *discovery.targets.write().await = new_targets;
                            }
                            Err(e) => {
                                error!("Failed to refresh nodes targets: {:?}", e);
                            }
                        }
                    }
                }
            }
        });
    }

    async fn discover_from_nodes(&self) -> Result<Vec<DiscoveredTarget>> {
        let mut all_targets = Vec::new();
        let all_nodes = self.node_store.state();

        for node in all_nodes {
            let targets = self
                .discover_from_node_internal(&node)
                .await
                .unwrap_or_default();
            all_targets.extend(targets);
        }

        Ok(all_targets)
    }

    async fn discover_from_node_internal(&self, node: &Node) -> Result<Vec<DiscoveredTarget>> {
        let mut targets = Vec::new();

        let node_name = match node.metadata.name.as_ref() {
            Some(name) => name,
            None => return Ok(targets),
        };

        let host_ip = if let Some(status) = &node.status {
            if let Some(addresses) = &status.addresses {
                addresses
                    .iter()
                    .find(|addr| addr.type_ == "InternalIP")
                    .or_else(|| addresses.first())
                    .map(|addr| addr.address.clone())
            } else {
                None
            }
        } else {
            None
        };

        let host_ip = match host_ip {
            Some(ip) => ip,
            None => {
                warn!("Node {} has no IP address", node_name);
                return Ok(targets);
            }
        };

        let port = match &self.config.port {
            Some(p) => p
                .parse::<u16>()
                .map_err(|_| format!("Invalid port: {}", p))?,
            None => {
                warn!(
                    "No port configured for role=node, skipping node {}",
                    node_name
                );
                return Ok(targets);
            }
        };

        let url = format!(
            "{}://{}:{}{}",
            self.config.scheme, host_ip, port, self.config.metrics_path
        );

        // ─── Node 角色：node 对象就在手，直接从变量取 labels ───
        let node_metadata = self
            .node_store
            .get(&ObjectRef::<Node>::new(node_name))
            .map(|n| {
                let node_ip = n
                    .status
                    .as_ref()
                    .and_then(|s| s.addresses.as_ref())
                    .and_then(|addrs| addrs.iter().find(|a| a.type_ == "InternalIP"))
                    .map(|a| a.address.clone());
                Arc::new(NodeMetadata {
                    name: node_name.to_string(),
                    labels: n.metadata.labels.clone().unwrap_or_default(),
                    node_ip,
                })
            });

        targets.push(DiscoveredTarget {
            url,
            job_name: self.config.job_name.clone(),
            node_metadata,
            pod_metadata: None,
            service_metadata: None,
        });

        Ok(targets)
    }
}

impl Drop for K8sServiceDiscovery {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}
