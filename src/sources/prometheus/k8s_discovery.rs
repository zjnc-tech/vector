use k8s_openapi::api::core::v1::{Endpoints, Node, Pod, Service, Namespace};
use kube::{api::{Api, ListParams}, Client, runtime::watcher};
use std::sync::Arc;
use futures::stream::Stream;
use futures::StreamExt;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use tracing::{error, info, warn};
use kube::runtime::WatchStreamExt;
use vector_lib::configurable::Configurable;
use tokio::time::Duration;

use super::metadata_cache::{MetadataCache, get_node_name_from_pod, PodMetadata, NodeMetadata, ServiceMetadata};

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
    pub node_ip: bool,
    
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
            node_ip: true,
            pod_ip: true,
            service_labels: LabelSelector::Specific(vec![]),
            service_annotations: LabelSelector::Specific(vec![]),
            node_labels: LabelSelector::Specific(vec![]),
            label_prefix: "".to_string(),
        }
    }
}

fn default_true() -> bool { true }
fn default_label_prefix() -> String { "".to_string() }

// ============================================================================
// ✅ 发现的目标（方案1：只存 ID，不存元数据）
// ============================================================================

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct DiscoveredTarget {
    pub url: String,
    
    // ✅ 只存标识信息，抓取时从缓存实时获取元数据
    pub pod_name: Option<String>,
    pub pod_namespace: Option<String>,
    pub node_name: Option<String>,
    pub service_name: Option<String>,
    pub service_namespace: Option<String>,
}

// ============================================================================
// Kubernetes 服务发现 - 通用基础设施
// ============================================================================

pub struct K8sServiceDiscovery {
    client: Client,
    config: KubernetesSdConfig,
    targets: Arc<RwLock<Vec<DiscoveredTarget>>>,
    pub metadata_cache: Arc<RwLock<MetadataCache>>,  // ✅ 改为 pub，供 scrape 使用
    shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Clone for K8sServiceDiscovery {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            targets: Arc::clone(&self.targets),
            metadata_cache: Arc::clone(&self.metadata_cache),
            shutdown_tx: Arc::clone(&self.shutdown_tx),
        }
    }
}

impl K8sServiceDiscovery {
    pub async fn new(config: KubernetesSdConfig) -> Result<Self> {
        let client = Client::try_default().await?;
        let targets = Arc::new(RwLock::new(Vec::new()));
        let metadata_cache = Arc::new(RwLock::new(MetadataCache::new()));
        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
        
        let discovery = Self {
            client,
            config,
            targets,
            metadata_cache,
            shutdown_tx: Arc::new(shutdown_tx),
        };
        
        let initial_targets = discovery.discover_targets_internal().await?;
        *discovery.targets.write().await = initial_targets;
        
        match discovery.config.role {
            KubernetesRole::Endpoints => {
                discovery.start_endpoints_watch_task();
                discovery.start_pod_watch_task();
                discovery.start_service_watch_task();
            }
            KubernetesRole::Node => {
                discovery.start_nodes_watch_task();
            }
        }
        
        discovery.start_node_watch_task();
        discovery.start_cache_stats_task();
        
        Ok(discovery)
    }
    
    pub async fn get_targets(&self) -> Vec<DiscoveredTarget> {
        self.targets.read().await.clone()
    }
    
    fn start_pod_watch_task(&self) {
        let cache = Arc::clone(&self.metadata_cache);
        let client = self.client.clone();
        let namespaces = self.config.namespaces.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Pod watch task shutting down");
                }
                _ = async {
                    for namespace in &namespaces {
                        let pods_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
                        let mut stream = Box::pin(
                            watcher(pods_api, Default::default()).applied_objects()
                        );
                        
                        while let Some(pod_result) = stream.next().await {
                            match pod_result {
                                Ok(pod) => {
                                    let pod_name = pod.metadata.name.clone().unwrap_or_default();
                                    let new_labels = pod.metadata.labels.unwrap_or_default();
                                    let new_annotations = pod.metadata.annotations.unwrap_or_default();
                                    let new_pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.clone());
                                    
                                    // ✅ 检查是否真的变化了
                                    let mut cache_guard = cache.write().await;
                                    let should_update = if let Some(existing) = cache_guard.get_pod_cached(namespace, &pod_name) {
                                        existing.labels != new_labels 
                                            || existing.annotations != new_annotations
                                            || existing.pod_ip != new_pod_ip
                                    } else {
                                        true
                                    };
                                    
                                    if should_update {
                                        let metadata = Arc::new(PodMetadata {
                                            name: pod.metadata.name.unwrap_or_default(),
                                            namespace: pod.metadata.namespace.unwrap_or_default(),
                                            labels: new_labels,
                                            annotations: new_annotations,
                                            pod_ip: new_pod_ip,
                                        });
                                        
                                        cache_guard.update_pod(namespace, &pod_name, metadata);
                                        info!("Updated cache for pod {}/{} (metadata changed)", namespace, pod_name);
                                    }
                                    drop(cache_guard);
                                }
                                Err(e) => {
                                    warn!("Pod watch error: {:?}", e);
                                }
                            }
                        }
                    }
                } => {}
            }
        });
    }

    fn start_node_watch_task(&self) {
        let cache = Arc::clone(&self.metadata_cache);
        let client = self.client.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Node watch task shutting down");
                }
                _ = async {
                    let nodes_api: Api<Node> = Api::all(client.clone());
                    let mut stream = Box::pin(
                        watcher(nodes_api, Default::default()).applied_objects()
                    );
                    
                    while let Some(node_result) = stream.next().await {
                        match node_result {
                            Ok(node) => {
                                let node_name = node.metadata.name.clone().unwrap_or_default();
                                
                                let node_ip = if let Some(status) = &node.status {
                                    if let Some(addresses) = &status.addresses {
                                        addresses.iter()
                                            .find(|addr| addr.type_ == "InternalIP")
                                            .or_else(|| addresses.first())
                                            .map(|addr| addr.address.clone())
                                    } else { None }
                                } else { None };
                                
                                let new_labels = node.metadata.labels.unwrap_or_default();
                                
                                // ✅ 检查是否真的变化了
                                let mut cache_guard = cache.write().await;
                                let should_update = if let Some(existing) = cache_guard.get_node_cached(&node_name) {
                                    // 比较标签和 IP 是否变化
                                    existing.labels != new_labels || existing.node_ip != node_ip
                                } else {
                                    // 缓存中没有，需要添加
                                    true
                                };
                                
                                if should_update {
                                    let metadata = Arc::new(NodeMetadata {
                                        name: node_name.clone(),
                                        labels: new_labels,
                                        node_ip,
                                    });
                                    
                                    cache_guard.update_node(&node_name, metadata);
                                    info!("Updated cache for node {} (metadata changed)", node_name);
                                }
                                drop(cache_guard);
                            }
                            Err(e) => {
                                warn!("Node watch error: {:?}", e);
                            }
                        }
                    }
                } => {}
            }
        });
    }

    fn start_service_watch_task(&self) {
        let cache = Arc::clone(&self.metadata_cache);
        let client = self.client.clone();
        let namespaces = self.config.namespaces.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Service watch task shutting down");
                }
                _ = async {
                    for namespace in &namespaces {
                        let services_api: Api<Service> = Api::namespaced(client.clone(), namespace);
                        let mut stream = Box::pin(
                            watcher(services_api, Default::default()).applied_objects()
                        );
                        
                        while let Some(service_result) = stream.next().await {
                            match service_result {
                                Ok(service) => {
                                    let service_name = service.metadata.name.clone().unwrap_or_default();
                                    let new_labels = service.metadata.labels.unwrap_or_default();
                                    let new_annotations = service.metadata.annotations.unwrap_or_default();
                                    
                                    // ✅ 检查是否真的变化了
                                    let mut cache_guard = cache.write().await;
                                    let should_update = if let Some(existing) = cache_guard.get_service_cached(namespace, &service_name) {
                                        existing.labels != new_labels || existing.annotations != new_annotations
                                    } else {
                                        true
                                    };
                                    
                                    if should_update {
                                        let metadata = Arc::new(ServiceMetadata {
                                            name: service.metadata.name.unwrap_or_default(),
                                            namespace: service.metadata.namespace.unwrap_or_default(),
                                            labels: new_labels,
                                            annotations: new_annotations,
                                        });
                                        
                                        cache_guard.update_service(namespace, &service_name, metadata);
                                        info!("Updated cache for service {}/{} (metadata changed)", namespace, service_name);
                                    }
                                    drop(cache_guard);
                                }
                                Err(e) => {
                                    warn!("Service watch error: {:?}", e);
                                }
                            }
                        }
                    }
                } => {}
            }
        });
    }

    fn start_cache_stats_task(&self) {
        let cache = Arc::clone(&self.metadata_cache);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = interval.tick() => {
                        let cache_guard = cache.read().await;
                        let stats = cache_guard.stats();
                        drop(cache_guard);
                        info!("Metadata cache stats: pods={}, nodes={}, services={}",
                              stats.pods_count, stats.nodes_count, stats.services_count);
                    }
                }
            }
        });
    }
    
    async fn discover_targets_internal(&self) -> Result<Vec<DiscoveredTarget>> {
        match self.config.role {
            KubernetesRole::Endpoints => self.discover_from_endpoints().await,
            KubernetesRole::Node => self.discover_from_nodes().await,
        }
    }

    // ============================================================================
    // Endpoints 角色实现
    // ============================================================================
    
    fn start_endpoints_watch_task(&self) {
        let discovery = self.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Endpoints watch task shutting down");
                }
                result = discovery.watch_and_update() => {
                    if let Err(e) = result {
                        error!("Endpoints watch task failed: {:?}", e);
                    }
                }
            }
        });
    }
    
    async fn watch_and_update(&self) -> Result<()> {
        let stream = self.watch_endpoints().await?;
        tokio::pin!(stream);

        while let Some((endpoint_key, new_targets)) = stream.next().await {
            let mut targets = self.targets.write().await;
            
            // ✅ 提取当前这个 endpoint 的旧 targets
            let old_targets: Vec<_> = targets.iter()
                .filter(|t| {
                    if let (Some(ref ns), Some(ref name)) = (&t.service_namespace, &t.service_name) {
                        format!("{}/{}", ns, name) == endpoint_key
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();
            
            // ✅ 比较 targets 是否真的变了
            let targets_changed = old_targets.len() != new_targets.len() 
                || !old_targets.iter().all(|old| {
                    new_targets.iter().any(|new| Self::targets_equal(old, new))
                });
            
            if targets_changed {
                // 只在真正变化时才更新
                targets.retain(|t| {
                    if let (Some(ref ns), Some(ref name)) = (&t.service_namespace, &t.service_name) {
                        format!("{}/{}", ns, name) != endpoint_key
                    } else {
                        true
                    }
                });
                
                targets.extend(new_targets.clone());
                
                info!("Updated targets for {}, total targets: {}", endpoint_key, targets.len());
            }
        }
        
        Ok(())
    }

    // ✅ 辅助函数：比较两个 target 是否相等
    fn targets_equal(a: &DiscoveredTarget, b: &DiscoveredTarget) -> bool {
        a.url == b.url 
            && a.pod_name == b.pod_name
            && a.pod_namespace == b.pod_namespace
            && a.node_name == b.node_name
            && a.service_name == b.service_name
            && a.service_namespace == b.service_namespace
    }
    
    async fn discover_from_endpoints(&self) -> Result<Vec<DiscoveredTarget>> {
        let mut all_targets = Vec::new();
        
        for namespace in self.get_namespaces().await? {
            let endpoints_api: Api<Endpoints> = Api::namespaced(self.client.clone(), &namespace);
            let services_api: Api<Service> = Api::namespaced(self.client.clone(), &namespace);
            let pods_api: Api<Pod> = Api::namespaced(self.client.clone(), &namespace);
            
            let lp = self.build_list_params();
            let endpoints_list = endpoints_api.list(&lp).await?;
            
            for endpoints in endpoints_list.items {
                let targets = self.discover_from_endpoints_internal(
                    &endpoints,
                    &services_api,
                    &pods_api,
                ).await.unwrap_or_default();
                
                all_targets.extend(targets);
            }
        }
        
        Ok(all_targets)
    }

    async fn watch_endpoints(&self) -> Result<impl Stream<Item = (String, Vec<DiscoveredTarget>)> + '_> {
        let client = self.client.clone();
        let config_data = self.config.clone();
        
        let stream = async_stream::stream! {
            for namespace in config_data.namespaces.iter() {
                let endpoints_api: Api<Endpoints> = Api::namespaced(client.clone(), namespace);
                let services_api: Api<Service> = Api::namespaced(client.clone(), namespace);
                let pods_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
                
                let mut watcher_config = kube::runtime::watcher::Config::default();
                if let Some(ref selector) = config_data.label_selector {
                    watcher_config = watcher_config.labels(selector);
                }
            
                if let Some(ref field_sel) = config_data.field_selector {
                    watcher_config = watcher_config.fields(field_sel);
                }
                
                let mut stream = Box::pin(watcher(endpoints_api, watcher_config).applied_objects());
                
                while let Some(endpoints_result) = stream.next().await {
                    match endpoints_result {
                        Ok(endpoints) => {
                            let targets = self.discover_from_endpoints_internal(
                                &endpoints,
                                &services_api,
                                &pods_api,
                            ).await.unwrap_or_default();
                            
                            if let Some(first) = targets.first() {
                                if let (Some(ref ns), Some(ref name)) = (&first.service_namespace, &first.service_name) {
                                    let endpoint_key = format!("{}/{}", ns, name);
                                    yield (endpoint_key, targets);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Watch error: {:?}", e);
                        }
                    }
                }
            }
        };
        
        Ok(stream)
    }

    async fn discover_from_endpoints_internal(
        &self,
        endpoints: &Endpoints,
        services_api: &Api<Service>,
        pods_api: &Api<Pod>,
    ) -> Result<Vec<DiscoveredTarget>> {
        let mut targets = Vec::new();
        let nodes_api: Api<Node> = Api::all(self.client.clone());
        
        let endpoints_name = match endpoints.metadata.name.as_ref() {
            Some(name) => name,
            None => return Ok(targets),
        };
        
        let namespace = endpoints.metadata.namespace.as_ref()
            .ok_or("Endpoints has no namespace")?;
        
        // ✅ 预加载 Service 元数据到缓存
        let mut cache = self.metadata_cache.write().await;
        let _ = cache.get_or_fetch_service(namespace, endpoints_name, services_api).await;
        
        if let Some(subsets) = &endpoints.subsets {
            for subset in subsets {
                if let Some(addresses) = &subset.addresses {
                    for address in addresses {
                        if let Some(ports) = &subset.ports {
                            for port in ports {
                                if let Some(ref port_filter) = self.config.port {
                                    let port_matches = if let Ok(port_num) = port_filter.parse::<i32>() {
                                        port.port == port_num
                                    } else {
                                        port.name.as_deref() == Some(port_filter.as_str())
                                    };
                                    
                                    if !port_matches {
                                        continue;
                                    }
                                }
                                
                                let url = format!(
                                    "{}://{}:{}{}",
                                    self.config.scheme,
                                    address.ip,
                                    port.port,
                                    self.config.metrics_path
                                );
                                
                                let (pod_name, pod_namespace, node_name) = if let Some(target_ref) = &address.target_ref {
                                    if target_ref.kind.as_deref() == Some("Pod") {
                                        if let Some(pod_name) = &target_ref.name {
                                            // ✅ 预加载 Pod 元数据到缓存
                                            let _ = cache.get_or_fetch_pod(namespace, pod_name, pods_api).await;
                                            
                                            let node_name = if let Ok(Some(node_name)) = get_node_name_from_pod(pod_name, namespace, pods_api).await {
                                                // ✅ 预加载 Node 元数据到缓存
                                                let _ = cache.get_or_fetch_node(&node_name, &nodes_api).await;
                                                Some(node_name)
                                            } else {
                                                None
                                            };
                                            
                                            (Some(pod_name.clone()), Some(namespace.to_string()), node_name)
                                        } else {
                                            (None, None, None)
                                        }
                                    } else {
                                        (None, None, None)
                                    }
                                } else {
                                    (None, None, None)
                                };
                                
                                targets.push(DiscoveredTarget {
                                    url,
                                    pod_name,
                                    pod_namespace,
                                    node_name,
                                    service_name: Some(endpoints_name.clone()),
                                    service_namespace: Some(namespace.clone()),
                                });
                            }
                        }
                    }
                }
            }
        }
        
        drop(cache);
        Ok(targets)
    }

    // ============================================================================
    // Node 角色实现
    // ============================================================================
    
    fn start_nodes_watch_task(&self) {
        let discovery = self.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Nodes watch task shutting down");
                }
                result = discovery.watch_nodes_and_update() => {
                    if let Err(e) = result {
                        error!("Nodes watch task failed: {:?}", e);
                    }
                }
            }
        });
    }

    async fn watch_nodes_and_update(&self) -> Result<()> {
        let stream = self.watch_nodes().await?;
        tokio::pin!(stream);

        while let Some((node_name, new_targets)) = stream.next().await {
            let mut targets = self.targets.write().await;
            
            // ✅ 提取当前 node 的旧 targets
            let old_targets: Vec<_> = targets.iter()
                .filter(|t| t.node_name.as_ref() == Some(&node_name))
                .cloned()
                .collect();
            
            // ✅ 比较 targets 是否变化
            let targets_changed = old_targets.len() != new_targets.len()
                || !old_targets.iter().all(|old| {
                    new_targets.iter().any(|new| Self::targets_equal(old, new))
                });
            
            if targets_changed {
                targets.retain(|t| t.node_name.as_ref() != Some(&node_name));
                targets.extend(new_targets);
                info!("Updated targets for node {}, total targets: {}", node_name, targets.len());
            }
        }
        
        Ok(())
    }

    async fn watch_nodes(&self) -> Result<impl Stream<Item = (String, Vec<DiscoveredTarget>)> + '_> {
        let client = self.client.clone();
        let config_data = self.config.clone();
        
        let stream = async_stream::stream! {
            let nodes_api: Api<Node> = Api::all(client.clone());
            
            let mut watcher_config = kube::runtime::watcher::Config::default();
            if let Some(ref selector) = config_data.label_selector {
                watcher_config = watcher_config.labels(selector);
            }
        
            if let Some(ref field_sel) = config_data.field_selector {
                watcher_config = watcher_config.fields(field_sel);
            }
            
            let mut stream = Box::pin(watcher(nodes_api, watcher_config).applied_objects());
            
            while let Some(node_result) = stream.next().await {
                match node_result {
                    Ok(node) => {
                        let node_name = node.metadata.name.clone().unwrap_or_default();
                        let targets = self.discover_from_node_internal(&node).await.unwrap_or_default();
                        yield (node_name, targets);
                    }
                    Err(e) => {
                        error!("Node watch error: {:?}", e);
                    }
                }
            }
        };
        
        Ok(stream)
    }

    async fn discover_from_nodes(&self) -> Result<Vec<DiscoveredTarget>> {
        let mut all_targets = Vec::new();
        
        let nodes_api: Api<Node> = Api::all(self.client.clone());
        let lp = self.build_list_params();
        let nodes_list = nodes_api.list(&lp).await?;
        
        for node in nodes_list.items {
            let targets = self.discover_from_node_internal(&node).await.unwrap_or_default();
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
        
        let node_ip = if let Some(status) = &node.status {
            if let Some(addresses) = &status.addresses {
                addresses.iter()
                    .find(|addr| addr.type_ == "InternalIP")
                    .or_else(|| addresses.first())
                    .map(|addr| addr.address.clone())
            } else {
                None
            }
        } else {
            None
        };
        
        let node_ip = match node_ip {
            Some(ip) => ip,
            None => {
                warn!("Node {} has no IP address", node_name);
                return Ok(targets);
            }
        };
        
        let port = match &self.config.port {
            Some(p) => p.parse::<u16>().map_err(|_| format!("Invalid port: {}", p))?,
            None => {
                warn!("No port configured for role=node, skipping node {}", node_name);
                return Ok(targets);
            }
        };
        
        let url = format!(
            "{}://{}:{}{}",
            self.config.scheme,
            node_ip,
            port,
            self.config.metrics_path
        );
        
        // ✅ 预加载 Node 元数据到缓存
        let mut cache = self.metadata_cache.write().await;
        let _ = cache.get_or_fetch_node(node_name, &Api::all(self.client.clone())).await;
        drop(cache);
        
        targets.push(DiscoveredTarget {
            url,
            pod_name: None,
            pod_namespace: None,
            node_name: Some(node_name.clone()),
            service_name: None,
            service_namespace: None,
        });
        
        Ok(targets)
    }

    async fn get_namespaces(&self) -> Result<Vec<String>> {
        if self.config.namespaces.is_empty() {
            let ns_api: Api<Namespace> = Api::all(self.client.clone());
            let ns_list = ns_api.list(&ListParams::default()).await?;
            Ok(ns_list.items.iter().filter_map(|ns| ns.metadata.name.clone()).collect())
        } else {
            Ok(self.config.namespaces.clone())
        }
    }

    fn build_list_params(&self) -> ListParams {
        let mut lp = ListParams::default();
        if let Some(ref selector) = self.config.label_selector {
            lp = lp.labels(selector);
        }
        if let Some(ref field_sel) = self.config.field_selector {
            lp = lp.fields(field_sel);
        }
        lp
    }
}

impl Drop for K8sServiceDiscovery {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}