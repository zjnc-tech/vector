use k8s_openapi::api::core::v1::{Endpoints, Pod, Service, Namespace};
use kube::{api::{Api, ListParams}, Client, runtime::watcher};
use std::sync::Arc;
use futures::stream::Stream;
use futures::StreamExt;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use tracing::{error, info};
use kube::runtime::WatchStreamExt;
use std::collections::BTreeMap;  
use vector_lib::configurable::Configurable;


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

    /// Port name or number to scrape. If not specified, scrapes all ports.
    /// Can be a port name (e.g., "metrics") or port number (e.g., "9090").
    /// Examples: "metrics", "9090", "prometheus"
    #[serde(default)]
    pub port: Option<String>,
    
    /// Metric path to scrape. Defaults to "/metrics".
    #[serde(default = "default_metrics_path")]
    pub metrics_path: String,
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
    
    /// Pod labels to include. Use "*" for all labels, empty array for none, or specify label keys.
    #[serde(default)]
    pub pod_labels: LabelSelector,
    
    /// Pod annotations to include. Use "*" for all, empty array for none, or specify annotation keys.
    #[serde(default)]
    pub pod_annotations: LabelSelector,
    
    /// Service labels to include. Use "*" for all labels, empty array for none, or specify label keys.
    #[serde(default)]
    pub service_labels: LabelSelector,
    
    /// Service annotations to include. Use "*" for all, empty array for none, or specify annotation keys.
    #[serde(default)]
    pub service_annotations: LabelSelector,
    
    /// Node labels to include. Use "*" for all labels, empty array for none, or specify label keys.
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
    /// Include all labels/annotations (use "*")
    All(String),
    /// Include specific labels/annotations by key
    Specific(Vec<String>),
}

impl Default for LabelSelector {
    fn default() -> Self {
        LabelSelector::Specific(vec![])
    }
}

impl LabelSelector {
    /// Check if should include all labels
    pub fn is_all(&self) -> bool {
        matches!(self, LabelSelector::All(s) if s == "*")
    }
    
    /// Check if should include no labels
    pub fn is_none(&self) -> bool {
        matches!(self, LabelSelector::Specific(v) if v.is_empty())
    }
    
    /// Get specific label keys if any
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
// 发现的目标
// ============================================================================

#[derive(Clone, Debug)]
#[allow(dead_code)] 
pub struct DiscoveredTarget {
    pub url: String,
    pub pod_metadata: Option<PodMetadata>,
    pub node_metadata: Option<NodeMetadata>,
    pub service_metadata: Option<ServiceMetadata>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct NodeMetadata {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub node_ip: Option<String>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct PodMetadata {
    pub name: String,
    pub namespace: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub pod_ip: Option<String>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ServiceMetadata {
    pub name: String,
    pub namespace: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

// ============================================================================
// Kubernetes 服务发现
// ============================================================================

pub struct K8sServiceDiscovery {
    client: Client,
    config: KubernetesSdConfig,
    targets: Arc<RwLock<Vec<DiscoveredTarget>>>,
}

impl Clone for K8sServiceDiscovery {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            targets: Arc::clone(&self.targets),
        }
    }
}

impl K8sServiceDiscovery {
    pub async fn new(config: KubernetesSdConfig) -> Result<Self> {
        let client = Client::try_default().await?;
        let targets = Arc::new(RwLock::new(Vec::new()));
        
        let discovery = Self { 
            client, 
            config,
            targets,
        };
        
        // 初始发现一次
        let initial_targets = discovery.discover_targets_internal().await?;
        *discovery.targets.write().await = initial_targets;
        
        // 启动后台 watch 任务
        discovery.start_watch_task();
        
        Ok(discovery)
    }
    
    /// 获取当前的 targets 列表 (供 interval 循环调用)
    pub async fn get_targets(&self) -> Vec<DiscoveredTarget> {
        self.targets.read().await.clone()
    }
    
    /// 启动后台任务持续监听 K8s 资源变化
    fn start_watch_task(&self) {
        let discovery = self.clone();
        tokio::spawn(async move {
            if let Err(e) = discovery.watch_and_update().await {
                error!("Watch task failed: {:?}", e);
            }
        });
    }
    
    /// 后台任务: 监听 K8s 资源变化并更新 targets
    async fn watch_and_update(&self) -> Result<()> {
        match self.config.role {
            KubernetesRole::Endpoints => {
                let stream = self.watch_endpoints().await?;
                tokio::pin!(stream);

                while let Some(new_targets) = stream.next().await {
                    info!("Kubernetes targets updated: {} endpoints", new_targets.len());
                    *self.targets.write().await = new_targets;
                }
            }
        }
        Ok(())
    }
    
    /// 内部方法: 一次性发现所有 targets
    async fn discover_targets_internal(&self) -> Result<Vec<DiscoveredTarget>> {
        match self.config.role {
            KubernetesRole::Endpoints => self.discover_from_endpoints().await,
        }
    }
    
    /// 从 Endpoints 发现所有 targets (一次性)
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

    /// 从 Endpoints 发现（和 Prometheus 设计一致）
    async fn watch_endpoints(&self) -> Result<impl Stream<Item = Vec<DiscoveredTarget>> + '_> {
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
                            
                            yield targets;
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

    /// 辅助方法：从单个 Endpoints 发现 targets，同时获取对应的 Service 元数据
    async fn discover_from_endpoints_internal(
        &self,
        endpoints: &Endpoints,
        services_api: &Api<Service>,
        pods_api: &Api<Pod>,
    ) -> Result<Vec<DiscoveredTarget>> {
        let mut targets = Vec::new();
        
        let endpoints_name = match endpoints.metadata.name.as_ref() {
            Some(name) => name,
            None => return Ok(targets),
        };
        
        let namespace = endpoints.metadata.namespace.as_ref()
            .ok_or("Endpoints has no namespace")?;
        
        // 获取对应的 Service
        let service_metadata = if let Ok(service) = services_api.get(endpoints_name).await {
            Some(ServiceMetadata {
                name: service.metadata.name.unwrap_or_default(),
                namespace: service.metadata.namespace.unwrap_or_default(),
                labels: service.metadata.labels.unwrap_or_default(),
                annotations: service.metadata.annotations.unwrap_or_default(),
            })
        } else {
            Some(ServiceMetadata {
                name: endpoints_name.clone(),
                namespace: namespace.clone(),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
            })
        };
        
        // 从 Endpoints 提取 Pod IP 和端口
        if let Some(subsets) = &endpoints.subsets {
            for subset in subsets {
                if let Some(addresses) = &subset.addresses {
                    for address in addresses {
                        if let Some(ports) = &subset.ports {
                            for port in ports {
                                // 端口过滤逻辑
                                if let Some(ref port_filter) = self.config.port {
                                    let port_matches = if let Ok(port_num) = port_filter.parse::<i32>() {
                                        // 按端口号过滤
                                        port.port == port_num
                                    } else {
                                        // 按端口名过滤
                                        port.name.as_deref() == Some(port_filter.as_str())
                                    };
                                    
                                    if !port_matches {
                                        continue; // 跳过不匹配的端口
                                    }
                                }
                                
                                // 使用配置的 metrics_path
                                let url = format!(
                                    "http://{}:{}{}",
                                    address.ip,
                                    port.port,
                                    self.config.metrics_path
                                );
                                
                                let (pod_metadata, node_metadata) = self.extract_pod_metadata(address, pods_api).await;
                                
                                targets.push(DiscoveredTarget {
                                    url,
                                    pod_metadata,
                                    node_metadata,
                                    service_metadata: service_metadata.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
        
        Ok(targets)
    }

    /// 辅助方法：提取 Pod 元数据
    async fn extract_pod_metadata(
        &self,
        address: &k8s_openapi::api::core::v1::EndpointAddress,
        pods_api: &Api<Pod>,
    ) -> (Option<PodMetadata>, Option<NodeMetadata>) {
        if let Some(target_ref) = &address.target_ref {
            if target_ref.kind.as_deref() == Some("Pod") {
                if let Some(pod_name) = &target_ref.name {
                    if let Ok((pod_meta, node_meta)) = self.get_pod_and_node_metadata(pods_api, pod_name).await {
                        return (Some(pod_meta), node_meta);
                    }
                }
            }
        }
        (None, None)
    }

    async fn get_pod_and_node_metadata(&self, pods_api: &Api<Pod>, pod_name: &str) -> Result<(PodMetadata, Option<NodeMetadata>)> {
        let pod = pods_api.get(pod_name).await?;
        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
        let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.clone());

        // 获取 Node 元数据
        let node_metadata = if let Some(ref node_name_val) = node_name {
            self.get_node_metadata(node_name_val).await.ok()
        } else {
            None
        };

        let pod_metadata = PodMetadata {
            name: pod.metadata.name.unwrap_or_default(),
            namespace: pod.metadata.namespace.unwrap_or_default(),
            labels: pod.metadata.labels.unwrap_or_default(),
            annotations: pod.metadata.annotations.unwrap_or_default(),
            pod_ip,
        };

        Ok((pod_metadata, node_metadata))
    }

    /// 获取 Node 的完整元数据
    async fn get_node_metadata(&self, node_name: &str) -> Result<NodeMetadata> {
        use k8s_openapi::api::core::v1::Node;
        
        let nodes_api: Api<Node> = Api::all(self.client.clone());
        let node = nodes_api.get(node_name).await?;
        
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
        
        Ok(NodeMetadata {
            name: node_name.to_string(),
            labels: node.metadata.labels.unwrap_or_default(),
            node_ip,
        })
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