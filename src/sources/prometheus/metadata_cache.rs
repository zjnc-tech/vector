use k8s_openapi::api::core::v1::{Node, Pod, Service};
use kube::api::Api;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// ============================================================================
// 元数据结构（Arc 共享版本）
// ============================================================================

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

// ============================================================================
// 元数据缓存（靠 Watch 主动失效）
// ============================================================================

pub struct MetadataCache {
    pods: HashMap<String, Arc<PodMetadata>>,
    nodes: HashMap<String, Arc<NodeMetadata>>,
    services: HashMap<String, Arc<ServiceMetadata>>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self {
            pods: HashMap::new(),
            nodes: HashMap::new(),
            services: HashMap::new(),
        }
    }

    /// 获取或拉取 Pod 元数据
    pub async fn get_or_fetch_pod(
        &mut self,
        namespace: &str,
        pod_name: &str,
        pods_api: &Api<Pod>,
    ) -> Option<Arc<PodMetadata>> {
        let key = format!("{}/{}", namespace, pod_name);

        if let Some(cached) = self.pods.get(&key) {
            return Some(Arc::clone(cached));
        }

        match pods_api.get(pod_name).await {
            Ok(pod) => {
                let metadata = Arc::new(PodMetadata {
                    name: pod.metadata.name.unwrap_or_default(),
                    namespace: pod.metadata.namespace.unwrap_or_default(),
                    labels: pod.metadata.labels.unwrap_or_default(),
                    annotations: pod.metadata.annotations.unwrap_or_default(),
                    pod_ip: pod.status.as_ref().and_then(|s| s.pod_ip.clone()),
                });

                self.pods.insert(key, Arc::clone(&metadata));
                Some(metadata)
            }
            Err(_) => None,
        }
    }

    /// 获取或拉取 Node 元数据
    pub async fn get_or_fetch_node(
        &mut self,
        node_name: &str,
        nodes_api: &Api<Node>,
    ) -> Option<Arc<NodeMetadata>> {
        if let Some(cached) = self.nodes.get(node_name) {
            return Some(Arc::clone(cached));
        }

        match nodes_api.get(node_name).await {
            Ok(node) => {
                let node_ip = if let Some(status) = &node.status {
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

                let metadata = Arc::new(NodeMetadata {
                    name: node_name.to_string(),
                    labels: node.metadata.labels.unwrap_or_default(),
                    node_ip,
                });

                self.nodes.insert(node_name.to_string(), Arc::clone(&metadata));
                Some(metadata)
            }
            Err(_) => None,
        }
    }

    /// 获取或拉取 Service 元数据
    pub async fn get_or_fetch_service(
        &mut self,
        namespace: &str,
        service_name: &str,
        services_api: &Api<Service>,
    ) -> Option<Arc<ServiceMetadata>> {
        let key = format!("{}/{}", namespace, service_name);

        if let Some(cached) = self.services.get(&key) {
            return Some(Arc::clone(cached));
        }

        match services_api.get(service_name).await {
            Ok(service) => {
                let metadata = Arc::new(ServiceMetadata {
                    name: service.metadata.name.unwrap_or_default(),
                    namespace: service.metadata.namespace.unwrap_or_default(),
                    labels: service.metadata.labels.unwrap_or_default(),
                    annotations: service.metadata.annotations.unwrap_or_default(),
                });

                self.services.insert(key, Arc::clone(&metadata));
                Some(metadata)
            }
            Err(_) => None,
        }
    }

    // ============================================================================
    // ✅ 新增：无锁查询方法（只读缓存，不触发 API 调用）
    // ============================================================================

    /// 从缓存获取 Pod 元数据（不会触发 API 调用）
    pub fn get_pod_cached(&self, namespace: &str, pod_name: &str) -> Option<Arc<PodMetadata>> {
        let key = format!("{}/{}", namespace, pod_name);
        self.pods.get(&key).cloned()
    }

    /// 从缓存获取 Node 元数据（不会触发 API 调用）
    pub fn get_node_cached(&self, node_name: &str) -> Option<Arc<NodeMetadata>> {
        self.nodes.get(node_name).cloned()
    }

    /// 从缓存获取 Service 元数据（不会触发 API 调用）
    pub fn get_service_cached(&self, namespace: &str, service_name: &str) -> Option<Arc<ServiceMetadata>> {
        let key = format!("{}/{}", namespace, service_name);
        self.services.get(&key).cloned()
    }

    // ============================================================================
    // Watch 主动更新方法
    // ============================================================================

    /// 主动失效并更新 Pod 缓存
    pub fn update_pod(&mut self, namespace: &str, pod_name: &str, metadata: Arc<PodMetadata>) {
        let key = format!("{}/{}", namespace, pod_name);
        self.pods.insert(key, metadata);
    }

    /// 主动失效并更新 Node 缓存
    pub fn update_node(&mut self, node_name: &str, metadata: Arc<NodeMetadata>) {
        self.nodes.insert(node_name.to_string(), metadata);
    }

    /// 主动失效并更新 Service 缓存
    pub fn update_service(&mut self, namespace: &str, service_name: &str, metadata: Arc<ServiceMetadata>) {
        let key = format!("{}/{}", namespace, service_name);
        self.services.insert(key, metadata);
    }

    // ✅ 删除单个 Pod
    pub fn remove_pod(&mut self, namespace: &str, pod_name: &str) {
        let key = format!("{}/{}", namespace, pod_name);
        self.pods.remove(&key);  // ✅ 直接从 HashMap 删除
    }

    // ✅ 清理整个 namespace 的 Pod（用于 Watch 重启）
    pub fn clear_namespace_pods(&mut self, namespace: &str) {
        // ✅ 保留不是该 namespace 的 Pod
        self.pods.retain(|k, _| !k.starts_with(&format!("{}/", namespace)));
    }

    // ✅ 删除单个 Service
    pub fn remove_service(&mut self, namespace: &str, service_name: &str) {
        let key = format!("{}/{}", namespace, service_name);
        self.services.remove(&key);
    }

    // ✅ 清理整个 namespace 的 Service
    pub fn clear_namespace_services(&mut self, namespace: &str) {
        self.services.retain(|k, _| !k.starts_with(&format!("{}/", namespace)));
    }

    // ✅ 删除单个 Node
    pub fn remove_node(&mut self, node_name: &str) {
        self.nodes.remove(node_name);
    }

    // ✅ 清理所有 Node（用于 Watch 重启）
    pub fn clear_all_nodes(&mut self) {
        self.nodes.clear();
    }
}


// ============================================================================
// 辅助函数：从 Pod 获取 Node 名称
// ============================================================================

pub async fn get_node_name_from_pod(
    pod_name: &str,
    _namespace: &str,
    pods_api: &Api<Pod>,
) -> Result<Option<String>> {
    let pod = pods_api.get(pod_name).await?;
    Ok(pod.spec.as_ref().and_then(|s| s.node_name.clone()))
}