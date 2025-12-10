// ============================================================================
// Namespace 级别的 Watch 管理器
// 
// 设计目标：
// 1. 每个 namespace 只启动一个 Pod Watch 和 Service Watch
// 2. 多个 prometheus_scrape source 共享同一个 namespace 的元数据缓存
// 3. 按需启动：只为配置的 namespace 启动 Watch
// 4. 引用计数：最后一个 source 退出时自动停止 Watch
// 5. Node Watch 全局共享：所有 source 共享一个 Node Watch
// ============================================================================

use k8s_openapi::api::core::v1::{Pod, Service, Node};
use kube::{api::Api, Client, runtime::watcher};
use kube::runtime::WatchStreamExt;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{RwLock, Mutex};
use futures::StreamExt;
use tracing::{info, warn};
use std::sync::OnceLock; 

use super::metadata_cache::{MetadataCache, PodMetadata, ServiceMetadata, NodeMetadata};

// ============================================================================
// 全局单例 Watch 管理器
// ============================================================================

/// 全局 Watch 管理器实例
pub static WATCH_MANAGER: OnceLock<NamespaceWatchManager> = OnceLock::new();

/// 获取全局 Watch 管理器（延迟初始化）
pub fn get_watch_manager() -> &'static NamespaceWatchManager {
    WATCH_MANAGER.get_or_init(|| NamespaceWatchManager::new())
}


/// Namespace 级别的 Watch 状态
struct NamespaceWatchState {
    /// 引用计数（有多少个 source 在使用这个 namespace）
    ref_count: usize,
    /// 停止信号发送器
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

/// Node Watch 状态
struct NodeWatchState {
    ref_count: usize,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

/// Namespace Watch 管理器
pub struct NamespaceWatchManager {
    /// Kubernetes 客户端
    client: Mutex<Option<Client>>,
    
    /// 全局元数据缓存（所有 namespace 共享）
    metadata_cache: Arc<RwLock<MetadataCache>>,
    
    /// 每个 namespace 的 Watch 状态
    namespace_watches: Arc<RwLock<HashMap<String, NamespaceWatchState>>>,
    
    /// Node Watch 状态（全局只需要一个）
    node_watch_state: Arc<RwLock<Option<NodeWatchState>>>,
}

impl NamespaceWatchManager {
    fn new() -> Self {
        Self {
            client: Mutex::new(None),
            metadata_cache: Arc::new(RwLock::new(MetadataCache::new())),
            namespace_watches: Arc::new(RwLock::new(HashMap::new())),
            node_watch_state: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 初始化 Kubernetes 客户端（只在第一次调用时执行）
    async fn ensure_client_initialized(&self) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
        let mut client_guard = self.client.lock().await;
        
        if client_guard.is_none() {
            let client = Client::try_default().await?;
            *client_guard = Some(client.clone());
            info!("✅ Initialized global Kubernetes client");
            Ok(client)
        } else {
            Ok(client_guard.as_ref().unwrap().clone())
        }
    }
    
    /// 获取全局元数据缓存的引用
    pub fn get_cache(&self) -> Arc<RwLock<MetadataCache>> {
        Arc::clone(&self.metadata_cache)
    }
    
    /// 注册一个 namespace（增加引用计数，必要时启动 Watch）
    pub async fn register_namespace(&self, namespace: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.ensure_client_initialized().await?;
        let mut watches = self.namespace_watches.write().await;
        
        if let Some(state) = watches.get_mut(namespace) {
            // 已经有 Watch 在运行，只增加引用计数
            state.ref_count += 1;
            info!("📊 Namespace '{}' watch ref_count++ = {}", namespace, state.ref_count);
        } else {
            // 首次注册，启动新的 Watch 任务
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            
            // 启动 Pod Watch
            self.start_pod_watch(client.clone(), namespace.to_string(), shutdown_rx.clone());
            
            // 启动 Service Watch
            self.start_service_watch(client.clone(), namespace.to_string(), shutdown_rx.clone());
            
            watches.insert(namespace.to_string(), NamespaceWatchState {
                ref_count: 1,
                shutdown_tx,
            });
            
            info!("🚀 Started Pod/Service watch for namespace '{}' (ref_count=1)", namespace);
        }
        
        Ok(())
    }
    
    /// 注销一个 namespace（减少引用计数，必要时停止 Watch）
    pub async fn unregister_namespace(&self, namespace: &str) {
        let mut watches = self.namespace_watches.write().await;
        
        if let Some(state) = watches.get_mut(namespace) {
            state.ref_count -= 1;
            info!("📊 Namespace '{}' watch ref_count-- = {}", namespace, state.ref_count);
            
            if state.ref_count == 0 {
                // 最后一个使用者退出，停止 Watch
                let _ = state.shutdown_tx.send(true);
                watches.remove(namespace);
                info!("🛑 Stopped Pod/Service watch for namespace '{}' (no more references)", namespace);
            }
        }
    }
    
    /// 注册 Node Watch（全局只需要一个）
    pub async fn register_node_watch(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.ensure_client_initialized().await?;
        let mut node_state = self.node_watch_state.write().await;
        
        if let Some(state) = node_state.as_mut() {
            state.ref_count += 1;
            info!("📊 Global Node watch ref_count++ = {}", state.ref_count);
        } else {
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            self.start_node_watch(client.clone(), shutdown_rx);
            
            *node_state = Some(NodeWatchState {
                ref_count: 1,
                shutdown_tx,
            });
            
            info!("🚀 Started global Node watch (ref_count=1)");
        }
        
        Ok(())
    }
    
    /// 注销 Node Watch
    pub async fn unregister_node_watch(&self) {
        let mut node_state = self.node_watch_state.write().await;
        
        if let Some(state) = node_state.as_mut() {
            state.ref_count -= 1;
            info!("📊 Global Node watch ref_count-- = {}", state.ref_count);
            
            if state.ref_count == 0 {
                let _ = state.shutdown_tx.send(true);
                *node_state = None;
                info!("🛑 Stopped global Node watch (no more references)");
            }
        }
    }
    
    // ========================================================================
    // Watch 任务实现
    // ========================================================================
    
    fn start_pod_watch(
        &self,
        client: Client,
        namespace: String,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let cache = Arc::clone(&self.metadata_cache);
        
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Pod watch for namespace '{}' shutting down", namespace);
                }
                _ = async {
                    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
                    
                    // ✅ 使用正确的 watcher API
                    let stream = watcher(pods_api, Default::default())
                        .default_backoff();
                    tokio::pin!(stream);
                    
                    while let Some(event) = stream.next().await {
                        match event {
                            // ✅ Apply 事件（创建/更新）
                            Ok(watcher::Event::Apply(pod))| Ok(watcher::Event::InitApply(pod)) => {
                                let pod_name = pod.metadata.name.clone().unwrap_or_default();
                                let new_labels = pod.metadata.labels.clone().unwrap_or_default();
                                let new_annotations = pod.metadata.annotations.clone().unwrap_or_default();
                                let new_pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.clone());
                                
                                // 去重：只在元数据真正变化时才更新
                                let mut cache_guard = cache.write().await;
                                let should_update = if let Some(existing) = cache_guard.get_pod_cached(&namespace, &pod_name) {
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
                                    
                                    cache_guard.update_pod(&namespace, &pod_name, metadata);
                                    info!("[Namespace Watch] Updated Pod {}/{}", namespace, pod_name);
                                }
                                drop(cache_guard);
                            }
                            
                            // ✅ Delete 事件
                            Ok(watcher::Event::Delete(pod)) => {
                                if let Some(pod_name) = pod.metadata.name.as_ref() {  // ✅ 使用 as_ref()
                                    let mut cache_guard = cache.write().await;
                                    cache_guard.remove_pod(&namespace, pod_name);
                                    info!("[Namespace Watch] Removed Pod {}/{} (deleted)", namespace, pod_name);
                                    drop(cache_guard);
                                }
                            }
                            
                            // ✅ InitApply 事件（首次同步）
                            Ok(watcher::Event::Init) => {
                                info!("[Namespace Watch] Pod watch initialized for namespace '{}'", namespace);
                            }
                            
                            // ✅ InitDone 事件（首次同步完成）
                            Ok(watcher::Event::InitDone) => {
                                info!("[Namespace Watch] Pod watch initial sync done for namespace '{}'", namespace);
                            }
                            
                            Err(e) => {
                                warn!("Pod watch error in namespace '{}': {:?}", namespace, e);
                            }
                        }
                    }
                } => {}
            }
        });
    }

    fn start_service_watch(
        &self,
        client: Client,
        namespace: String,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let cache = Arc::clone(&self.metadata_cache);
        
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Service watch for namespace '{}' shutting down", namespace);
                }
                _ = async {
                    let services_api: Api<Service> = Api::namespaced(client.clone(), &namespace);
                    let stream = watcher(services_api, Default::default()).default_backoff();
                    tokio::pin!(stream);
                    
                    while let Some(event) = stream.next().await {
                        match event {
                            Ok(watcher::Event::Apply(service))| Ok(watcher::Event::InitApply(service)) => {
                                let service_name = service.metadata.name.clone().unwrap_or_default();
                                let new_labels = service.metadata.labels.clone().unwrap_or_default();
                                let new_annotations = service.metadata.annotations.clone().unwrap_or_default();
                                
                                let mut cache_guard = cache.write().await;
                                let should_update = if let Some(existing) = cache_guard.get_service_cached(&namespace, &service_name) {
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
                                    
                                    cache_guard.update_service(&namespace, &service_name, metadata);
                                    info!("[Namespace Watch] Updated Service {}/{}", namespace, service_name);
                                }
                                drop(cache_guard);
                            }
                            
                            Ok(watcher::Event::Delete(service)) => {
                                if let Some(service_name) = service.metadata.name.as_ref() {  // ✅ 使用 as_ref()
                                    let mut cache_guard = cache.write().await;
                                    cache_guard.remove_service(&namespace, service_name);
                                    info!("[Namespace Watch] Removed Service {}/{} (deleted)", 
                                        namespace, service_name);
                                    drop(cache_guard);
                                }
                            }
                            
                            Ok(watcher::Event::Init) => {
                                info!("[Namespace Watch] Service watch initialized for namespace '{}'", namespace);
                            }
                            
                            Ok(watcher::Event::InitDone) => {
                                info!("[Namespace Watch] Service watch initial sync done for namespace '{}'", namespace);
                            }
                            
                            Err(e) => {
                                warn!("Service watch error in namespace '{}': {:?}", namespace, e);
                            }
                        }
                    }
                } => {}
            }
        });
    }

    fn start_node_watch(
        &self,
        client: Client,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let cache = Arc::clone(&self.metadata_cache);
        
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Global Node watch shutting down");
                }
                _ = async {
                    let nodes_api: Api<Node> = Api::all(client.clone());
                    let stream = watcher(nodes_api, Default::default()).default_backoff();
                    tokio::pin!(stream);
                    
                    while let Some(event) = stream.next().await {
                        match event {
                            Ok(watcher::Event::Apply(node)) | Ok(watcher::Event::InitApply(node))=> {
                                let node_name = node.metadata.name.clone().unwrap_or_default();
                                
                                let node_ip = if let Some(status) = &node.status {
                                    if let Some(addresses) = &status.addresses {
                                        addresses.iter()
                                            .find(|addr| addr.type_ == "InternalIP")
                                            .or_else(|| addresses.first())
                                            .map(|addr| addr.address.clone())
                                    } else { None }
                                } else { None };
                                
                                let new_labels = node.metadata.labels.clone().unwrap_or_default();
                                
                                let mut cache_guard = cache.write().await;
                                let should_update = if let Some(existing) = cache_guard.get_node_cached(&node_name) {
                                    existing.labels != new_labels || existing.node_ip != node_ip
                                } else {
                                    true
                                };
                                
                                if should_update {
                                    let metadata = Arc::new(NodeMetadata {
                                        name: node_name.clone(),
                                        labels: new_labels,
                                        node_ip,
                                    });
                                    
                                    cache_guard.update_node(&node_name, metadata);
                                    info!("[Global Watch] Updated Node {}", node_name);
                                }
                                drop(cache_guard);
                            }
                            
                            Ok(watcher::Event::Delete(node)) => {
                                if let Some(node_name) = node.metadata.name.as_ref() {  // ✅ 使用 as_ref()
                                    let mut cache_guard = cache.write().await;
                                    cache_guard.remove_node(node_name);
                                    info!("[Global Watch] Removed Node {} (deleted)", node_name);
                                    drop(cache_guard);
                                }
                            }
                            
                            Ok(watcher::Event::Init) => {
                                info!("[Global Watch] Node watch initialized");
                            }
                            
                            Ok(watcher::Event::InitDone) => {
                                info!("[Global Watch] Node watch initial sync done");
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
}
// ============================================================================
// 辅助结构：Watch Guard（RAII 模式自动注册/注销）
// ============================================================================

/// Namespace Watch Guard（Drop 时自动注销）
#[derive(Clone)]
pub struct NamespaceWatchGuard {
    namespace: String,
}

impl NamespaceWatchGuard {
    pub async fn new(namespace: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        get_watch_manager().register_namespace(&namespace).await?;  // ✅ 使用函数调用
        Ok(Self { namespace })
    }
}

impl Drop for NamespaceWatchGuard {
    fn drop(&mut self) {
        let namespace = self.namespace.clone();
        tokio::spawn(async move {
            get_watch_manager().unregister_namespace(&namespace).await;  // ✅ 使用函数调用
        });
    }
}

/// Node Watch Guard（Drop 时自动注销）
#[derive(Clone)]
pub struct NodeWatchGuard;

impl NodeWatchGuard {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        get_watch_manager().register_node_watch().await?;  // ✅ 使用函数调用
        Ok(Self)
    }
}

impl Drop for NodeWatchGuard {
    fn drop(&mut self) {
        tokio::spawn(async move {
            get_watch_manager().unregister_node_watch().await;  // ✅ 使用函数调用
        });
    }
}