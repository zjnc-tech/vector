use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::time;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::sources::dcgm::v1alpha1::pod_resources_lister_client::PodResourcesListerClient;
use crate::sources::dcgm::v1alpha1::ListPodResourcesRequest;

#[derive(Debug, Clone, Default)]
pub struct PodInfo {
    pub pod: String,
    pub namespace: String,
    pub container: String,
}

#[derive(Debug, Clone)]
pub struct PodResourcesMapper {
    inner: Arc<RwLock<HashMap<String, PodInfo>>>,
}

impl Default for PodResourcesMapper {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl PodResourcesMapper {
    pub fn new() -> Self {
        Self::default()
    }

    /// 更新 GPU UUID -> PodInfo 映射
    pub fn update(&self, data: &HashMap<String, PodInfo>) {
        let mut writer = self.inner.write().unwrap();
        *writer = data.clone(); // 如果数据量大，也可以用 `.clear()` + `.extend()` 避免重新分配
    }

    /// 通过 GPU UUID 查询 PodInfo
    pub fn get(&self, gpu_id: &str) -> Option<PodInfo> {
        let reader = self.inner.read().unwrap();
        reader.get(gpu_id).cloned()
    }
}

/// 一个独立结构体，周期刷新 `PodResourcesMapper`
pub struct PodResourcesRefresher {
    mapper: PodResourcesMapper,
}

impl PodResourcesRefresher {
    pub const fn new(mapper: PodResourcesMapper) -> Self {
        Self { mapper }
    }

    pub async fn run(self) {
        let uds_path = "/var/lib/kubelet/pod-resources/kubelet.sock";

        if !Path::new(uds_path).exists() {
            warn!("PodResources socket not found at: {}", uds_path);
            return;
        }

        let mapper = self.mapper;
        loop {
            match Self::connect_to_kubelet(uds_path).await {
                Ok(mut client) => match client.list(ListPodResourcesRequest {}).await {
                    Ok(resp) => {
                        let mut map = HashMap::new();
                        for pod in resp.into_inner().pod_resources {
                            let pod_name = pod.name.clone();
                            let namespace = pod.namespace.clone();
                            for container in pod.containers {
                                let container_name = container.name.clone();
                                for device in container.devices {
                                    if device.resource_name != "nvidia.com/gpu" {
                                        continue;
                                    }

                                    for id in device.device_ids {
                                        map.insert(
                                            id.clone(),
                                            PodInfo {
                                                pod: pod_name.clone(),
                                                namespace: namespace.clone(),
                                                container: container_name.clone(),
                                            },
                                        );
                                    }
                                }
                            }
                        }
                        mapper.update(&map);
                    }
                    Err(e) => {
                        warn!("Failed to list pod resources: {:?}", e);
                    }
                },
                Err(e) => {
                    warn!("Failed to connect to pod resources: {:?}", e);
                }
            }

            time::sleep(Duration::from_secs(10)).await;
        }
    }

    async fn connect_to_kubelet(
        socket_path: &str,
    ) -> Result<PodResourcesListerClient<Channel>, tonic::transport::Error> {
        let path = socket_path.to_string();

        let endpoint = Endpoint::try_from("http://[::]:50051")?;
        let channel = endpoint
            .connect_with_connector(service_fn(move |_: Uri| {
                tokio::net::UnixStream::connect(path.clone())
            }))
            .await?;

        Ok(PodResourcesListerClient::new(channel))
    }
}
