use std::sync::Arc;
use tokio::sync::RwLock;
use vector_lib::event::Metric;
use super::k8s_discovery::{DiscoveredTarget, MetadataLabelsConfig, LabelSelector};
use super::metadata_cache::MetadataCache;

/// 从缓存获取元数据并附加到指标
pub async fn add_metadata_to_metric(
    metric: &mut Metric,
    target: &DiscoveredTarget,
    config: &MetadataLabelsConfig,
    honor_labels: bool,
    cache: &Arc<RwLock<MetadataCache>>,
) {

    // ✅ 添加 job 标签，job_name 是必填的，直接使用
    let tag_name = format!("{}job", config.label_prefix);
    if !honor_labels || metric.tag_value(&tag_name).is_none() {
        metric.replace_tag(tag_name, target.job_name.clone());
    }

    let endpoint = format!("{}endpoint", config.label_prefix);
    if !honor_labels || metric.tag_value(&endpoint).is_none() {
        metric.replace_tag(endpoint, target.url.clone());
    }
    let cache_guard = cache.read().await;
    
    // Pod 元数据
    if let (Some(ref pod_ns), Some(ref pod_name)) = (&target.pod_namespace, &target.pod_name) {
        if let Some(pod_meta) = cache_guard.get_pod_cached(pod_ns, pod_name) {
            if config.namespace {
                let tag_name = format!("{}namespace", config.label_prefix);
                if !honor_labels || metric.tag_value(&tag_name).is_none() {
                    metric.replace_tag(tag_name, pod_meta.namespace.clone());
                }
            }
            
            if config.pod_name {
                let tag_name = format!("{}pod", config.label_prefix);
                if !honor_labels || metric.tag_value(&tag_name).is_none() {
                    metric.replace_tag(tag_name, pod_meta.name.clone());
                }
            }
            
            if config.pod_ip {
                if let Some(ref ip) = pod_meta.pod_ip {
                    let tag_name = format!("{}pod_ip", config.label_prefix);
                    if !honor_labels || metric.tag_value(&tag_name).is_none() {
                        metric.replace_tag(tag_name, ip.clone());
                    }
                }
            }
            
            add_labels_to_metric(
                metric,
                &pod_meta.labels,
                &config.pod_labels,
                &format!("{}pod_label_", config.label_prefix),
                honor_labels,
            );
            
            add_labels_to_metric(
                metric,
                &pod_meta.annotations,
                &config.pod_annotations,
                &format!("{}pod_annotation_", config.label_prefix),
                honor_labels,
            );
        }
    }
    
    // Node 元数据
    if let Some(ref node_name) = target.node_name {
        if let Some(node_meta) = cache_guard.get_node_cached(node_name) {
            if config.node_name {
                let tag_name = format!("{}node", config.label_prefix);
                if !honor_labels || metric.tag_value(&tag_name).is_none() {
                    metric.replace_tag(tag_name, node_meta.name.clone());
                }
            }
            
            if config.host_ip {
                if let Some(ref ip) = node_meta.node_ip {
                    let tag_name = format!("{}host_ip", config.label_prefix);
                    if !honor_labels || metric.tag_value(&tag_name).is_none() {
                        metric.replace_tag(tag_name, ip.clone());
                    }
                }
            }
            
            add_labels_to_metric(
                metric,
                &node_meta.labels,
                &config.node_labels,
                &format!("{}node_label_", config.label_prefix),
                honor_labels,
            );
        }
    }
    
    // Service 元数据
    if let (Some(ref svc_ns), Some(ref svc_name)) = (&target.service_namespace, &target.service_name) {
        if let Some(svc_meta) = cache_guard.get_service_cached(svc_ns, svc_name) {
            add_labels_to_metric(
                metric,
                &svc_meta.labels,
                &config.service_labels,
                &format!("{}service_label_", config.label_prefix),
                honor_labels,
            );
            
            add_labels_to_metric(
                metric,
                &svc_meta.annotations,
                &config.service_annotations,
                &format!("{}service_annotation_", config.label_prefix),
                honor_labels,
            );
        }
    }
    
    drop(cache_guard);
}

/// 辅助函数：根据配置添加标签到指标
fn add_labels_to_metric(
    metric: &mut Metric,
    all_labels: &std::collections::BTreeMap<String, String>,
    selector: &LabelSelector,
    prefix: &str,
    honor_labels: bool,
) {
    let selected_labels = match selector {
        // 包含所有标签
        LabelSelector::All(s) if s == "*" => all_labels.clone(),
        
        // 包含指定标签
        LabelSelector::Specific(keys) if !keys.is_empty() => {
            all_labels
                .iter()
                .filter(|(k, _)| keys.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        
        // 不包含任何标签
        _ => return,
    };
    
    for (key, value) in selected_labels {
        let tag_name = format!("{}{}", prefix, key);
        if !honor_labels || metric.tag_value(&tag_name).is_none() {
            metric.replace_tag(tag_name, value);
        }
    }
}

/// 将 Kubernetes label 名称转换为合法的 Prometheus label 名称
pub fn sanitize_label_name(name: &str) -> String {
    name.replace('.', "_")
        .replace('/', "_")
        .replace('-', "_")
}