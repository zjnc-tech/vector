use std::collections::BTreeMap;
use vector_lib::event::Metric;

use super::k8s_discovery::{
    DiscoveredTarget,
    MetadataLabelsConfig,
    PodMetadata,
    NodeMetadata,
    ServiceMetadata,
};

/// 添加所有 Kubernetes 元数据到 metric
pub fn add_metadata_to_metric(
    metric: &mut Metric,
    target: &DiscoveredTarget,
    config: &MetadataLabelsConfig,
) {
    let prefix = &config.label_prefix;
    metric.replace_tag(
            format!("{}endpoint", prefix),
            target.url.clone()
        );
    
    // 添加 Pod 元数据
    if let Some(ref pod_meta) = target.pod_metadata {
        add_pod_metadata(metric, pod_meta, config, prefix);
    }
    
    // 添加 Node 元数据
    if let Some(ref node_meta) = target.node_metadata {
        add_node_metadata(metric, node_meta, config, prefix);
    }
    
    // 添加 Service 元数据
    if let Some(ref service_meta) = target.service_metadata {
        add_service_metadata(metric, service_meta, config, prefix);
    }
}

/// 添加 Pod 基础字段和 labels/annotations
fn add_pod_metadata(
    metric: &mut Metric,
    pod_meta: &PodMetadata,
    config: &MetadataLabelsConfig,
    prefix: &str,
) {
    // 基础字段
    if config.namespace {
        metric.replace_tag(
            format!("{}namespace", prefix),
            pod_meta.namespace.clone()
        );
    }
    
    if config.pod_name {
        metric.replace_tag(
            format!("{}pod", prefix),
            pod_meta.name.clone()
        );
    }
    
    if config.pod_ip {
        if let Some(ref pod_ip) = pod_meta.pod_ip {
            metric.replace_tag(
                format!("{}pod_ip", prefix),
                pod_ip.clone()
            );
        }
    }
    
    // Pod labels
    add_labels_to_metric(
        metric,
        &pod_meta.labels,
        &config.pod_labels,
        prefix,
        "pod_label_"
    );
    
    // Pod annotations
    add_labels_to_metric(
        metric,
        &pod_meta.annotations,
        &config.pod_annotations,
        prefix,
        "pod_annotation_"
    );
}

/// 添加 Node 基础字段和 labels
fn add_node_metadata(
    metric: &mut Metric,
    node_meta: &NodeMetadata,
    config: &MetadataLabelsConfig,
    prefix: &str,
) {
    if config.node_name {
        metric.replace_tag(
            format!("{}node", prefix),
            node_meta.name.clone()
        );
    }
    
    if config.node_ip {
        if let Some(ref node_ip) = node_meta.node_ip {
            metric.replace_tag(
                format!("{}node_ip", prefix),
                node_ip.clone()
            );
        }
    }
    
    // Node labels
    add_labels_to_metric(
        metric,
        &node_meta.labels,
        &config.node_labels,
        prefix,
        "node_label_"
    );
}

/// 添加 Service 基础字段和 labels/annotations
fn add_service_metadata(
    metric: &mut Metric,
    service_meta: &ServiceMetadata,
    config: &MetadataLabelsConfig,
    prefix: &str,
) {
    // Service name 总是添加
    metric.replace_tag(
        format!("{}service", prefix),
        service_meta.name.clone()
    );
    
    // Service labels
    add_labels_to_metric(
        metric,
        &service_meta.labels,
        &config.service_labels,
        prefix,
        "service_label_"
    );
    
    // Service annotations
    add_labels_to_metric(
        metric,
        &service_meta.annotations,
        &config.service_annotations,
        prefix,
        "service_annotation_"
    );
}

/// 通用的 labels/annotations 添加逻辑
fn add_labels_to_metric(
    metric: &mut Metric,
    source_labels: &BTreeMap<String, String>,
    selector: &super::k8s_discovery::LabelSelector,
    prefix: &str,
    label_type: &str,
) {
    use super::k8s_discovery::LabelSelector;
    
    match selector {
        LabelSelector::All(_) => {
            // 添加所有 labels
            for (key, value) in source_labels {
                let full_key = format!("{}{}{}", prefix, label_type, sanitize_label_name(key));
                metric.replace_tag(full_key, value.clone());
            }
        }
        LabelSelector::Specific(keys) if !keys.is_empty() => {
            // 只添加指定的 labels
            for key in keys {
                if let Some(value) = source_labels.get(key) {
                    let full_key = format!("{}{}{}", prefix, label_type, sanitize_label_name(key));
                    metric.replace_tag(full_key, value.clone());
                }
            }
        }
        _ => {} // 不添加任何 labels
    }
}

/// 将 Kubernetes label 名称转换为合法的 Prometheus label 名称
pub fn sanitize_label_name(name: &str) -> String {
    name.replace('.', "_")
        .replace('/', "_")
        .replace('-', "_")
}