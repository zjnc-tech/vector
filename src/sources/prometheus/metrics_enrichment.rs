use vector_lib::event::Metric;

use super::k8s_discovery::{DiscoveredTarget, LabelSelector, MetadataLabelsConfig};

/// 从缓存获取元数据并附加到指标
pub async fn add_metadata_to_metric(
    metric: &mut Metric,
    target: &DiscoveredTarget,
    config: &MetadataLabelsConfig,
    honor_labels: bool,
) {
    // 添加 job 标签，job_name 是必填的，直接使用
    let tag_name = format!("{}job", config.label_prefix);
    if !honor_labels || metric.tag_value(&tag_name).is_none() {
        metric.replace_tag(tag_name, target.job_name.clone());
    }

    let endpoint = format!("{}endpoint", config.label_prefix);
    if !honor_labels || metric.tag_value(&endpoint).is_none() {
        metric.replace_tag(endpoint, target.url.clone());
    }

    // Pod 元数据
    if let Some(pod_meta) = &target.pod_metadata {
        if config.namespace {
            let tag_name = format!("{}namespace", config.label_prefix);
            add_metadata_tag(metric, tag_name, pod_meta.namespace.clone(), honor_labels);
        }

        if config.pod_name {
            let tag_name = format!("{}pod", config.label_prefix);
            add_metadata_tag(metric, tag_name, pod_meta.name.clone(), honor_labels);
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

    // Node 元数据
    if let Some(node_meta) = &target.node_metadata {
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

    // Service 元数据
    if let Some(svc_meta) = &target.service_metadata {
        if config.service_name {
            let tag_name = format!("{}service", config.label_prefix);
            if !honor_labels || metric.tag_value(&tag_name).is_none() {
                metric.replace_tag(tag_name, svc_meta.name.clone());
            }
        }

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
        LabelSelector::Specific(keys) if !keys.is_empty() => all_labels
            .iter()
            .filter(|(k, _)| keys.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),

        // 不包含任何标签
        _ => return,
    };

    for (key, value) in selected_labels {
        let tag_name = format!("{}{}", prefix, sanitize_label_name(&key));
        if !honor_labels || metric.tag_value(&tag_name).is_none() {
            metric.replace_tag(tag_name, value);
        }
    }
}

/// Adds target Pod metadata, retaining a conflicting metric `pod` or
/// `namespace` tag as `exported_<tag>` when target metadata takes precedence.
fn add_metadata_tag(metric: &mut Metric, tag_name: String, value: String, honor_labels: bool) {
    if honor_labels {
        if metric.tag_value(&tag_name).is_none() {
            metric.replace_tag(tag_name, value);
        }
    } else if let Some(original_value) = metric.replace_tag(tag_name.clone(), value) {
        metric.replace_tag(format!("exported_{tag_name}"), original_value);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use vector_lib::event::{MetricKind, MetricValue};

    use super::*;
    use crate::sources::prometheus::k8s_discovery::{DiscoveredTarget, PodMetadata};

    #[tokio::test]
    async fn preserves_metric_pod_and_namespace_when_target_metadata_overrides_them() {
        let target = DiscoveredTarget {
            url: "http://node-exporter:9111/metrics".to_owned(),
            job_name: "node-exporter".to_owned(),
            node_metadata: None,
            service_metadata: None,
            pod_metadata: Some(Arc::new(PodMetadata {
                name: "vector-scraper".to_owned(),
                namespace: "monitoring".to_owned(),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                pod_ip: None,
            })),
        };
        let mut metric = Metric::new(
            "dcgm_job_info",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 1.0 },
        );
        metric.replace_tag("pod".to_owned(), "workload-pod".to_owned());
        metric.replace_tag("namespace".to_owned(), "workload-namespace".to_owned());

        add_metadata_to_metric(
            &mut metric,
            &target,
            &MetadataLabelsConfig::default(),
            false,
        )
        .await;

        assert_eq!(metric.tag_value("pod"), Some("vector-scraper".to_owned()));
        assert_eq!(metric.tag_value("namespace"), Some("monitoring".to_owned()));
        assert_eq!(
            metric.tag_value("exported_pod"),
            Some("workload-pod".to_owned())
        );
        assert_eq!(
            metric.tag_value("exported_namespace"),
            Some("workload-namespace".to_owned())
        );
    }
}

/// 将 Kubernetes label 名称转换为合法的 Prometheus label 名称
pub fn sanitize_label_name(name: &str) -> String {
    name.replace('.', "_").replace('/', "_").replace('-', "_")
}
