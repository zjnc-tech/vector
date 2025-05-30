use chrono::Utc;
use std::collections::HashMap;
use std::ffi::CStr;
use std::time::Duration;

use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::metric::{Metric, MetricKind, MetricTags, MetricValue},
};
use vector_lib::configurable::configurable_component;
use crate::sources::dcgm::bindings::*;

#[allow(
    unused_imports,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code
)]
mod bindings;
mod collector;
mod dcgm_field_maps;

/// Configuration of one group of the `dcgm`.
#[configurable_component(source("dcgm", "dcgm group data."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct DcgmMetricGroup {
    /// 采集间隔（秒）
    pub scrape_interval_secs: u64,

    /// 要采集的 DCGM 字段名列表，例如 ["DCGM_FI_DEV_POWER_USAGE", "DCGM_FI_DEV_XID_ERRORS"]
    pub fields: Vec<String>,
}

/// Configuration for the `dcgm` source.
#[configurable_component(source("dcgm", "Collect dcgm data."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct DcgmMetricsConfig {
    /// 每个组的配置
    pub groups: HashMap<String, DcgmMetricGroup>,
}

impl_generate_config_from_default!(DcgmMetricGroup);

impl_generate_config_from_default!(DcgmMetricsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "dcgm")]
impl SourceConfig for crate::sources::dcgm::DcgmMetricsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let shutdown = cx.shutdown.clone();
        let out = cx.out.clone();
        let groups = self.groups.clone();

        let field_groups = resolve_all_fields(&self.groups)?.clone();

        Ok(Box::pin(async move {
            let handle = collector::init_dcgm().map_err(|e| {
                error!("DCGM init failed: {}", e);
            })?;

            let mut join_handles = vec![];

            for (group_name, gconf) in groups {
                let mut field_ids = field_groups.get(&group_name).unwrap().clone();
                let mut out = out.clone();
                let shutdown = shutdown.clone();
                let handle = handle.clone();

                let join = tokio::spawn(async move {
                    let interval =
                        tokio::time::interval(Duration::from_secs(gconf.scrape_interval_secs));

                    // 注册字段
                    if let Err(e) = collector::register_fields(
                        handle,
                        &mut field_ids,
                        &group_name,
                        &format!("{}_field_group", group_name),
                        gconf.scrape_interval_secs,
                    ) {
                        error!("Group {} register_fields failed: {}", group_name, e);
                        return;
                    }

                    let mut interval = interval;
                    loop {
                        tokio::select! {
                            _ = shutdown.clone() => {
                                info!("Shutting down group '{}'", group_name);
                                break;
                            }

                            _ = interval.tick() => {
                                match collector::collect_metrics_by_fields(handle, &field_ids) {
                                    Ok(data) => {
                                        let metrics = map_metrics(group_name.clone(), data);
                                        if let Err(e) = out.send_batch(metrics).await {
                                            error!("Failed to send metrics for group {}: {}", group_name, e);
                                            break;
                                        }
                                    }
                                    Err(e) => warn!("Failed to collect group {}: {}", group_name, e),
                                }
                            }
                        }
                    }
                });

                join_handles.push(join);
            }

            // 等待所有任务结束
            for handle in join_handles {
                let _ = handle.await;
            }

            Ok(())
        }))
    }

    fn outputs(&self, _: vector_lib::config::LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

pub fn map_metrics(
    group_name: String,
    data: Vec<(u32, Vec<dcgmFieldValue_v1>)>,
) -> Vec<Metric> {
    let now = Utc::now();
    let mut metrics = Vec::new();

    for (gpu_id, field_values) in data {
        for field_value in field_values {
            let fid = field_value.fieldId;
            let field_type = field_value.fieldType;
            let metric_name = resolve_field_id_to_name(fid).unwrap_or("unknown_metric");

            let mut tags = MetricTags::default();
            tags.insert("gpu_id".into(), gpu_id.to_string());
            tags.insert("group".into(), group_name.clone());

            if field_type == DCGM_FT_STRING as u16 {
                let cstr = unsafe { CStr::from_ptr(field_value.value.str_.as_ptr()) };
                match cstr.to_str() {
                    Ok(s) => {
                        tags.insert(metric_name.to_string(), s.to_string());
                        metrics.push(
                            Metric::new(
                                metric_name.to_string(),
                                MetricKind::Absolute,
                                MetricValue::Gauge { value: 1.0 },
                            )
                                .with_timestamp(Some(now))
                                .with_tags(Some(tags)),
                        );
                    }
                    Err(_) => {
                        eprintln!("Invalid UTF-8 string for field {}", fid);
                    }
                }
                continue;
            }

            // 构造数值类 MetricValue
            let metric_value = match field_type {
                val if val == DCGM_FT_DOUBLE as u16 => MetricValue::Gauge {
                    value: unsafe { field_value.value.dbl },
                },
                val if val == DCGM_FT_INT64 as u16 || val == DCGM_FT_TIMESTAMP as u16 => {
                    MetricValue::Gauge {
                        value: unsafe { field_value.value.i64_ as f64 },
                    }
                }
                val if val == DCGM_FT_BINARY as u16 => {
                    eprintln!("Binary type unsupported for field {}", fid);
                    continue;
                }
                _ => {
                    eprintln!("Unknown field type {} for field {}", field_type, fid);
                    continue;
                }
            };

            metrics.push(
                Metric::new(
                    metric_name.to_string(),
                    MetricKind::Absolute,
                    metric_value,
                )
                    .with_timestamp(Some(now))
                    .with_tags(Some(tags)),
            );
        }
    }

    metrics
}

fn resolve_all_fields(
    groups: &HashMap<String, DcgmMetricGroup>,
) -> crate::Result<HashMap<String, Vec<u16>>> {
    let mut field_groups: HashMap<String, Vec<u16>> = HashMap::new();

    for (group_name, group_cfg) in groups {
        let mut ids = Vec::new();
        for field_name in &group_cfg.fields {
            match resolve_field_name(field_name) {
                Some(id) => ids.push(id),
                None => {
                    return Err(format!("Unknown DCGM field name: {}", field_name).into());
                }
            }
        }
        field_groups.insert(group_name.clone(), ids);
    }

    Ok(field_groups)
}

pub fn resolve_field_name(name: &str) -> Option<u16> {
    dcgm_field_maps::FIELD_NAME_TO_ID.get(name).copied()
}

pub fn resolve_field_id_to_name(field_id: u16) -> Option<&'static str> {
    dcgm_field_maps::FIELD_ID_TO_NAME.get(&field_id).copied()
}
