use chrono::Utc;
use std::collections::HashMap;
use std::ffi::CStr;
use std::time::Duration;

use crate::sources::dcgm::bindings::*;
use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::metric::{Metric, MetricKind, MetricTags, MetricValue},
};
use vector_lib::configurable::configurable_component;

#[allow(
    unused_imports,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    clippy::all,
    clippy::trivially_copy_pass_by_ref,
    clippy::missing_const_for_fn
)]
mod bindings;
mod collector;
mod dcgm_field_maps;

#[allow(warnings, clippy::pedantic, clippy::nursery)]
pub(crate) mod v1alpha1 {
    include!(concat!(env!("OUT_DIR"), "/v1alpha1.rs"));
}

pub mod pod_resource_mapper;
mod proto_gen;

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

        let mut field_groups = resolve_all_fields(&self.groups)?.clone();

        // 为每个字段组添加DCGM_FI_DEV_UUID
        for fields in field_groups.values_mut() {
            // 添加DCGM_FI_DEV_UUID，避免重复添加
            if !fields.contains(&(DCGM_FI_DEV_UUID as u16)) {
                fields.push(DCGM_FI_DEV_UUID as u16);
            }
            if !fields.contains(&(DCGM_FI_DEV_NAME as u16)) {
                fields.push(DCGM_FI_DEV_NAME as u16);
            }
        }

        Ok(Box::pin(async move {
            let handle = collector::init_dcgm().map_err(|e| {
                error!("DCGM init failed: {}", e);
            })?;

            let pod_mapper = pod_resource_mapper::PodResourcesMapper::new();
            let refresher = pod_resource_mapper::PodResourcesRefresher::new(pod_mapper.clone());
            tokio::spawn(async move {
                refresher.run().await;
            });

            let mut join_handles = vec![];

            for (group_name, gconf) in groups {
                let mut field_ids = field_groups.get(&group_name).unwrap().clone();
                let mut out = out.clone();
                let shutdown = shutdown.clone();
                let pod_mapper = pod_mapper.clone();

                let join = tokio::spawn(async move {
                    let interval =
                        tokio::time::interval(Duration::from_secs(gconf.scrape_interval_secs));

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
                                        let metrics = map_metrics(group_name.clone(), data, &pod_mapper);
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
    pod_mapper: &pod_resource_mapper::PodResourcesMapper,
) -> Vec<Metric> {
    let now = Utc::now();
    let mut metrics = Vec::new();

    for (gpu_id, field_values) in data {
        let mut common_tags = MetricTags::default();
        common_tags.insert("gpu_id".into(), gpu_id.to_string());
        common_tags.insert("group".into(), group_name.clone());

        let mut gpu_uuid: Option<String> = None;

        // 提取 UUID、model_name 和字符串字段
        for field in &field_values {
            if let FieldType::String = FieldType::from(field.fieldType) {
                let fid = field.fieldId;
                let cstr = unsafe { CStr::from_ptr(field.value.str_.as_ptr()) };
                let str_val = match cstr.to_str() {
                    Ok(v) => v,
                    Err(_) => {
                        warn!("Invalid UTF-8 for field {} on GPU {}", fid, gpu_id);
                        continue;
                    }
                };

                match fid {
                    id if id == DCGM_FI_DEV_UUID as u16 => {
                        gpu_uuid = Some(str_val.to_string());
                        common_tags.insert("UUID".to_string(), str_val.to_string());
                    }
                    id if id == DCGM_FI_DEV_NAME as u16 => {
                        common_tags.insert("model_name".to_string(), str_val.to_string());
                    }
                    _ => {
                        let key = resolve_field_id_to_name(fid).unwrap_or("unknown_field");
                        common_tags.insert(key.to_string(), str_val.to_string());
                    }
                }
            }
        }

        // 通过 UUID 查询 Pod 信息
        if let Some(uuid) = gpu_uuid.as_ref() {
            if let Some(pod_info) = pod_mapper.get(uuid) {
                common_tags.insert("namespace".into(), pod_info.namespace.clone());
                common_tags.insert("pod".into(), pod_info.pod.clone());
                common_tags.insert("container".into(), pod_info.container.clone());
            }
        }

        // 处理数值字段
        for field in &field_values {
            let fid = field.fieldId;
            let metric_name = resolve_field_id_to_name(fid).unwrap_or("unknown_metric");

            let value = match FieldType::from(field.fieldType) {
                FieldType::Double => Some(unsafe { field.value.dbl }),
                FieldType::Int64 | FieldType::Timestamp => Some(unsafe { field.value.i64_ as f64 }),
                FieldType::Binary => {
                    warn!("Binary field {} not supported", fid);
                    None
                }
                FieldType::String => None,
                FieldType::Unknown(t) => {
                    warn!("Unknown field type {} for field {}", t, fid);
                    None
                }
            };

            if let Some(val) = value {
                metrics.push(
                    Metric::new(
                        metric_name.to_string(),
                        MetricKind::Absolute,
                        MetricValue::Gauge { value: val },
                    )
                    .with_timestamp(Some(now))
                    .with_tags(Some(common_tags.clone())),
                );
            }
        }
    }

    metrics
}

enum FieldType {
    String,
    Double,
    Int64,
    Timestamp,
    Binary,
    Unknown(u16),
}

impl From<u16> for FieldType {
    fn from(v: u16) -> Self {
        match v {
            x if x == DCGM_FT_STRING as u16 => FieldType::String,
            x if x == DCGM_FT_DOUBLE as u16 => FieldType::Double,
            x if x == DCGM_FT_INT64 as u16 => FieldType::Int64,
            x if x == DCGM_FT_TIMESTAMP as u16 => FieldType::Timestamp,
            x if x == DCGM_FT_BINARY as u16 => FieldType::Binary,
            other => FieldType::Unknown(other),
        }
    }
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
