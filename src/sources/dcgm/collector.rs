use crate::sources::dcgm::bindings::*;
use std::ffi::CString;
use std::mem::zeroed;
use std::os::raw::{c_int, c_longlong};

pub fn init_dcgm() -> Result<dcgmHandle_t, i32> {
    let mut handle: dcgmHandle_t = 0;

    let ret = unsafe { dcgmInit() };
    if ret != 0 {
        return Err(ret);
    }

    let ret = unsafe {
        dcgmStartEmbedded(
            dcgmOperationMode_enum_DCGM_OPERATION_MODE_MANUAL,
            &mut handle,
        )
    };
    if ret != 0 {
        return Err(ret);
    }

    Ok(handle)
}

pub fn register_fields(
    handle: dcgmHandle_t,
    field_ids: &mut [u16],
    group_name: &str,
    field_group_name: &str,
    update_freq_usec: u64,
) -> Result<(dcgmGpuGrp_t, dcgmFieldGrp_t), i32> {
    let mut field_group_id: dcgmFieldGrp_t = 0;

    let group_name_cstr = CString::new(group_name).unwrap();
    let field_group_cstr = CString::new(field_group_name).unwrap();

    // 创建默认 GPU 分组
    let mut group_id: dcgmGpuGrp_t = 0;
    let ret = unsafe {
        dcgmGroupCreate(
            handle,
            dcgmGroupType_enum_DCGM_GROUP_DEFAULT,
            group_name_cstr.as_ptr(),
            &mut group_id,
        )
    };
    if ret != dcgmReturn_enum_DCGM_ST_OK {
        eprintln!("dcgmGroupCreate failed: {}", ret);
        return Err(ret);
    }

    // 创建字段组
    let ret = unsafe {
        dcgmFieldGroupCreate(
            handle,
            field_ids.len() as i32,
            field_ids.as_mut_ptr(),
            field_group_cstr.as_ptr(),
            &mut field_group_id,
        )
    };
    if ret != dcgmReturn_enum_DCGM_ST_OK {
        eprintln!("dcgmFieldGroupCreate failed: {}", ret);
        return Err(ret);
    }

    // Watch 字段
    let watch_ret = unsafe {
        dcgmWatchFields(
            handle,
            group_id,
            field_group_id,
            update_freq_usec as c_longlong,
            600.0,
            600 as c_int,
        )
    };

    if watch_ret != dcgmReturn_enum_DCGM_ST_OK {
        eprintln!("dcgmWatchFields failed: {}", watch_ret);
        return Err(watch_ret);
    }

    Ok((group_id, field_group_id))
}

pub fn list_all_gpus(handle: dcgmHandle_t) -> Result<Vec<u32>, i32> {
    const MAX_GPU_COUNT: usize = 32;
    let mut gpu_ids = [0u32; MAX_GPU_COUNT];
    let mut count: i32 = 0;

    let ret = unsafe { dcgmGetAllDevices(handle, gpu_ids.as_mut_ptr(), &mut count) };
    if ret != 0 {
        return Err(ret);
    }

    Ok(gpu_ids[..count as usize].to_vec())
}

pub fn collect_metrics_by_fields(
    handle: dcgmHandle_t,
    field_ids: &[u16],
) -> Result<Vec<(u32, Vec<dcgmFieldValue_v1>)>, i32> {
    let update_ret = unsafe { dcgmUpdateAllFields(handle, 1) };
    if update_ret != 0 {
        eprintln!("Failed to update all fields: {}", update_ret);
        return Err(update_ret);
    }

    let gpu_ids = list_all_gpus(handle)?;
    let mut results = Vec::new();

    for &gpu_id in &gpu_ids {
        let mut group_result = Vec::new();

        for &fid in field_ids {
            let mut field_value: dcgmFieldValue_v1 = unsafe { zeroed() };

            let ret = unsafe {
                dcgmGetLatestValuesForFields(
                    handle,
                    gpu_id as i32,
                    &fid as *const u16 as *mut u16,
                    1,
                    &mut field_value,
                )
            };

            if ret != 0 || field_value.status != 0 {
                eprintln!(
                    "Failed to collect field {} for GPU {}: dcgm_ret = {}, field_status = {}",
                    fid, gpu_id, ret, field_value.status
                );
                continue;
            }

            group_result.push(field_value);
        }

        if !group_result.is_empty() {
            results.push((gpu_id, group_result));
        }
    }

    Ok(results)
}