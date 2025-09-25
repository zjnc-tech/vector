use crate::sources::dcgm::bindings::*;
use std::ffi::CString;
use std::mem::zeroed;
use std::os::raw::{c_int, c_longlong};
use std::sync::atomic::{AtomicBool, Ordering};

// DCGM函数指针类型定义
type DcgmInitFn = unsafe extern "C" fn() -> dcgmReturn_t;
type DcgmStartEmbeddedFn =
    unsafe extern "C" fn(dcgmOperationMode_t, *mut dcgmHandle_t) -> dcgmReturn_t;
type DcgmGetAllDevicesFn = unsafe extern "C" fn(dcgmHandle_t, *mut u32, *mut c_int) -> dcgmReturn_t;
type DcgmGroupCreateFn = unsafe extern "C" fn(
    dcgmHandle_t,
    dcgmGroupType_t,
    *const i8,
    *mut dcgmGpuGrp_t,
) -> dcgmReturn_t;
type DcgmFieldGroupCreateFn = unsafe extern "C" fn(
    dcgmHandle_t,
    c_int,
    *mut u16,
    *const i8,
    *mut dcgmFieldGrp_t,
) -> dcgmReturn_t;
type DcgmWatchFieldsFn = unsafe extern "C" fn(
    dcgmHandle_t,
    dcgmGpuGrp_t,
    dcgmFieldGrp_t,
    c_longlong,
    f64,
    c_int,
) -> dcgmReturn_t;
type DcgmUpdateAllFieldsFn = unsafe extern "C" fn(dcgmHandle_t, c_int) -> dcgmReturn_t;
type DcgmGetLatestValuesForFieldsFn = unsafe extern "C" fn(
    dcgmHandle_t,
    c_int,
    *mut u16,
    u32,
    *mut dcgmFieldValue_v1,
) -> dcgmReturn_t;

struct DcgmFunctions {
    dcgm_init: DcgmInitFn,
    dcgm_start_embedded: DcgmStartEmbeddedFn,
    dcgm_get_all_devices: DcgmGetAllDevicesFn,
    dcgm_group_create: DcgmGroupCreateFn,
    dcgm_field_group_create: DcgmFieldGroupCreateFn,
    dcgm_watch_fields: DcgmWatchFieldsFn,
    dcgm_update_all_fields: DcgmUpdateAllFieldsFn,
    dcgm_get_latest_values_for_fields: DcgmGetLatestValuesForFieldsFn,
}

static DCGM_AVAILABLE: AtomicBool = AtomicBool::new(true);
static mut DCGM_FUNCTIONS: Option<DcgmFunctions> = None;

// 加载DCGM库
fn load_dcgm_library() -> Result<(), i32> {
    static INIT: std::sync::Once = std::sync::Once::new();

    INIT.call_once(|| {
        unsafe {
            // 尝试加载动态库
            let lib_result = libloading::Library::new("libdcgm.so.2")
                .or_else(|_| libloading::Library::new("libdcgm.so"))
                .or_else(|_| libloading::Library::new("/usr/lib/x86_64-linux-gnu/libdcgm.so.2"))
                .or_else(|_| libloading::Library::new("/usr/lib/libdcgm.so.2"))
                .or_else(|_| libloading::Library::new("/lib/libdcgm.so.2"));

            match lib_result {
                Ok(lib) => {
                    // 获取函数指针
                    let dcgm_init: libloading::Symbol<DcgmInitFn> = lib.get(b"dcgmInit").unwrap();
                    let dcgm_start_embedded: libloading::Symbol<DcgmStartEmbeddedFn> =
                        lib.get(b"dcgmStartEmbedded").unwrap();
                    let dcgm_get_all_devices: libloading::Symbol<DcgmGetAllDevicesFn> =
                        lib.get(b"dcgmGetAllDevices").unwrap();
                    let dcgm_group_create: libloading::Symbol<DcgmGroupCreateFn> =
                        lib.get(b"dcgmGroupCreate").unwrap();
                    let dcgm_field_group_create: libloading::Symbol<DcgmFieldGroupCreateFn> =
                        lib.get(b"dcgmFieldGroupCreate").unwrap();
                    let dcgm_watch_fields: libloading::Symbol<DcgmWatchFieldsFn> =
                        lib.get(b"dcgmWatchFields").unwrap();
                    let dcgm_update_all_fields: libloading::Symbol<DcgmUpdateAllFieldsFn> =
                        lib.get(b"dcgmUpdateAllFields").unwrap();
                    let dcgm_get_latest_values_for_fields: libloading::Symbol<
                        DcgmGetLatestValuesForFieldsFn,
                    > = lib.get(b"dcgmGetLatestValuesForFields").unwrap();

                    DCGM_FUNCTIONS = Some(DcgmFunctions {
                        dcgm_init: *dcgm_init,
                        dcgm_start_embedded: *dcgm_start_embedded,
                        dcgm_get_all_devices: *dcgm_get_all_devices,
                        dcgm_group_create: *dcgm_group_create,
                        dcgm_field_group_create: *dcgm_field_group_create,
                        dcgm_watch_fields: *dcgm_watch_fields,
                        dcgm_update_all_fields: *dcgm_update_all_fields,
                        dcgm_get_latest_values_for_fields: *dcgm_get_latest_values_for_fields,
                    });

                    // 保持库的引用，防止被释放
                    std::mem::forget(lib);
                    DCGM_AVAILABLE.store(true, Ordering::Relaxed);
                }
                Err(e) => {
                    DCGM_AVAILABLE.store(false, Ordering::Relaxed);
                    error!("Failed to load DCGM library: {}", e);
                }
            }
        }
    });

    if DCGM_AVAILABLE.load(Ordering::Relaxed) {
        Ok(())
    } else {
        Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND)
    }
}

pub fn init_dcgm() -> Result<dcgmHandle_t, i32> {
    load_dcgm_library()?;

    if !DCGM_AVAILABLE.load(Ordering::Relaxed) {
        return Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND);
    }

    let mut handle: dcgmHandle_t = 0;

    unsafe {
        if let Some(ref funcs) = DCGM_FUNCTIONS {
            let ret = (funcs.dcgm_init)();
            if ret != dcgmReturn_enum_DCGM_ST_OK {
                return Err(ret);
            }

            let ret = (funcs.dcgm_start_embedded)(
                dcgmOperationMode_enum_DCGM_OPERATION_MODE_MANUAL,
                &mut handle,
            );
            if ret != dcgmReturn_enum_DCGM_ST_OK {
                return Err(ret);
            }

            Ok(handle)
        } else {
            Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND)
        }
    }
}

pub fn register_fields(
    handle: dcgmHandle_t,
    field_ids: &mut [u16],
    group_name: &str,
    field_group_name: &str,
    update_freq_sec: u64,
) -> Result<(dcgmGpuGrp_t, dcgmFieldGrp_t), i32> {
    if !DCGM_AVAILABLE.load(Ordering::Relaxed) {
        return Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND);
    }

    let mut field_group_id: dcgmFieldGrp_t = 0;

    // 检查 group_name 是否为空
    if group_name.is_empty() {
        warn!("Group name is empty");
        return Err(dcgmReturn_enum_DCGM_ST_BADPARAM);
    }

    // 检查 field_group_name 是否为空
    if field_group_name.is_empty() {
        warn!("Field group name is empty");
        return Err(dcgmReturn_enum_DCGM_ST_BADPARAM);
    }

    let group_name_cstr = match CString::new(group_name) {
        Ok(cstr) => cstr,
        Err(_) => {
            warn!("Group name contains null bytes: {}", group_name);
            return Err(dcgmReturn_enum_DCGM_ST_BADPARAM);
        }
    };

    let field_group_cstr = match CString::new(field_group_name) {
        Ok(cstr) => cstr,
        Err(_) => {
            warn!("Field group name contains null bytes: {}", field_group_name);
            return Err(dcgmReturn_enum_DCGM_ST_BADPARAM);
        }
    };

    // 检查字段 ID 列表是否为空
    if field_ids.is_empty() {
        warn!("Field IDs list is empty");
        return Err(dcgmReturn_enum_DCGM_ST_BADPARAM);
    }

    let update_freq_usec = update_freq_sec * 1_000_000;

    unsafe {
        if let Some(ref funcs) = DCGM_FUNCTIONS {
            // 创建默认 GPU 分组
            let mut group_id: dcgmGpuGrp_t = 0;
            let ret = (funcs.dcgm_group_create)(
                handle,
                dcgmGroupType_enum_DCGM_GROUP_DEFAULT,
                group_name_cstr.as_ptr(),
                &mut group_id,
            );
            if ret != dcgmReturn_enum_DCGM_ST_OK {
                warn!("dcgmGroupCreate failed: {} for group '{}'", ret, group_name);
                // 添加更多调试信息
                match ret {
                    dcgmReturn_enum_DCGM_ST_BADPARAM => {
                        warn!("DCGM_ST_BADPARAM: Bad parameter passed to function");
                        warn!("  handle: {:?}", handle);
                        warn!("  group_type: {}", dcgmGroupType_enum_DCGM_GROUP_DEFAULT);
                        warn!("  group_name: {:?}", group_name_cstr);
                        warn!("  group_id_ptr: {:?}", &mut group_id);
                    }
                    dcgmReturn_enum_DCGM_ST_INIT_ERROR => {
                        warn!("DCGM_ST_INIT_ERROR: DCGM has not been initialized")
                    }
                    dcgmReturn_enum_DCGM_ST_NOT_SUPPORTED => {
                        warn!("DCGM_ST_NOT_SUPPORTED: Function not supported")
                    }
                    dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND => {
                        warn!("DCGM_ST_LIBRARY_NOT_FOUND: DCGM library not found")
                    }
                    _ => warn!("Other DCGM error: {}", ret),
                }
                return Err(ret);
            }

            // 创建字段组
            let ret = (funcs.dcgm_field_group_create)(
                handle,
                field_ids.len() as c_int, // 确保类型正确
                field_ids.as_mut_ptr(),
                field_group_cstr.as_ptr(),
                &mut field_group_id,
            );
            if ret != dcgmReturn_enum_DCGM_ST_OK {
                warn!(
                    "dcgmFieldGroupCreate failed: {} for field group '{}'",
                    ret, field_group_name
                );
                return Err(ret);
            }

            // Watch 字段
            let watch_ret = (funcs.dcgm_watch_fields)(
                handle,
                group_id,
                field_group_id,
                update_freq_usec as c_longlong,
                600.0, // 10分钟超时
                1,     // 1表示立即返回，而不是等待第一次数据收集
            );

            if watch_ret != dcgmReturn_enum_DCGM_ST_OK {
                warn!("dcgmWatchFields failed: {}", watch_ret);
                return Err(watch_ret);
            }

            Ok((group_id, field_group_id))
        } else {
            warn!("DCGM functions not loaded");
            Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND)
        }
    }
}

pub fn list_all_gpus(handle: dcgmHandle_t) -> Result<Vec<u32>, i32> {
    if !DCGM_AVAILABLE.load(Ordering::Relaxed) {
        return Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND);
    }

    const MAX_GPU_COUNT: usize = 32;
    let mut gpu_ids = [0u32; MAX_GPU_COUNT];
    let mut count: c_int = 0;

    unsafe {
        if let Some(ref funcs) = DCGM_FUNCTIONS {
            let ret = (funcs.dcgm_get_all_devices)(handle, gpu_ids.as_mut_ptr(), &mut count);
            if ret != dcgmReturn_enum_DCGM_ST_OK {
                return Err(ret);
            }

            Ok(gpu_ids[..count as usize].to_vec())
        } else {
            Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND)
        }
    }
}

pub fn collect_metrics_by_fields(
    handle: dcgmHandle_t,
    field_ids: &[u16],
) -> Result<Vec<(u32, Vec<dcgmFieldValue_v1>)>, i32> {
    if !DCGM_AVAILABLE.load(Ordering::Relaxed) {
        return Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND);
    }

    unsafe {
        if let Some(ref funcs) = DCGM_FUNCTIONS {
            let update_ret = (funcs.dcgm_update_all_fields)(handle, 1);
            if update_ret != dcgmReturn_enum_DCGM_ST_OK {
                warn!("Failed to update all fields: {}", update_ret);
                return Err(update_ret);
            }

            let gpu_ids = list_all_gpus(handle)?;
            let mut results = Vec::new();

            for &gpu_id in &gpu_ids {
                let mut group_result = Vec::new();

                for &fid in field_ids {
                    let mut field_value: dcgmFieldValue_v1 = zeroed();

                    let ret = (funcs.dcgm_get_latest_values_for_fields)(
                        handle,
                        gpu_id as i32,
                        &fid as *const u16 as *mut u16,
                        1,
                        &mut field_value,
                    );

                    match (ret, field_value.status) {
                        (ret, status) if ret == dcgmReturn_enum_DCGM_ST_OK => {
                            if status == dcgmReturn_enum_DCGM_ST_NO_DATA {
                                warn!(
                                    "No data for field {} on GPU {}, status: {}",
                                    fid, gpu_id, status
                                );
                            } else if status != dcgmReturn_enum_DCGM_ST_OK {
                                warn!(
                                    "Field {} on GPU {} has non-OK status: {}",
                                    fid, gpu_id, status
                                );
                            }

                            group_result.push(field_value);
                        }
                        (err, status) => {
                            warn!(
                                "Failed to collect field {} for GPU {}: dcgm_ret = {}, field_status = {}",
                                fid, gpu_id, err, status
                            );
                        }
                    }
                }

                if !group_result.is_empty() {
                    results.push((gpu_id, group_result));
                }
            }

            Ok(results)
        } else {
            Err(dcgmReturn_enum_DCGM_ST_LIBRARY_NOT_FOUND)
        }
    }
}
