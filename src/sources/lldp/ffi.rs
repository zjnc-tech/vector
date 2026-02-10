use std::error::Error as StdError;
use std::ffi::CStr;
use std::fmt;
use std::process::Command;
use std::ptr;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};

/// LLDP consts
pub const LLDPCTL_K_PORT_CHASSIS: u32 = 1208;

/// LLDP error
#[derive(Debug)]
pub enum LldpError {
    ConnectionFailed,
    InterfaceFetchFailed,
    NullPointer(&'static str),
    ThreadJoinFailed,
    LibraryNotAvailable(String),
}

impl fmt::Display for LldpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LldpError::ConnectionFailed => {
                write!(f, "LLDP connection failed")
            }
            LldpError::InterfaceFetchFailed => write!(f, "Failed to fetch network interfaces"),
            LldpError::NullPointer(ctx) => write!(f, "Null pointer encountered in: {}", ctx),
            LldpError::ThreadJoinFailed => write!(f, "Blocking thread error"),
            LldpError::LibraryNotAvailable(msg) => write!(f, "LLDP library not available: {}", msg),
        }
    }
}

impl StdError for LldpError {}

/// LLDP data
#[derive(Debug, Clone)]
pub struct LldpInterface {
    pub name: String,
    pub device_name: String,
}

#[derive(Debug, Clone)]
pub struct LldpNeighbor {
    pub local_interface: String,
    pub local_device: String,
    pub remote_device: String,
    pub remote_port: String,
}

// 动态加载lldp库的函数指针
type LldpctlNewFn = unsafe extern "C" fn(
    send: Option<
        unsafe extern "C" fn(
            *mut std::os::raw::c_void,
            *const u8,
            usize,
            *mut std::os::raw::c_void,
        ) -> isize,
    >,
    recv: Option<
        unsafe extern "C" fn(
            *mut std::os::raw::c_void,
            *const u8,
            usize,
            *mut std::os::raw::c_void,
        ) -> isize,
    >,
    user_data: *mut std::os::raw::c_void,
) -> *mut std::os::raw::c_void;

type LldpctlReleaseFn = unsafe extern "C" fn(*mut std::os::raw::c_void) -> i32;
type LldpctlGetInterfacesFn =
    unsafe extern "C" fn(*mut std::os::raw::c_void) -> *mut std::os::raw::c_void;
type LldpctlAtomIterFn =
    unsafe extern "C" fn(*mut std::os::raw::c_void) -> *mut std::os::raw::c_void;
type LldpctlAtomIterNextFn = unsafe extern "C" fn(
    *mut std::os::raw::c_void,
    *mut std::os::raw::c_void,
) -> *mut std::os::raw::c_void;
type LldpctlAtomIterValueFn = unsafe extern "C" fn(
    *mut std::os::raw::c_void,
    *mut std::os::raw::c_void,
) -> *mut std::os::raw::c_void;
type LldpctlAtomGetStrFn =
    unsafe extern "C" fn(*mut std::os::raw::c_void, u32) -> *const std::os::raw::c_char;
type LldpctlGetPortFn =
    unsafe extern "C" fn(*mut std::os::raw::c_void) -> *mut std::os::raw::c_void;
type LldpctlAtomGetFn =
    unsafe extern "C" fn(*mut std::os::raw::c_void, u32) -> *mut std::os::raw::c_void;
type LldpctlAtomDecRefFn = unsafe extern "C" fn(*mut std::os::raw::c_void);

struct LldpFunctions {
    lldpctl_new: LldpctlNewFn,
    lldpctl_release: LldpctlReleaseFn,
    lldpctl_get_interfaces: LldpctlGetInterfacesFn,
    lldpctl_atom_iter: LldpctlAtomIterFn,
    lldpctl_atom_iter_next: LldpctlAtomIterNextFn,
    lldpctl_atom_iter_value: LldpctlAtomIterValueFn,
    lldpctl_atom_get_str: LldpctlAtomGetStrFn,
    lldpctl_get_port: LldpctlGetPortFn,
    lldpctl_atom_get: LldpctlAtomGetFn,
    lldpctl_atom_dec_ref: LldpctlAtomDecRefFn,
}

static LLDP_AVAILABLE: AtomicBool = AtomicBool::new(true);
static mut LLDP_FUNCTIONS: Option<LldpFunctions> = None;

/// 获取最后一次系统错误消息
fn get_last_error_message() -> String {
    use std::io;
    
    // 尝试获取系统错误信息
    if let Some(err) = io::Error::last_os_error().raw_os_error() {
        match err {
            2 => "No such file or directory".to_string(),
            13 => "Permission denied".to_string(),
            111 => "Connection refused".to_string(),
            _ => format!("System error: {}", err),
        }
    } else {
        // 如果无法获取具体错误码，返回通用消息
        "Unknown connection error".to_string()
    }
}

fn load_lldp_library() -> Result<(), LldpError> {
    static INIT: std::sync::Once = std::sync::Once::new();

    INIT.call_once(|| {
        unsafe {
            // 尝试加载动态库
            let lib_result = libloading::Library::new("liblldpctl.so.4")
                .or_else(|_| libloading::Library::new("liblldpctl.so"))
                .or_else(|_| libloading::Library::new("/usr/lib/x86_64-linux-gnu/liblldpctl.so.4"))
                .or_else(|_| libloading::Library::new("/usr/lib/liblldpctl.so.4"))
                .or_else(|_| libloading::Library::new("/lib/liblldpctl.so.4"));

            match lib_result {
                Ok(lib) => {
                    // 获取函数指针
                    let lldpctl_new: libloading::Symbol<LldpctlNewFn> =
                        lib.get(b"lldpctl_new").unwrap();
                    let lldpctl_release: libloading::Symbol<LldpctlReleaseFn> =
                        lib.get(b"lldpctl_release").unwrap();
                    let lldpctl_get_interfaces: libloading::Symbol<LldpctlGetInterfacesFn> =
                        lib.get(b"lldpctl_get_interfaces").unwrap();
                    let lldpctl_atom_iter: libloading::Symbol<LldpctlAtomIterFn> =
                        lib.get(b"lldpctl_atom_iter").unwrap();
                    let lldpctl_atom_iter_next: libloading::Symbol<LldpctlAtomIterNextFn> =
                        lib.get(b"lldpctl_atom_iter_next").unwrap();
                    let lldpctl_atom_iter_value: libloading::Symbol<LldpctlAtomIterValueFn> =
                        lib.get(b"lldpctl_atom_iter_value").unwrap();
                    let lldpctl_atom_get_str: libloading::Symbol<LldpctlAtomGetStrFn> =
                        lib.get(b"lldpctl_atom_get_str").unwrap();
                    let lldpctl_get_port: libloading::Symbol<LldpctlGetPortFn> =
                        lib.get(b"lldpctl_get_port").unwrap();
                    let lldpctl_atom_get: libloading::Symbol<LldpctlAtomGetFn> =
                        lib.get(b"lldpctl_atom_get").unwrap();
                    let lldpctl_atom_dec_ref: libloading::Symbol<LldpctlAtomDecRefFn> =
                        lib.get(b"lldpctl_atom_dec_ref").unwrap();

                    LLDP_FUNCTIONS = Some(LldpFunctions {
                        lldpctl_new: *lldpctl_new,
                        lldpctl_release: *lldpctl_release,
                        lldpctl_get_interfaces: *lldpctl_get_interfaces,
                        lldpctl_atom_iter: *lldpctl_atom_iter,
                        lldpctl_atom_iter_next: *lldpctl_atom_iter_next,
                        lldpctl_atom_iter_value: *lldpctl_atom_iter_value,
                        lldpctl_atom_get_str: *lldpctl_atom_get_str,
                        lldpctl_get_port: *lldpctl_get_port,
                        lldpctl_atom_get: *lldpctl_atom_get,
                        lldpctl_atom_dec_ref: *lldpctl_atom_dec_ref,
                    });

                    // 保持库的引用，防止被释放
                    std::mem::forget(lib);
                    LLDP_AVAILABLE.store(true, Ordering::Relaxed);
                }
                Err(e) => {
                    LLDP_AVAILABLE.store(false, Ordering::Relaxed);
                    error!("Failed to load LLDP library: {}", e);
                }
            }
        }
    });

    if LLDP_AVAILABLE.load(Ordering::Relaxed) {
        Ok(())
    } else {
        Err(LldpError::LibraryNotAvailable(
            "liblldpctl library not found".to_string(),
        ))
    }
}

/// RAII
struct AtomGuard {
    ptr: *mut std::os::raw::c_void,
}

impl AtomGuard {
    const fn new(ptr: *mut std::os::raw::c_void) -> Self {
        Self { ptr }
    }

    const fn ptr(&self) -> *mut std::os::raw::c_void {
        self.ptr
    }
}

impl Drop for AtomGuard {
    fn drop(&mut self) {
        if LLDP_AVAILABLE.load(Ordering::Relaxed) {
            unsafe {
                if let Some(ref funcs) = LLDP_FUNCTIONS {
                    (funcs.lldpctl_atom_dec_ref)(self.ptr);
                }
            }
        }
    }
}

pub struct LldpHandle {
    conn: *mut std::os::raw::c_void,
}

impl LldpHandle {
    pub fn new() -> Result<Self, LldpError> {
        load_lldp_library()?;

        if !LLDP_AVAILABLE.load(Ordering::Relaxed) {
            return Err(LldpError::LibraryNotAvailable(
                "LLDP library not available".to_string(),
            ));
        }

        unsafe {
            if let Some(ref funcs) = LLDP_FUNCTIONS {
                let conn = (funcs.lldpctl_new)(None, None, ptr::null_mut());
                if conn.is_null() {
                    // 检查是否是socket连接错误
                    let err_msg = get_last_error_message();
                    if err_msg.contains("unable to connect to socket") 
                        || err_msg.contains("No such file or directory") 
                        || err_msg.contains("Connection refused") {
                        warn!("LLDP socket connection failed: {}, falling back to command line mode", err_msg);
                        return Err(LldpError::LibraryNotAvailable(
                            format!("LLDP socket unavailable: {}", err_msg)
                        ));
                    }
                    Err(LldpError::ConnectionFailed)
                } else {
                    Ok(Self { conn })
                }
            } else {
                Err(LldpError::LibraryNotAvailable(
                    "LLDP functions not loaded".to_string(),
                ))
            }
        }
    }

    pub fn get_interfaces(&self) -> Result<Vec<LldpInterface>, LldpError> {
        if !LLDP_AVAILABLE.load(Ordering::Relaxed) {
            return Err(LldpError::LibraryNotAvailable(
                "LLDP library not available".to_string(),
            ));
        }

        unsafe {
            if let Some(ref funcs) = LLDP_FUNCTIONS {
                let interfaces = (funcs.lldpctl_get_interfaces)(self.conn);
                if interfaces.is_null() {
                    // 检查是否是socket连接错误导致的接口获取失败
                    let err_msg = get_last_error_message();
                    if err_msg.contains("unable to connect to socket") 
                        || err_msg.contains("No such file or directory") 
                        || err_msg.contains("Connection refused") {
                        warn!("LLDP socket connection failed when fetching interfaces: {}, falling back to command line mode", err_msg);
                        return Err(LldpError::LibraryNotAvailable(
                            format!("LLDP socket unavailable when fetching interfaces: {}", err_msg)
                        ));
                    }
                    return Err(LldpError::InterfaceFetchFailed);
                }

                let interfaces = AtomGuard::new(interfaces);
                let mut result = Vec::new();

                let mut iter = (funcs.lldpctl_atom_iter)(interfaces.ptr());
                while !iter.is_null() {
                    let interface_atom = (funcs.lldpctl_atom_iter_value)(interfaces.ptr(), iter);
                    iter = (funcs.lldpctl_atom_iter_next)(interfaces.ptr(), iter);
                    if interface_atom.is_null() {
                        continue;
                    }

                    let interface = AtomGuard::new(interface_atom);

                    let name = get_string_property(
                        interface.ptr(),
                        1000, /* lldpctl_key_t_lldpctl_k_interface_name */
                    )
                    .unwrap_or_default();

                    let port_ptr = (funcs.lldpctl_get_port)(interface.ptr());
                    if port_ptr.is_null() {
                        continue;
                    }
                    let port = AtomGuard::new(port_ptr);

                    let chassis_ptr = (funcs.lldpctl_atom_get)(port.ptr(), LLDPCTL_K_PORT_CHASSIS);
                    if chassis_ptr.is_null() {
                        continue;
                    }

                    let chassis = AtomGuard::new(chassis_ptr);
                    let device_name = get_string_property(
                        chassis.ptr(),
                        1803, /* lldpctl_key_t_lldpctl_k_chassis_name */
                    )
                    .unwrap_or_default();

                    result.push(LldpInterface { name, device_name });
                }

                Ok(result)
            } else {
                Err(LldpError::LibraryNotAvailable(
                    "LLDP functions not loaded".to_string(),
                ))
            }
        }
    }

    pub fn get_neighbors(&self) -> Result<Vec<LldpNeighbor>, LldpError> {
        if !LLDP_AVAILABLE.load(Ordering::Relaxed) {
            return Err(LldpError::LibraryNotAvailable(
                "LLDP library not available".to_string(),
            ));
        }

        unsafe {
            if let Some(ref funcs) = LLDP_FUNCTIONS {
                let interface_list = (funcs.lldpctl_get_interfaces)(self.conn);
                if interface_list.is_null() {
                    // 检查是否是socket连接错误导致的接口获取失败
                    let err_msg = get_last_error_message();
                    if err_msg.contains("unable to connect to socket") 
                        || err_msg.contains("No such file or directory") 
                        || err_msg.contains("Connection refused") {
                        warn!("LLDP socket connection failed when fetching neighbors: {}, falling back to command line mode", err_msg);
                        return Err(LldpError::LibraryNotAvailable(
                            format!("LLDP socket unavailable when fetching neighbors: {}", err_msg)
                        ));
                    }
                    return Err(LldpError::InterfaceFetchFailed);
                }
                let interface_list = AtomGuard::new(interface_list);
                let mut result = Vec::new();

                let mut iter = (funcs.lldpctl_atom_iter)(interface_list.ptr());
                while !iter.is_null() {
                    let interface_ptr = (funcs.lldpctl_atom_iter_value)(interface_list.ptr(), iter);
                    iter = (funcs.lldpctl_atom_iter_next)(interface_list.ptr(), iter);

                    if interface_ptr.is_null() {
                        continue;
                    }
                    let interface = AtomGuard::new(interface_ptr);

                    let interface_name = get_string_property(
                        interface.ptr(),
                        1000, /* lldpctl_key_t_lldpctl_k_interface_name */
                    )
                    .unwrap_or_default();

                    let port_ptr = (funcs.lldpctl_get_port)(interface.ptr());
                    if port_ptr.is_null() {
                        continue;
                    }
                    let port = AtomGuard::new(port_ptr);

                    let chassis_ptr = (funcs.lldpctl_atom_get)(port.ptr(), LLDPCTL_K_PORT_CHASSIS);
                    if chassis_ptr.is_null() {
                        continue;
                    }

                    let chassis = AtomGuard::new(chassis_ptr);
                    let local_chassis_name = get_string_property(
                        chassis.ptr(),
                        1803, /* lldpctl_key_t_lldpctl_k_chassis_name */
                    )
                    .unwrap_or_default();

                    // 获取 neighbor 列表
                    let neighbors_ptr = (funcs.lldpctl_atom_get)(
                        port.ptr(),
                        1200 as u32, // lldpctl_key_t_lldpctl_k_port_neighbors
                    );
                    if neighbors_ptr.is_null() {
                        continue;
                    }
                    let neighbors = AtomGuard::new(neighbors_ptr);

                    // 遍历每个 neighbor
                    let mut n_iter = (funcs.lldpctl_atom_iter)(neighbors.ptr());
                    while !n_iter.is_null() {
                        let neighbor_ptr = (funcs.lldpctl_atom_iter_value)(neighbors.ptr(), n_iter);
                        n_iter = (funcs.lldpctl_atom_iter_next)(neighbors.ptr(), n_iter);

                        if neighbor_ptr.is_null() {
                            continue;
                        }

                        let neighbor = AtomGuard::new(neighbor_ptr);

                        let remote_device = get_string_property(
                            neighbor.ptr(),
                            1803, /* lldpctl_key_t_lldpctl_k_chassis_name */
                        )
                        .unwrap_or_default();
                        let remote_port = get_string_property(
                            neighbor.ptr(),
                            1204, /* lldpctl_key_t_lldpctl_k_port_id */
                        )
                        .unwrap_or_default();

                        result.push(LldpNeighbor {
                            local_interface: interface_name.clone(),
                            local_device: local_chassis_name.clone(),
                            remote_device,
                            remote_port,
                        });
                    }
                }

                Ok(result)
            } else {
                Err(LldpError::LibraryNotAvailable(
                    "LLDP functions not loaded".to_string(),
                ))
            }
        }
    }
}

impl Drop for LldpHandle {
    fn drop(&mut self) {
        if LLDP_AVAILABLE.load(Ordering::Relaxed) {
            unsafe {
                if let Some(ref funcs) = LLDP_FUNCTIONS {
                    (funcs.lldpctl_release)(self.conn);
                }
            }
        }
    }
}

fn get_string_property(atom: *mut std::os::raw::c_void, key: u32) -> Result<String, LldpError> {
    if !LLDP_AVAILABLE.load(Ordering::Relaxed) {
        return Err(LldpError::LibraryNotAvailable(
            "LLDP library not available".to_string(),
        ));
    }

    unsafe {
        if let Some(ref funcs) = LLDP_FUNCTIONS {
            let cstr = (funcs.lldpctl_atom_get_str)(atom, key);

            if cstr.is_null() {
                Err(LldpError::NullPointer("get_string_property"))
            } else {
                let value = CStr::from_ptr(cstr).to_string_lossy().into_owned();
                Ok(value)
            }
        } else {
            Err(LldpError::LibraryNotAvailable(
                "LLDP functions not loaded".to_string(),
            ))
        }
    }
}

pub async fn get_lldp_interfaces_async() -> Result<Vec<LldpInterface>, LldpError> {
    // 首先尝试使用liblldpctl库
    let library_result = tokio::task::spawn_blocking(|| {
        let handle = LldpHandle::new()?;
        handle.get_interfaces()
    })
    .await
    .map_err(|_| LldpError::ThreadJoinFailed)?;

    match library_result {
        Ok(interfaces) => Ok(interfaces),
        Err(LldpError::LibraryNotAvailable(_)) => {
            // 如果库不可用，回退到lldptool命令行工具
            warn!("LLDP library not available for interfaces, falling back to lldptool");
            get_lldp_interfaces_via_lldptool().await
        }
        Err(e) => Err(e),
    }
}

pub async fn get_lldp_neighbors_async() -> Result<Vec<LldpNeighbor>, LldpError> {
    // 首先尝试使用liblldpctl库
    let library_result = tokio::task::spawn_blocking(|| {
        let handle = LldpHandle::new()?;
        handle.get_neighbors()
    })
    .await
    .map_err(|_| LldpError::ThreadJoinFailed)?;

    match library_result {
        Ok(neighbors) => Ok(neighbors),
        Err(LldpError::LibraryNotAvailable(_)) => {
            // 如果库不可用，回退到lldptool命令行工具
            warn!("LLDP library not available, falling back to lldptool");
            get_lldp_neighbors_via_lldptool().await
        }
        Err(e) => Err(e),
    }
}

/// 使用lldptool命令行工具获取LLDP接口信息
async fn get_lldp_interfaces_via_lldptool() -> Result<Vec<LldpInterface>, LldpError> {
    tokio::task::spawn_blocking(|| {
        // 首先检查lldptool是否可用
        if !is_lldptool_available() {
            warn!("lldptool not found, using basic interface discovery");
            return get_basic_interfaces();
        }

        let mut all_interfaces = Vec::new();
        let local_device_name = get_local_device_name().unwrap_or_default();

        // 使用lldptool直接获取接口信息
        // 方法1: 尝试使用 lldptool -i any -p 获取所有LLDP接口
        warn!(message = "Executing lldptool -i any -p command");
        let lldptool_result = Command::new("lldptool")
            .args(["-i", "any", "-p"])
            .output();

        match lldptool_result {
            Ok(output) => {
                warn!(message = "lldptool command completed",
                       success = output.status.success(),
                       exit_code = output.status.code().unwrap_or(-1));
                        
                if output.status.success() {
                    let stdout = str::from_utf8(&output.stdout)
                        .map_err(|_| LldpError::LibraryNotAvailable("Invalid UTF-8 in lldptool output".to_string()))?;

                    warn!(message = "lldptool output", raw_output = stdout);
                    
                    // 解析lldptool输出获取接口列表
                    for line in stdout.lines() {
                        warn!(message = "Processing lldptool output line", line = line);
                        let line = line.trim();
                        if line.is_empty() || line.starts_with("Interface") {
                            warn!(message = "Skipping header or empty line");
                            continue;
                        }
                        
                        // 提取接口名称
                        if let Some(interface_name) = extract_interface_from_lldptool_line(line) {
                            debug!(message = "Adding interface to result", interface = &interface_name);
                            all_interfaces.push(LldpInterface {
                                name: interface_name,
                                device_name: local_device_name.clone(),
                            });
                        }
                    }
                    warn!(message = "Processed all lldptool lines", interface_count = all_interfaces.len());
                } else {
                    warn!(message = "lldptool command failed", 
                          stderr = String::from_utf8_lossy(&output.stderr).trim());
                }
            }
            Err(e) => {
                warn!(message = "Failed to execute lldptool command", error = e.to_string());
            }
        }

        // 如果lldptool -p失败或没有找到接口，使用备用方法
        if all_interfaces.is_empty() {
            warn!("No LLDP interfaces found via lldptool -p, using alternative discovery");
            all_interfaces = discover_interfaces_via_lldptool_commands(local_device_name)?;
        }

        Ok(all_interfaces)
    })
    .await
    .map_err(|_| LldpError::ThreadJoinFailed)?
}

/// 使用lldptool命令行工具获取LLDP邻居信息
async fn get_lldp_neighbors_via_lldptool() -> Result<Vec<LldpNeighbor>, LldpError> {
    tokio::task::spawn_blocking(|| {
        // 首先检查lldptool是否可用
        if !is_lldptool_available() {
            warn!("lldptool not found, returning empty neighbor list");
            return Ok(Vec::new());
        }

        let local_device_name = get_local_device_name().unwrap_or_default();
        let mut all_neighbors = Vec::new();

        // 使用lldptool发现接口并获取邻居信息
        let interfaces = discover_interfaces_via_lldptool_commands(local_device_name)?;
        
        for interface_info in interfaces {
            let neighbors = get_lldp_neighbors_for_interface(&interface_info.name)?;
            all_neighbors.extend(neighbors);
        }

        Ok(all_neighbors)
    })
    .await
    .map_err(|_| LldpError::ThreadJoinFailed)?
}

/// 检查lldptool命令是否可用
fn is_lldptool_available() -> bool {
    let check_result = Command::new("which")
        .arg("lldptool")
        .output();
    
    if let Ok(output) = check_result {
        output.status.success()
    } else {
        false
    }
}

/// 获取基本的网络接口信息（当lldptool不可用时）
fn get_basic_interfaces() -> Result<Vec<LldpInterface>, LldpError> {
    let local_device_name = get_local_device_name().unwrap_or_default();
    let mut interfaces = Vec::new();
    
    // 使用常见的接口名称
    let common_interfaces = ["eth0", "enp0s3", "ens33", "wlan0"];
    
    for interface in &common_interfaces {
        // 简单检查接口是否存在（通过尝试读取/proc/net/dev）
        let proc_path = format!("/proc/net/dev");
        if let Ok(content) = std::fs::read_to_string(&proc_path) {
            if content.contains(interface) {
                interfaces.push(LldpInterface {
                    name: interface.to_string(),
                    device_name: local_device_name.clone(),
                });
            }
        }
    }
    
    // 如果没有找到常见接口，至少返回一个默认接口
    if interfaces.is_empty() {
        interfaces.push(LldpInterface {
            name: "unknown".to_string(),
            device_name: local_device_name,
        });
    }
    
    Ok(interfaces)
}

/// 从lldptool输出行中提取接口名称
fn extract_interface_from_lldptool_line(line: &str) -> Option<String> {
    // 记录原始输入用于调试
    debug!(message = "Processing LLDP interface line", raw_line = line);
    
    // lldptool -p 输出格式通常是: "eth0"
    let interface = line.trim();
    
    // 记录处理后的接口名称
    debug!(message = "Trimmed interface name", trimmed_name = interface);
    
    if interface.is_empty() {
        debug!(message = "Skipping empty interface line");
        return None;
    }
    
    // 过滤掉不需要的虚拟接口
    if interface == "lo" {
        debug!(message = "Skipping loopback interface");
        return None;
    }
    
    if interface.starts_with("docker") {
        debug!(message = "Skipping docker interface", interface = interface);
        return None;
    }
    
    if interface.starts_with("veth") {
        debug!(message = "Skipping veth interface", interface = interface);
        return None;
    }
    
    if interface.starts_with("br-") {
        debug!(message = "Skipping bridge interface", interface = interface);
        return None;
    }
    
    // 记录最终接受的接口名称
    debug!(message = "Accepting interface", final_interface = interface);
    Some(interface.to_string())
}

/// 通过lldptool命令发现接口
fn discover_interfaces_via_lldptool_commands(local_device_name: String) -> Result<Vec<LldpInterface>, LldpError> {
    let mut interfaces = Vec::new();
    
    // 方法1: 尝试常见的网络接口名称
    let common_interfaces = ["eth0", "eth1", "enp0s3", "enp0s8", "ens33", "wlan0"];
    
    for interface in &common_interfaces {
        // 检查接口是否存在且启用了LLDP
        let check_result = Command::new("lldptool")
            .args(["-t", "-i", interface])
            .output();
            
        if let Ok(output) = check_result {
            if output.status.success() {
                interfaces.push(LldpInterface {
                    name: interface.to_string(),
                    device_name: local_device_name.clone(),
                });
            }
        }
    }
    
    // 如果还是没有找到接口，至少返回本地设备信息
    if interfaces.is_empty() {
        interfaces.push(LldpInterface {
            name: "unknown".to_string(),
            device_name: local_device_name,
        });
    }
    
    Ok(interfaces)
}

/// 获取单个接口的LLDP邻居信息
fn get_lldp_neighbors_for_interface(interface: &str) -> Result<Vec<LldpNeighbor>, LldpError> {
    let mut neighbors = Vec::new();

    // 首先检查lldptool是否可用
    if !is_lldptool_available() {
        warn!("lldptool not found, skipping LLDP neighbor discovery for interface {}", interface);
        return Ok(neighbors);
    }

    // 执行命令: lldptool -tni <interface>
    let output = Command::new("lldptool")
        .args(["-tni", interface])
        .output()
        .map_err(|e| LldpError::LibraryNotAvailable(format!("Failed to execute lldptool: {}", e)))?;

    if !output.status.success() {
        // lldptool可能未安装或接口不支持LLDP
        warn!("lldptool failed for interface {}: {}", interface,
               String::from_utf8_lossy(&output.stderr));
        return Ok(neighbors);
    }

    let stdout = str::from_utf8(&output.stdout)
        .map_err(|_| LldpError::LibraryNotAvailable("Invalid UTF-8 in lldptool output".to_string()))?;

    // 解析lldptool输出
    let mut current_neighbor: Option<(String, String)> = None; // (remote_device, remote_port)
    
    for line in stdout.lines() {
        let line = line.trim();
        
        // 查找Chassis ID (设备名)
        if line.starts_with("Chassis ID") {
            if let Some(colon_pos) = line.find(':') {
                let chassis_id = line[colon_pos + 1..].trim();
                if let Some((_, existing_port)) = current_neighbor.take() {
                    // 如果已经有端口信息，创建邻居记录
                    neighbors.push(LldpNeighbor {
                        local_interface: interface.to_string(),
                        local_device: get_local_device_name().unwrap_or_default(),
                        remote_device: chassis_id.to_string(),
                        remote_port: existing_port,
                    });
                }
                current_neighbor = Some((chassis_id.to_string(), String::new()));
            }
        }
        // 查找Port ID (端口名)
        else if line.starts_with("Port ID") {
            if let Some(colon_pos) = line.find(':') {
                let port_id = line[colon_pos + 1..].trim();
                if let Some((existing_device, _)) = current_neighbor.take() {
                    // 创建邻居记录
                    neighbors.push(LldpNeighbor {
                        local_interface: interface.to_string(),
                        local_device: get_local_device_name().unwrap_or_default(),
                        remote_device: existing_device,
                        remote_port: port_id.to_string(),
                    });
                } else {
                    // 只有端口信息，暂存
                    current_neighbor = Some((String::new(), port_id.to_string()));
                }
            }
        }
    }
    
    // 处理最后可能剩余的邻居信息
    if let Some((remote_device, remote_port)) = current_neighbor {
        if !remote_device.is_empty() && !remote_port.is_empty() {
            neighbors.push(LldpNeighbor {
                local_interface: interface.to_string(),
                local_device: get_local_device_name().unwrap_or_default(),
                remote_device,
                remote_port,
            });
        }
    }

    Ok(neighbors)
}

/// 获取本地设备名称
fn get_local_device_name() -> Result<String, LldpError> {
    // 使用hostname命令获取设备名
    let output = Command::new("hostname")
        .output()
        .map_err(|e| LldpError::LibraryNotAvailable(format!("Failed to execute hostname: {}", e)))?;

    if !output.status.success() {
        return Ok("unknown".to_string());
    }

    let hostname = str::from_utf8(&output.stdout)
        .map_err(|_| LldpError::LibraryNotAvailable("Invalid UTF-8 in hostname output".to_string()))?
        .trim()
        .to_string();

    Ok(hostname)
}
