use std::error::Error as StdError;
use std::ffi::CStr;
use std::fmt;
use std::ptr;
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
    tokio::task::spawn_blocking(|| {
        let handle = LldpHandle::new()?;
        handle.get_interfaces()
    })
    .await
    .map_err(|_| LldpError::ThreadJoinFailed)?
}

pub async fn get_lldp_neighbors_async() -> Result<Vec<LldpNeighbor>, LldpError> {
    tokio::task::spawn_blocking(|| {
        let handle = LldpHandle::new()?;
        handle.get_neighbors()
    })
    .await
    .map_err(|_| LldpError::ThreadJoinFailed)?
}
