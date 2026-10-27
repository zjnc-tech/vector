//! C ABI for the synchronous runtime. See include/vector_runtime.h for the
//! pointer, lifetime, concurrency and ownership contract of every exported call.
#![allow(clippy::missing_safety_doc)]

use std::{
    cell::RefCell,
    ffi::{c_char, CString},
    panic::{catch_unwind, AssertUnwindSafe},
    ptr,
    sync::{Mutex, MutexGuard},
    time::Duration,
};
use vector_runtime::{
    event::ObjectMap, BlockingSourceHandle, Error, ExternalSendError, LogEvent, OwnedRuntime,
    Pipeline, Status, TryEmitError, Value, MAX_BATCH_EVENTS,
};

const OK: i32 = 0;
const INVALID: i32 = 1;
const CONFIG: i32 = 2;
const UNKNOWN_SOURCE: i32 = 3;
const CLOSED: i32 = 4;
const FULL: i32 = 5;
const TIMEOUT: i32 = 6;
const DELIVERY: i32 = 7;
const INTERNAL: i32 = 8;
const PANIC: i32 = 9;
type Result<T> = std::result::Result<T, Failure>;

struct Failure(i32, String);
fn invalid(message: impl Into<String>) -> Failure {
    Failure(INVALID, message.into())
}
impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        let code = match &error {
            Error::Config(_) => CONFIG,
            Error::UnknownSource(_) => UNKNOWN_SOURCE,
            Error::Encode(_) | Error::Send(ExternalSendError::TooLarge) => INVALID,
            Error::Send(ExternalSendError::Closed) => CLOSED,
            Error::Timeout(_) => TIMEOUT,
            Error::Delivery => DELIVERY,
            _ => INTERNAL,
        };
        Self(code, error.to_string())
    }
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn boundary(action: impl FnOnce() -> Result<()>) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(action));
    let (code, message) = match result {
        Ok(Ok(())) => (OK, String::new()),
        Ok(Err(Failure(code, message))) => (code, message),
        Err(_) => (PANIC, "Rust panic caught at C ABI boundary".into()),
    };
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = CString::new(message.replace('\0', "\\0")).unwrap();
    });
    code
}

#[no_mangle]
pub extern "C" fn vr_abi_version() -> u32 {
    1
}

#[no_mangle]
pub extern "C" fn vr_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ptr())
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct VrSlice {
    pub data: *const u8,
    pub len: usize,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VrArray {
    pub data: *const VrValue,
    pub len: usize,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VrObject {
    pub data: *const VrField,
    pub len: usize,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub union VrPayload {
    pub integer: i64,
    pub unsigned_integer: u64,
    pub number: f64,
    pub boolean: u8,
    pub bytes: VrSlice,
    pub array: VrArray,
    pub object: VrObject,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VrValue {
    pub kind: u32,
    pub payload: VrPayload,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VrField {
    pub key: VrSlice,
    pub value: VrValue,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct VrOptions {
    pub worker_threads: u32,
    pub shutdown_timeout_ms: u64,
    pub flush_timeout_ms: u64,
}

struct State {
    runtime: Option<OwnedRuntime>,
    terminal: u32,
}
pub struct VrRuntime {
    state: Mutex<State>,
}
pub struct VrSource {
    source: BlockingSourceHandle,
}

// These functions cannot validate dangling pointers or claimed allocation lengths.
// Such memory validity is a caller obligation, as with other C APIs.
unsafe fn slice<'a, T>(data: *const T, len: usize) -> Result<&'a [T]> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null()
        || (data as usize) % std::mem::align_of::<T>() != 0
        || len > isize::MAX as usize / std::mem::size_of::<T>()
    {
        return Err(invalid("null, misaligned or oversized input buffer"));
    }
    Ok(std::slice::from_raw_parts(data, len))
}
unsafe fn text<'a>(input: VrSlice) -> Result<&'a str> {
    std::str::from_utf8(slice(input.data, input.len)?).map_err(|_| invalid("expected UTF-8 text"))
}
unsafe fn reference<'a, T>(input: *const T) -> Result<&'a T> {
    slice(input, 1).map(|items| &items[0])
}
unsafe fn output<T>(out: *mut T, value: T) -> Result<()> {
    if out.is_null() || (out as usize) % std::mem::align_of::<T>() != 0 {
        return Err(invalid("null or misaligned output pointer"));
    }
    ptr::write(out, value);
    Ok(())
}
fn lock(runtime: &VrRuntime) -> Result<MutexGuard<'_, State>> {
    runtime
        .state
        .lock()
        .map_err(|_| Failure(INTERNAL, "runtime lock poisoned".into()))
}
fn active(state: &State) -> Result<&OwnedRuntime> {
    state
        .runtime
        .as_ref()
        .ok_or_else(|| Failure(CLOSED, "runtime is closed".into()))
}

unsafe fn object(input: VrObject, depth: usize) -> Result<Value> {
    if depth > 64 {
        return Err(invalid(
            "event nesting exceeds 64 levels (possibly a cycle)",
        ));
    }
    let mut fields = ObjectMap::new();
    for field in slice(input.data, input.len)? {
        let key = text(field.key)?;
        if fields
            .insert(key.into(), value(&field.value, depth + 1)?)
            .is_some()
        {
            return Err(invalid("duplicate object key"));
        }
    }
    Ok(Value::Object(fields))
}
unsafe fn value(input: &VrValue, depth: usize) -> Result<Value> {
    if depth > 64 {
        return Err(invalid(
            "event nesting exceeds 64 levels (possibly a cycle)",
        ));
    }
    match input.kind {
        0 => Ok(Value::Null),
        1 => match input.payload.boolean {
            0 => Ok(Value::Boolean(false)),
            1 => Ok(Value::Boolean(true)),
            _ => Err(invalid("boolean must be 0 or 1")),
        },
        2 => Ok(Value::Integer(input.payload.integer)),
        3 => i64::try_from(input.payload.unsigned_integer)
            .map(Value::Integer)
            .map_err(|_| invalid("unsigned integer exceeds INT64_MAX")),
        4 => {
            let number = input.payload.number;
            if !number.is_finite() {
                return Err(invalid("float must be finite"));
            }
            Ok(Value::from(number))
        }
        5 => Ok(Value::from(text(input.payload.bytes)?.to_owned())),
        6 => {
            let bytes = input.payload.bytes;
            Ok(Value::Bytes(slice(bytes.data, bytes.len)?.to_vec().into()))
        }
        7 => {
            let array = input.payload.array;
            slice(array.data, array.len)?
                .iter()
                .map(|item| value(item, depth + 1))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        }
        8 => object(input.payload.object, depth),
        _ => Err(invalid("unknown value kind")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn vr_options_default(out: *mut VrOptions) -> i32 {
    boundary(|| {
        output(
            out,
            VrOptions {
                worker_threads: 2,
                shutdown_timeout_ms: 60_000,
                flush_timeout_ms: 30_000,
            },
        )
    })
}

#[no_mangle]
pub unsafe extern "C" fn vr_runtime_start(
    yaml: VrSlice,
    options: *const VrOptions,
    out: *mut *mut VrRuntime,
) -> i32 {
    boundary(|| {
        output(out, ptr::null_mut())?;
        let mut pipeline = Pipeline::from_yaml(text(yaml)?)?;
        let workers = if options.is_null() {
            2
        } else {
            let options = reference(options)?;
            if options.worker_threads == 0 {
                return Err(invalid("worker_threads must be positive"));
            }
            pipeline = pipeline
                .with_shutdown_timeout(Duration::from_millis(options.shutdown_timeout_ms))
                .with_flush_timeout(Duration::from_millis(options.flush_timeout_ms));
            options.worker_threads as usize
        };
        let runtime = pipeline.start_owned_with_workers(workers)?;
        output(
            out,
            Box::into_raw(Box::new(VrRuntime {
                state: Mutex::new(State {
                    runtime: Some(runtime),
                    terminal: 0,
                }),
            })),
        )
    })
}

#[no_mangle]
pub unsafe extern "C" fn vr_runtime_source(
    runtime: *const VrRuntime,
    name: VrSlice,
    out: *mut *mut VrSource,
) -> i32 {
    boundary(|| {
        output(out, ptr::null_mut())?;
        let state = lock(reference(runtime)?)?;
        let source = active(&state)?.source(text(name)?)?;
        output(out, Box::into_raw(Box::new(VrSource { source })))
    })
}

#[no_mangle]
pub unsafe extern "C" fn vr_source_emit_json(source: *const VrSource, json: VrSlice) -> i32 {
    boundary(|| {
        let mut source = reference(source)?.source.clone();
        source.emit_json(text(json)?)?;
        Ok(())
    })
}

unsafe fn emit(
    source: *const VrSource,
    events: *const VrObject,
    count: usize,
    mode: u32,
) -> Result<()> {
    if mode > 1 {
        return Err(invalid("mode must be VR_BLOCKING or VR_TRY"));
    }
    if count > MAX_BATCH_EVENTS {
        return Err(invalid("batch exceeds 1000 events"));
    }
    let mut source = reference(source)?.source.clone();
    let logs = slice(events, count)?
        .iter()
        .map(|event| object(*event, 0).map(LogEvent::from))
        .collect::<Result<Vec<_>>>()?;
    if mode == 0 {
        source.emit_batch(logs)?;
    } else {
        source.try_emit_batch(logs).map_err(|error| match error {
            TryEmitError::Full(_) => Failure(
                FULL,
                "source queue is full; retry the original input".into(),
            ),
            TryEmitError::Closed(_) => Failure(CLOSED, "source is closed".into()),
            TryEmitError::TooLarge(_) => invalid("batch exceeds 1000 events"),
            TryEmitError::UnknownSource(name, _) => Failure(UNKNOWN_SOURCE, name),
        })?;
    }
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn vr_source_emit(
    source: *const VrSource,
    event: VrObject,
    mode: u32,
) -> i32 {
    boundary(|| emit(source, &event, 1, mode))
}
#[no_mangle]
pub unsafe extern "C" fn vr_source_emit_batch(
    source: *const VrSource,
    events: *const VrObject,
    count: usize,
    mode: u32,
) -> i32 {
    boundary(|| emit(source, events, count, mode))
}
#[no_mangle]
pub unsafe extern "C" fn vr_runtime_flush(runtime: *const VrRuntime) -> i32 {
    boundary(|| {
        let state = lock(reference(runtime)?)?;
        active(&state)?.flush()?;
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn vr_runtime_status(runtime: *const VrRuntime, out: *mut u32) -> i32 {
    boundary(|| {
        let state = lock(reference(runtime)?)?;
        let status =
            state
                .runtime
                .as_ref()
                .map_or(state.terminal, |runtime| match runtime.status() {
                    Status::Running => 0,
                    Status::Stopping => 1,
                    Status::Stopped => 2,
                    Status::Failed(_) => 3,
                });
        output(out, status)
    })
}

#[no_mangle]
pub unsafe extern "C" fn vr_runtime_stop(runtime: *const VrRuntime) -> i32 {
    boundary(|| {
        let runtime_ref = reference(runtime)?;
        let owner = {
            let mut state = lock(runtime_ref)?;
            match state.runtime.take() {
                Some(owner) => {
                    state.terminal = 1;
                    owner
                }
                None if state.terminal == 2 => return Ok(()),
                None => {
                    return Err(Failure(
                        CLOSED,
                        "runtime stopping or shutdown previously failed".into(),
                    ))
                }
            }
        };
        let result = owner.stop();
        lock(runtime_ref)?.terminal = if result.is_ok() { 2 } else { 3 };
        result.map_err(Into::into)
    })
}

#[no_mangle]
pub unsafe extern "C" fn vr_source_destroy(source: *mut VrSource) -> i32 {
    boundary(|| {
        if !source.is_null() {
            reference(source)?;
            drop(Box::from_raw(source));
        }
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn vr_runtime_destroy(runtime: *mut VrRuntime) -> i32 {
    boundary(|| {
        if !runtime.is_null() {
            reference(runtime)?;
            drop(Box::from_raw(runtime));
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn handles_are_thread_safe() {
        fn check<T: Send + Sync>() {}
        check::<VrSource>();
        check::<VrRuntime>();
    }
    #[test]
    fn boundary_contains_panics_and_clears_errors() {
        assert_eq!(boundary(|| panic!("test panic")), PANIC);
        assert!(!unsafe { std::ffi::CStr::from_ptr(vr_last_error()) }
            .to_bytes()
            .is_empty());
        assert_eq!(boundary(|| Ok(())), OK);
        assert!(unsafe { std::ffi::CStr::from_ptr(vr_last_error()) }
            .to_bytes()
            .is_empty());
    }
}
