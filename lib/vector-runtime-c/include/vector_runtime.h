#ifndef VECTOR_RUNTIME_H
#define VECTOR_RUNTIME_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ABI v1. All status codes and tags have fixed-width representations. */
#define VR_ABI_VERSION 1u
#define VR_MAX_BATCH_EVENTS 1000u
typedef int32_t vr_result;
enum {
    VR_OK = 0, VR_INVALID_ARGUMENT = 1, VR_CONFIG_ERROR = 2,
    VR_UNKNOWN_SOURCE = 3, VR_CLOSED = 4, VR_QUEUE_FULL = 5,
    VR_TIMEOUT = 6, VR_DELIVERY_ERROR = 7, VR_INTERNAL_ERROR = 8,
    VR_PANIC = 9
};
enum { VR_RUNNING = 0, VR_STOPPING = 1, VR_STOPPED = 2, VR_FAILED = 3 };
enum { VR_BLOCKING = 0, VR_TRY = 1 };
enum {
    VR_NULL = 0, VR_BOOL = 1, VR_I64 = 2, VR_U64 = 3, VR_F64 = 4,
    VR_STRING = 5, VR_BYTES = 6, VR_ARRAY = 7, VR_OBJECT = 8
};

typedef struct vr_runtime vr_runtime;
typedef struct vr_source vr_source;
typedef struct vr_value vr_value;
typedef struct vr_field vr_field;

/* Borrowed buffers, not NUL-terminated. NULL is allowed only with len == 0.
 * YAML, source names, keys and VR_STRING must be UTF-8. VR_BYTES is arbitrary.
 * All buffers, nested values and pointer outputs must be valid, aligned and
 * remain unchanged throughout the call. No pointer is retained after return.
 * Handles must originate from this library; dangling pointers are undefined
 * behavior. Input/output buffers must not overlap or alias library handles.
 */
typedef struct { const uint8_t *data; size_t len; } vr_slice;
typedef struct { const vr_value *data; size_t len; } vr_array;
typedef struct { const vr_field *data; size_t len; } vr_object;
typedef union {
    int64_t integer;
    uint64_t unsigned_integer;
    double number;
    uint8_t boolean;
    vr_slice bytes; /* VR_STRING and VR_BYTES */
    vr_array array;
    vr_object object;
} vr_payload;
struct vr_value { uint32_t kind; vr_payload payload; };
struct vr_field { vr_slice key; vr_value value; };

/* Use vr_options_default, then override individual fields. NULL options at
 * start uses defaults. worker_threads > 0; timeouts are milliseconds (0 allowed).
 * Do not add fields to this v1 struct; future ABI layouts require new symbols.
 */
typedef struct {
    uint32_t worker_threads;
    uint64_t shutdown_timeout_ms;
    uint64_t flush_timeout_ms;
} vr_options;

uint32_t vr_abi_version(void);
/* Thread-local UTF-8 diagnostic, owned by the library. Do not free or modify.
 * Valid until the next vr_result-returning call on the SAME thread, or thread
 * exit. Successful calls clear it. vr_last_error/vr_abi_version do not clear it.
 * Copy the message before cleanup calls if it needs to be retained.
 */
const char *vr_last_error(void);
vr_result vr_options_default(vr_options *out);

/* Creates and starts a runtime owning its Tokio worker threads. *out is set to
 * NULL before parsing; on success the caller owns it and must destroy it once.
 * All out pointers below are required. This API uses blocking OS-thread calls;
 * do not call blocking operations from an entered Rust Tokio runtime.
 */
vr_result vr_runtime_start(vr_slice yaml, const vr_options *options, vr_runtime **out);
vr_result vr_runtime_source(const vr_runtime *runtime, vr_slice name, vr_source **out);

/* Resolve a source once and reuse it. Multiple threads may call emit on the
 * same source handle. Each call copies the payload into native Vector values;
 * the caller retains ownership on BOTH success and failure, including FULL.
 * Native fields avoid JSON serialization/parsing. Keys containing '.' are
 * literal. Object keys must be unique; integers fit int64_t (including U64);
 * floats must be finite; bool is 0/1; nesting is limited to 64 levels.
 * Only the union member matching kind is read. NULL reads no payload.
 * JSON must be an object; this convenience function uses blocking admission.
 */
vr_result vr_source_emit_json(const vr_source *source, vr_slice json);
vr_result vr_source_emit(const vr_source *source, vr_object event, uint32_t mode);
/* Up to VR_MAX_BATCH_EVENTS. All events are converted before any are queued.
 * mode: BLOCKING waits for queue capacity (not delivery); TRY returns FULL
 * without waiting for capacity, but still validates and copies the input.
 * An empty batch is a no-op and does not probe whether the source is closed.
 */
vr_result vr_source_emit_batch(const vr_source *source, const vr_object *events,
                               size_t count, uint32_t mode);

/* Wait for finalization of earlier submissions. Does not force aggregation
 * windows or guarantee remote disk durability. Delivery errors are sticky.
 * Runtime operations are synchronized; stop may wait for a concurrent flush
 * (bounded by flush_timeout_ms). Emit can proceed during flush.
 */
vr_result vr_runtime_flush(const vr_runtime *runtime);
vr_result vr_runtime_status(const vr_runtime *runtime, uint32_t *out);
/* Drain and stop; successful stop is idempotent. Retained sources then return
 * CLOSED. A concurrent stop already in progress returns CLOSED. A stop failure
 * leaves the runtime terminal; it cannot be restarted. Stop does not free it.
 */
vr_result vr_runtime_stop(const vr_runtime *runtime);

/* NULL is a no-op. Destroy each handle exactly once; destruction must not race
 * ANY call using that handle. Source handles can outlive the runtime handle.
 * Call stop before runtime_destroy for deterministic draining. Destroy without
 * stop requests background shutdown and does not wait for delivery.
 * Keep the shared library loaded for the process lifetime after first use;
 * dynamic unload (dlclose), fork with live runtimes, and signal-handler calls
 * are unsupported. Rust unwinding panics become VR_PANIC; invalid C memory and
 * process aborts (including allocation failure) cannot be caught.
 */
vr_result vr_source_destroy(vr_source *source);
vr_result vr_runtime_destroy(vr_runtime *runtime);

#ifdef __cplusplus
}
#endif
#endif
