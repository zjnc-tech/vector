#include "vector_runtime.h"
#include <stdlib.h>
static void go_vr_i64(vr_value *v, int64_t x) { v->kind = VR_I64; v->payload.integer = x; }
static void go_vr_u64(vr_value *v, uint64_t x) { v->kind = VR_U64; v->payload.unsigned_integer = x; }
static void go_vr_f64(vr_value *v, double x) { v->kind = VR_F64; v->payload.number = x; }
static void go_vr_bool(vr_value *v, uint8_t x) { v->kind = VR_BOOL; v->payload.boolean = x; }
static void go_vr_bytes(vr_value *v, uint32_t kind, vr_slice x) { v->kind = kind; v->payload.bytes = x; }
static void go_vr_array(vr_value *v, vr_value *p, size_t n) { v->kind = VR_ARRAY; v->payload.array = (vr_array){p,n}; }
static void go_vr_object(vr_value *v, vr_object x) { v->kind = VR_OBJECT; v->payload.object = x; }
