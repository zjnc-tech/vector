package vectorruntime

/*
#include "bridge.h"
*/
import "C"
import (
	"fmt"
	"reflect"
	"unsafe"
)

// All trees live in C memory: no C allocation ever contains a Go pointer.
type arena struct{ pointers []unsafe.Pointer }

func (a *arena) alloc(n, size int) unsafe.Pointer {
	if n == 0 {
		return nil
	}
	p := C.calloc(C.size_t(n), C.size_t(size))
	if p == nil {
		panic("vector runtime: C allocation failed")
	}
	a.pointers = append(a.pointers, p)
	return p
}
func (a *arena) free() {
	for _, p := range a.pointers {
		C.free(p)
	}
}
func (a *arena) bytes(b []byte) C.vr_slice {
	p := a.alloc(len(b), 1)
	copy(unsafe.Slice((*byte)(p), len(b)), b)
	return C.vr_slice{data: (*C.uint8_t)(p), len: C.size_t(len(b))}
}
func (a *arena) object(event Event, depth int) (C.vr_object, error) {
	return a.mapValue(reflect.ValueOf(event), depth)
}
func (a *arena) mapValue(v reflect.Value, depth int) (C.vr_object, error) {
	if depth > 64 {
		return C.vr_object{}, invalid("event nesting exceeds 64 levels (possibly cyclic input)")
	}
	if v.Type().Key().Kind() != reflect.String {
		return C.vr_object{}, invalid("object keys must be strings")
	}
	p := (*C.vr_field)(a.alloc(v.Len(), C.sizeof_vr_field))
	fields := unsafe.Slice(p, v.Len())
	it := v.MapRange()
	i := 0
	for it.Next() {
		fields[i].key = a.bytes([]byte(it.Key().String()))
		if err := a.value(&fields[i].value, it.Value(), depth+1); err != nil {
			return C.vr_object{}, err
		}
		i++
	}
	return C.vr_object{data: p, len: C.size_t(v.Len())}, nil
}
func (a *arena) value(out *C.vr_value, v reflect.Value, depth int) error {
	if depth > 64 {
		return invalid("event nesting exceeds 64 levels (possibly cyclic input)")
	}
	for v.IsValid() && v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if !v.IsValid() {
		return nil
	}
	switch v.Kind() {
	case reflect.Bool:
		var b C.uint8_t
		if v.Bool() {
			b = 1
		}
		C.go_vr_bool(out, b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		C.go_vr_i64(out, C.int64_t(v.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		C.go_vr_u64(out, C.uint64_t(v.Uint()))
	case reflect.Float32, reflect.Float64:
		C.go_vr_f64(out, C.double(v.Float()))
	case reflect.String:
		C.go_vr_bytes(out, C.VR_STRING, a.bytes([]byte(v.String())))
	case reflect.Map:
		obj, err := a.mapValue(v, depth)
		if err != nil {
			return err
		}
		C.go_vr_object(out, obj)
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			C.go_vr_bytes(out, C.VR_BYTES, a.bytes(v.Bytes()))
			return nil
		}
		p := (*C.vr_value)(a.alloc(v.Len(), C.sizeof_vr_value))
		values := unsafe.Slice(p, v.Len())
		for i := range values {
			if err := a.value(&values[i], v.Index(i), depth+1); err != nil {
				return err
			}
		}
		C.go_vr_array(out, p, C.size_t(v.Len()))
	default:
		return invalid(fmt.Sprintf("unsupported event value type: %s", v.Type()))
	}
	return nil
}
