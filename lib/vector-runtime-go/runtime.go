// Package vectorruntime embeds Vector through its C ABI. Calls are synchronous.
// Call Close explicitly to drain a runtime before process exit.
package vectorruntime

/*
#cgo LDFLAGS: -lvector_runtime_c
#include "bridge.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

const MaxBatchEvents = int(C.VR_MAX_BATCH_EVENTS)

// Code is a stable C ABI error code and can be used with errors.Is.
type Code int32

const (
	ErrInvalidArgument Code = C.VR_INVALID_ARGUMENT
	ErrConfig          Code = C.VR_CONFIG_ERROR
	ErrUnknownSource   Code = C.VR_UNKNOWN_SOURCE
	ErrClosed          Code = C.VR_CLOSED
	ErrQueueFull       Code = C.VR_QUEUE_FULL
	ErrTimeout         Code = C.VR_TIMEOUT
	ErrDelivery        Code = C.VR_DELIVERY_ERROR
	ErrInternal        Code = C.VR_INTERNAL_ERROR
	ErrPanic           Code = C.VR_PANIC
)

func (c Code) Error() string { return fmt.Sprintf("vector runtime error %d", c) }

// Error carries the native diagnostic and machine-readable code.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string     { return e.Message }
func (e *Error) Unwrap() error     { return e.Code }
func invalid(message string) error { return &Error{ErrInvalidArgument, message} }

// Pin the goroutine so the diagnostic is copied from the calling OS thread.
func call(fn func() C.vr_result) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if code := fn(); code != C.VR_OK {
		return &Error{Code(code), C.GoString(C.vr_last_error())}
	}
	return nil
}

type Status uint32

const (
	Running  Status = C.VR_RUNNING
	Stopping Status = C.VR_STOPPING
	Stopped  Status = C.VR_STOPPED
	Failed   Status = C.VR_FAILED
)

// Options configures the private Tokio runtime. Timeouts have millisecond resolution.
type Options struct {
	WorkerThreads                 uint32
	ShutdownTimeout, FlushTimeout time.Duration
}

func DefaultOptions() Options { return Options{2, 60 * time.Second, 30 * time.Second} }

type runtimeState struct {
	mu  sync.RWMutex
	ptr *C.vr_runtime
}

// Runtime is safe for concurrent use. Copies share ownership of the same handle.
type Runtime struct{ state *runtimeState }
type sourceState struct {
	mu  sync.RWMutex
	ptr *C.vr_source
}

// Source is a reusable, concurrent source handle. It does not keep Runtime alive.
type Source struct{ state *sourceState }

// Start parses YAML and starts a runtime. nil options selects defaults.
func Start(yaml string, options *Options) (*Runtime, error) {
	if C.vr_abi_version() != C.VR_ABI_VERSION {
		return nil, &Error{ErrInternal, "incompatible Vector C ABI"}
	}
	opts := DefaultOptions()
	if options != nil {
		opts = *options
	}
	if opts.WorkerThreads == 0 || opts.ShutdownTimeout < 0 || opts.FlushTimeout < 0 {
		return nil, invalid("worker threads must be positive and timeouts nonnegative")
	}
	native := C.vr_options{worker_threads: C.uint32_t(opts.WorkerThreads), shutdown_timeout_ms: C.uint64_t(opts.ShutdownTimeout / time.Millisecond), flush_timeout_ms: C.uint64_t(opts.FlushTimeout / time.Millisecond)}
	var a arena
	defer a.free()
	input := a.bytes([]byte(yaml))
	s := &runtimeState{}
	if err := call(func() C.vr_result { return C.vr_runtime_start(input, &native, &s.ptr) }); err != nil {
		return nil, err
	}
	runtime.SetFinalizer(s, func(s *runtimeState) { C.vr_runtime_destroy(s.ptr) })
	return &Runtime{s}, nil
}
func (r *Runtime) with(fn func(*C.vr_runtime) C.vr_result) error {
	if r == nil || r.state == nil {
		return ErrClosed
	}
	s := r.state
	s.mu.RLock()
	defer s.mu.RUnlock()
	defer runtime.KeepAlive(s)
	if s.ptr == nil {
		return ErrClosed
	}
	return call(func() C.vr_result { return fn(s.ptr) })
}
func (r *Runtime) Source(name string) (*Source, error) {
	var a arena
	defer a.free()
	input := a.bytes([]byte(name))
	s := &sourceState{}
	err := r.with(func(p *C.vr_runtime) C.vr_result { return C.vr_runtime_source(p, input, &s.ptr) })
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(s, func(s *sourceState) { C.vr_source_destroy(s.ptr) })
	return &Source{s}, nil
}
func (r *Runtime) Flush() error {
	return r.with(func(p *C.vr_runtime) C.vr_result { return C.vr_runtime_flush(p) })
}

// Stop drains the pipeline, retaining the handle for status queries. Successful Stop is idempotent.
func (r *Runtime) Stop() error {
	return r.with(func(p *C.vr_runtime) C.vr_result { return C.vr_runtime_stop(p) })
}
func (r *Runtime) Status() (Status, error) {
	var status C.uint32_t
	err := r.with(func(p *C.vr_runtime) C.vr_result { return C.vr_runtime_status(p, &status) })
	return Status(status), err
}

// Close stops and frees the runtime, even if draining fails. It is idempotent.
func (r *Runtime) Close() error {
	if r == nil || r.state == nil {
		return nil
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		return nil
	}
	err := call(func() C.vr_result { return C.vr_runtime_stop(s.ptr) })
	cleanup := call(func() C.vr_result { return C.vr_runtime_destroy(s.ptr) })
	s.ptr = nil
	runtime.SetFinalizer(s, nil)
	if err != nil {
		return err
	}
	return cleanup
}
func (s *Source) with(fn func(*C.vr_source) C.vr_result) error {
	if s == nil || s.state == nil {
		return ErrClosed
	}
	st := s.state
	st.mu.RLock()
	defer st.mu.RUnlock()
	defer runtime.KeepAlive(st)
	if st.ptr == nil {
		return ErrClosed
	}
	return call(func() C.vr_result { return fn(st.ptr) })
}

// Close frees this source handle; it does not stop the pipeline.
func (s *Source) Close() error {
	if s == nil || s.state == nil {
		return nil
	}
	st := s.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.ptr == nil {
		return nil
	}
	err := call(func() C.vr_result { return C.vr_source_destroy(st.ptr) })
	st.ptr = nil
	runtime.SetFinalizer(st, nil)
	return err
}

// Event is an object with literal string keys (dots are not paths).
type Event = map[string]any

func (s *Source) Emit(event Event) error            { return s.emitBatch([]Event{event}, C.VR_BLOCKING) }
func (s *Source) TryEmit(event Event) error         { return s.emitBatch([]Event{event}, C.VR_TRY) }
func (s *Source) EmitBatch(events []Event) error    { return s.emitBatch(events, C.VR_BLOCKING) }
func (s *Source) TryEmitBatch(events []Event) error { return s.emitBatch(events, C.VR_TRY) }
func (s *Source) EmitJSON(json string) error {
	var a arena
	defer a.free()
	input := a.bytes([]byte(json))
	return s.with(func(p *C.vr_source) C.vr_result { return C.vr_source_emit_json(p, input) })
}
func (s *Source) emitBatch(events []Event, mode C.uint32_t) error {
	if len(events) > MaxBatchEvents {
		return invalid("batch exceeds MaxBatchEvents")
	}
	var a arena
	defer a.free()
	ptr := (*C.vr_object)(a.alloc(len(events), C.sizeof_vr_object))
	objects := unsafe.Slice(ptr, len(events))
	for i, event := range events {
		obj, err := a.object(event, 0)
		if err != nil {
			return err
		}
		objects[i] = obj
	}
	return s.with(func(p *C.vr_source) C.vr_result { return C.vr_source_emit_batch(p, ptr, C.size_t(len(events)), mode) })
}
func (r *Runtime) send(name string, fn func(*Source) error) error {
	s, err := r.Source(name)
	if err != nil {
		return err
	}
	defer s.Close()
	return fn(s)
}
func (r *Runtime) Emit(name string, event Event) error {
	return r.send(name, func(s *Source) error { return s.Emit(event) })
}
func (r *Runtime) TryEmit(name string, event Event) error {
	return r.send(name, func(s *Source) error { return s.TryEmit(event) })
}
func (r *Runtime) EmitBatch(name string, events []Event) error {
	return r.send(name, func(s *Source) error { return s.EmitBatch(events) })
}
func (r *Runtime) TryEmitBatch(name string, events []Event) error {
	return r.send(name, func(s *Source) error { return s.TryEmitBatch(events) })
}
func (r *Runtime) EmitJSON(name, json string) error {
	return r.send(name, func(s *Source) error { return s.EmitJSON(json) })
}
