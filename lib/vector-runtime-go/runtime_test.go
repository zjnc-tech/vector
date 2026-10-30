package vectorruntime

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

const testYAML = `sources:
  input:
    type: external
transforms:
  check:
    type: remap
    inputs: [input]
    source: '.checked = true'
sinks:
  output:
    type: console
    inputs: [check]
    encoding:
      codec: json
`

func require(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func TestDelivery(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeHelper$")
	cmd.Env = append(os.Environ(), "VECTOR_GO_TEST_HELPER=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	count := 0
	seen := map[string]bool{}
	for _, line := range strings.Split(string(output), "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var event map[string]any
		require(t, json.Unmarshal([]byte(line), &event))
		count++
		if event["checked"] != true {
			t.Fatalf("missing transform: %s", line)
		}
		if id, ok := event["id"].(string); ok {
			seen[id] = true
			if id == "native" {
				nested := event["nested"].(map[string]any)
				if nested["literal.key"] != "value" || event["blob"] != "raw" || event["nul"] != "a\x00b" {
					t.Fatal(event)
				}
				values := event["values"].([]any)
				if len(values) != 5 || values[0] != float64(-42) || values[1] != 1.25 || values[2] != "hello" || values[3] != true || values[4] != nil {
					t.Fatal(values)
				}
				if !strings.Contains(line, `"max":9223372036854775807`) || !strings.Contains(line, `"min":-9223372036854775808`) {
					t.Fatal(line)
				}
			}
		}
	}
	if count != 44 || !seen["native"] || !seen["json"] || !seen["batch1"] || !seen["batch2"] || seen["invalid-batch"] {
		t.Fatalf("count=%d seen=%v output=%s", count, seen, output)
	}
}
func TestNativeHelper(t *testing.T) {
	if os.Getenv("VECTOR_GO_TEST_HELPER") != "1" {
		t.Skip("subprocess integration helper")
	}
	r, err := Start(testYAML, nil)
	require(t, err)
	defer r.Close()
	s, err := r.Source("input")
	require(t, err)
	defer s.Close()
	status, err := r.Status()
	require(t, err)
	if status != Running {
		t.Fatal(status)
	}
	_, err = r.Source("missing")
	if !errors.Is(err, ErrUnknownSource) {
		t.Fatal(err)
	}
	require(t, s.Emit(Event{"id": "native", "min": int64(math.MinInt64), "max": uint64(math.MaxInt64), "blob": []byte("raw"), "nul": "a\x00b", "nested": map[string]string{"literal.key": "value"}, "values": []any{-42, 1.25, "hello", true, nil}}))
	require(t, r.EmitJSON("input", `{"id":"json"}`))
	require(t, r.TryEmitBatch("input", []Event{{"id": "batch1"}, {"id": "batch2"}}))
	cycle := Event{}
	cycle["self"] = cycle
	for _, event := range []Event{{"bad": uint64(math.MaxUint64)}, {"bad": math.NaN()}, {"bad": math.Inf(1)}, {"bad": make(chan int)}, {"bad": "\xff"}, {"bad": map[int]int{1: 2}}, cycle} {
		if err := s.EmitBatch([]Event{{"id": "invalid-batch"}, event}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("expected invalid argument, got %v", err)
		}
	}
	if err := s.EmitBatch(make([]Event, MaxBatchEvents+1)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := s.EmitJSON("[]"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := s.Emit(Event{"producer": i, "seq": j}); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	require(t, r.Flush())
	require(t, r.Stop())
	require(t, r.Stop())
	status, err = r.Status()
	require(t, err)
	if status != Stopped {
		t.Fatal(status)
	}
	if err := s.Emit(Event{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	require(t, r.Close())
	require(t, r.Close())
	if err := s.TryEmit(Event{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	require(t, s.Close())
	require(t, s.Close())
}
func TestErrors(t *testing.T) {
	if _, err := Start("[", nil); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	} else {
		var native *Error
		if !errors.As(err, &native) || native.Message == "" {
			t.Fatal(err)
		}
	}
	for _, opts := range []Options{{}, {1, -time.Second, time.Second}, {1, time.Second, -time.Second}} {
		if _, err := Start(testYAML, &opts); !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
	var r Runtime
	require(t, r.Close())
	if err := r.Flush(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	var s Source
	require(t, s.Close())
	if err := s.Emit(Event{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestConcurrentClose(t *testing.T) {
	r, err := Start(testYAML, nil)
	require(t, err)
	defer r.Close()
	s, err := r.Source("input")
	require(t, err)
	defer s.Close()
	copySource := *s
	copyRuntime := *r
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				err := s.Emit(Event{"concurrent": true})
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Error(err)
				}
			}
		}()
	}
	require(t, copyRuntime.Close())
	require(t, copySource.Close())
	wg.Wait()
	if err := r.Flush(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
