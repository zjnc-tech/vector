package main

import (
	"log"
	vector "vector-runtime-go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() (err error) {
	r, err := vector.Start(`sources:
  input:
    type: external
sinks:
  output:
    type: console
    inputs: [input]
    encoding:
      codec: json
`, nil)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := r.Close(); err == nil {
			err = closeErr
		}
	}()
	source, err := r.Source("input")
	if err != nil {
		return err
	}
	defer source.Close()
	if err = source.Emit(vector.Event{"pid": 42, "size": 4096}); err != nil {
		return err
	}
	if err = source.EmitBatch([]vector.Event{{"pid": 42, "size": 8192}, {"pid": 43, "size": 16}}); err != nil {
		return err
	}
	return r.Flush()
}
