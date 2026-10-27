"""Integration tests against an installed wheel and a local ClickHouse mock."""
import concurrent.futures
import dataclasses
import http.server
import json
import queue
import threading
import unittest

from vector_runtime import (
    Pipeline, external_event, ClosedError, DeliveryError, QueueFullError,
    MAX_BATCH_EVENTS,
)


class Mock:
    def __init__(self, status=200, blocked=False):
        self.received = queue.Queue()
        self.gate = threading.Event()
        if not blocked:
            self.gate.set()
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                if self.headers.get("Transfer-Encoding") == "chunked":
                    chunks = []
                    while True:
                        count = int(self.rfile.readline().split(b";")[0], 16)
                        if not count:
                            self.rfile.readline()
                            break
                        chunks.append(self.rfile.read(count))
                        self.rfile.read(2)
                    body = b"".join(chunks)
                else:
                    body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                for line in body.splitlines():
                    outer.received.put(json.loads(line))
                outer.gate.wait(10)
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def yaml(self):
        return """
healthchecks: {enabled: false}
sources:
  input: {type: external}
transforms:
  mark:
    type: remap
    inputs: [input]
    source: '.checked = true'
sinks:
  out:
    type: clickhouse
    inputs: [mark]
    endpoint: http://127.0.0.1:%d
    table: events
    compression: none
    batch: {max_events: 1, timeout_secs: 0.01}
""" % self.server.server_port

    def close(self):
        self.gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


class RuntimeTests(unittest.TestCase):
    def mock(self, **kwargs):
        mock = Mock(**kwargs)
        self.addCleanup(mock.close)
        return mock

    def runtime(self, mock, **kwargs):
        runtime = Pipeline.from_yaml(mock.yaml(), **kwargs).start()

        def cleanup():
            mock.gate.set()
            try:
                runtime.stop()
            except (ClosedError, DeliveryError, TimeoutError):
                pass
        self.addCleanup(cleanup)
        return runtime

    def test_dict_dataclass_batch_and_context(self):
        mock = self.mock()

        @external_event
        @dataclasses.dataclass(frozen=True)
        class Allocation:
            pid: int = dataclasses.field(metadata={"external_name": "process.pid"})
            size: int = 0

        with Pipeline(mock.yaml()).start() as runtime:
            source = runtime.source("input")
            event = {"pid": 1, "ok": True, "absent": None, "bytes": b"hello",
                     "min": -(2**63), "max": 2**63-1,
                     "values": [1, 2.5, "x"], "object": {"nested": "yes"}}
            source.emit(event)
            source.emit(Allocation(3, 4096))
            source.emit_batch([{"pid": 4}, Allocation(5, 8192)])
            runtime.emit_json("input", '{"pid":6}')
            runtime.try_emit("input", {"pid": 7})
            source.try_emit_batch([{"pid": 8}])
            runtime.flush()
            rows = [mock.received.get(timeout=5) for _ in range(7)]
            native = next(row for row in rows if row.get("pid") == 1)
            self.assertEqual(native["values"], event["values"])
            self.assertEqual(native["min"], -(2**63))
            self.assertEqual(native["max"], 2**63-1)
            self.assertIs(native["ok"], True)
            self.assertEqual(native["bytes"], "hello")
            self.assertNotIn("checked", event)  # Caller input is never mutated.
            self.assertEqual(sum("process.pid" in row for row in rows), 2)
            self.assertTrue(all(row["checked"] for row in rows))
            self.assertEqual(runtime.status, "running")
            self.assertIsInstance(runtime.warnings, list)
        self.assertEqual(runtime.status, "stopped")
        runtime.close()  # Idempotent.
        with self.assertRaises(ClosedError):
            source.emit({"pid": 9})

    def test_validation_and_atomic_batch_conversion(self):
        mock = self.mock()
        runtime = self.runtime(mock)
        invalid = [(OverflowError, {"n": 2**63}),
                   (OverflowError, {"n": -(2**63)-1}),
                   (ValueError, {"n": float("nan")}),
                   (ValueError, {"n": float("inf")}),
                   (TypeError, {1: "bad key"}),
                   (TypeError, {"x": object()}), (TypeError, [1, 2])]
        for expected, value in invalid:
            with self.subTest(value=value), self.assertRaises(expected):
                runtime.emit("input", value)
        cyclic = {}
        cyclic["self"] = cyclic
        with self.assertRaises(ValueError):
            runtime.emit("input", cyclic)
        with self.assertRaises(TypeError):
            runtime.emit_batch("input", [{"valid": True}, {"bad": object()}])
        with self.assertRaises(ValueError):
            runtime.emit_batch("input", [{}] * (MAX_BATCH_EVENTS + 1))
        with self.assertRaises(KeyError):
            runtime.emit("missing", {})
        runtime.flush()
        self.assertTrue(mock.received.empty())

    def test_gil_release_flush_and_concurrent_producers(self):
        mock = self.mock(blocked=True)
        runtime = self.runtime(mock, flush_timeout=3)
        runtime.emit("input", {"pid": 1})
        mock.received.get(timeout=5)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            flushed = pool.submit(runtime.flush)
            # The Python HTTP handler and this timer both need the GIL. A binding
            # holding it during flush would time out before either can run.
            timer = threading.Timer(0.05, mock.gate.set)
            timer.start()
            flushed.result(timeout=5)
            timer.join()
            source = runtime.source("input")
            sends = [pool.submit(source.emit, {"pid": n}) for n in range(20)]
            for sent in sends:
                sent.result(timeout=5)
        runtime.flush()
        self.assertEqual(mock.received.qsize(), 20)

    def test_delivery_failure(self):
        mock = self.mock(status=400)
        runtime = self.runtime(mock)
        runtime.emit("input", {"pid": 1})
        with self.assertRaises(DeliveryError):
            runtime.flush()
        with self.assertRaises(DeliveryError):
            runtime.stop()
        self.assertEqual(runtime.status, "failed")

    def test_deadlines_and_closed_handles(self):
        mock = self.mock(blocked=True)
        runtime = self.runtime(mock, flush_timeout=0.05, shutdown_timeout=0.05)
        source = runtime.source("input")
        source.emit({"pid": 1})
        mock.received.get(timeout=5)
        with self.assertRaises(TimeoutError):
            runtime.flush()
        with self.assertRaises(TimeoutError):
            runtime.stop()
        with self.assertRaises(ClosedError):
            source.try_emit({"pid": 2})

    def test_configuration_one_shot_and_decorator(self):
        with self.assertRaises(ValueError):
            Pipeline("sources: {input: {type: stdin}}")
        for kwargs in ({"worker_threads": 0}, {"flush_timeout": float("nan")}, {"shutdown_timeout": -1}):
            with self.assertRaises(ValueError):
                Pipeline("", **kwargs)
        mock = self.mock()
        pipeline = Pipeline(mock.yaml())
        runtime = pipeline.start()
        runtime.stop()
        with self.assertRaises(ClosedError):
            pipeline.start()

        @external_event
        class Base:
            pid: int

        @external_event
        class Child(Base):
            size: int
        self.assertEqual(Child.__vector_event_fields__, (("pid", "pid"), ("size", "size")))
        with self.assertRaises(ValueError):
            @external_event
            class Duplicate:
                first: int
                second: int = dataclasses.field(metadata={"external_name": "first"})

    def test_context_preserves_original_exception(self):
        mock = self.mock(status=400)
        with self.assertRaisesRegex(ValueError, "original error"):
            with Pipeline(mock.yaml()).start() as runtime:
                runtime.emit("input", {"pid": 1})
                raise ValueError("original error")

    def test_backpressure_preserves_python_input(self):
        mock = self.mock(blocked=True)
        runtime = self.runtime(mock, shutdown_timeout=0.1)
        source = runtime.source("input")
        events = [{"pid": n} for n in range(MAX_BATCH_EVENTS)]
        for _ in range(100):
            try:
                source.try_emit_batch(events)
            except QueueFullError:
                break
        else:
            self.fail("bounded ingress never applied backpressure")
        self.assertEqual(events[0], {"pid": 0})
        self.assertEqual(len(events), MAX_BATCH_EVENTS)
        mock.gate.set()


if __name__ == "__main__":
    unittest.main()
