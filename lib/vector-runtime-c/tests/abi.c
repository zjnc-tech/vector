#include "vector_runtime.h"
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

static vr_slice text(const char *s) {
    vr_slice result = {(const uint8_t *)s, strlen(s)};
    return result;
}
static void ok(vr_result result) {
    if (result != VR_OK) fprintf(stderr, "C ABI error %d: %s\n", result, vr_last_error());
    assert(result == VR_OK);
}
static void *check_tls(void *unused) {
    (void)unused;
    assert(strlen(vr_last_error()) == 0);
    assert(vr_runtime_status(NULL, NULL) == VR_INVALID_ARGUMENT);
    assert(strlen(vr_last_error()) > 0);
    ok(vr_source_destroy(NULL));
    assert(strlen(vr_last_error()) == 0);
    return NULL;
}
typedef struct { vr_source *source; int64_t id; } producer;
static void *produce(void *arg) {
    producer *p = arg;
    for (int64_t i = 0; i < 10; ++i) {
        vr_field fields[] = {
            {text("id"), {.kind = VR_STRING, .payload.bytes = text("thread")}},
            {text("producer"), {.kind = VR_I64, .payload.integer = p->id}},
            {text("seq"), {.kind = VR_I64, .payload.integer = i}},
        };
        ok(vr_source_emit(p->source, (vr_object){fields, 3}, VR_BLOCKING));
    }
    return NULL;
}

int main(void) {
    assert(vr_abi_version() == VR_ABI_VERSION);
    assert(vr_options_default(NULL) == VR_INVALID_ARGUMENT);
    char saved[256];
    snprintf(saved, sizeof(saved), "%s", vr_last_error());
    pthread_t thread;
    assert(pthread_create(&thread, NULL, check_tls, NULL) == 0);
    assert(pthread_join(thread, NULL) == 0);
    assert(strcmp(saved, vr_last_error()) == 0);

    vr_options options;
    ok(vr_options_default(&options));
    assert(options.worker_threads == 2 && options.flush_timeout_ms == 30000);
    vr_runtime *runtime = NULL;
    vr_slice bad = {(const uint8_t *)"\xff", 1};
    assert(vr_runtime_start(bad, NULL, &runtime) == VR_INVALID_ARGUMENT);
    assert(runtime == NULL);
    assert(vr_runtime_start(text("["), NULL, &runtime) == VR_CONFIG_ERROR);
    assert(runtime == NULL);
    const char *yaml =
        "sources:\n  input:\n    type: external\n"
        "transforms:\n  enrich:\n    type: remap\n    inputs: [input]\n"
        "    source: '.checked = true'\n"
        "sinks:\n  output:\n    type: console\n    inputs: [enrich]\n"
        "    encoding:\n      codec: json\n";
    options.worker_threads = 0;
    assert(vr_runtime_start(text(yaml), &options, &runtime) == VR_INVALID_ARGUMENT);
    ok(vr_options_default(&options));
    ok(vr_runtime_start(text(yaml), &options, &runtime));
    uint32_t status = 99;
    ok(vr_runtime_status(runtime, &status));
    assert(status == VR_RUNNING);
    vr_source *source = NULL;
    assert(vr_runtime_source(runtime, text("missing"), &source) == VR_UNKNOWN_SOURCE);
    assert(source == NULL);
    ok(vr_runtime_source(runtime, text("input"), &source));

    vr_value array[] = {
        {.kind = VR_I64, .payload.integer = -42},
        {.kind = VR_F64, .payload.number = 1.25},
        {.kind = VR_STRING, .payload.bytes = text("hello")},
        {.kind = VR_BOOL, .payload.boolean = 1},
        {.kind = VR_NULL},
    };
    vr_field nested[] = {
        {text("literal.key"), {.kind = VR_STRING, .payload.bytes = text("value")}},
    };
    vr_field fields[] = {
        {text("id"), {.kind = VR_STRING, .payload.bytes = text("native")}},
        {text("values"), {.kind = VR_ARRAY, .payload.array = {array, 5}}},
        {text("nested"), {.kind = VR_OBJECT, .payload.object = {nested, 1}}},
        {text("min"), {.kind = VR_I64, .payload.integer = INT64_MIN}},
        {text("max"), {.kind = VR_U64, .payload.unsigned_integer = INT64_MAX}},
        {text("blob"), {.kind = VR_BYTES, .payload.bytes = text("raw")}},
    };
    ok(vr_source_emit(source, (vr_object){fields, 6}, VR_TRY));
    ok(vr_source_emit_json(source, text("{\"id\":\"json\",\"pid\":1234}")));
    assert(vr_source_emit_json(source, text("[]")) == VR_INVALID_ARGUMENT);
    assert(vr_source_emit_json(source, text("{\"n\":18446744073709551615}")) == VR_INVALID_ARGUMENT);

    vr_field batch_fields[] = {
        {text("id"), {.kind = VR_STRING, .payload.bytes = text("batch1")}},
        {text("id"), {.kind = VR_STRING, .payload.bytes = text("batch2")}},
    };
    vr_object batch[] = {{&batch_fields[0], 1}, {&batch_fields[1], 1}};
    ok(vr_source_emit_batch(source, batch, 2, VR_BLOCKING));
    ok(vr_source_emit_batch(source, NULL, 0, VR_BLOCKING));
    assert(vr_source_emit_batch(source, NULL, 1001, VR_BLOCKING) == VR_INVALID_ARGUMENT);
    assert(vr_source_emit_batch(source, NULL, 1, VR_BLOCKING) == VR_INVALID_ARGUMENT);
    assert(vr_source_emit(source, (vr_object){NULL, 0}, 99) == VR_INVALID_ARGUMENT);

    /* Bad second event must not cause partial submission of the first. */
    batch_fields[1].value.kind = 99;
    assert(vr_source_emit_batch(source, batch, 2, VR_BLOCKING) == VR_INVALID_ARGUMENT);
    vr_field invalid = {text("bad"), {.kind = VR_F64, .payload.number = NAN}};
    assert(vr_source_emit(source, (vr_object){&invalid, 1}, VR_BLOCKING) == VR_INVALID_ARGUMENT);
    invalid.value = (vr_value){.kind = VR_U64, .payload.unsigned_integer = UINT64_MAX};
    assert(vr_source_emit(source, (vr_object){&invalid, 1}, VR_BLOCKING) == VR_INVALID_ARGUMENT);
    invalid.value = (vr_value){.kind = VR_BOOL, .payload.boolean = 2};
    assert(vr_source_emit(source, (vr_object){&invalid, 1}, VR_BLOCKING) == VR_INVALID_ARGUMENT);
    vr_value cycle = {.kind = VR_ARRAY};
    cycle.payload.array = (vr_array){&cycle, 1};
    invalid.value = cycle;
    assert(vr_source_emit(source, (vr_object){&invalid, 1}, VR_BLOCKING) == VR_INVALID_ARGUMENT);
    vr_field duplicates[] = {nested[0], nested[0]};
    assert(vr_source_emit(source, (vr_object){duplicates, 2}, VR_BLOCKING) == VR_INVALID_ARGUMENT);

    pthread_t threads[4];
    producer producers[4];
    for (int i = 0; i < 4; ++i) {
        producers[i] = (producer){source, i};
        assert(pthread_create(&threads[i], NULL, produce, &producers[i]) == 0);
    }
    for (int i = 0; i < 4; ++i) assert(pthread_join(threads[i], NULL) == 0);
    ok(vr_runtime_flush(runtime));
    ok(vr_runtime_stop(runtime));
    ok(vr_runtime_stop(runtime));
    ok(vr_runtime_status(runtime, &status));
    assert(status == VR_STOPPED);
    assert(vr_runtime_flush(runtime) == VR_CLOSED);
    ok(vr_runtime_destroy(runtime));
    assert(vr_source_emit_json(source, text("{}")) == VR_CLOSED);
    ok(vr_source_destroy(source));
    ok(vr_runtime_destroy(NULL));
    return 0;
}
