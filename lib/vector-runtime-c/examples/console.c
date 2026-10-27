#include "vector_runtime.h"
#include <stdio.h>
#include <string.h>

static vr_slice text(const char *s) {
    vr_slice result = {(const uint8_t *)s, strlen(s)};
    return result;
}

int main(void) {
    const char *yaml =
        "sources:\n"
        "  musa_malloc:\n"
        "    type: external\n"
        "transforms:\n"
        "  add_field:\n"
        "    type: remap\n"
        "    inputs: [musa_malloc]\n"
        "    source: '.event_type = \"musa_malloc\"'\n"
        "sinks:\n"
        "  output:\n"
        "    type: console\n"
        "    inputs: [add_field]\n"
        "    encoding:\n"
        "      codec: json\n";
    vr_runtime *runtime = NULL;
    vr_source *source = NULL;
    int failed = 1;
#define CHECK(call) do { if ((call) != VR_OK) { \
    fprintf(stderr, "%s: %s\n", #call, vr_last_error()); goto cleanup; \
} } while (0)

    if (vr_abi_version() != VR_ABI_VERSION) return 1;
    CHECK(vr_runtime_start(text(yaml), NULL, &runtime));
    CHECK(vr_runtime_source(runtime, text("musa_malloc"), &source));
    vr_field fields[] = {
        {text("pid"), {.kind = VR_I64, .payload.integer = 1234}},
        {text("size"), {.kind = VR_U64, .payload.unsigned_integer = 4096}},
    };
    vr_object event = {fields, sizeof(fields) / sizeof(fields[0])};
    CHECK(vr_source_emit(source, event, VR_BLOCKING));
    CHECK(vr_source_emit_json(source, text("{\"pid\":5678,\"size\":8192}")));
    CHECK(vr_runtime_flush(runtime));
    CHECK(vr_runtime_stop(runtime));
    failed = 0;
cleanup:
    vr_source_destroy(source);
    if (failed && runtime) vr_runtime_stop(runtime);
    vr_runtime_destroy(runtime);
    return failed;
}
