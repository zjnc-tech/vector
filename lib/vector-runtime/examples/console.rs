use vector_runtime::{ExternalSourceEvent, Pipeline};

#[derive(ExternalSourceEvent)]
struct MusaMallocEvent {
    pid: u32,
    size: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml = std::fs::read_to_string("pipeline.yaml")?;
    let runtime = Pipeline::from_yaml(&yaml)?.start_owned()?;
    runtime.emit(
        "musa_malloc",
        MusaMallocEvent {
            pid: 1234,
            size: 4096,
        },
    )?;
    runtime.flush()?;
    runtime.stop()?;
    Ok(())
}
