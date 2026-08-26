fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../../proto/sync/v1/sync.proto")?;
    Ok(())
}
