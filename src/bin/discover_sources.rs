use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("[INFO] 🔭 [Discovery] Autonomous GitHub source discovery starting");

    let (new_sources, active_sources) = proxyrift::source_discovery::discover_and_write().await?;

    println!(
        "[INFO] 🔭 [Discovery] Complete | New: {} | Active: {}",
        new_sources, active_sources
    );

    Ok(())
}
