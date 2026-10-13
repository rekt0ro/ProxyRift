use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("[INFO] 🔭 [Discovery] Autonomous GitHub source discovery starting");

    proxyrift::source_discovery::discover_and_write().await?;

    Ok(())
}
