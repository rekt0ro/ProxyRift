use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use proxyrift::{clash, singbox};
use std::fs;
use std::path::{Path, PathBuf};

fn encode_file(path: &str) -> Result<(), String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };

    let payload = String::from_utf8_lossy(&bytes);
    let payload = payload.trim_end_matches(['\r', '\n']);

    let encoded = STANDARD.encode(payload.as_bytes());
    let output = output_path(path);
    let temporary = output.with_file_name(format!(
        ".{}.tmp",
        output
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("subscription-base64.txt")
    ));

    let result =
        fs::write(&temporary, format!("{encoded}\n")).and_then(|_| fs::rename(&temporary, &output));

    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(error.to_string());
    }

    Ok(())
}

fn output_path(path: &str) -> PathBuf {
    let path = Path::new(path);

    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(|stem| path.with_file_name(format!("{stem}-base64.txt")))
        .unwrap_or_else(|| PathBuf::from(format!("{}-base64.txt", path.display())))
}

fn render_clash_file(input: &str, output: &str) -> Result<(), String> {
    let count = clash::render_file(input, output)?;
    println!(
        "[INFO] 🧩 [Clash/Mihomo] Rendered {} configs | {}",
        count, output
    );
    Ok(())
}

fn render_singbox_file(input: &str, output: &str) -> Result<(), String> {
    let count = singbox::render_subscription_file(input, output)?;
    println!(
        "[INFO] 🧩 [sing-box] Rendered {} configs | {}",
        count, output
    );
    Ok(())
}

fn main() -> Result<(), String> {
    encode_file("subscriptions/all.txt")?;
    encode_file("subscriptions/light.txt")?;
    render_clash_file("subscriptions/all.txt", "subscriptions/all-clash.yaml")?;
    render_clash_file("subscriptions/light.txt", "subscriptions/light-clash.yaml")?;
    render_singbox_file("subscriptions/all.txt", "subscriptions/all-singbox.json")?;
    render_singbox_file("subscriptions/light.txt", "subscriptions/light-singbox.json")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::output_path;
    use std::path::Path;

    #[test]
    fn keeps_base64_output_next_to_source() {
        assert_eq!(
            output_path("subscriptions/all.txt"),
            Path::new("subscriptions/all-base64.txt")
        );
        assert_eq!(
            output_path("subscriptions/light.txt"),
            Path::new("subscriptions/light-base64.txt")
        );
    }

    #[test]
    fn temporary_output_uses_same_directory() {
        let output = output_path("subscriptions/all.txt");
        let temporary = output.with_file_name(format!(
            ".{}.tmp",
            output.file_name().unwrap().to_str().unwrap()
        ));
        assert_eq!(temporary, Path::new("subscriptions/.all-base64.txt.tmp"));
    }
}
