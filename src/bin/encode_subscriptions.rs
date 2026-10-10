use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use proxyrift::{clash, singbox};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

fn decode_vmess_payload(config: &str) -> Option<Vec<u8>> {
    let payload = config.split_once("://")?.1.split('#').next()?.trim();
    let mut padded = payload.to_string();
    while !padded.len().is_multiple_of(4) {
        padded.push('=');
    }

    for candidate in [payload, padded.as_str()] {
        for decoded in [
            STANDARD.decode(candidate),
            URL_SAFE.decode(candidate),
            URL_SAFE_NO_PAD.decode(candidate),
        ]
        .into_iter()
        .flatten()
        {
            if serde_json::from_slice::<Value>(&decoded).is_ok() {
                return Some(decoded);
            }
        }
    }

    None
}

fn rename_subscription_configs(configs: &[String]) -> Result<Vec<String>, String> {
    let mut renamed = Vec::with_capacity(configs.len());
    for config in configs
        .iter()
        .map(|config| config.trim())
        .filter(|config| !config.is_empty() && !config.starts_with('#'))
    {
        let index = renamed.len() + 1;
        let name = format!("ProxyRift {:03}", index);
        let scheme = config
            .split_once("://")
            .map(|(scheme, _)| scheme.to_ascii_lowercase())
            .unwrap_or_default();

        if scheme == "vmess" {
            let decoded = decode_vmess_payload(config).ok_or_else(|| {
                "cannot decode VMess config while assigning subscription names".to_string()
            })?;
            let mut value: Value = serde_json::from_slice(&decoded).map_err(|error| {
                format!("invalid VMess JSON while assigning subscription names: {error}")
            })?;
            let object = value
                .as_object_mut()
                .ok_or_else(|| "VMess payload must be a JSON object".to_string())?;
            object.insert("ps".to_string(), Value::String(name));
            let encoded = STANDARD.encode(
                serde_json::to_vec(&value)
                    .map_err(|error| format!("cannot serialize VMess config name: {error}"))?,
            );
            renamed.push(format!("vmess://{encoded}"));
        } else {
            let base = config.split('#').next().unwrap_or(config);
            renamed.push(format!("{base}#ProxyRift%20{index:03}"));
        }
    }

    Ok(renamed)
}

fn rename_subscription_file(path: &str) -> Result<(), String> {
    let input = fs::read_to_string(path).map_err(|error| format!("cannot read {path}: {error}"))?;
    let configs = input.lines().map(str::to_string).collect::<Vec<_>>();
    let renamed = rename_subscription_configs(&configs)?;
    let output = if renamed.is_empty() {
        String::new()
    } else {
        format!("{}\n", renamed.join("\n"))
    };

    let path = Path::new(path);
    let temporary = path.with_file_name(format!(
        ".{}.names.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("subscription.txt")
    ));
    fs::write(&temporary, output)
        .and_then(|_| fs::rename(&temporary, path))
        .map_err(|error| {
            let _ = fs::remove_file(&temporary);
            format!(
                "cannot write renamed subscription {}: {error}",
                path.display()
            )
        })
}

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
    rename_subscription_file("subscriptions/all.txt")?;
    rename_subscription_file("subscriptions/light.txt")?;
    encode_file("subscriptions/all.txt")?;
    encode_file("subscriptions/light.txt")?;
    render_clash_file("subscriptions/all.txt", "subscriptions/all-clash.yaml")?;
    render_clash_file("subscriptions/light.txt", "subscriptions/light-clash.yaml")?;
    render_singbox_file("subscriptions/all.txt", "subscriptions/all-singbox.json")?;
    render_singbox_file(
        "subscriptions/light.txt",
        "subscriptions/light-singbox.json",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{decode_vmess_payload, output_path, rename_subscription_configs};
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde_json::Value;
    use std::path::Path;

    #[test]
    fn renames_each_subscription_sequentially_across_protocols() {
        let configs = vec![
            "vless://00000000-0000-0000-0000-000000000001@example.com:443#old".to_string(),
            "socks5://proxy.example.com:1080#old".to_string(),
        ];

        let renamed = rename_subscription_configs(&configs).unwrap();
        assert!(renamed[0].ends_with("#ProxyRift%20001"));
        assert!(renamed[1].ends_with("#ProxyRift%20002"));
    }

    #[test]
    fn renames_vmess_ps_field_for_subscription_consistency() {
        let payload = serde_json::json!({
            "v": "2",
            "ps": "old name",
            "add": "example.com",
            "port": "443",
            "id": "00000000-0000-0000-0000-000000000001",
            "net": "tcp"
        });
        let config = format!("vmess://{}", STANDARD.encode(payload.to_string()));
        let renamed = rename_subscription_configs(&[config]).unwrap();
        let decoded = decode_vmess_payload(&renamed[0]).unwrap();
        let value: Value = serde_json::from_slice(&decoded).unwrap();

        assert_eq!(value["ps"], "ProxyRift 001");
    }

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
