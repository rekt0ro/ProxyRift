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

fn subscription_config_count(path: &str) -> Result<usize, String> {
    let content =
        fs::read_to_string(path).map_err(|error| format!("cannot read {path}: {error}"))?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && line.contains("://"))
        .count())
}

fn render_clash_file(input: &str, output: &str) -> Option<usize> {
    let source_count = match subscription_config_count(input) {
        Ok(count) => count,
        Err(error) => {
            eprintln!("[WARN] [Clash/Mihomo] Could not count source configs for {output}: {error}");
            return None;
        }
    };
    let rendered = match clash::render_file(input, output) {
        Ok(count) => count,
        Err(error) => {
            eprintln!(
                "[WARN] [Clash/Mihomo] Could not generate {output}: {error}; retaining the last published export"
            );
            return None;
        }
    };
    let Some(omitted) = source_count.checked_sub(rendered) else {
        eprintln!(
            "[WARN] [Clash/Mihomo] Renderer returned {rendered} entries for {source_count} source configs in {output}; retaining the last published export"
        );
        return None;
    };
    println!(
        "[INFO] [Clash/Mihomo] Rendered {rendered}/{source_count} configs | Omitted: {omitted} | {output}"
    );
    Some(rendered)
}

fn render_singbox_file(input: &str, output: &str) -> Result<usize, String> {
    let source_count = subscription_config_count(input)?;
    let rendered = singbox::render_subscription_file(input, output)?;
    let omitted = source_count.checked_sub(rendered).ok_or_else(|| {
        format!("sing-box renderer returned {rendered} entries for {source_count} source configs")
    })?;
    println!(
        "[INFO] [sing-box] Rendered {rendered}/{source_count} configs | Omitted: {omitted} | {output}"
    );
    Ok(rendered)
}

fn ensure_complete_light_render(source_count: usize, rendered_count: usize) -> Result<(), String> {
    if source_count == rendered_count {
        Ok(())
    } else {
        Err(format!(
            "Light sing-box output is incomplete: rendered {rendered_count}/{source_count} configs; refusing to publish entries that the consumer cannot import"
        ))
    }
}

fn main() -> Result<(), String> {
    rename_subscription_file("subscriptions/all.txt")?;
    rename_subscription_file("subscriptions/light.txt")?;
    encode_file("subscriptions/all.txt")?;
    encode_file("subscriptions/light.txt")?;

    let all_source_count = subscription_config_count("subscriptions/all.txt")?;
    let light_source_count = subscription_config_count("subscriptions/light.txt")?;
    let all_clash_count =
        render_clash_file("subscriptions/all.txt", "subscriptions/all-clash.yaml");
    let light_clash_count =
        render_clash_file("subscriptions/light.txt", "subscriptions/light-clash.yaml");
    let all_singbox_count =
        render_singbox_file("subscriptions/all.txt", "subscriptions/all-singbox.json")?;
    let light_singbox_count = render_singbox_file(
        "subscriptions/light.txt",
        "subscriptions/light-singbox.json",
    )?;
    ensure_complete_light_render(light_source_count, light_singbox_count)?;

    let counts = serde_json::json!({
        "all": {
            "source": all_source_count,
            "clash": all_clash_count,
            "singbox": all_singbox_count,
        },
        "light": {
            "source": light_source_count,
            "clash": light_clash_count,
            "singbox": light_singbox_count,
        },
    });
    let counts_json = serde_json::to_string_pretty(&counts)
        .map_err(|error| format!("cannot serialize subscription output counts: {error}"))?;
    fs::write(
        "subscriptions/.output-counts.json",
        format!("{counts_json}\n"),
    )
    .map_err(|error| format!("cannot write subscription output counts: {error}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        decode_vmess_payload, ensure_complete_light_render, output_path,
        rename_subscription_configs,
    };
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

    #[test]
    fn clash_render_failure_does_not_abort_optional_export() {
        use super::render_clash_file;
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "proxyrift-clash-render-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&directory).unwrap();
        let input = directory.join("source.txt");
        let output = directory.join("clash.yaml");
        fs::write(&input, "tuic://password@example.com:443\n").unwrap();

        assert_eq!(
            render_clash_file(input.to_str().unwrap(), output.to_str().unwrap()),
            None
        );
        assert!(!output.exists());

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn light_singbox_output_must_preserve_all_source_entries() {
        assert!(ensure_complete_light_render(200, 200).is_ok());
        assert!(ensure_complete_light_render(200, 199).is_err());
    }
}
