use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use percent_encoding::percent_decode_str;
use serde_json::Value;
use std::fs;
use url::Url;

fn clean(config: &str) -> &str {
    config.split('#').next().unwrap_or(config)
}

fn scheme_of(config: &str) -> &str {
    config
        .split_once("://")
        .map(|(scheme, _)| scheme)
        .unwrap_or_default()
}

fn decode_component(value: &str) -> Result<String, String> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|error| error.to_string())
}

fn query(url: &Url, name: &str) -> Option<String> {
    url.query_pairs().find_map(|(key, value)| {
        key.eq_ignore_ascii_case(name)
            .then_some(value.into_owned())
            .filter(|value| !value.is_empty())
    })
}

fn query_any(url: &Url, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| query(url, name))
}

fn query_list(url: &Url, name: &str) -> Vec<String> {
    url.query_pairs()
        .filter_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value.into_owned()))
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn json_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Bool(value)) => Some(value.to_string()),
        Some(Value::Number(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn json_bool(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Bool(value)) => Some(*value),
        Some(Value::String(value)) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn b64decode(value: &str) -> Option<Vec<u8>> {
    let value = value.trim();
    let mut padded = value.to_string();
    while !padded.len().is_multiple_of(4) {
        padded.push('=');
    }

    [value, padded.as_str()]
        .into_iter()
        .flat_map(|candidate| {
            [
                STANDARD.decode(candidate),
                URL_SAFE.decode(candidate),
                URL_SAFE_NO_PAD.decode(candidate),
            ]
        })
        .find_map(Result::ok)
}

fn vmess_json(config: &str) -> Result<Value, String> {
    let payload = clean(config)
        .split_once("://")
        .ok_or_else(|| "invalid VMess URL".to_string())?
        .1;
    let decoded = b64decode(payload).ok_or_else(|| "invalid VMess base64".to_string())?;
    serde_json::from_slice(&decoded).map_err(|error| error.to_string())
}

fn endpoint_from_url(url: &Url) -> Result<(String, u16), String> {
    let server = url
        .host_str()
        .ok_or_else(|| "missing server".to_string())?
        .to_string();
    let port = url.port().ok_or_else(|| "missing port".to_string())?;
    if port == 0 {
        return Err("invalid port".to_string());
    }
    Ok((server, port))
}

fn yaml_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn push_field(lines: &mut Vec<String>, indent: usize, key: &str, value: &str) {
    lines.push(format!(
        "{}{}: {}",
        " ".repeat(indent),
        key,
        yaml_quote(value)
    ));
}

fn push_raw_field(lines: &mut Vec<String>, indent: usize, key: &str, value: &str) {
    lines.push(format!("{}{}: {}", " ".repeat(indent), key, value));
}

fn push_alpn(lines: &mut Vec<String>, indent: usize, values: &[String]) {
    if values.is_empty() {
        return;
    }

    lines.push(format!("{}alpn:", " ".repeat(indent)));
    for value in values {
        lines.push(format!("{}- {}", " ".repeat(indent + 2), yaml_quote(value)));
    }
}

fn push_ws_opts(
    lines: &mut Vec<String>,
    indent: usize,
    path: Option<&str>,
    host: Option<&str>,
    max_early_data: Option<&str>,
    early_data_header_name: Option<&str>,
) {
    if path.is_none()
        && host.is_none()
        && max_early_data.is_none()
        && early_data_header_name.is_none()
    {
        return;
    }

    lines.push(format!("{}ws-opts:", " ".repeat(indent)));
    if let Some(path) = path.filter(|value| !value.is_empty()) {
        push_field(lines, indent + 2, "path", path);
    }
    if let Some(host) = host.filter(|value| !value.is_empty()) {
        lines.push(format!("{}headers:", " ".repeat(indent + 2)));
        push_field(lines, indent + 4, "Host", host);
    }
    if let Some(value) = max_early_data.filter(|value| !value.is_empty()) {
        push_raw_field(lines, indent + 2, "max-early-data", value);
    }
    if let Some(value) = early_data_header_name.filter(|value| !value.is_empty()) {
        push_field(lines, indent + 2, "early-data-header-name", value);
    }
}

fn push_http_opts(lines: &mut Vec<String>, indent: usize, path: Option<&str>, host: Option<&str>) {
    if path.is_none() && host.is_none() {
        return;
    }

    lines.push(format!("{}http-opts:", " ".repeat(indent)));
    if let Some(path) = path.filter(|value| !value.is_empty()) {
        lines.push(format!("{}path:", " ".repeat(indent + 2)));
        lines.push(format!("{}- {}", " ".repeat(indent + 4), yaml_quote(path)));
    }
    if let Some(host) = host.filter(|value| !value.is_empty()) {
        lines.push(format!("{}headers:", " ".repeat(indent + 2)));
        lines.push(format!("{}Host:", " ".repeat(indent + 4)));
        lines.push(format!("{}- {}", " ".repeat(indent + 6), yaml_quote(host)));
    }
}

fn push_h2_opts(lines: &mut Vec<String>, indent: usize, path: Option<&str>, host: Option<&str>) {
    if path.is_none() && host.is_none() {
        return;
    }

    lines.push(format!("{}h2-opts:", " ".repeat(indent)));
    if let Some(host) = host.filter(|value| !value.is_empty()) {
        lines.push(format!("{}host:", " ".repeat(indent + 2)));
        lines.push(format!("{}- {}", " ".repeat(indent + 4), yaml_quote(host)));
    }
    if let Some(path) = path.filter(|value| !value.is_empty()) {
        push_field(lines, indent + 2, "path", path);
    }
}

fn push_grpc_opts(lines: &mut Vec<String>, indent: usize, service_name: Option<&str>) {
    if let Some(service_name) = service_name.filter(|value| !value.is_empty()) {
        lines.push(format!("{}grpc-opts:", " ".repeat(indent)));
        push_field(lines, indent + 2, "grpc-service-name", service_name);
    }
}

fn push_httpupgrade_opts(
    lines: &mut Vec<String>,
    indent: usize,
    path: Option<&str>,
    host: Option<&str>,
) {
    if path.is_none() && host.is_none() {
        return;
    }

    lines.push(format!("{}httpupgrade-opts:", " ".repeat(indent)));
    if let Some(path) = path.filter(|value| !value.is_empty()) {
        push_field(lines, indent + 2, "path", path);
    }
    if let Some(host) = host.filter(|value| !value.is_empty()) {
        push_field(lines, indent + 2, "host", host);
    }
}

fn push_xhttp_opts(lines: &mut Vec<String>, indent: usize, path: Option<&str>, host: Option<&str>) {
    if path.is_none() && host.is_none() {
        return;
    }

    lines.push(format!("{}xhttp-opts:", " ".repeat(indent)));
    if let Some(path) = path.filter(|value| !value.is_empty()) {
        push_field(lines, indent + 2, "path", path);
    }
    if let Some(host) = host.filter(|value| !value.is_empty()) {
        push_field(lines, indent + 2, "host", host);
    }
}

fn push_tls(
    lines: &mut Vec<String>,
    indent: usize,
    enabled: bool,
    server_name: Option<&str>,
    client_fingerprint: Option<&str>,
    fingerprint: Option<&str>,
    skip_cert_verify: bool,
    reality_public_key: Option<&str>,
    reality_short_id: Option<&str>,
) {
    if !enabled
        && server_name.is_none()
        && client_fingerprint.is_none()
        && fingerprint.is_none()
        && !skip_cert_verify
        && reality_public_key.is_none()
        && reality_short_id.is_none()
    {
        return;
    }

    if enabled {
        push_raw_field(lines, indent, "tls", "true");
    }
    if let Some(value) = server_name.filter(|value| !value.is_empty()) {
        push_field(lines, indent, "servername", value);
    }
    if let Some(value) = client_fingerprint.filter(|value| !value.is_empty()) {
        push_field(lines, indent, "client-fingerprint", value);
    }
    if let Some(value) = fingerprint.filter(|value| !value.is_empty()) {
        push_field(lines, indent, "fingerprint", value);
    }
    if skip_cert_verify {
        push_raw_field(lines, indent, "skip-cert-verify", "true");
    }

    if reality_public_key.is_some() || reality_short_id.is_some() {
        lines.push(format!("{}reality-opts:", " ".repeat(indent)));
        if let Some(value) = reality_public_key.filter(|value| !value.is_empty()) {
            push_field(lines, indent + 2, "public-key", value);
        }
        if let Some(value) = reality_short_id.filter(|value| !value.is_empty()) {
            push_field(lines, indent + 2, "short-id", value);
        }
    }
}

fn push_transport(
    lines: &mut Vec<String>,
    indent: usize,
    network: &str,
    path: Option<&str>,
    host: Option<&str>,
    service_name: Option<&str>,
    max_early_data: Option<&str>,
    early_data_header_name: Option<&str>,
) -> Result<(), String> {
    match network.trim().to_ascii_lowercase().as_str() {
        "" | "tcp" | "raw" | "none" => Ok(()),
        "ws" => {
            push_field(lines, indent, "network", "ws");
            push_ws_opts(
                lines,
                indent,
                path,
                host,
                max_early_data,
                early_data_header_name,
            );
            Ok(())
        }
        "grpc" => {
            push_field(lines, indent, "network", "grpc");
            push_grpc_opts(lines, indent, service_name);
            Ok(())
        }
        "h2" => {
            push_field(lines, indent, "network", "h2");
            push_h2_opts(lines, indent, path, host);
            Ok(())
        }
        "http" => {
            push_field(lines, indent, "network", "http");
            push_http_opts(lines, indent, path, host);
            Ok(())
        }
        "httpupgrade" => {
            push_field(lines, indent, "network", "httpupgrade");
            push_httpupgrade_opts(lines, indent, path, host);
            Ok(())
        }
        "xhttp" | "splithttp" => {
            push_field(lines, indent, "network", "xhttp");
            push_xhttp_opts(lines, indent, path, host);
            Ok(())
        }
        other => Err(format!("unsupported Clash/Mihomo transport {other}")),
    }
}

fn name_from_config(config: &str, fallback_index: usize) -> String {
    let fragment = config
        .split_once('#')
        .map(|(_, fragment)| fragment)
        .unwrap_or_default();

    if !fragment.is_empty() {
        if let Ok(decoded) = decode_component(fragment) {
            let name = decoded.trim();
            if !name.is_empty() {
                return name.to_string();
            }
        }
    }

    format!("ProxyRift {:03}", fallback_index + 1)
}

fn convert_vless(config: &str, index: usize) -> Result<String, String> {
    let url = Url::parse(clean(config)).map_err(|error| error.to_string())?;
    let (server, port) = endpoint_from_url(&url)?;
    let uuid = decode_component(url.username())?;
    if uuid.is_empty() {
        return Err("VLESS UUID missing".to_string());
    }

    let security = query(&url, "security")
        .unwrap_or_else(|| "none".to_string())
        .to_ascii_lowercase();
    if !matches!(security.as_str(), "none" | "tls" | "reality") {
        return Err(format!("unsupported VLESS security {security}"));
    }

    let network = query(&url, "type").unwrap_or_else(|| "tcp".to_string());
    let host = query(&url, "host");
    let path = query(&url, "path");
    let service_name = query_any(&url, &["serviceName", "service-name", "grpc-service-name"]);
    let max_early_data = query_any(&url, &["ed", "maxEarlyData", "max_early_data"]);
    let early_data_header_name = query_any(
        &url,
        &["eh", "earlyDataHeaderName", "early_data_header_name"],
    );
    let alpn = query_list(&url, "alpn");
    let sni = query_any(&url, &["sni", "servername"]);
    let client_fingerprint = query_any(&url, &["fp", "client-fingerprint"]);
    let skip_cert_verify = query_any(&url, &["allowInsecure", "insecure"]).is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    });
    let flow = query(&url, "flow");
    let encryption = query(&url, "encryption");
    let public_key = query_any(&url, &["pbk", "publicKey", "public-key"]);
    let short_id = query_any(&url, &["sid", "shortId", "short-id"]);

    let mut lines = vec![format!(
        "  - name: {}",
        yaml_quote(&name_from_config(config, index))
    )];
    push_field(&mut lines, 4, "type", "vless");
    push_field(&mut lines, 4, "server", &server);
    push_raw_field(&mut lines, 4, "port", &port.to_string());
    push_raw_field(&mut lines, 4, "udp", "true");
    push_field(&mut lines, 4, "uuid", &uuid);

    if let Some(flow) = flow.filter(|value| !value.is_empty()) {
        push_field(&mut lines, 4, "flow", flow);
    }
    if let Some(encryption) = encryption.filter(|value| !value.is_empty()) {
        push_field(&mut lines, 4, "encryption", encryption);
    }

    push_tls(
        &mut lines,
        4,
        security != "none",
        sni.as_deref().or(Some(server.as_str())),
        client_fingerprint.as_deref(),
        None,
        skip_cert_verify,
        (security == "reality")
            .then_some(public_key.as_deref())
            .flatten(),
        (security == "reality")
            .then_some(short_id.as_deref())
            .flatten(),
    );
    push_alpn(&mut lines, 4, &alpn);
    push_transport(
        &mut lines,
        4,
        &network,
        path.as_deref(),
        host.as_deref(),
        service_name.as_deref(),
        max_early_data.as_deref(),
        early_data_header_name.as_deref(),
    )?;

    Ok(lines.join("\n"))
}

fn convert_vmess(config: &str, index: usize) -> Result<String, String> {
    let object = vmess_json(config)?;
    let server = json_string(object.get("add"))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "VMess server missing".to_string())?;
    let port = json_string(object.get("port"))
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port != 0)
        .ok_or_else(|| "VMess port missing".to_string())?;
    let uuid = json_string(object.get("id"))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "VMess UUID missing".to_string())?;

    let network = json_string(object.get("net")).unwrap_or_else(|| "tcp".to_string());
    let host = json_string(object.get("host"));
    let path = json_string(object.get("path"));
    let service_name =
        json_string(object.get("serviceName")).or_else(|| json_string(object.get("service_name")));
    let max_early_data =
        json_string(object.get("ed")).or_else(|| json_string(object.get("maxEarlyData")));
    let early_data_header_name =
        json_string(object.get("eh")).or_else(|| json_string(object.get("earlyDataHeaderName")));
    let security = json_string(object.get("tls"))
        .unwrap_or_default()
        .to_ascii_lowercase();
    let tls_enabled = matches!(security.as_str(), "tls" | "true" | "1" | "yes");
    let sni = json_string(object.get("sni"))
        .or_else(|| json_string(object.get("servername")))
        .or_else(|| host.clone());
    let client_fingerprint =
        json_string(object.get("fp")).or_else(|| json_string(object.get("clientFingerprint")));
    let skip_cert_verify = json_bool(object.get("skipCertVerify")).unwrap_or(false);
    let alpn = match object.get("alpn") {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(|value| json_string(Some(value)))
            .collect(),
        Some(Value::String(value)) => value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        _ => Vec::new(),
    };
    let flow = json_string(object.get("flow"));
    let public_key = json_string(object.get("pbk"))
        .or_else(|| json_string(object.get("publicKey")))
        .or_else(|| json_string(object.get("public-key")));
    let short_id = json_string(object.get("sid"))
        .or_else(|| json_string(object.get("shortId")))
        .or_else(|| json_string(object.get("short-id")));
    let name = json_string(object.get("ps")).unwrap_or_else(|| name_from_config(config, index));
    let alter_id = json_string(object.get("aid"))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .to_string();
    let cipher = json_string(object.get("scy")).unwrap_or_else(|| "auto".to_string());

    let mut lines = vec![format!("  - name: {}", yaml_quote(name.trim()))];
    push_field(&mut lines, 4, "type", "vmess");
    push_field(&mut lines, 4, "server", &server);
    push_raw_field(&mut lines, 4, "port", &port.to_string());
    push_raw_field(&mut lines, 4, "udp", "true");
    push_field(&mut lines, 4, "uuid", &uuid);
    push_raw_field(&mut lines, 4, "alterId", &alter_id);
    push_field(&mut lines, 4, "cipher", &cipher);

    if let Some(packet_encoding) = json_string(object.get("packetEncoding"))
        .or_else(|| json_string(object.get("packet-encoding")))
        .filter(|value| !value.is_empty())
    {
        push_field(&mut lines, 4, "packet-encoding", &packet_encoding);
    }

    push_tls(
        &mut lines,
        4,
        tls_enabled,
        sni.as_deref(),
        client_fingerprint.as_deref(),
        None,
        skip_cert_verify,
        public_key.as_deref(),
        short_id.as_deref(),
    );
    push_alpn(&mut lines, 4, &alpn);
    push_transport(
        &mut lines,
        4,
        &network,
        path.as_deref(),
        host.as_deref(),
        service_name.as_deref(),
        max_early_data.as_deref(),
        early_data_header_name.as_deref(),
    )?;

    if let Some(flow) = flow.filter(|value| !value.is_empty()) {
        push_field(&mut lines, 4, "flow", flow);
    }

    Ok(lines.join("\n"))
}

fn convert_trojan(config: &str, index: usize) -> Result<String, String> {
    let url = Url::parse(clean(config)).map_err(|error| error.to_string())?;
    let (server, port) = endpoint_from_url(&url)?;
    let password = decode_component(url.username())?;
    if password.is_empty() {
        return Err("Trojan password missing".to_string());
    }

    let network = query(&url, "type").unwrap_or_else(|| "tcp".to_string());
    let host = query(&url, "host");
    let path = query(&url, "path");
    let service_name = query_any(&url, &["serviceName", "service-name", "grpc-service-name"]);
    let max_early_data = query_any(&url, &["ed", "maxEarlyData", "max_early_data"]);
    let early_data_header_name = query_any(
        &url,
        &["eh", "earlyDataHeaderName", "early_data_header_name"],
    );
    let alpn = query_list(&url, "alpn");
    let sni = query_any(&url, &["sni", "servername"]).or_else(|| Some(server.clone()));
    let client_fingerprint = query_any(&url, &["fp", "clientFingerprint", "client-fingerprint"]);
    let fingerprint = query_any(&url, &["fingerprint", "certFingerprint"]);
    let skip_cert_verify = query_any(&url, &["allowInsecure", "insecure"]).is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    });
    let public_key = query_any(&url, &["pbk", "publicKey", "public-key"]);
    let short_id = query_any(&url, &["sid", "shortId", "short-id"]);

    let mut lines = vec![format!(
        "  - name: {}",
        yaml_quote(&name_from_config(config, index))
    )];
    push_field(&mut lines, 4, "type", "trojan");
    push_field(&mut lines, 4, "server", &server);
    push_raw_field(&mut lines, 4, "port", &port.to_string());
    push_raw_field(&mut lines, 4, "udp", "true");
    push_field(&mut lines, 4, "password", &password);
    push_tls(
        &mut lines,
        4,
        true,
        sni.as_deref(),
        client_fingerprint.as_deref(),
        fingerprint.as_deref(),
        skip_cert_verify,
        public_key.as_deref(),
        short_id.as_deref(),
    );
    push_alpn(&mut lines, 4, &alpn);
    push_transport(
        &mut lines,
        4,
        &network,
        path.as_deref(),
        host.as_deref(),
        service_name.as_deref(),
        max_early_data.as_deref(),
        early_data_header_name.as_deref(),
    )?;

    Ok(lines.join("\n"))
}

fn convert_shadowsocks(config: &str, index: usize) -> Result<String, String> {
    let cleaned = clean(config)
        .strip_prefix("ss://")
        .ok_or_else(|| "invalid Shadowsocks URL".to_string())?;

    let (server, port, cipher, password) = if let Some((userinfo, remote)) =
        cleaned.rsplit_once('@')
    {
        let remote_url =
            Url::parse(&format!("http://{remote}")).map_err(|error| error.to_string())?;
        let (server, port) = endpoint_from_url(&remote_url)?;
        let decoded = decode_component(userinfo)?;
        let credentials = b64decode(&decoded)
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .unwrap_or(decoded);
        let (cipher, password) = credentials
            .split_once(':')
            .ok_or_else(|| "invalid Shadowsocks credentials".to_string())?;
        (server, port, cipher.to_string(), password.to_string())
    } else {
        let payload = cleaned.split('#').next().unwrap_or_default();
        let decoded = b64decode(&decode_component(payload)?)
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .ok_or_else(|| "invalid legacy Shadowsocks base64".to_string())?;
        let (credentials, remote) = decoded
            .rsplit_once('@')
            .ok_or_else(|| "invalid legacy Shadowsocks payload".to_string())?;
        let (cipher, password) = credentials
            .split_once(':')
            .ok_or_else(|| "invalid Shadowsocks credentials".to_string())?;
        let remote_url =
            Url::parse(&format!("http://{remote}")).map_err(|error| error.to_string())?;
        let (server, port) = endpoint_from_url(&remote_url)?;
        (server, port, cipher.to_string(), password.to_string())
    };

    let mut lines = vec![format!(
        "  - name: {}",
        yaml_quote(&name_from_config(config, index))
    )];
    push_field(&mut lines, 4, "type", "ss");
    push_field(&mut lines, 4, "server", &server);
    push_raw_field(&mut lines, 4, "port", &port.to_string());
    push_field(&mut lines, 4, "cipher", &cipher);
    push_field(&mut lines, 4, "password", &password);
    push_raw_field(&mut lines, 4, "udp", "true");

    Ok(lines.join("\n"))
}

fn convert_hysteria2(config: &str, index: usize) -> Result<String, String> {
    let cleaned = clean(config);
    let rest = cleaned
        .strip_prefix("hysteria2://")
        .or_else(|| cleaned.strip_prefix("hy2://"))
        .ok_or_else(|| "invalid Hysteria2 URL".to_string())?;

    let (authority, query_string) = rest.split_once('?').unwrap_or((rest, ""));
    let authority = authority.split('#').next().unwrap_or(authority);
    let (password_raw, host_port) = authority
        .rsplit_once('@')
        .ok_or_else(|| "Hysteria2 password missing".to_string())?;
    let password = decode_component(password_raw)?;
    if password.is_empty() {
        return Err("Hysteria2 password missing".to_string());
    }

    let host_port_url =
        Url::parse(&format!("http://{host_port}")).map_err(|error| error.to_string())?;
    let server = host_port_url
        .host_str()
        .ok_or_else(|| "Hysteria2 server missing".to_string())?
        .to_string();
    let port = host_port_url
        .port()
        .ok_or_else(|| "Hysteria2 port missing".to_string())?;

    let params = url::form_urlencoded::parse(query_string.as_bytes());
    let values = params.collect::<Vec<_>>();
    let get = |names: &[&str]| -> Option<String> {
        values.iter().find_map(|(key, value)| {
            names
                .iter()
                .any(|name| key.eq_ignore_ascii_case(name))
                .then_some(value.to_string())
                .filter(|value| !value.is_empty())
        })
    };

    let sni = get(&["sni", "servername"]).or_else(|| Some(server.clone()));
    let skip_cert_verify = get(&["insecure", "allowInsecure"]).is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    });
    let fingerprint = get(&["pinSHA256", "fingerprint", "pin-sha256"]);
    let alpn = values
        .iter()
        .filter_map(|(key, value)| {
            key.eq_ignore_ascii_case("alpn")
                .then_some(value.to_string())
        })
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let obfs = get(&["obfs"]);
    let obfs_password = get(&["obfs-password", "obfsPassword"]);
    let up = get(&["up", "upmbps"]);
    let down = get(&["down", "downmbps"]);

    let mut lines = vec![format!(
        "  - name: {}",
        yaml_quote(&name_from_config(config, index))
    )];
    push_field(&mut lines, 4, "type", "hysteria2");
    push_field(&mut lines, 4, "server", &server);
    push_raw_field(&mut lines, 4, "port", &port.to_string());
    push_raw_field(&mut lines, 4, "udp", "true");
    push_field(&mut lines, 4, "password", &password);

    if let Some(up) = up.filter(|value| !value.is_empty()) {
        push_field(&mut lines, 4, "up", up);
    }
    if let Some(down) = down.filter(|value| !value.is_empty()) {
        push_field(&mut lines, 4, "down", down);
    }
    if let Some(obfs) = obfs.filter(|value| !value.is_empty()) {
        push_field(&mut lines, 4, "obfs", obfs);
    }
    if let Some(password) = obfs_password.filter(|value| !value.is_empty()) {
        push_field(&mut lines, 4, "obfs-password", password);
    }

    push_tls(
        &mut lines,
        4,
        true,
        sni.as_deref(),
        None,
        fingerprint.as_deref(),
        skip_cert_verify,
        None,
        None,
    );
    push_alpn(&mut lines, 4, &alpn);

    Ok(lines.join("\n"))
}

fn convert_socks(config: &str, index: usize) -> Result<String, String> {
    let url = Url::parse(clean(config)).map_err(|error| error.to_string())?;
    let scheme = url.scheme().to_ascii_lowercase();
    if matches!(scheme.as_str(), "socks4" | "socks4a") {
        return Err(format!("unsupported SOCKS variant {scheme}"));
    }
    let (server, port) = endpoint_from_url(&url)?;

    let mut lines = vec![format!(
        "  - name: {}",
        yaml_quote(&name_from_config(config, index))
    )];
    push_field(&mut lines, 4, "type", "socks5");
    push_field(&mut lines, 4, "server", &server);
    push_raw_field(&mut lines, 4, "port", &port.to_string());
    push_raw_field(&mut lines, 4, "udp", "true");

    if !url.username().is_empty() {
        push_field(
            &mut lines,
            4,
            "username",
            &decode_component(url.username())?,
        );
        if let Some(password) = url.password() {
            push_field(&mut lines, 4, "password", &decode_component(password)?);
        }
    }

    Ok(lines.join("\n"))
}

fn convert_http(config: &str, index: usize) -> Result<String, String> {
    let url = Url::parse(clean(config)).map_err(|error| error.to_string())?;
    let (server, port) = endpoint_from_url(&url)?;

    let mut lines = vec![format!(
        "  - name: {}",
        yaml_quote(&name_from_config(config, index))
    )];
    push_field(&mut lines, 4, "type", "http");
    push_field(&mut lines, 4, "server", &server);
    push_raw_field(&mut lines, 4, "port", &port.to_string());

    if !url.username().is_empty() {
        push_field(
            &mut lines,
            4,
            "username",
            &decode_component(url.username())?,
        );
        if let Some(password) = url.password() {
            push_field(&mut lines, 4, "password", &decode_component(password)?);
        }
    }

    Ok(lines.join("\n"))
}

fn convert_config(config: &str, index: usize) -> Result<String, String> {
    match scheme_of(config).to_ascii_lowercase().as_str() {
        "vless" => convert_vless(config, index),
        "vmess" => convert_vmess(config, index),
        "trojan" => convert_trojan(config, index),
        "ss" => convert_shadowsocks(config, index),
        "hysteria2" | "hy2" => convert_hysteria2(config, index),
        "socks" | "socks5" | "socks5h" => convert_socks(config, index),
        "http" => convert_http(config, index),
        scheme => Err(format!("unsupported Clash/Mihomo proxy type {scheme}")),
    }
}

pub fn render(configs: &[String]) -> Result<String, String> {
    let mut lines = vec!["proxies:".to_string()];
    let mut rendered = 0usize;

    for (index, config) in configs.iter().enumerate() {
        match convert_config(config, index) {
            Ok(proxy) => {
                lines.push(proxy);
                rendered += 1;
            }
            Err(error) => {
                eprintln!(
                    "[WARN] ⚠️ [Clash/Mihomo] Skipping config {} | {}",
                    index + 1,
                    error
                );
            }
        }
    }

    if rendered == 0 {
        return Err("no configs could be rendered for Clash/Mihomo".to_string());
    }

    lines.push(String::new());
    Ok(lines.join("\n"))
}

pub fn render_file(input: &str, output: &str) -> Result<usize, String> {
    let content = fs::read_to_string(input).map_err(|error| error.to_string())?;
    let configs = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && line.contains("://"))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    let yaml = render(&configs)?;
    fs::write(output, yaml).map_err(|error| error.to_string())?;
    Ok(configs.len())
}

#[cfg(test)]
mod tests {
    use super::render;

    #[test]
    fn renders_vless_reality_and_websocket() {
        let configs = vec![
            "vless://00000000-0000-0000-0000-000000000001@example.com:443?security=reality&type=ws&host=cdn.example.com&path=%2Fws&sni=cdn.example.com&fp=chrome&pbk=public-key&sid=abcd#VLESS%20001".to_string(),
        ];

        let yaml = render(&configs).unwrap();
        assert!(yaml.contains("type: 'vless'"));
        assert!(yaml.contains("server: 'example.com'"));
        assert!(yaml.contains("network: 'ws'"));
        assert!(yaml.contains("reality-opts:"));
        assert!(yaml.contains("public-key: 'public-key'"));
        assert!(yaml.contains("short-id: 'abcd'"));
    }

    #[test]
    fn renders_vmess() {
        let configs = vec![
            "vmess://eyJhZGQiOiJ2bWVzcy5leGFtcGxlLmNvbSIsInBvcnQiOiI0NDMiLCJpZCI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMSIsImFpZCI6IjAiLCJzY3kiOiJhdXRvIiwibmV0IjoidGNwIiwidGxzIjoidGxzIiwicHMiOiJWTWVzcyAwMDEifQ==".to_string(),
        ];

        let yaml = render(&configs).unwrap();
        assert!(yaml.contains("type: 'vmess'"));
        assert!(yaml.contains("cipher: 'auto'"));
        assert!(yaml.contains("servername: 'vmess.example.com'"));
    }

    #[test]
    fn renders_shadowsocks() {
        let configs = vec![
            "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@example.com:443#SS%20001".to_string(),
        ];

        let yaml = render(&configs).unwrap();
        assert!(yaml.contains("type: 'ss'"));
        assert!(yaml.contains("cipher: 'aes-128-gcm'"));
        assert!(yaml.contains("password: 'password'"));
    }

    #[test]
    fn renders_hysteria2() {
        let configs = vec![
            "hy2://password@example.com:443?sni=example.com&obfs=salamander&obfs-password=secret#Hysteria2%20001".to_string(),
        ];

        let yaml = render(&configs).unwrap();
        assert!(yaml.contains("type: 'hysteria2'"));
        assert!(yaml.contains("obfs: 'salamander'"));
        assert!(yaml.contains("obfs-password: 'secret'"));
    }

    #[test]
    fn skips_unsupported_protocols() {
        let configs = vec!["tuic://password@example.com:443".to_string()];
        assert!(render(&configs).is_err());
    }
}
