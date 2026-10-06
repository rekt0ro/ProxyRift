use base64::{engine::general_purpose, Engine as _};
use lgbm::{
    parameters::{Boosting, Objective, Verbosity},
    Booster, Dataset, Field, MatBuf, Parameters, PredictType,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::BufRead;
use std::sync::Arc;

const DEFAULT_SCORE: f64 = 0.5;
const DEFAULT_TRAINING_PATH: &str = "subscriptions/light-training.jsonl";
const DEFAULT_SCORE_PATH: &str = "/tmp/proxyrift/lightgbm-scores.json";
const MIN_TRAINING_ROWS: usize = 500;
const MIN_POSITIVE_ROWS: usize = 50;
const MIN_NEGATIVE_ROWS: usize = 50;
const TRAINING_ITERATIONS: usize = 140;
const MAX_SCORE: f64 = 1.0;
const MIN_SCORE: f64 = 0.0;

const PROTOCOLS: &[&str] = &[
    "vless",
    "vmess",
    "trojan",
    "ss",
    "socks",
    "socks4",
    "socks4a",
    "socks5",
    "socks5h",
    "http",
    "hysteria",
    "hysteria2",
    "hy2",
    "tuic",
    "wg",
    "other",
];

const BACKENDS: &[&str] = &["sing-box", "xray", "fallback", "other"];

const TRANSPORTS: &[&str] = &[
    "tcp",
    "ws",
    "grpc",
    "xhttp",
    "splithttp",
    "h2",
    "httpupgrade",
    "quic",
    "wireguard",
    "raw",
    "default",
    "other",
];

const SECURITIES: &[&str] = &["none", "tls", "reality", "xtls", "default", "other"];

const PORT_BUCKETS: &[&str] = &["web", "alt-web", "other", "unknown"];

#[derive(Clone, Debug, Default)]
pub struct LightGbmScores {
    scores: HashMap<String, f64>,
    training_rows: usize,
    trained: bool,
}

impl LightGbmScores {
    pub fn train_and_score(candidates: &[String]) -> Result<Self, String> {
        if candidates.is_empty() {
            return Ok(Self::default());
        }

        let (training_features, training_labels, positive, negative) =
            load_training(DEFAULT_TRAINING_PATH)?;

        if training_features.len() < MIN_TRAINING_ROWS
            || positive < MIN_POSITIVE_ROWS
            || negative < MIN_NEGATIVE_ROWS
        {
            let scores = candidates
                .iter()
                .map(|config| (config.clone(), DEFAULT_SCORE))
                .collect::<HashMap<_, _>>();
            let result = Self {
                scores,
                training_rows: training_features.len(),
                trained: false,
            };
            result.save(DEFAULT_SCORE_PATH)?;
            return Ok(result);
        }

        let train_mat = MatBuf::from_rows_non_empty(&training_features)
            .map_err(|error| format!("failed to build LightGBM training matrix: {error}"))?
            .ok_or_else(|| "LightGBM training matrix is empty".to_string())?;

        let mut parameters = Parameters::new();
        parameters.push("objective", Objective::Binary);
        parameters.push("boosting", Boosting::Gbdt);
        parameters.push("verbosity", Verbosity::Fatal);
        parameters.push("learning_rate", 0.05);
        parameters.push("num_leaves", 15i32);
        parameters.push("min_data_in_leaf", 40i32);
        parameters.push("feature_fraction", 0.90);
        parameters.push("bagging_fraction", 0.90);
        parameters.push("bagging_freq", 1i32);
        parameters.push("lambda_l1", 0.10);
        parameters.push("lambda_l2", 0.10);
        parameters.push("scale_pos_weight", negative as f64 / positive as f64);
        parameters.push("seed", 42i32);
        parameters.push("feature_fraction_seed", 42i32);
        parameters.push("bagging_seed", 42i32);
        parameters.push("num_threads", 4i32);
        parameters.push("force_col_wise", true);
        parameters.push("deterministic", true);

        let mut train = Dataset::from_mat(&train_mat, None, &parameters)
            .map_err(|error| format!("failed to create LightGBM dataset: {error}"))?;
        train
            .set_field(Field::LABEL, &training_labels)
            .map_err(|error| format!("failed to attach LightGBM labels: {error}"))?;

        let mut booster = Booster::new(Arc::new(train), &parameters)
            .map_err(|error| format!("failed to create LightGBM booster: {error}"))?;

        for _ in 0..TRAINING_ITERATIONS {
            if booster
                .update_one_iter()
                .map_err(|error| format!("LightGBM training failed: {error}"))?
            {
                break;
            }
        }

        let candidate_features = candidates
            .iter()
            .map(|config| config_feature_vector(config))
            .collect::<Vec<_>>();

        let candidate_mat = MatBuf::from_rows_non_empty(&candidate_features)
            .map_err(|error| format!("failed to build LightGBM candidate matrix: {error}"))?
            .ok_or_else(|| "LightGBM candidate matrix is empty".to_string())?;

        let prediction_parameters = Parameters::new();
        let prediction = booster
            .predict_for_mat(
                &candidate_mat,
                PredictType::Normal,
                0,
                None,
                &prediction_parameters,
            )
            .map_err(|error| format!("LightGBM prediction failed: {error}"))?;

        if prediction.values().len() != candidates.len() {
            return Err(format!(
                "LightGBM returned {} scores for {} candidates",
                prediction.values().len(),
                candidates.len()
            ));
        }

        let scores = candidates
            .iter()
            .zip(prediction.values())
            .map(|(config, score)| (config.clone(), score.clamp(MIN_SCORE, MAX_SCORE)))
            .collect::<HashMap<_, _>>();

        let result = Self {
            scores,
            training_rows: training_features.len(),
            trained: true,
        };
        result.save(DEFAULT_SCORE_PATH)?;
        println!(
            "[INFO] 🧠 [LightGBM] Rust model trained | Rows: {} | Pass: {} | Fail: {} | Features: {} | Candidates: {}",
            training_features.len(),
            positive,
            negative,
            config_feature_vector("").len(),
            candidates.len()
        );
        Ok(result)
    }

    fn save(&self, path: &str) -> Result<(), String> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create LightGBM score directory: {error}"))?;
        }

        let payload = serde_json::json!({
            "version": 2,
            "trained": self.trained,
            "training_rows": self.training_rows,
            "feature_count": config_feature_vector("").len(),
            "scores": self.scores,
        });
        let body = serde_json::to_vec(&payload)
            .map_err(|error| format!("failed to serialize LightGBM scores: {error}"))?;
        let temporary = format!("{path}.tmp");
        fs::write(&temporary, body)
            .map_err(|error| format!("failed to write LightGBM score file: {error}"))?;
        fs::rename(&temporary, path)
            .map_err(|error| format!("failed to publish LightGBM score file: {error}"))
    }

    pub fn from_file(path: &str) -> Result<Self, String> {
        let content = fs::read_to_string(path)
            .map_err(|error| format!("failed to read LightGBM score file {path}: {error}"))?;
        let value: Value = serde_json::from_str(&content)
            .map_err(|error| format!("invalid LightGBM score file: {error}"))?;

        let training_rows = value
            .get("training_rows")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize;
        let trained = value
            .get("trained")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let mut scores = HashMap::new();
        if let Some(entries) = value.get("scores").and_then(Value::as_object) {
            for (config, score) in entries {
                let Some(score) = score.as_f64() else {
                    continue;
                };
                scores.insert(config.clone(), score.clamp(MIN_SCORE, MAX_SCORE));
            }
        }

        Ok(Self {
            scores,
            training_rows,
            trained,
        })
    }

    pub fn score(&self, config: &str) -> f64 {
        self.scores
            .get(config)
            .copied()
            .unwrap_or(DEFAULT_SCORE)
            .clamp(MIN_SCORE, MAX_SCORE)
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    pub fn training_rows(&self) -> usize {
        self.training_rows
    }

    pub fn trained(&self) -> bool {
        self.trained
    }
}

fn load_training(path: &str) -> Result<(Vec<Vec<f64>>, Vec<f32>, usize, usize), String> {
    let file = fs::File::open(path)
        .map_err(|error| format!("failed to open LightGBM training data {path}: {error}"))?;
    let reader = std::io::BufReader::new(file);

    let mut features = Vec::new();
    let mut labels = Vec::new();
    let mut positive = 0usize;
    let mut negative = 0usize;

    for line in reader.lines() {
        let line =
            line.map_err(|error| format!("failed to read LightGBM training row: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }

        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };

        let Some(label) = value.get("label").and_then(Value::as_object) else {
            continue;
        };
        let Some(strict_pass) = label.get("strict_pass").and_then(Value::as_bool) else {
            continue;
        };
        if label
            .get("strict_checks")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            == 0
        {
            continue;
        }

        let Some(stored_features) = value.get("features").and_then(Value::as_object) else {
            continue;
        };

        let Some(vector) = training_feature_vector(stored_features) else {
            continue;
        };

        features.push(vector);
        labels.push(if strict_pass { 1.0 } else { 0.0 });
        if strict_pass {
            positive += 1;
        } else {
            negative += 1;
        }
    }

    Ok((features, labels, positive, negative))
}

fn training_feature_vector(fields: &serde_json::Map<String, Value>) -> Option<Vec<f64>> {
    let protocol = fields.get("protocol")?.as_str()?.to_ascii_lowercase();
    let backend = fields.get("backend")?.as_str()?.to_ascii_lowercase();
    let transport = fields.get("transport")?.as_str()?.to_ascii_lowercase();
    let security = fields.get("security")?.as_str()?.to_ascii_lowercase();
    let port = fields.get("port")?.as_u64().unwrap_or_default();

    Some(structural_vector(
        &protocol,
        &backend,
        &transport,
        &security,
        port,
        fields
            .get("query_parameter_count")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        fields
            .get("has_sni")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        fields
            .get("has_host")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        fields
            .get("has_path")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        fields
            .get("tls_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        fields
            .get("reality_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    ))
}

fn config_feature_vector(config: &str) -> Vec<f64> {
    let fields = parse_config_features(config);
    structural_vector(
        &fields.protocol,
        &fields.backend,
        &fields.transport,
        &fields.security,
        fields.port,
        fields.query_parameter_count,
        fields.has_sni,
        fields.has_host,
        fields.has_path,
        fields.tls_enabled,
        fields.reality_enabled,
    )
}

#[derive(Debug, Default)]
struct ConfigFeatures {
    protocol: String,
    backend: String,
    transport: String,
    security: String,
    port: u64,
    query_parameter_count: u64,
    has_sni: bool,
    has_host: bool,
    has_path: bool,
    tls_enabled: bool,
    reality_enabled: bool,
}

fn parse_config_features(config: &str) -> ConfigFeatures {
    let cleaned = config.split('#').next().unwrap_or(config);
    let parsed = url::Url::parse(cleaned).ok();
    let scheme = parsed
        .as_ref()
        .map(|value| value.scheme().to_ascii_lowercase())
        .unwrap_or_else(|| "other".to_string());

    if scheme == "vmess" {
        if let Some(payload) = decode_vmess(config) {
            let transport = payload
                .get("net")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "default".to_string());
            let tls_enabled = payload.get("tls").map(value_boolish).unwrap_or(false);
            let security = if tls_enabled {
                "tls".to_string()
            } else {
                "none".to_string()
            };
            let port = payload
                .get("port")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let has_sni = ["sni", "serverName", "servername"].iter().any(|key| {
                payload
                    .get(*key)
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
            });
            let has_host = ["host", "Host"].iter().any(|key| {
                payload
                    .get(*key)
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
            });
            let has_path = payload
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty());

            return finish_features(
                "vmess".to_string(),
                transport,
                security,
                port,
                0,
                has_sni,
                has_host,
                has_path,
            );
        }
    }

    let protocol = normalize_protocol(&scheme);
    let query = parsed
        .as_ref()
        .map(|value| {
            value
                .query_pairs()
                .map(|(name, value)| (name.into_owned(), value.into_owned()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let transport = first_query_value(&query, &["type", "network", "transport", "net"])
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default_transport(&protocol));

    let security = first_query_value(&query, &["security", "tls"])
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default_security(&protocol));

    let port = parsed
        .as_ref()
        .and_then(url::Url::port)
        .map(u64::from)
        .unwrap_or_else(|| known_port(&protocol));

    let mut query_parameter_names = HashSet::new();
    for (name, _) in &query {
        query_parameter_names.insert(name.to_string());
    }

    let has_sni = has_query_key(&query, &["sni", "serverName", "servername"]);
    let has_host = has_query_key(&query, &["host", "authority"]);
    let has_path = parsed
        .as_ref()
        .is_some_and(|value| value.path() != "/" && !value.path().is_empty())
        || has_query_key(&query, &["path"]);

    finish_features(
        protocol,
        transport,
        security,
        port,
        query_parameter_names.len() as u64,
        has_sni,
        has_host,
        has_path,
    )
}

fn finish_features(
    protocol: String,
    transport: String,
    security: String,
    port: u64,
    query_parameter_count: u64,
    has_sni: bool,
    has_host: bool,
    has_path: bool,
) -> ConfigFeatures {
    let protocol = normalize_protocol(&protocol);
    let transport = normalize_transport(&transport);
    let security = normalize_security(&security);

    let backend = if matches!(
        protocol.as_str(),
        "http" | "socks" | "socks4" | "socks4a" | "socks5" | "socks5h"
    ) {
        "xray"
    } else if security == "reality" {
        "fallback"
    } else if matches!(transport.as_str(), "xhttp" | "splithttp") {
        "xray"
    } else {
        "sing-box"
    };

    ConfigFeatures {
        protocol,
        backend: backend.to_string(),
        transport,
        tls_enabled: matches!(security.as_str(), "tls" | "reality" | "xtls"),
        reality_enabled: security == "reality",
        security,
        port,
        query_parameter_count,
        has_sni,
        has_host,
        has_path,
    }
}

fn decode_vmess(config: &str) -> Option<Value> {
    let payload = config.split_once("://")?.1.split('#').next()?.trim();
    let mut candidates = vec![payload.to_string()];
    let normalized = payload.replace('-', "+").replace('_', "/");
    if normalized != payload {
        candidates.push(normalized);
    }

    for candidate in candidates {
        let mut padded = candidate;
        while !padded.len().is_multiple_of(4) {
            padded.push('=');
        }

        let Ok(decoded) = general_purpose::STANDARD.decode(padded) else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<Value>(&decoded) else {
            continue;
        };
        if value.is_object() {
            return Some(value);
        }
    }

    None
}

fn value_boolish(value: &Value) -> bool {
    match value {
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on" | "tls"
        ),
        _ => false,
    }
}

fn first_query_value(query: &[(String, String)], names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        query
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim().to_ascii_lowercase())
    })
}

fn has_query_key(query: &[(String, String)], names: &[&str]) -> bool {
    query.iter().any(|(key, value)| {
        names.iter().any(|name| key.eq_ignore_ascii_case(name)) && !value.trim().is_empty()
    })
}

fn normalize_protocol(protocol: &str) -> String {
    if PROTOCOLS.contains(&protocol) {
        protocol.to_string()
    } else {
        "other".to_string()
    }
}

fn normalize_transport(transport: &str) -> String {
    if TRANSPORTS.contains(&transport) {
        transport.to_string()
    } else {
        "other".to_string()
    }
}

fn normalize_security(security: &str) -> String {
    if SECURITIES.contains(&security) {
        security.to_string()
    } else {
        "other".to_string()
    }
}

fn default_transport(protocol: &str) -> String {
    match protocol {
        "hysteria" | "hysteria2" | "hy2" | "tuic" => "quic".to_string(),
        "wg" => "wireguard".to_string(),
        _ => "default".to_string(),
    }
}

fn default_security(protocol: &str) -> String {
    match protocol {
        "trojan" | "https" => "tls".to_string(),
        _ => "default".to_string(),
    }
}

fn known_port(protocol: &str) -> u64 {
    match protocol {
        "vless" | "vmess" | "trojan" | "ss" | "hysteria" | "hysteria2" | "hy2" | "tuic" => 443,
        "http" => 80,
        "https" => 443,
        _ => 0,
    }
}

fn port_bucket(port: u64) -> &'static str {
    match port {
        80 | 443 => "web",
        8080 | 8443 | 2053 | 2083 | 2087 | 2096 => "alt-web",
        0 => "unknown",
        _ => "other",
    }
}

fn one_hot(value: &str, vocabulary: &[&str]) -> Vec<f64> {
    let mut result = vocabulary
        .iter()
        .map(|item| f64::from(value == *item))
        .collect::<Vec<_>>();
    result.push(f64::from(!vocabulary.contains(&value)));
    result
}

fn structural_vector(
    protocol: &str,
    backend: &str,
    transport: &str,
    security: &str,
    port: u64,
    query_parameter_count: u64,
    has_sni: bool,
    has_host: bool,
    has_path: bool,
    tls_enabled: bool,
    reality_enabled: bool,
) -> Vec<f64> {
    let mut vector = Vec::with_capacity(53);
    vector.extend(one_hot(protocol, PROTOCOLS));
    vector.extend(one_hot(backend, BACKENDS));
    vector.extend(one_hot(transport, TRANSPORTS));
    vector.extend(one_hot(security, SECURITIES));
    vector.extend(one_hot(port_bucket(port), PORT_BUCKETS));
    vector.push((query_parameter_count.min(32) as f64) / 32.0);
    vector.push(f64::from(has_sni));
    vector.push(f64::from(has_host));
    vector.push(f64::from(has_path));
    vector.push(f64::from(tls_enabled));
    vector.push(f64::from(reality_enabled));
    vector
}

#[cfg(test)]
mod tests {
    use super::{config_feature_vector, parse_config_features, port_bucket};

    #[test]
    fn structural_feature_vector_has_stable_width() {
        assert_eq!(config_feature_vector("vless://example.com:443").len(), 53);
    }

    #[test]
    fn port_buckets_match_training_model() {
        assert_eq!(port_bucket(80), "web");
        assert_eq!(port_bucket(443), "web");
        assert_eq!(port_bucket(8443), "alt-web");
        assert_eq!(port_bucket(0), "unknown");
    }

    #[test]
    fn parses_structural_vless_features() {
        let features = parse_config_features(
            "vless://token@example.com:443?security=tls&type=ws&sni=edge.example.com&host=cdn.example.com&path=/proxy#label",
        );
        assert_eq!(features.protocol, "vless");
        assert_eq!(features.transport, "ws");
        assert_eq!(features.security, "tls");
        assert_eq!(features.backend, "sing-box");
        assert_eq!(features.port, 443);
        assert_eq!(features.query_parameter_count, 5);
        assert!(features.has_sni);
        assert!(features.has_host);
        assert!(features.has_path);
        assert!(features.tls_enabled);
    }
}
