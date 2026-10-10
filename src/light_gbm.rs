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
const MIN_TRAINING_ROWS: usize = 5_000;
const MIN_POSITIVE_ROWS: usize = 500;
const MIN_NEGATIVE_ROWS: usize = 500;
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
    model_report: Value,
}

const STRUCTURAL_FEATURE_COUNT: usize = 53;
const HISTORY_FEATURE_COUNT: usize = 13;
const MODEL_FEATURE_COUNT: usize = STRUCTURAL_FEATURE_COUNT + HISTORY_FEATURE_COUNT;
const HOLDOUT_FRACTION_NUMERATOR: usize = 1;
const HOLDOUT_FRACTION_DENOMINATOR: usize = 5;
const MIN_RELATIVE_BRIER_IMPROVEMENT: f64 = 0.005;

impl LightGbmScores {
    pub fn train_and_score(candidates: &[String]) -> Result<Self, String> {
        if candidates.is_empty() {
            return Ok(Self::default());
        }

        let data = load_training(DEFAULT_TRAINING_PATH)?;
        let prediction_time = current_epoch_seconds();
        let candidate_features = candidates
            .iter()
            .map(|config| {
                let mut features = config_feature_vector(config);
                let fingerprint = candidate_fingerprint(config);
                let family = feature_signature(&features);
                features.extend(history_feature_vector(
                    data.exact_history.get(&fingerprint),
                    data.family_history.get(&family),
                    prediction_time,
                ));
                features
            })
            .collect::<Vec<_>>();

        let strict = match train_target_model(
            &data.examples,
            candidates,
            &candidate_features,
            ModelTarget::Strict,
        ) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("[WARN] [LightGBM] Strict model unavailable: {error}");
                TargetResult::unavailable(ModelTarget::Strict, error)
            }
        };
        let transfer = match train_target_model(
            &data.examples,
            candidates,
            &candidate_features,
            ModelTarget::Transfer,
        ) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("[WARN] [LightGBM] Transfer model unavailable: {error}");
                TargetResult::unavailable(ModelTarget::Transfer, error)
            }
        };
        let stream = match train_target_model(
            &data.examples,
            candidates,
            &candidate_features,
            ModelTarget::Stream,
        ) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("[WARN] [LightGBM] Stream model unavailable: {error}");
                TargetResult::unavailable(ModelTarget::Stream, error)
            }
        };
        let end_to_end = match train_target_model(
            &data.examples,
            candidates,
            &candidate_features,
            ModelTarget::EndToEnd,
        ) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("[WARN] [LightGBM] End-to-end model unavailable: {error}");
                TargetResult::unavailable(ModelTarget::EndToEnd, error)
            }
        };

        let mut models = Vec::new();
        if strict.accepted {
            models.push((&strict, 0.50_f64));
        }
        if transfer.accepted {
            models.push((&transfer, 0.30_f64));
        }
        if stream.accepted {
            models.push((&stream, 0.20_f64));
        }
        let weight_sum = models.iter().map(|(_, weight)| *weight).sum::<f64>();
        let ranking_strategy = if end_to_end.accepted {
            "end_to_end"
        } else if weight_sum > 0.0 {
            "stage_blend_fallback"
        } else {
            "neutral_default"
        };
        let mut scores = HashMap::with_capacity(candidates.len());
        for candidate in candidates {
            let score = if end_to_end.accepted {
                end_to_end
                    .predictions
                    .get(candidate)
                    .copied()
                    .unwrap_or(DEFAULT_SCORE)
            } else if weight_sum > 0.0 {
                models
                    .iter()
                    .map(|(model, weight)| {
                        weight
                            * model
                                .predictions
                                .get(candidate)
                                .copied()
                                .unwrap_or(DEFAULT_SCORE)
                    })
                    .sum::<f64>()
                    / weight_sum
            } else {
                DEFAULT_SCORE
            };
            scores.insert(
                candidate.clone(),
                if score.is_finite() {
                    score.clamp(MIN_SCORE, MAX_SCORE)
                } else {
                    DEFAULT_SCORE
                },
            );
        }

        let trained = end_to_end.accepted || weight_sum > 0.0;
        let model_report = serde_json::json!({
            "validation": "walk_forward_time_split",
            "baseline": "training_prevalence_brier",
            "ranking_strategy": ranking_strategy,
            "composite_weights": {"strict": 0.50, "transfer": 0.30, "stream": 0.20},
            "promotion_minimum_relative_brier_improvement": MIN_RELATIVE_BRIER_IMPROVEMENT,
            "promotion_top_20pct_lift_minimum": 1.0,
            "history_features": HISTORY_FEATURE_COUNT,
            "model_feature_count": MODEL_FEATURE_COUNT,
            "targets": {
                "strict": strict.report,
                "transfer": transfer.report,
                "stream": stream.report,
                "end_to_end": end_to_end.report,
            }
        });
        let result = Self {
            scores,
            training_rows: data.examples.len(),
            trained,
            model_report,
        };
        result.save(DEFAULT_SCORE_PATH)?;

        let promoted_models = models.len() + if end_to_end.accepted { 1 } else { 0 };
        println!(
            "[INFO] 🧠 [LightGBM] Temporal multi-target training | Rows: {} | Features: {} | Candidates: {} | Promoted models: {} | Ranking: {}",
            data.examples.len(),
            MODEL_FEATURE_COUNT,
            candidates.len(),
            promoted_models,
            ranking_strategy
        );
        for (name, target) in [
            ("strict", &strict),
            ("transfer", &transfer),
            ("stream", &stream),
            ("end_to_end", &end_to_end),
        ] {
            println!(
                "[INFO] 🧠 [LightGBM] Temporal validation | Target: {} | Accepted: {} | Train: {} | Holdout: {} | Brier: {} | Baseline: {} | Top-20% lift: {} | Reason: {}",
                name,
                target.accepted,
                target.report.get("train_rows").and_then(Value::as_u64).unwrap_or(0),
                target.report.get("holdout_rows").and_then(Value::as_u64).unwrap_or(0),
                target.report.get("model_brier").and_then(Value::as_f64).map(|v| format!("{v:.5}")).unwrap_or_else(|| "n/a".to_string()),
                target.report.get("baseline_brier").and_then(Value::as_f64).map(|v| format!("{v:.5}")).unwrap_or_else(|| "n/a".to_string()),
                target.report.get("top_20pct_lift").and_then(Value::as_f64).map(|v| format!("{v:.3}")).unwrap_or_else(|| "n/a".to_string()),
                temporal_reason_label(target.report.get("reason").and_then(Value::as_str).unwrap_or("unknown"))
            );
        }
        Ok(result)
    }

    fn save(&self, path: &str) -> Result<(), String> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create LightGBM score directory: {error}"))?;
        }

        let payload = serde_json::json!({
            "version": 4,
            "trained": self.trained,
            "training_rows": self.training_rows,
            "feature_count": MODEL_FEATURE_COUNT,
            "model_report": self.model_report,
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
        let model_report = value.get("model_report").cloned().unwrap_or(Value::Null);

        let mut scores = HashMap::new();
        if let Some(entries) = value.get("scores").and_then(Value::as_object) {
            for (config, score) in entries {
                let Some(score) = score.as_f64() else {
                    continue;
                };
                if score.is_finite() {
                    scores.insert(config.clone(), score.clamp(MIN_SCORE, MAX_SCORE));
                }
            }
        }

        Ok(Self {
            scores,
            training_rows,
            trained,
            model_report,
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

    pub fn model_report(&self) -> &Value {
        &self.model_report
    }
}

#[derive(Clone, Copy, Debug)]
enum ModelTarget {
    Strict,
    Transfer,
    Stream,
    EndToEnd,
}

impl ModelTarget {
    fn name(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Transfer => "transfer",
            Self::Stream => "stream",
            Self::EndToEnd => "end_to_end",
        }
    }

    fn label(self, row: &TrainingExample) -> Option<bool> {
        match self {
            Self::Strict => Some(row.strict_pass),
            Self::Transfer => row.transfer_pass,
            Self::Stream => row.stream_pass,
            Self::EndToEnd => {
                if !row.strict_pass {
                    Some(false)
                } else {
                    match row.transfer_pass {
                        Some(false) => Some(false),
                        Some(true) => row.stream_pass,
                        None => None,
                    }
                }
            }
        }
    }

    fn minimum_rows(self) -> usize {
        match self {
            Self::Strict | Self::EndToEnd => MIN_TRAINING_ROWS,
            Self::Transfer => 1_000,
            Self::Stream => 500,
        }
    }

    fn minimum_training_class_rows(self) -> usize {
        match self {
            Self::Strict | Self::EndToEnd => MIN_POSITIVE_ROWS.max(MIN_NEGATIVE_ROWS),
            Self::Transfer => 100,
            Self::Stream => 50,
        }
    }

    fn minimum_timestamps(self) -> usize {
        match self {
            Self::Strict | Self::EndToEnd => 20,
            Self::Transfer | Self::Stream => 10,
        }
    }

    fn minimum_holdout_class_rows(self) -> usize {
        match self {
            Self::Strict | Self::EndToEnd => 100,
            Self::Transfer => 50,
            Self::Stream => 25,
        }
    }

    fn minimum_unique_candidates(self) -> usize {
        match self {
            Self::Strict | Self::EndToEnd => 1_000,
            Self::Transfer => 100,
            Self::Stream => 50,
        }
    }

    fn seed(self) -> i32 {
        match self {
            Self::Strict => 42,
            Self::Transfer => 43,
            Self::Stream => 44,
            Self::EndToEnd => 45,
        }
    }
}

#[derive(Clone, Debug)]
struct TrainingExample {
    observed_at: u64,
    candidate_fingerprint: String,
    features: Vec<f64>,
    strict_pass: bool,
    transfer_pass: Option<bool>,
    stream_pass: Option<bool>,
}

#[derive(Clone, Debug)]
struct RawTrainingRow {
    observed_at: u64,
    candidate_fingerprint: String,
    structural: Vec<f64>,
    family: String,
    strict_pass: bool,
    transfer_pass: Option<bool>,
    stream_pass: Option<bool>,
    early_attempts: u64,
    early_success_rate: f64,
    early_median_ms: f64,
    early_jitter_ms: f64,
    early_throughput_kbps: f64,
}

#[derive(Clone, Debug, Default)]
struct RollingStats {
    observations: usize,
    strict_passes: usize,
    transfer_tests: usize,
    transfer_passes: usize,
    stream_tests: usize,
    stream_passes: usize,
    early_metrics: usize,
    early_success_rate_sum: f64,
    early_median_ms_sum: f64,
    early_jitter_ms_sum: f64,
    early_throughput_kbps_sum: f64,
    last_seen: u64,
}

impl RollingStats {
    fn record(&mut self, row: &RawTrainingRow) {
        self.observations += 1;
        self.strict_passes += usize::from(row.strict_pass);
        if let Some(passed) = row.transfer_pass {
            self.transfer_tests += 1;
            self.transfer_passes += usize::from(passed);
        }
        if let Some(passed) = row.stream_pass {
            self.stream_tests += 1;
            self.stream_passes += usize::from(passed);
        }
        if row.early_attempts > 0 {
            self.early_metrics += 1;
            self.early_success_rate_sum += row.early_success_rate.clamp(0.0, 1.0);
            self.early_median_ms_sum += row.early_median_ms.clamp(0.0, 30_000.0);
            self.early_jitter_ms_sum += row.early_jitter_ms.clamp(0.0, 30_000.0);
            self.early_throughput_kbps_sum += row.early_throughput_kbps.clamp(0.0, 1_000_000.0);
        }
        self.last_seen = self.last_seen.max(row.observed_at);
    }

    fn strict_rate(&self) -> f64 {
        (self.strict_passes as f64 + 2.0) / (self.observations as f64 + 4.0)
    }

    fn transfer_rate(&self) -> f64 {
        (self.transfer_passes as f64 + 2.0) / (self.transfer_tests as f64 + 4.0)
    }

    fn stream_rate(&self) -> f64 {
        (self.stream_passes as f64 + 2.0) / (self.stream_tests as f64 + 4.0)
    }

    fn performance_features(&self) -> [f64; 4] {
        if self.early_metrics == 0 {
            return [0.5, 0.5, 0.5, 0.5];
        }
        let count = self.early_metrics as f64;
        [
            (self.early_success_rate_sum / count).clamp(0.0, 1.0),
            normalized_log(self.early_median_ms_sum / count, 30_000.0),
            normalized_log(self.early_jitter_ms_sum / count, 30_000.0),
            normalized_log(self.early_throughput_kbps_sum / count, 1_000_000.0),
        ]
    }
}

struct TrainingData {
    examples: Vec<TrainingExample>,
    exact_history: HashMap<String, RollingStats>,
    family_history: HashMap<String, RollingStats>,
}

fn temporal_reason_label(reason: &str) -> String {
    match reason {
        "promoted_after_temporal_validation" => "Promoted after temporal validation".to_string(),
        "model_did_not_beat_temporal_baseline" => {
            "Model did not beat temporal baseline".to_string()
        }
        "top_20pct_below_random_selection_baseline" => {
            "Top-20% below random selection baseline".to_string()
        }
        other => {
            let mut readable = other.replace('_', " ");
            if let Some(first) = readable.get_mut(..1) {
                first.make_ascii_uppercase();
            }
            readable
        }
    }
}

struct TargetResult {
    accepted: bool,
    predictions: HashMap<String, f64>,
    report: Value,
}

impl TargetResult {
    fn unavailable(target: ModelTarget, reason: String) -> Self {
        Self {
            accepted: false,
            predictions: HashMap::new(),
            report: serde_json::json!({
                "target": target.name(),
                "accepted": false,
                "reason": reason,
            }),
        }
    }
}

fn current_epoch_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn finite_feature(fields: &serde_json::Map<String, Value>, name: &str) -> f64 {
    fields
        .get(name)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .unwrap_or_default()
}

fn load_training(path: &str) -> Result<TrainingData, String> {
    let file = fs::File::open(path)
        .map_err(|error| format!("failed to open LightGBM training data {path}: {error}"))?;
    let reader = std::io::BufReader::new(file);
    let mut raw_rows = Vec::new();

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
        let Some(structural) = training_feature_vector(stored_features) else {
            continue;
        };
        let Some(candidate_fingerprint) = value
            .get("candidate_fingerprint")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            continue;
        };
        let transfer_tested = label
            .get("transfer_tested")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let transfer_pass = if transfer_tested {
            label.get("transfer_pass").and_then(Value::as_bool)
        } else {
            None
        };
        let stream_tested = label
            .get("stream_tested")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let stream_pass = if stream_tested {
            label.get("stream_pass").and_then(Value::as_bool)
        } else {
            None
        };
        let observed_at = value
            .get("observed_at")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let family = feature_signature(&structural);
        raw_rows.push(RawTrainingRow {
            observed_at,
            candidate_fingerprint,
            structural,
            family,
            strict_pass,
            transfer_pass,
            stream_pass,
            early_attempts: stored_features
                .get("early_attempts")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            early_success_rate: finite_feature(stored_features, "early_success_rate"),
            early_median_ms: finite_feature(stored_features, "early_median_ms"),
            early_jitter_ms: finite_feature(stored_features, "early_jitter_ms"),
            early_throughput_kbps: finite_feature(stored_features, "early_throughput_kbps"),
        });
    }

    raw_rows.sort_unstable_by_key(|row| row.observed_at);
    let mut exact_history = HashMap::<String, RollingStats>::new();
    let mut family_history = HashMap::<String, RollingStats>::new();
    let mut examples = Vec::with_capacity(raw_rows.len());
    let mut index = 0usize;

    while index < raw_rows.len() {
        let timestamp = raw_rows[index].observed_at;
        let mut end = index + 1;
        while end < raw_rows.len() && raw_rows[end].observed_at == timestamp {
            end += 1;
        }

        for row in &raw_rows[index..end] {
            let exact = exact_history.get(&row.candidate_fingerprint);
            let family = family_history.get(&row.family);
            let mut features = row.structural.clone();
            features.extend(history_feature_vector(exact, family, timestamp));
            if features.len() != MODEL_FEATURE_COUNT {
                return Err(format!(
                    "LightGBM feature contract mismatch: expected {MODEL_FEATURE_COUNT}, got {}",
                    features.len()
                ));
            }
            examples.push(TrainingExample {
                observed_at: row.observed_at,
                candidate_fingerprint: row.candidate_fingerprint.clone(),
                features,
                strict_pass: row.strict_pass,
                transfer_pass: row.transfer_pass,
                stream_pass: row.stream_pass,
            });
        }

        for row in &raw_rows[index..end] {
            exact_history
                .entry(row.candidate_fingerprint.clone())
                .or_default()
                .record(row);
            family_history
                .entry(row.family.clone())
                .or_default()
                .record(row);
        }
        index = end;
    }

    Ok(TrainingData {
        examples,
        exact_history,
        family_history,
    })
}

fn normalized_log(value: f64, maximum: f64) -> f64 {
    if !value.is_finite() || maximum <= 0.0 {
        return 0.0;
    }
    ((1.0 + value.clamp(0.0, maximum)).ln() / (1.0 + maximum).ln()).clamp(0.0, 1.0)
}

fn history_feature_vector(
    exact: Option<&RollingStats>,
    family: Option<&RollingStats>,
    now: u64,
) -> Vec<f64> {
    let exact = exact.cloned().unwrap_or_default();
    let family = family.cloned().unwrap_or_default();
    let family_performance = family.performance_features();
    let age_secs = now.saturating_sub(family.last_seen) as f64;
    let recency = if family.observations == 0 {
        0.0
    } else {
        (-age_secs / (30.0 * 24.0 * 60.0 * 60.0)).exp()
    };

    vec![
        normalized_log(exact.observations as f64, 50_000.0),
        exact.strict_rate(),
        exact.transfer_rate(),
        exact.stream_rate(),
        normalized_log(family.observations as f64, 50_000.0),
        family.strict_rate(),
        family.transfer_rate(),
        family.stream_rate(),
        family_performance[0],
        family_performance[1],
        family_performance[2],
        family_performance[3],
        recency.clamp(0.0, 1.0),
    ]
}

fn feature_signature(features: &[f64]) -> String {
    features
        .iter()
        .map(|feature| format!("{:016x}", feature.to_bits()))
        .collect::<Vec<_>>()
        .join("")
}

fn candidate_fingerprint(config: &str) -> String {
    let cleaned = config.split('#').next().unwrap_or(config);
    let identity = if cleaned
        .split_once("://")
        .map(|(scheme, _)| scheme.eq_ignore_ascii_case("vmess"))
        .unwrap_or(false)
    {
        decode_vmess(cleaned)
            .and_then(|mut value| {
                if let Value::Object(object) = &mut value {
                    object.remove("ps");
                }
                serde_json::to_string(&value).ok()
            })
            .unwrap_or_else(|| cleaned.to_string())
    } else {
        cleaned.to_string()
    };
    let first = fnv64(identity.as_bytes(), 0xcbf29ce484222325);
    let second = fnv64(identity.as_bytes(), 0x9e3779b97f4a7c15);
    format!("{first:016x}{second:016x}")
}

fn fnv64(input: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for byte in input {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn brier_score(predictions: &[f64], labels: &[f32]) -> f64 {
    if predictions.is_empty() || predictions.len() != labels.len() {
        return f64::INFINITY;
    }
    predictions
        .iter()
        .zip(labels)
        .map(|(prediction, label)| {
            let difference = prediction.clamp(0.0, 1.0) - f64::from(*label);
            difference * difference
        })
        .sum::<f64>()
        / predictions.len() as f64
}

fn top_quintile_pass_rate(predictions: &[f64], labels: &[f32]) -> f64 {
    if predictions.is_empty() || predictions.len() != labels.len() {
        return f64::NAN;
    }

    let mut indices = (0..predictions.len()).collect::<Vec<_>>();
    indices.sort_unstable_by(|left, right| predictions[*right].total_cmp(&predictions[*left]));

    let top_count = predictions.len().div_ceil(5).max(1);
    let mut selected_passes = 0.0_f64;
    let mut remaining = top_count;
    let mut start = 0;

    while start < indices.len() && remaining > 0 {
        let score = predictions[indices[start]];
        let mut end = start + 1;
        while end < indices.len() && predictions[indices[end]].total_cmp(&score).is_eq() {
            end += 1;
        }

        let group_size = end - start;
        let selected_from_group = remaining.min(group_size);
        let group_passes = indices[start..end]
            .iter()
            .filter(|index| labels[**index] >= 0.5)
            .count();
        selected_passes += (selected_from_group as f64 / group_size as f64) * group_passes as f64;
        remaining -= selected_from_group;
        start = end;
    }

    selected_passes / top_count as f64
}

fn fit_booster(features: &[Vec<f64>], labels: &[f32], seed: i32) -> Result<Booster, String> {
    if features.is_empty() || features.len() != labels.len() {
        return Err("cannot train LightGBM with empty or mismatched training rows".to_string());
    }
    let matrix = MatBuf::from_rows_non_empty(features)
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
    parameters.push("seed", seed);
    parameters.push("feature_fraction_seed", seed);
    parameters.push("bagging_seed", seed);
    parameters.push("num_threads", 4i32);
    parameters.push("force_col_wise", true);
    parameters.push("deterministic", true);

    let mut train = Dataset::from_mat(&matrix, None, &parameters)
        .map_err(|error| format!("failed to create LightGBM dataset: {error}"))?;
    train
        .set_field(Field::LABEL, labels)
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
    Ok(booster)
}

fn predict_booster(booster: &Booster, features: &[Vec<f64>]) -> Result<Vec<f64>, String> {
    if features.is_empty() {
        return Ok(Vec::new());
    }
    let matrix = MatBuf::from_rows_non_empty(features)
        .map_err(|error| format!("failed to build LightGBM prediction matrix: {error}"))?
        .ok_or_else(|| "LightGBM prediction matrix is empty".to_string())?;
    let prediction = booster
        .predict_for_mat(&matrix, PredictType::Normal, 0, None, &Parameters::new())
        .map_err(|error| format!("LightGBM prediction failed: {error}"))?;
    if prediction.values().len() != features.len() {
        return Err(format!(
            "LightGBM returned {} scores for {} rows",
            prediction.values().len(),
            features.len()
        ));
    }
    Ok(prediction
        .values()
        .iter()
        .map(|score| {
            if score.is_finite() {
                score.clamp(MIN_SCORE, MAX_SCORE)
            } else {
                DEFAULT_SCORE
            }
        })
        .collect())
}

fn train_target_model(
    examples: &[TrainingExample],
    candidates: &[String],
    candidate_features: &[Vec<f64>],
    target: ModelTarget,
) -> Result<TargetResult, String> {
    let mut rows = examples
        .iter()
        .filter_map(|row| target.label(row).map(|label| (row, label)))
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(row, _)| row.observed_at);
    let positive = rows.iter().filter(|(_, label)| *label).count();
    let negative = rows.len().saturating_sub(positive);
    let unique_candidates = rows
        .iter()
        .map(|(row, _)| row.candidate_fingerprint.as_str())
        .collect::<HashSet<_>>()
        .len();
    if rows.len() < target.minimum_rows()
        || positive < target.minimum_training_class_rows()
        || negative < target.minimum_training_class_rows()
        || unique_candidates < target.minimum_unique_candidates()
    {
        return Ok(TargetResult::unavailable(
            target,
            format!(
                "insufficient labelled data (rows={}, pass={}, fail={}, unique_candidates={})",
                rows.len(),
                positive,
                negative,
                unique_candidates
            ),
        ));
    }

    let mut timestamps = rows
        .iter()
        .map(|(row, _)| row.observed_at)
        .collect::<Vec<_>>();
    timestamps.sort_unstable();
    timestamps.dedup();
    if timestamps.len() < target.minimum_timestamps() {
        return Ok(TargetResult::unavailable(
            target,
            format!(
                "insufficient distinct observation timestamps (found={}, required={})",
                timestamps.len(),
                target.minimum_timestamps()
            ),
        ));
    }
    let holdout_start_index = (timestamps.len()
        * (HOLDOUT_FRACTION_DENOMINATOR - HOLDOUT_FRACTION_NUMERATOR))
        / HOLDOUT_FRACTION_DENOMINATOR;
    let holdout_start_index = holdout_start_index.clamp(1, timestamps.len() - 1);
    let cutoff = timestamps[holdout_start_index];
    let training = rows
        .iter()
        .filter(|(row, _)| row.observed_at < cutoff)
        .collect::<Vec<_>>();
    let holdout = rows
        .iter()
        .filter(|(row, _)| row.observed_at >= cutoff)
        .collect::<Vec<_>>();
    let training_positive = training.iter().filter(|(_, label)| *label).count();
    let training_negative = training.len().saturating_sub(training_positive);
    let holdout_positive = holdout.iter().filter(|(_, label)| *label).count();
    let holdout_negative = holdout.len().saturating_sub(holdout_positive);

    if training.len() < target.minimum_rows() / 2
        || training_positive < target.minimum_training_class_rows()
        || training_negative < target.minimum_training_class_rows()
        || holdout.len() < 100
        || holdout_positive < target.minimum_holdout_class_rows()
        || holdout_negative < target.minimum_holdout_class_rows()
    {
        let mut result = TargetResult::unavailable(
            target,
            format!(
                "time holdout lacks representative classes (train={}/{}/{}, holdout={}/{}/{})",
                training.len(),
                training_positive,
                training_negative,
                holdout.len(),
                holdout_positive,
                holdout_negative
            ),
        );
        result.report["train_rows"] = Value::from(training.len() as u64);
        result.report["holdout_rows"] = Value::from(holdout.len() as u64);
        return Ok(result);
    }

    let train_features = training
        .iter()
        .map(|(row, _)| row.features.clone())
        .collect::<Vec<_>>();
    let train_labels = training
        .iter()
        .map(|(_, label)| if *label { 1.0_f32 } else { 0.0_f32 })
        .collect::<Vec<_>>();
    let holdout_features = holdout
        .iter()
        .map(|(row, _)| row.features.clone())
        .collect::<Vec<_>>();
    let holdout_labels = holdout
        .iter()
        .map(|(_, label)| if *label { 1.0_f32 } else { 0.0_f32 })
        .collect::<Vec<_>>();
    let baseline_rate = train_labels
        .iter()
        .map(|label| f64::from(*label))
        .sum::<f64>()
        / train_labels.len() as f64;
    let evaluation_booster = fit_booster(&train_features, &train_labels, target.seed())?;
    let holdout_predictions = predict_booster(&evaluation_booster, &holdout_features)?;
    let model_brier = brier_score(&holdout_predictions, &holdout_labels);
    let baseline_brier = brier_score(&vec![baseline_rate; holdout_labels.len()], &holdout_labels);
    let relative_improvement = if baseline_brier > 0.0 {
        (baseline_brier - model_brier) / baseline_brier
    } else {
        0.0
    };
    let holdout_pass_rate = holdout_positive as f64 / holdout.len() as f64;
    let model_top_20pct_pass_rate = top_quintile_pass_rate(&holdout_predictions, &holdout_labels);
    let top_20pct_lift = if holdout_pass_rate > 0.0 {
        model_top_20pct_pass_rate / holdout_pass_rate
    } else {
        0.0
    };
    let brier_accepted = model_brier.is_finite()
        && baseline_brier.is_finite()
        && relative_improvement >= MIN_RELATIVE_BRIER_IMPROVEMENT;
    let ranking_accepted = model_top_20pct_pass_rate.is_finite()
        && model_top_20pct_pass_rate + 1e-9 >= holdout_pass_rate;
    let accepted = brier_accepted && ranking_accepted;
    let reason = if !brier_accepted {
        "model_did_not_beat_temporal_baseline"
    } else if !ranking_accepted {
        "top_20pct_below_random_selection_baseline"
    } else {
        "better_brier_and_non_degrading_top_20pct"
    };

    let mut report = serde_json::json!({
        "target": target.name(),
        "accepted": accepted,
        "labelled_rows": rows.len(),
        "pass_rows": positive,
        "fail_rows": negative,
        "train_rows": training.len(),
        "train_pass_rows": training_positive,
        "train_fail_rows": training_negative,
        "holdout_rows": holdout.len(),
        "holdout_pass_rows": holdout_positive,
        "holdout_fail_rows": holdout_negative,
        "cutoff_timestamp": cutoff,
        "model_brier": model_brier,
        "baseline_brier": baseline_brier,
        "relative_brier_improvement": relative_improvement,
        "holdout_pass_rate": holdout_pass_rate,
        "model_top_20pct_pass_rate": model_top_20pct_pass_rate,
        "top_20pct_lift": top_20pct_lift,
        "top_20pct_non_degrading": ranking_accepted,
        "reason": reason,
    });
    if !accepted {
        return Ok(TargetResult {
            accepted: false,
            predictions: HashMap::new(),
            report,
        });
    }

    let all_features = rows
        .iter()
        .map(|(row, _)| row.features.clone())
        .collect::<Vec<_>>();
    let all_labels = rows
        .iter()
        .map(|(_, label)| if *label { 1.0_f32 } else { 0.0_f32 })
        .collect::<Vec<_>>();
    let production_booster = fit_booster(&all_features, &all_labels, target.seed())?;
    let predictions = predict_booster(&production_booster, candidate_features)?;
    if predictions.len() != candidates.len() {
        return Err(format!(
            "{} model produced {} candidate predictions for {} candidates",
            target.name(),
            predictions.len(),
            candidates.len()
        ));
    }
    let predictions = candidates
        .iter()
        .zip(predictions)
        .map(|(config, score)| (config.clone(), score))
        .collect();

    report["reason"] = Value::from("promoted_after_temporal_validation");
    Ok(TargetResult {
        accepted: true,
        predictions,
        report,
    })
}

fn training_feature_vector(fields: &serde_json::Map<String, Value>) -> Option<Vec<f64>> {
    let protocol = normalize_protocol(
        fields
            .get("protocol")?
            .as_str()?
            .to_ascii_lowercase()
            .as_str(),
    );
    let raw_backend = fields.get("backend")?.as_str()?.to_ascii_lowercase();
    let backend = if BACKENDS.contains(&raw_backend.as_str()) {
        raw_backend
    } else {
        "other".to_string()
    };
    let raw_transport = fields.get("transport")?.as_str()?.to_ascii_lowercase();
    let transport = normalize_transport(if raw_transport == "vmess-default" {
        "default"
    } else {
        raw_transport.as_str()
    });
    let security = normalize_security(
        fields
            .get("security")?
            .as_str()?
            .to_ascii_lowercase()
            .as_str(),
    );
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

    let query_parameter_count = query.len() as u64;

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
        query_parameter_count,
        has_sni,
        has_host,
        has_path,
    )
}

#[allow(clippy::too_many_arguments)]
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

#[allow(clippy::too_many_arguments)]
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
    use super::{
        candidate_fingerprint, config_feature_vector, history_feature_vector, load_training,
        parse_config_features, port_bucket, top_quintile_pass_rate, ModelTarget, TrainingExample,
        HISTORY_FEATURE_COUNT, MODEL_FEATURE_COUNT,
    };
    use serde_json::{json, Map, Value};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> String {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        format!(
            "{}/proxyrift-light-gbm-{nonce}.jsonl",
            std::env::temp_dir().display()
        )
    }

    fn stored_row(timestamp: u64, fingerprint: &str, passed: bool) -> Value {
        json!({
            "schema_version": 2,
            "observed_at": timestamp,
            "candidate_fingerprint": fingerprint,
            "observation_id": format!("{timestamp}:{fingerprint}:{passed}"),
            "features": {
                "protocol": "vless",
                "backend": "sing-box",
                "transport": "tcp",
                "security": "tls",
                "port": 443,
                "query_parameter_count": 0,
                "has_sni": true,
                "has_host": false,
                "has_path": false,
                "tls_enabled": true,
                "reality_enabled": false,
                "early_attempts": 0,
                "early_success_rate": 0.0,
                "early_median_ms": 0.0,
                "early_min_ms": 0.0,
                "early_jitter_ms": 0.0,
                "early_throughput_kbps": 0.0,
                "history_checks": 0,
                "history_pass_rate": 0.5
            },
            "label": {
                "strict_pass": passed,
                "strict_checks": 1,
                "transfer_tested": false,
                "transfer_pass": null,
                "stream_tested": false,
                "stream_pass": null
            }
        })
    }

    fn training_example(
        strict_pass: bool,
        transfer_pass: Option<bool>,
        stream_pass: Option<bool>,
    ) -> TrainingExample {
        TrainingExample {
            observed_at: 1,
            candidate_fingerprint: "candidate".to_string(),
            features: Vec::new(),
            strict_pass,
            transfer_pass,
            stream_pass,
        }
    }

    #[test]
    fn end_to_end_labels_respect_censored_pipeline_stages() {
        assert_eq!(
            ModelTarget::EndToEnd.label(&training_example(false, None, None)),
            Some(false)
        );
        assert_eq!(
            ModelTarget::EndToEnd.label(&training_example(true, Some(false), None)),
            Some(false)
        );
        assert_eq!(
            ModelTarget::EndToEnd.label(&training_example(true, Some(true), Some(false))),
            Some(false)
        );
        assert_eq!(
            ModelTarget::EndToEnd.label(&training_example(true, Some(true), Some(true))),
            Some(true)
        );
        assert_eq!(
            ModelTarget::EndToEnd.label(&training_example(true, None, None)),
            None
        );
        assert_eq!(
            ModelTarget::EndToEnd.label(&training_example(true, Some(true), None)),
            None
        );
    }

    #[test]
    fn top_quintile_metric_rewards_a_useful_ranking() {
        let predictions = [0.1, 0.2, 0.8, 0.4, 0.9];
        let labels = [0.0, 0.0, 1.0, 0.0, 1.0];
        assert_eq!(top_quintile_pass_rate(&predictions, &labels), 1.0);
    }

    #[test]
    fn top_quintile_metric_detects_an_inverted_ranking() {
        let predictions = [0.9, 0.8, 0.2, 0.4, 0.1];
        let labels = [0.0, 0.0, 1.0, 1.0, 1.0];
        assert_eq!(top_quintile_pass_rate(&predictions, &labels), 0.0);
    }

    #[test]
    fn tied_scores_use_expected_top_quintile_rate() {
        let predictions = [0.5, 0.5, 0.5, 0.5, 0.5];
        let labels = [0.0, 0.0, 1.0, 1.0, 1.0];
        assert!((top_quintile_pass_rate(&predictions, &labels) - 0.6).abs() < 1e-9);
    }

    #[test]
    fn structural_feature_vector_has_stable_width() {
        assert_eq!(config_feature_vector("vless://example.com:443").len(), 53);
        assert_eq!(MODEL_FEATURE_COUNT, 53 + HISTORY_FEATURE_COUNT);
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

    #[test]
    fn query_parameter_feature_counts_pairs_consistently() {
        let config = "vless://token@example.com:443?type=ws&fp=chrome&fp=safari";
        let parsed = parse_config_features(config);
        let structural = config_feature_vector(config);
        let fields = Map::from_iter([
            ("protocol".to_string(), Value::from("vless")),
            ("backend".to_string(), Value::from("sing-box")),
            ("transport".to_string(), Value::from("ws")),
            ("security".to_string(), Value::from("default")),
            ("port".to_string(), Value::from(443_u64)),
            ("query_parameter_count".to_string(), Value::from(3_u64)),
            ("has_sni".to_string(), Value::from(false)),
            ("has_host".to_string(), Value::from(false)),
            ("has_path".to_string(), Value::from(false)),
            ("tls_enabled".to_string(), Value::from(false)),
            ("reality_enabled".to_string(), Value::from(false)),
        ]);
        let stored = super::training_feature_vector(&fields).expect("stored features");
        assert_eq!(parsed.query_parameter_count, 3);
        assert_eq!(structural, stored);
    }

    #[test]
    fn temporal_history_features_do_not_leak_same_run_labels() {
        let path = temp_path();
        let mut rows = Vec::new();
        for index in 0..3 {
            rows.push(serde_json::to_string(&stored_row(100, "same-candidate", true)).unwrap());
            if index == 0 {
                rows.push(
                    serde_json::to_string(&stored_row(100, "other-candidate", false)).unwrap(),
                );
            }
        }
        rows.push(serde_json::to_string(&stored_row(200, "same-candidate", true)).unwrap());
        fs::write(&path, rows.join("\n") + "\n").expect("write training data");

        let data = load_training(&path).expect("load training data");
        assert!(data
            .examples
            .iter()
            .all(|row| row.transfer_pass.is_none() && row.stream_pass.is_none()));
        let same_time = data
            .examples
            .iter()
            .filter(|row| row.observed_at == 100)
            .collect::<Vec<_>>();
        assert!(same_time.iter().all(|row| row.features[53] == 0.0));
        let later = data
            .examples
            .iter()
            .find(|row| row.observed_at == 200)
            .expect("later row");
        assert!((later.features[54] - (5.0 / 7.0)).abs() < 1e-9);
        assert_eq!(later.features.len(), MODEL_FEATURE_COUNT);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn fingerprints_ignore_config_labels() {
        assert_eq!(
            candidate_fingerprint("vless://token@example.com:443#label-a"),
            candidate_fingerprint("vless://token@example.com:443#label-b")
        );
    }

    #[test]
    fn history_vector_uses_neutral_priors_for_unseen_candidates() {
        let vector = history_feature_vector(None, None, 100);
        assert_eq!(vector.len(), HISTORY_FEATURE_COUNT);
        assert_eq!(vector[0], 0.0);
        assert_eq!(vector[1], 0.5);
        assert_eq!(vector[5], 0.5);
        assert_eq!(vector[12], 0.0);
    }
}
