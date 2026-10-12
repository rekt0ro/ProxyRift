use crate::light_training::FEATURE_EXTRACTION_VERSION;
use base64::{engine::general_purpose, Engine as _};
use lgbm::{
    parameters::{Boosting, Objective, Verbosity},
    Booster, Dataset, Field, MatBuf, Parameters, PredictType,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
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
const RECENCY_WEIGHT_HALF_LIFE_SECS: f64 = 3.0 * 24.0 * 60.0 * 60.0;
const MIN_RECENCY_WEIGHT: f64 = 0.10;

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
        let historical_fallback = candidate_features
            .iter()
            .any(|features| historical_prior_has_signal(features));
        let ranking_strategy = if end_to_end.accepted {
            "end_to_end"
        } else if weight_sum > 0.0 {
            "stage_model_fallback"
        } else if historical_fallback {
            "historical_prior_fallback"
        } else {
            "neutral_default"
        };
        let mut scores = HashMap::with_capacity(candidates.len());
        for (candidate, features) in candidates.iter().zip(&candidate_features) {
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
                historical_prior_score(features)
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
            "validation": "single_temporal_holdout_unseen_candidates_first_observation",
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
        "model_did_not_beat_unseen_candidate_baseline" => {
            "Model did not beat unseen-candidate baseline".to_string()
        }
        "top_20pct_below_random_selection_baseline" => {
            "Top-20% below random selection baseline".to_string()
        }
        "top_20pct_below_unseen_candidate_baseline" => {
            "Top-20% below unseen-candidate random-selection baseline".to_string()
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
    let stored_rows = crate::light_training::load_compatible_rows_for_training(path)?;
    let mut raw_rows = Vec::new();

    for value in stored_rows {
        if value
            .get("feature_extraction_version")
            .and_then(Value::as_u64)
            != Some(FEATURE_EXTRACTION_VERSION)
        {
            continue;
        }
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
        let stream_validation_version = label
            .get("stream_validation_version")
            .and_then(Value::as_u64)
            .unwrap_or(1);
        let stream_tested = stream_validation_version >= 2
            && label
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

fn denormalized_log(value: f64, maximum: f64) -> f64 {
    if !value.is_finite() || maximum <= 0.0 {
        return 0.0;
    }

    ((value.clamp(0.0, 1.0) * (1.0 + maximum).ln()).exp() - 1.0).clamp(0.0, maximum)
}

fn historical_prior_has_signal(features: &[f64]) -> bool {
    if features.len() < MODEL_FEATURE_COUNT {
        return false;
    }

    let history = MODEL_FEATURE_COUNT - HISTORY_FEATURE_COUNT;
    features[history] > 0.0 || features[history + 4] > 0.0
}

fn historical_prior_score(features: &[f64]) -> f64 {
    if features.len() < MODEL_FEATURE_COUNT {
        return DEFAULT_SCORE;
    }

    let history = MODEL_FEATURE_COUNT - HISTORY_FEATURE_COUNT;
    let exact_observations = denormalized_log(features[history], 50_000.0);
    let family_observations = denormalized_log(features[history + 4], 50_000.0);
    let exact_weight = exact_observations / (exact_observations + 8.0);
    let family_weight =
        family_observations / (family_observations + 20.0) * features[history + 12].clamp(0.0, 1.0);
    let total_weight = exact_weight + family_weight;

    if total_weight <= 0.0 {
        return DEFAULT_SCORE;
    }

    let exact_quality = 0.65 * features[history + 1].clamp(0.0, 1.0)
        + 0.20 * features[history + 2].clamp(0.0, 1.0)
        + 0.15 * features[history + 3].clamp(0.0, 1.0);
    let family_quality = 0.55 * features[history + 5].clamp(0.0, 1.0)
        + 0.20 * features[history + 6].clamp(0.0, 1.0)
        + 0.15 * features[history + 7].clamp(0.0, 1.0)
        + 0.10 * features[history + 8].clamp(0.0, 1.0);
    let observed_quality =
        (exact_weight * exact_quality + family_weight * family_quality) / total_weight;
    let confidence = total_weight / (total_weight + 0.5);
    let performance_adjustment = family_weight / total_weight
        * (0.08 * (features[history + 11].clamp(0.0, 1.0) - 0.5)
            - 0.04 * (features[history + 9].clamp(0.0, 1.0) - 0.5)
            - 0.02 * (features[history + 10].clamp(0.0, 1.0) - 0.5));

    (DEFAULT_SCORE
        + confidence * (observed_quality - DEFAULT_SCORE)
        + confidence * performance_adjustment)
        .clamp(MIN_SCORE, MAX_SCORE)
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

fn recency_weight(observed_at: u64, as_of: u64) -> f32 {
    let age_secs = as_of.saturating_sub(observed_at) as f64;
    2.0_f64
        .powf(-age_secs / RECENCY_WEIGHT_HALF_LIFE_SECS)
        .max(MIN_RECENCY_WEIGHT)
        .clamp(MIN_RECENCY_WEIGHT, 1.0) as f32
}

fn fit_booster(features: &[Vec<f64>], labels: &[f32], seed: i32) -> Result<Booster, String> {
    fit_booster_with_optional_weights(features, labels, None, seed)
}

fn fit_booster_with_weights(
    features: &[Vec<f64>],
    labels: &[f32],
    sample_weights: &[f32],
    seed: i32,
) -> Result<Booster, String> {
    fit_booster_with_optional_weights(features, labels, Some(sample_weights), seed)
}

fn fit_booster_with_optional_weights(
    features: &[Vec<f64>],
    labels: &[f32],
    sample_weights: Option<&[f32]>,
    seed: i32,
) -> Result<Booster, String> {
    if features.is_empty() || features.len() != labels.len() {
        return Err("cannot train LightGBM with empty or mismatched training rows".to_string());
    }
    if sample_weights.is_some_and(|weights| weights.len() != labels.len()) {
        return Err("cannot train LightGBM with mismatched sample weights".to_string());
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
    if let Some(weights) = sample_weights {
        train
            .set_field(Field::WEIGHT, weights)
            .map_err(|error| format!("failed to attach LightGBM sample weights: {error}"))?;
    }
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

fn target_label_history(
    rows: &[(&TrainingExample, bool)],
    cutoff: u64,
) -> (HashSet<String>, HashMap<String, u64>) {
    let previously_seen_candidates = rows
        .iter()
        .filter(|(row, _)| row.observed_at < cutoff)
        .map(|(row, _)| row.candidate_fingerprint.clone())
        .collect::<HashSet<_>>();
    let mut first_observed_at = HashMap::<String, u64>::new();
    for (row, _) in rows {
        first_observed_at
            .entry(row.candidate_fingerprint.clone())
            .and_modify(|timestamp| *timestamp = (*timestamp).min(row.observed_at))
            .or_insert(row.observed_at);
    }
    (previously_seen_candidates, first_observed_at)
}

fn first_unseen_holdout_indices(
    rows: &[(&TrainingExample, bool)],
    cutoff: u64,
    previously_seen_candidates: &HashSet<String>,
    first_observed_at: &HashMap<String, u64>,
) -> Vec<usize> {
    let mut selected_candidates = HashSet::new();

    rows.iter()
        .enumerate()
        .filter_map(|(index, (row, _))| {
            if row.observed_at < cutoff
                || previously_seen_candidates.contains(&row.candidate_fingerprint)
                || first_observed_at.get(&row.candidate_fingerprint) != Some(&row.observed_at)
            {
                return None;
            }
            selected_candidates
                .insert(row.candidate_fingerprint.clone())
                .then_some(index)
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
struct ValidationMetrics {
    model_brier: f64,
    baseline_brier: f64,
    relative_brier_improvement: f64,
    model_top_20pct_pass_rate: f64,
    holdout_pass_rate: f64,
    brier_accepted: bool,
    ranking_accepted: bool,
    accepted: bool,
}

fn evaluate_predictions(
    predictions: &[f64],
    labels: &[f32],
    baseline_rate: f64,
) -> ValidationMetrics {
    let baseline_predictions = vec![baseline_rate; labels.len()];
    let baseline_brier = brier_score(&baseline_predictions, labels);
    let model_brier = brier_score(predictions, labels);
    let model_top_20pct_pass_rate = top_quintile_pass_rate(predictions, labels);
    let holdout_pass_rate = if labels.is_empty() {
        0.0
    } else {
        labels.iter().map(|label| f64::from(*label)).sum::<f64>() / labels.len() as f64
    };
    let relative_brier_improvement = if baseline_brier > 0.0 {
        (baseline_brier - model_brier) / baseline_brier
    } else {
        0.0
    };
    let brier_accepted = model_brier.is_finite()
        && baseline_brier.is_finite()
        && relative_brier_improvement >= MIN_RELATIVE_BRIER_IMPROVEMENT;
    let ranking_accepted = model_top_20pct_pass_rate.is_finite()
        && model_top_20pct_pass_rate + 1e-9 >= holdout_pass_rate;

    ValidationMetrics {
        model_brier,
        baseline_brier,
        relative_brier_improvement,
        model_top_20pct_pass_rate,
        holdout_pass_rate,
        brier_accepted,
        ranking_accepted,
        accepted: brier_accepted && ranking_accepted,
    }
}

fn temporal_cutoff(timestamps: &[u64]) -> Option<u64> {
    let mut unique = timestamps.to_vec();
    unique.sort_unstable();
    unique.dedup();
    if unique.len() < 2 {
        return None;
    }
    let index = (unique.len() * (HOLDOUT_FRACTION_DENOMINATOR - HOLDOUT_FRACTION_NUMERATOR))
        / HOLDOUT_FRACTION_DENOMINATOR;
    Some(unique[index.clamp(1, unique.len() - 1)])
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
        let mut result = TargetResult::unavailable(
            target,
            format!(
                "insufficient labelled data (rows={}, pass={}, fail={}, unique_candidates={})",
                rows.len(),
                positive,
                negative,
                unique_candidates
            ),
        );
        result.report["labelled_rows"] = Value::from(rows.len() as u64);
        result.report["pass_rows"] = Value::from(positive as u64);
        result.report["fail_rows"] = Value::from(negative as u64);
        result.report["unique_candidates"] = Value::from(unique_candidates as u64);
        return Ok(result);
    }

    let timestamps = rows
        .iter()
        .map(|(row, _)| row.observed_at)
        .collect::<Vec<_>>();
    let mut unique_timestamps = timestamps.clone();
    unique_timestamps.sort_unstable();
    unique_timestamps.dedup();
    if unique_timestamps.len() < target.minimum_timestamps() {
        let mut result = TargetResult::unavailable(
            target,
            format!(
                "insufficient distinct observation timestamps (found={}, required={})",
                unique_timestamps.len(),
                target.minimum_timestamps()
            ),
        );
        result.report["labelled_rows"] = Value::from(rows.len() as u64);
        result.report["pass_rows"] = Value::from(positive as u64);
        result.report["fail_rows"] = Value::from(negative as u64);
        result.report["unique_candidates"] = Value::from(unique_candidates as u64);
        result.report["distinct_timestamps"] = Value::from(unique_timestamps.len() as u64);
        return Ok(result);
    }

    let cutoff = temporal_cutoff(&unique_timestamps)
        .ok_or_else(|| "unable to construct final temporal holdout".to_string())?;
    let training = rows
        .iter()
        .filter(|(row, _)| row.observed_at < cutoff)
        .copied()
        .collect::<Vec<_>>();
    let (previously_seen_candidates, first_observed_at) = target_label_history(&rows, cutoff);
    let eligible_unseen_holdout_rows = rows
        .iter()
        .filter(|(row, _)| {
            row.observed_at >= cutoff
                && !previously_seen_candidates.contains(&row.candidate_fingerprint)
        })
        .count();
    let holdout_indices = first_unseen_holdout_indices(
        &rows,
        cutoff,
        &previously_seen_candidates,
        &first_observed_at,
    );
    let holdout = holdout_indices
        .iter()
        .map(|index| rows[*index])
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
                "unseen-candidate temporal holdout lacks representative classes (train={}/{}/{}, holdout={}/{}/{})",
                training.len(),
                training_positive,
                training_negative,
                holdout.len(),
                holdout_positive,
                holdout_negative
            ),
        );
        result.report["evaluation_strategy"] =
            Value::from("single_temporal_holdout_unseen_candidates_first_observation");
        result.report["labelled_rows"] = Value::from(rows.len() as u64);
        result.report["pass_rows"] = Value::from(positive as u64);
        result.report["fail_rows"] = Value::from(negative as u64);
        result.report["unique_candidates"] = Value::from(unique_candidates as u64);
        result.report["train_rows"] = Value::from(training.len() as u64);
        result.report["train_pass_rows"] = Value::from(training_positive as u64);
        result.report["train_fail_rows"] = Value::from(training_negative as u64);
        result.report["holdout_rows"] = Value::from(holdout.len() as u64);
        result.report["holdout_pass_rows"] = Value::from(holdout_positive as u64);
        result.report["holdout_fail_rows"] = Value::from(holdout_negative as u64);
        result.report["holdout_unique_candidates"] = Value::from(holdout.len() as u64);
        result.report["holdout_eligible_unseen_rows"] =
            Value::from(eligible_unseen_holdout_rows as u64);
        result.report["holdout_repeated_observations_excluded"] =
            Value::from(eligible_unseen_holdout_rows.saturating_sub(holdout.len()) as u64);
        return Ok(result);
    }

    let training_timestamps = training
        .iter()
        .map(|(row, _)| row.observed_at)
        .collect::<Vec<_>>();
    let mut unique_training_timestamps = training_timestamps;
    unique_training_timestamps.sort_unstable();
    unique_training_timestamps.dedup();

    let mut use_weighted = false;
    let mut selection_experiment = serde_json::json!({
        "selection_evaluation_strategy": "inner_temporal_holdout_unseen_candidates_first_observation",
        "selection_available": false,
        "selection_reason": "insufficient distinct timestamps for independent strategy selection",
        "selection_training_rows": null,
        "selection_holdout_rows": null,
        "selection_holdout_pass_rows": null,
        "selection_holdout_fail_rows": null,
        "selection_baseline_brier": null,
        "unweighted_brier": null,
        "unweighted_relative_brier_improvement": null,
        "unweighted_top_20pct_pass_rate": null,
        "unweighted_accepted": null,
        "weighted_brier": null,
        "weighted_relative_brier_improvement": null,
        "weighted_top_20pct_pass_rate": null,
        "weighted_accepted": null,
        "half_life_days": 3.0,
        "minimum_sample_weight": MIN_RECENCY_WEIGHT,
        "selected_strategy": "unweighted_default_insufficient_selection_data"
    });

    if unique_training_timestamps.len() >= 5 {
        if let Some(selection_cutoff) = temporal_cutoff(&unique_training_timestamps) {
            let selection_training = training
                .iter()
                .filter(|(row, _)| row.observed_at < selection_cutoff)
                .copied()
                .collect::<Vec<_>>();
            let (selection_seen_candidates, selection_first_observed_at) =
                target_label_history(&training, selection_cutoff);
            let selection_eligible_unseen_rows = training
                .iter()
                .filter(|(row, _)| {
                    row.observed_at >= selection_cutoff
                        && !selection_seen_candidates.contains(&row.candidate_fingerprint)
                })
                .count();
            let selection_holdout_indices = first_unseen_holdout_indices(
                &training,
                selection_cutoff,
                &selection_seen_candidates,
                &selection_first_observed_at,
            );
            let selection_holdout = selection_holdout_indices
                .iter()
                .map(|index| training[*index])
                .collect::<Vec<_>>();
            let selection_train_positive = selection_training
                .iter()
                .filter(|(_, label)| *label)
                .count();
            let selection_train_negative = selection_training
                .len()
                .saturating_sub(selection_train_positive);
            let selection_holdout_positive =
                selection_holdout.iter().filter(|(_, label)| *label).count();
            let selection_holdout_negative = selection_holdout
                .len()
                .saturating_sub(selection_holdout_positive);
            let minimum_selection_holdout_class_rows =
                target.minimum_holdout_class_rows().clamp(25, 50);
            let selection_data_is_representative = selection_training.len()
                >= target.minimum_rows() / 2
                && selection_train_positive >= target.minimum_training_class_rows()
                && selection_train_negative >= target.minimum_training_class_rows()
                && selection_holdout.len() >= 100
                && selection_holdout_positive >= minimum_selection_holdout_class_rows
                && selection_holdout_negative >= minimum_selection_holdout_class_rows;

            if selection_data_is_representative {
                let selection_train_features = selection_training
                    .iter()
                    .map(|(row, _)| row.features.clone())
                    .collect::<Vec<_>>();
                let selection_train_labels = selection_training
                    .iter()
                    .map(|(_, label)| if *label { 1.0_f32 } else { 0.0_f32 })
                    .collect::<Vec<_>>();
                let selection_holdout_features = selection_holdout
                    .iter()
                    .map(|(row, _)| row.features.clone())
                    .collect::<Vec<_>>();
                let selection_holdout_labels = selection_holdout
                    .iter()
                    .map(|(_, label)| if *label { 1.0_f32 } else { 0.0_f32 })
                    .collect::<Vec<_>>();
                let selection_baseline_rate = selection_train_labels
                    .iter()
                    .map(|label| f64::from(*label))
                    .sum::<f64>()
                    / selection_train_labels.len() as f64;
                let selection_train_as_of = selection_training
                    .iter()
                    .map(|(row, _)| row.observed_at)
                    .max()
                    .unwrap_or(selection_cutoff);
                let selection_train_weights = selection_training
                    .iter()
                    .map(|(row, _)| recency_weight(row.observed_at, selection_train_as_of))
                    .collect::<Vec<_>>();

                let selection_unweighted_booster = fit_booster(
                    &selection_train_features,
                    &selection_train_labels,
                    target.seed(),
                )?;
                let selection_unweighted_predictions =
                    predict_booster(&selection_unweighted_booster, &selection_holdout_features)?;
                let selection_unweighted = evaluate_predictions(
                    &selection_unweighted_predictions,
                    &selection_holdout_labels,
                    selection_baseline_rate,
                );

                let selection_weighted_booster = fit_booster_with_weights(
                    &selection_train_features,
                    &selection_train_labels,
                    &selection_train_weights,
                    target.seed(),
                )?;
                let selection_weighted_predictions =
                    predict_booster(&selection_weighted_booster, &selection_holdout_features)?;
                let selection_weighted = evaluate_predictions(
                    &selection_weighted_predictions,
                    &selection_holdout_labels,
                    selection_baseline_rate,
                );

                let weighted_is_better = selection_weighted.model_brier + 1e-9
                    < selection_unweighted.model_brier
                    || ((selection_weighted.model_brier - selection_unweighted.model_brier).abs()
                        <= 1e-9
                        && selection_weighted.model_top_20pct_pass_rate
                            > selection_unweighted.model_top_20pct_pass_rate + 1e-9);
                use_weighted = selection_weighted.accepted
                    && (!selection_unweighted.accepted || weighted_is_better);
                let selected_strategy = if use_weighted {
                    "recency_weighted"
                } else if selection_unweighted.accepted {
                    "unweighted"
                } else {
                    "unweighted_default_no_variant_passed_selection_gate"
                };
                let selection_reason = if use_weighted {
                    "recency_weighted_passed_inner_selection_gate"
                } else if selection_unweighted.accepted {
                    "unweighted_passed_inner_selection_gate"
                } else {
                    "neither_variant_passed_inner_selection_gate_defaulted_unweighted"
                };
                selection_experiment = serde_json::json!({
                    "selection_evaluation_strategy": "inner_temporal_holdout_unseen_candidates_first_observation",
                    "selection_available": true,
                    "selection_reason": selection_reason,
                    "selection_cutoff_timestamp": selection_cutoff,
                    "selection_training_rows": selection_training.len(),
                    "selection_holdout_rows": selection_holdout.len(),
                    "selection_holdout_pass_rows": selection_holdout_positive,
                    "selection_holdout_fail_rows": selection_holdout_negative,
                    "selection_eligible_unseen_rows": selection_eligible_unseen_rows,
                    "selection_repeated_observations_excluded": selection_eligible_unseen_rows.saturating_sub(selection_holdout.len()),
                    "selection_baseline_brier": selection_unweighted.baseline_brier,
                    "unweighted_brier": selection_unweighted.model_brier,
                    "unweighted_relative_brier_improvement": selection_unweighted.relative_brier_improvement,
                    "unweighted_top_20pct_pass_rate": selection_unweighted.model_top_20pct_pass_rate,
                    "unweighted_accepted": selection_unweighted.accepted,
                    "unweighted_brier_accepted": selection_unweighted.brier_accepted,
                    "unweighted_ranking_accepted": selection_unweighted.ranking_accepted,
                    "weighted_brier": selection_weighted.model_brier,
                    "weighted_relative_brier_improvement": selection_weighted.relative_brier_improvement,
                    "weighted_top_20pct_pass_rate": selection_weighted.model_top_20pct_pass_rate,
                    "weighted_accepted": selection_weighted.accepted,
                    "weighted_brier_accepted": selection_weighted.brier_accepted,
                    "weighted_ranking_accepted": selection_weighted.ranking_accepted,
                    "half_life_days": 3.0,
                    "minimum_sample_weight": MIN_RECENCY_WEIGHT,
                    "selected_strategy": selected_strategy
                });
            } else {
                selection_experiment["selection_reason"] = Value::from(format!(
                    "inner temporal holdout lacks representative classes (train={}/{}/{}, holdout={}/{}/{})",
                    selection_training.len(),
                    selection_train_positive,
                    selection_train_negative,
                    selection_holdout.len(),
                    selection_holdout_positive,
                    selection_holdout_negative
                ));
                selection_experiment["selection_training_rows"] =
                    Value::from(selection_training.len() as u64);
                selection_experiment["selection_holdout_rows"] =
                    Value::from(selection_holdout.len() as u64);
                selection_experiment["selection_holdout_pass_rows"] =
                    Value::from(selection_holdout_positive as u64);
                selection_experiment["selection_holdout_fail_rows"] =
                    Value::from(selection_holdout_negative as u64);
            }
        }
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
    let train_as_of = training
        .iter()
        .map(|(row, _)| row.observed_at)
        .max()
        .unwrap_or(cutoff);
    let train_weights = training
        .iter()
        .map(|(row, _)| recency_weight(row.observed_at, train_as_of))
        .collect::<Vec<_>>();

    let selected_booster = if use_weighted {
        fit_booster_with_weights(
            &train_features,
            &train_labels,
            &train_weights,
            target.seed(),
        )?
    } else {
        fit_booster(&train_features, &train_labels, target.seed())?
    };
    let selected_predictions = predict_booster(&selected_booster, &holdout_features)?;
    let metrics = evaluate_predictions(&selected_predictions, &holdout_labels, baseline_rate);
    let accepted = metrics.accepted;
    let reason = if !metrics.brier_accepted {
        "model_did_not_beat_unseen_candidate_baseline"
    } else if !metrics.ranking_accepted {
        "top_20pct_below_unseen_candidate_baseline"
    } else {
        "promoted_after_independent_temporal_holdout"
    };
    let top_20pct_lift = if metrics.holdout_pass_rate > 0.0 {
        metrics.model_top_20pct_pass_rate / metrics.holdout_pass_rate
    } else {
        0.0
    };

    let mut report = serde_json::json!({
        "target": target.name(),
        "accepted": accepted,
        "evaluation_strategy": "single_temporal_holdout_unseen_candidates_first_observation",
        "labelled_rows": rows.len(),
        "pass_rows": positive,
        "fail_rows": negative,
        "unique_candidates": unique_candidates,
        "train_rows": training.len(),
        "train_pass_rows": training_positive,
        "train_fail_rows": training_negative,
        "holdout_rows": holdout.len(),
        "holdout_unique_candidates": holdout.len(),
        "holdout_eligible_unseen_rows": eligible_unseen_holdout_rows,
        "holdout_repeated_observations_excluded": eligible_unseen_holdout_rows
            .saturating_sub(holdout.len()),
        "holdout_pass_rows": holdout_positive,
        "holdout_fail_rows": holdout_negative,
        "cutoff_timestamp": cutoff,
        "model_brier": metrics.model_brier,
        "baseline_brier": metrics.baseline_brier,
        "relative_brier_improvement": metrics.relative_brier_improvement,
        "holdout_pass_rate": metrics.holdout_pass_rate,
        "model_top_20pct_pass_rate": metrics.model_top_20pct_pass_rate,
        "top_20pct_lift": top_20pct_lift,
        "top_20pct_non_degrading": metrics.ranking_accepted,
        "reason": reason,
        "recency_weighting_experiment": selection_experiment
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
    let production_booster = if use_weighted {
        let as_of = current_epoch_seconds();
        let production_weights = rows
            .iter()
            .map(|(row, _)| recency_weight(row.observed_at, as_of))
            .collect::<Vec<_>>();
        fit_booster_with_weights(
            &all_features,
            &all_labels,
            &production_weights,
            target.seed(),
        )?
    } else {
        fit_booster(&all_features, &all_labels, target.seed())?
    };
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

pub fn backend_for_config(config: &str) -> &'static str {
    let Ok(url) = url::Url::parse(config.split('#').next().unwrap_or(config)) else {
        return "sing-box";
    };

    let query_value = |names: &[&str]| {
        url.query_pairs()
            .find(|(key, value)| {
                names.iter().any(|name| key.eq_ignore_ascii_case(name)) && !value.is_empty()
            })
            .map(|(_, value)| value.into_owned())
            .unwrap_or_default()
    };
    let has_query_key = |names: &[&str]| {
        url.query_pairs()
            .any(|(key, _)| names.iter().any(|name| key.eq_ignore_ascii_case(name)))
    };

    let xray_only_tls_extensions = || {
        let pcs = query_value(&["pcs", "pinnedPeerCertSha256"]);
        let vcn = query_value(&["vcn", "verifyPeerCertByName"]);
        let ech = query_value(&["ech"]);
        !pcs.is_empty() || !vcn.is_empty() || ech.contains("://")
    };

    let transport = query_value(&["type", "network"]).to_ascii_lowercase();
    let security = query_value(&["security"]).to_ascii_lowercase();
    let scheme = url.scheme().to_ascii_lowercase();

    if matches!(scheme.as_str(), "socks4" | "socks4a") {
        return "sing-box";
    }
    if matches!(scheme.as_str(), "http" | "socks" | "socks5" | "socks5h") {
        return "xray";
    }
    if scheme == "tuic" {
        return "sing-box";
    }

    if scheme == "vmess" {
        if let Some(value) = decode_vmess(config) {
            let network = value
                .get("net")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();

            if matches!(network.as_str(), "xhttp" | "splithttp" | "kcp" | "mkcp") {
                return "xray";
            }
            if network == "quic" {
                return "sing-box";
            }

            if network == "grpc" {
                let mode = value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_ascii_lowercase();
                let has_authority = ["authority", "host"].iter().any(|key| {
                    value
                        .get(*key)
                        .and_then(Value::as_str)
                        .is_some_and(|item| !item.is_empty())
                });

                if mode == "multi" || has_authority {
                    return "xray";
                }
            }

            let has_certificate_extension = ["pcs", "vcn"].iter().any(|key| {
                value
                    .get(*key)
                    .and_then(Value::as_str)
                    .is_some_and(|item| !item.is_empty())
            });
            let ech = value
                .get("ech")
                .and_then(Value::as_str)
                .unwrap_or("")
                .replace(' ', "+");
            if has_certificate_extension || ech.contains("://") {
                return "xray";
            }
        }
    }

    let legacy_raw_http = (transport.is_empty() || transport == "tcp" || transport == "raw")
        && query_value(&["headerType"]).eq_ignore_ascii_case("http");
    if legacy_raw_http && !xray_only_tls_extensions() {
        return "sing-box";
    }

    if matches!(transport.as_str(), "xhttp" | "splithttp" | "kcp" | "mkcp") {
        return "xray";
    }
    if transport == "quic" {
        return "sing-box";
    }
    if transport == "grpc"
        && (has_query_key(&["authority", "host"])
            || query_value(&["mode"]).eq_ignore_ascii_case("multi"))
    {
        return "xray";
    }

    if xray_only_tls_extensions() {
        return "xray";
    }

    if matches!(scheme.as_str(), "hysteria2" | "hy2") && has_query_key(&["pinSHA256"]) {
        return "xray";
    }

    if scheme == "vless" {
        let flow = query_value(&["flow"]).to_ascii_lowercase();
        if !flow.is_empty() && flow != "xtls-rprx-vision" {
            return "xray";
        }

        if has_query_key(&["fm", "finalmask"]) {
            return "xray";
        }
        let encryption = query_value(&["encryption"]);
        if !encryption.is_empty() && !encryption.eq_ignore_ascii_case("none") {
            return "xray";
        }
        if !query_value(&["extra"]).is_empty() {
            return "xray";
        }
    }

    if security == "reality" {
        return "fallback";
    }

    "sing-box"
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
        backend_for_config, candidate_fingerprint, config_feature_vector, denormalized_log,
        first_unseen_holdout_indices, historical_prior_has_signal, historical_prior_score,
        history_feature_vector, load_training, parse_config_features, port_bucket, recency_weight,
        target_label_history, temporal_cutoff, top_quintile_pass_rate, ModelTarget,
        TrainingExample, HISTORY_FEATURE_COUNT, MIN_RECENCY_WEIGHT, MODEL_FEATURE_COUNT,
    };
    use base64::Engine as _;
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
            "feature_extraction_version": super::FEATURE_EXTRACTION_VERSION,
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
    fn recency_weights_decay_by_half_life_and_respect_the_floor() {
        let now = 20 * 24 * 60 * 60;
        let fresh = recency_weight(now, now);
        let three_days_old = recency_weight(now - 3 * 24 * 60 * 60, now);
        let six_days_old = recency_weight(now - 6 * 24 * 60 * 60, now);
        let very_old = recency_weight(0, now);

        assert!((fresh - 1.0).abs() < 1e-6);
        assert!((three_days_old - 0.5).abs() < 1e-6);
        assert!((six_days_old - 0.25).abs() < 1e-6);
        assert!((very_old - MIN_RECENCY_WEIGHT as f32).abs() < 1e-6);
        assert!(fresh > three_days_old);
        assert!(three_days_old > six_days_old);
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
    fn runtime_backend_selection_preserves_legacy_model_feature_semantics() {
        let vmess_payload = super::general_purpose::STANDARD.encode(
            json!({
                "add": "node.example.com",
                "port": "443",
                "id": "00000000-0000-0000-0000-000000000001",
                "net": "grpc",
                "type": "multi",
                "host": "front.example.com",
                "tls": "tls"
            })
            .to_string(),
        );
        let vmess = format!("vmess://{vmess_payload}");
        let cases = [
            (
                "vless://id@example.com:443?flow=custom-flow",
                "xray",
                "sing-box",
            ),
            (vmess.as_str(), "xray", "sing-box"),
            (
                "hysteria2://password@example.com:443?pinSHA256=abc",
                "xray",
                "sing-box",
            ),
            (
                "hysteria2://password@example.com:443?pinSHA256=",
                "xray",
                "sing-box",
            ),
            (
                "vless://id@example.com:443?type=grpc&host=",
                "xray",
                "sing-box",
            ),
            ("vless://id@example.com:443?finalmask=", "xray", "sing-box"),
            (
                "vless://id@example.com:443?security=reality",
                "fallback",
                "fallback",
            ),
            ("vless://id@example.com:443?type=ws", "sing-box", "sing-box"),
        ];

        for (config, runtime_backend, feature_backend) in cases {
            assert_eq!(backend_for_config(config), runtime_backend, "{config}");
            assert_eq!(
                parse_config_features(config).backend,
                feature_backend,
                "{config}"
            );
        }
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
    fn legacy_three_mib_stream_labels_are_not_mixed_with_sustained_nine_mib_labels() {
        let path = temp_path();
        let base_timestamp = super::current_epoch_seconds().saturating_sub(10_000);
        let mut legacy = stored_row(base_timestamp + 100, "legacy-stream", true);
        legacy["label"]["transfer_tested"] = Value::from(true);
        legacy["label"]["transfer_pass"] = Value::from(true);
        legacy["label"]["stream_tested"] = Value::from(true);
        legacy["label"]["stream_pass"] = Value::from(true);

        let mut sustained = stored_row(base_timestamp + 101, "sustained-stream", true);
        sustained["label"]["transfer_tested"] = Value::from(true);
        sustained["label"]["transfer_pass"] = Value::from(true);
        sustained["label"]["stream_tested"] = Value::from(true);
        sustained["label"]["stream_pass"] = Value::from(false);
        sustained["label"]["stream_validation_version"] = Value::from(2);

        fs::write(
            &path,
            [
                serde_json::to_string(&legacy).unwrap(),
                serde_json::to_string(&sustained).unwrap(),
            ]
            .join("\n")
                + "\n",
        )
        .expect("write stream protocol fixtures");

        let data = load_training(&path).expect("load training data");
        let legacy_row = data
            .examples
            .iter()
            .find(|row| row.candidate_fingerprint == "legacy-stream")
            .expect("legacy candidate");
        let sustained_row = data
            .examples
            .iter()
            .find(|row| row.candidate_fingerprint == "sustained-stream")
            .expect("sustained candidate");

        assert_eq!(legacy_row.transfer_pass, Some(true));
        assert_eq!(legacy_row.stream_pass, None);
        assert_eq!(sustained_row.transfer_pass, Some(true));
        assert_eq!(sustained_row.stream_pass, Some(false));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn temporal_history_features_do_not_leak_same_run_labels() {
        let path = temp_path();
        let base_timestamp = super::current_epoch_seconds().saturating_sub(10_000);
        let mut rows = Vec::new();
        for index in 0..3 {
            let mut repeated = stored_row(base_timestamp + 100, "same-candidate", true);
            repeated["observation_id"] =
                Value::from(format!("{}:same-candidate:{index}", base_timestamp + 100));
            rows.push(serde_json::to_string(&repeated).unwrap());
            if index == 0 {
                rows.push(
                    serde_json::to_string(&stored_row(
                        base_timestamp + 100,
                        "other-candidate",
                        false,
                    ))
                    .unwrap(),
                );
            }
        }
        rows.push(
            serde_json::to_string(&stored_row(base_timestamp + 200, "same-candidate", true))
                .unwrap(),
        );
        let mut legacy = stored_row(base_timestamp + 250, "legacy-features", true);
        legacy
            .as_object_mut()
            .expect("legacy row object")
            .remove("feature_extraction_version");
        rows.push(serde_json::to_string(&legacy).unwrap());
        let mut stale = stored_row(base_timestamp + 300, "stale-features", true);
        stale["feature_extraction_version"] =
            Value::from(super::FEATURE_EXTRACTION_VERSION.saturating_sub(1));
        rows.push(serde_json::to_string(&stale).unwrap());
        fs::write(&path, rows.join("\n") + "\n").expect("write training data");

        let data = load_training(&path).expect("load training data");
        assert!(data
            .examples
            .iter()
            .any(|row| row.candidate_fingerprint == "legacy-features"));
        assert!(data
            .examples
            .iter()
            .all(|row| row.candidate_fingerprint != "stale-features"));
        assert!(data
            .examples
            .iter()
            .all(|row| row.transfer_pass.is_none() && row.stream_pass.is_none()));
        let same_time = data
            .examples
            .iter()
            .filter(|row| row.observed_at == base_timestamp + 100)
            .collect::<Vec<_>>();
        assert!(same_time.iter().all(|row| row.features[53] == 0.0));
        let later = data
            .examples
            .iter()
            .find(|row| row.observed_at == base_timestamp + 200)
            .expect("later row");
        assert!((later.features[54] - (5.0 / 7.0)).abs() < 1e-9);
        assert_eq!(later.features.len(), MODEL_FEATURE_COUNT);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn temporal_holdout_uses_target_label_history_and_keeps_only_first_observation() {
        let example = |observed_at: u64, candidate_fingerprint: &str| TrainingExample {
            observed_at,
            candidate_fingerprint: candidate_fingerprint.to_string(),
            features: Vec::new(),
            strict_pass: true,
            transfer_pass: None,
            stream_pass: None,
        };
        let known_before_cutoff = example(1, "known");
        let new_first = example(10, "new-a");
        let new_repeat = example(11, "new-a");
        let new_other = example(12, "new-b");
        let known_after_cutoff = example(13, "known");
        let seen_by_an_unlabelled_stage = example(14, "seen-other-stage");
        let rows = vec![
            (&known_before_cutoff, true),
            (&new_first, true),
            (&new_repeat, false),
            (&new_other, false),
            (&known_after_cutoff, false),
            (&seen_by_an_unlabelled_stage, false),
        ];
        let (previously_seen_candidates, first_observed_at) = target_label_history(&rows, 10);
        assert_eq!(
            previously_seen_candidates,
            std::collections::HashSet::from(["known".to_string()])
        );
        assert_eq!(first_observed_at["seen-other-stage"], 14);

        assert_eq!(
            first_unseen_holdout_indices(
                &rows,
                10,
                &previously_seen_candidates,
                &first_observed_at,
            ),
            vec![1, 3, 5]
        );
    }

    #[test]
    fn recency_selection_holdout_precedes_the_independent_final_holdout() {
        let examples = (1..=10)
            .map(|timestamp| TrainingExample {
                observed_at: timestamp,
                candidate_fingerprint: format!("candidate-{timestamp}"),
                features: vec![timestamp as f64],
                strict_pass: timestamp % 2 == 0,
                transfer_pass: None,
                stream_pass: None,
            })
            .collect::<Vec<_>>();
        let rows = examples
            .iter()
            .map(|row| (row, row.strict_pass))
            .collect::<Vec<_>>();
        let timestamps = rows
            .iter()
            .map(|(row, _)| row.observed_at)
            .collect::<Vec<_>>();
        let final_cutoff = temporal_cutoff(&timestamps).expect("final temporal cutoff");
        let training = rows
            .iter()
            .filter(|(row, _)| row.observed_at < final_cutoff)
            .copied()
            .collect::<Vec<_>>();
        let training_timestamps = training
            .iter()
            .map(|(row, _)| row.observed_at)
            .collect::<Vec<_>>();
        let selection_cutoff =
            temporal_cutoff(&training_timestamps).expect("inner selection cutoff");

        assert!(selection_cutoff < final_cutoff);
        assert!(training
            .iter()
            .all(|(row, _)| row.observed_at < final_cutoff));

        let (seen_before_selection, first_seen_in_training) =
            target_label_history(&training, selection_cutoff);
        let selection_indices = first_unseen_holdout_indices(
            &training,
            selection_cutoff,
            &seen_before_selection,
            &first_seen_in_training,
        );
        assert!(!selection_indices.is_empty());
        assert!(selection_indices.iter().all(|index| {
            let timestamp = training[*index].0.observed_at;
            timestamp >= selection_cutoff && timestamp < final_cutoff
        }));

        let (seen_before_final, first_seen_overall) = target_label_history(&rows, final_cutoff);
        let final_indices = first_unseen_holdout_indices(
            &rows,
            final_cutoff,
            &seen_before_final,
            &first_seen_overall,
        );
        assert!(!final_indices.is_empty());
        assert!(final_indices
            .iter()
            .all(|index| rows[*index].0.observed_at >= final_cutoff));
    }

    #[test]
    fn backend_feature_extraction_preserves_legacy_training_semantics() {
        let config = "socks4://127.0.0.1:1080";
        assert_eq!(parse_config_features(config).backend, "xray");
        assert_eq!(backend_for_config(config), "sing-box");
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

    #[test]
    fn historical_fallback_ranks_observed_successes_and_leaves_unseen_candidates_neutral() {
        let history = MODEL_FEATURE_COUNT - HISTORY_FEATURE_COUNT;
        let mut reliable = vec![0.0; MODEL_FEATURE_COUNT];
        reliable[history] = super::normalized_log(20.0, 50_000.0);
        reliable[history + 1] = 0.95;
        reliable[history + 2] = 0.90;
        reliable[history + 3] = 0.90;
        reliable[history + 4] = super::normalized_log(200.0, 50_000.0);
        reliable[history + 5] = 0.92;
        reliable[history + 6] = 0.88;
        reliable[history + 7] = 0.90;
        reliable[history + 8] = 0.95;
        reliable[history + 9] = 0.10;
        reliable[history + 10] = 0.10;
        reliable[history + 11] = 0.90;
        reliable[history + 12] = 0.95;

        let mut unreliable = reliable.clone();
        unreliable[history + 1] = 0.10;
        unreliable[history + 2] = 0.15;
        unreliable[history + 3] = 0.10;
        unreliable[history + 5] = 0.12;
        unreliable[history + 6] = 0.15;
        unreliable[history + 7] = 0.10;
        unreliable[history + 8] = 0.15;
        unreliable[history + 9] = 0.90;
        unreliable[history + 10] = 0.90;
        unreliable[history + 11] = 0.10;

        let unseen = vec![0.0; MODEL_FEATURE_COUNT];
        assert!(historical_prior_has_signal(&reliable));
        assert!(historical_prior_score(&reliable) > historical_prior_score(&unreliable));
        assert!(!historical_prior_has_signal(&unseen));
        assert_eq!(historical_prior_score(&unseen), 0.5);
        assert_eq!(denormalized_log(0.0, 50_000.0), 0.0);
    }
}
