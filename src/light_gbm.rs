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
const SPEED_HISTORY_FEATURE_COUNT: usize = 8;
const SPEED_MODEL_FEATURE_COUNT: usize = STRUCTURAL_FEATURE_COUNT + SPEED_HISTORY_FEATURE_COUNT;
const MIN_SPEED_ROWS: usize = 60;
const MIN_SPEED_TRAIN_ROWS: usize = 40;
const MIN_SPEED_CANDIDATES: usize = 40;
const MIN_SPEED_TIMESTAMPS: usize = 6;
const MIN_SPEED_HOLDOUT_CANDIDATES: usize = 10;
const MIN_TOP_QUINTILE_SPEED_LIFT: f64 = 1.05;
const SPEED_MODEL_SEED: i32 = 1709;
const SPEED_HISTORY_HALF_LIFE_SECS: f64 = 24.0 * 60.0 * 60.0;

impl LightGbmScores {
    pub fn train_and_score(candidates: &[String]) -> Result<Self, String> {
        if candidates.is_empty() {
            return Ok(Self::default());
        }

        let now = current_epoch_seconds();
        let observations = match load_speed_observations(DEFAULT_TRAINING_PATH) {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!("Speed history unavailable; using the default ranking: {error}");
                Vec::new()
            }
        };
        let speed_logs = observations
            .iter()
            .map(|row| row.speed_kbps.ln())
            .collect::<Vec<_>>();
        let median_log_speed = median(&speed_logs).unwrap_or_else(|| 10_000.0_f64.ln());
        let feature_rows = build_speed_training_features(&observations, 10_000.0_f64.ln());
        let (accepted, mut report) = evaluate_speed_regressor(&observations, &feature_rows, now);

        let mut booster = None;
        if accepted {
            let targets = observations
                .iter()
                .map(|row| row.speed_kbps.ln() as f32)
                .collect::<Vec<_>>();
            let weights = speed_sample_weights(&observations, now);
            match fit_speed_regression_booster(&feature_rows, &targets, &weights, SPEED_MODEL_SEED)
            {
                Ok(model) => booster = Some(model),
                Err(error) => {
                    eprintln!(
                        "Speed model training failed; using recent speed measurements: {error}"
                    );
                    report["accepted"] = Value::Bool(false);
                    report["reason"] = Value::String("final_model_fit_failed".to_string());
                    report["fit_error"] = Value::String(error);
                }
            }
        }

        let exact_history = speed_history_by_key(&observations, |row| row.fingerprint.clone());
        let family_history = speed_history_by_key(&observations, |row| row.family.clone());
        let candidate_features = candidates
            .iter()
            .map(|config| {
                let structural = config_feature_vector(config);
                let mut features = structural.clone();
                let fingerprint = candidate_fingerprint(config);
                let family = feature_signature(&structural);
                features.extend(
                    exact_history
                        .get(&fingerprint)
                        .map(|history| history.feature_values(now, median_log_speed))
                        .unwrap_or_else(|| {
                            SpeedHistory::default().feature_values(now, median_log_speed)
                        }),
                );
                features.extend(
                    family_history
                        .get(&family)
                        .map(|history| history.feature_values(now, median_log_speed))
                        .unwrap_or_else(|| {
                            SpeedHistory::default().feature_values(now, median_log_speed)
                        }),
                );
                features
            })
            .collect::<Vec<_>>();

        let ranking_strategy = if booster.is_some() {
            "stream_speed_regression"
        } else if observations.is_empty() {
            "neutral_default"
        } else {
            "historical_stream_speed_prior_fallback"
        };
        let predicted_log_speeds = if let Some(model) = booster.as_ref() {
            predict_speed_regression(model, &candidate_features)?
        } else {
            candidates
                .iter()
                .map(|config| {
                    historical_speed_prediction(
                        config,
                        &exact_history,
                        &family_history,
                        median_log_speed,
                    )
                })
                .collect()
        };

        let mut scores = HashMap::with_capacity(candidates.len());
        for (candidate, predicted) in candidates.iter().zip(predicted_log_speeds) {
            let score = speed_ranking_score(predicted, median_log_speed);
            scores.insert(
                candidate.clone(),
                if score.is_finite() {
                    score.clamp(MIN_SCORE, MAX_SCORE)
                } else {
                    DEFAULT_SCORE
                },
            );
        }

        let candidate_count = candidates
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>()
            .len();
        if scores.len() != candidate_count {
            return Err(format!(
                "incomplete LightGBM candidate score coverage: expected {candidate_count}, scored {}",
                scores.len()
            ));
        }

        report["ranking_strategy"] = Value::String(ranking_strategy.to_string());
        report["target"] = Value::String("log(stream_throughput_kbps)".to_string());
        report["target_unit"] = Value::String("kbps".to_string());
        report["feature_count"] = Value::from(SPEED_MODEL_FEATURE_COUNT);
        report["speed_samples"] = Value::from(observations.len());
        report["unique_candidates"] = Value::from(
            observations
                .iter()
                .map(|row| row.fingerprint.as_str())
                .collect::<HashSet<_>>()
                .len(),
        );
        report["unique_timestamps"] = Value::from(
            observations
                .iter()
                .map(|row| row.observed_at)
                .collect::<HashSet<_>>()
                .len(),
        );
        report["candidate_count"] = Value::from(candidate_count);
        report["scored_candidate_count"] = Value::from(scores.len());

        let result = Self {
            scores,
            training_rows: observations.len(),
            trained: booster.is_some(),
            model_report: report,
        };
        result.save(DEFAULT_SCORE_PATH)?;
        let ranking_label = match ranking_strategy {
            "stream_speed_regression" => "speed model",
            "historical_stream_speed_prior_fallback" => "recent speed estimates",
            _ => "neutral ranking",
        };
        let model_status = if result.trained {
            "active"
        } else {
            "not enabled"
        };
        println!(
            "Speed ranking: {} samples from the last 72 hours across {} candidates. Model: {} ({}).",
            result.training_rows,
            result.model_report.get("unique_candidates").and_then(Value::as_u64).unwrap_or(0),
            ranking_label,
            model_status
        );
        Ok(result)
    }

    fn neutral_for_candidates_at_path(
        candidates: &[String],
        reason: &str,
        path: &str,
    ) -> Result<Self, String> {
        let scores = candidates
            .iter()
            .cloned()
            .map(|candidate| (candidate, DEFAULT_SCORE))
            .collect::<HashMap<_, _>>();
        let candidate_count = candidates
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>()
            .len();
        if scores.len() != candidate_count {
            return Err(format!(
                "incomplete neutral candidate score coverage: expected {candidate_count}, scored {}",
                scores.len()
            ));
        }
        let model_report = serde_json::json!({
            "model": "LightGBM GBDT regression",
            "validation": "chronological_80_20_first_observation_of_unseen_candidates",
            "baseline": "training_median_log_stream_throughput",
            "ranking_strategy": "neutral_default",
            "target": "log(stream_throughput_kbps)",
            "target_unit": "kbps",
            "accepted": false,
            "reason": reason,
            "cutoff_timestamp": null,
            "speed_samples": 0,
            "unique_candidates": 0,
            "unique_timestamps": 0,
            "train_rows": 0,
            "holdout_rows": 0,
            "baseline_log_mae": null,
            "model_log_mae": null,
            "relative_mae_improvement": null,
            "top_20pct_speed_lift": null,
            "minimum_relative_mae_improvement": 0.01,
            "minimum_top_20pct_speed_lift": MIN_TOP_QUINTILE_SPEED_LIFT,
            "candidate_count": candidate_count,
            "scored_candidate_count": scores.len()
        });
        let result = Self {
            scores,
            training_rows: 0,
            trained: false,
            model_report,
        };
        result.save(path)?;
        Ok(result)
    }

    pub fn neutral_for_candidates(candidates: &[String], reason: &str) -> Result<Self, String> {
        Self::neutral_for_candidates_at_path(candidates, reason, DEFAULT_SCORE_PATH)
    }

    fn save(&self, path: &str) -> Result<(), String> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create score directory: {error}"))?;
        }
        let payload = serde_json::json!({
            "version": 5,
            "trained": self.trained,
            "training_rows": self.training_rows,
            "feature_count": SPEED_MODEL_FEATURE_COUNT,
            "model_report": self.model_report,
            "scores": self.scores,
        });
        let body = serde_json::to_vec(&payload)
            .map_err(|error| format!("failed to serialize LightGBM scores: {error}"))?;
        let temporary = format!("{path}.tmp");
        fs::write(&temporary, body)
            .map_err(|error| format!("failed to write LightGBM scores: {error}"))?;
        if let Err(error) = fs::rename(&temporary, path) {
            let _ = fs::remove_file(&temporary);
            return Err(format!("failed to publish LightGBM scores: {error}"));
        }
        Ok(())
    }

    pub fn from_file(path: &str) -> Result<Self, String> {
        let content = fs::read_to_string(path)
            .map_err(|error| format!("failed to read LightGBM score file {path}: {error}"))?;
        let value: Value = serde_json::from_str(&content)
            .map_err(|error| format!("invalid LightGBM score file: {error}"))?;
        if value.get("version").and_then(Value::as_u64) != Some(5)
            || value.get("feature_count").and_then(Value::as_u64)
                != Some(SPEED_MODEL_FEATURE_COUNT as u64)
        {
            return Err(
                "incompatible LightGBM score schema; expected stream-speed model v5".to_string(),
            );
        }

        let training_rows = value
            .get("training_rows")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize;
        let trained = value
            .get("trained")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let model_report = value.get("model_report").cloned().unwrap_or(Value::Null);
        if trained
            && model_report.get("ranking_strategy").and_then(Value::as_str)
                != Some("stream_speed_regression")
        {
            return Err("trained LightGBM score file is not a speed-regression model".to_string());
        }

        let mut scores = HashMap::new();
        if let Some(entries) = value.get("scores").and_then(Value::as_object) {
            for (config, score) in entries {
                if let Some(score) = score.as_f64().filter(|score| score.is_finite()) {
                    scores.insert(config.clone(), score.clamp(MIN_SCORE, MAX_SCORE));
                }
            }
        }
        let expected = model_report
            .get("candidate_count")
            .and_then(Value::as_u64)
            .ok_or_else(|| "LightGBM score file is missing candidate score coverage metadata".to_string())?;
        if expected as usize != scores.len() {
            return Err(format!(
                "incomplete LightGBM candidate score coverage: expected {expected}, found {}",
                scores.len()
            ));
        }
        let reported_scored = model_report
            .get("scored_candidate_count")
            .and_then(Value::as_u64)
            .ok_or_else(|| "LightGBM score file is missing scored-candidate count metadata".to_string())?;
        if reported_scored as usize != scores.len() {
            return Err(format!(
                "inconsistent LightGBM scored-candidate count: report says {reported_scored}, score map has {}",
                scores.len()
            ));
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

#[derive(Clone, Debug)]
struct SpeedObservation {
    observed_at: u64,
    fingerprint: String,
    family: String,
    structural: Vec<f64>,
    speed_kbps: f64,
}

#[derive(Clone, Debug, Default)]
struct SpeedHistory {
    count: usize,
    log_speed_sum: f64,
    best_log_speed: f64,
    last_seen: u64,
}

impl SpeedHistory {
    fn record(&mut self, log_speed: f64, observed_at: u64) {
        if !log_speed.is_finite() {
            return;
        }
        self.count = self.count.saturating_add(1);
        self.log_speed_sum += log_speed;
        self.best_log_speed = if self.count == 1 {
            log_speed
        } else {
            self.best_log_speed.max(log_speed)
        };
        self.last_seen = self.last_seen.max(observed_at);
    }

    fn average_log_speed(&self, fallback: f64) -> f64 {
        if self.count == 0 {
            fallback
        } else {
            self.log_speed_sum / self.count as f64
        }
    }

    fn feature_values(&self, now: u64, fallback: f64) -> [f64; 4] {
        if self.count == 0 {
            return [fallback, fallback, 0.0, 0.0];
        }
        let average = self.average_log_speed(fallback);
        let best = if self.best_log_speed.is_finite() {
            self.best_log_speed
        } else {
            average
        };
        let count = ((self.count as f64 + 1.0).ln() / 50_001.0_f64.ln()).clamp(0.0, 1.0);
        let age = now.saturating_sub(self.last_seen) as f64;
        let recency = (-age / SPEED_HISTORY_HALF_LIFE_SECS).exp().clamp(0.0, 1.0);
        [average, best, count, recency]
    }
}

fn current_epoch_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn finite_positive(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite() && *number > 0.0)
}

fn load_speed_observations(path: &str) -> Result<Vec<SpeedObservation>, String> {
    let rows = crate::light_training::load_compatible_rows_for_training(path)?;
    let now = current_epoch_seconds();
    let mut observations = Vec::with_capacity(rows.len());
    for row in rows {
        if row
            .get("feature_extraction_version")
            .and_then(Value::as_u64)
            != Some(FEATURE_EXTRACTION_VERSION)
            || row.pointer("/label/strict_pass").and_then(Value::as_bool) != Some(true)
            || row.pointer("/label/transfer_pass").and_then(Value::as_bool) != Some(true)
            || row.pointer("/label/stream_pass").and_then(Value::as_bool) != Some(true)
        {
            continue;
        }
        let (Some(observed_at), Some(fingerprint), Some(speed_kbps), Some(duration_ms)) = (
            row.get("observed_at").and_then(Value::as_u64),
            row.get("candidate_fingerprint").and_then(Value::as_str),
            finite_positive(row.get("stream_throughput_kbps")),
            finite_positive(row.get("stream_duration_ms")),
        ) else {
            continue;
        };
        if observed_at > now
            || now.saturating_sub(observed_at) >= 3 * 24 * 60 * 60
            || duration_ms > 15_000.0
        {
            continue;
        }
        let Some(fields) = row.get("features").and_then(Value::as_object) else {
            continue;
        };
        let Some(structural) = training_feature_vector(fields) else {
            continue;
        };
        if structural.len() != STRUCTURAL_FEATURE_COUNT {
            continue;
        }
        let family = feature_signature(&structural);
        observations.push(SpeedObservation {
            observed_at,
            fingerprint: fingerprint.to_string(),
            family,
            structural,
            speed_kbps,
        });
    }
    observations.sort_unstable_by(|a, b| {
        a.observed_at
            .cmp(&b.observed_at)
            .then_with(|| a.fingerprint.cmp(&b.fingerprint))
    });
    Ok(observations)
}

fn speed_history_by_key(
    observations: &[SpeedObservation],
    key: impl Fn(&SpeedObservation) -> String,
) -> HashMap<String, SpeedHistory> {
    let mut history = HashMap::new();
    for row in observations {
        history
            .entry(key(row))
            .or_insert_with(SpeedHistory::default)
            .record(row.speed_kbps.ln(), row.observed_at);
    }
    history
}

fn speed_features_for_observation(
    row: &SpeedObservation,
    timestamp: u64,
    exact_history: &HashMap<String, SpeedHistory>,
    family_history: &HashMap<String, SpeedHistory>,
    fallback_log_speed: f64,
) -> Vec<f64> {
    let mut features = row.structural.clone();
    features.extend(
        exact_history
            .get(&row.fingerprint)
            .map(|history| history.feature_values(timestamp, fallback_log_speed))
            .unwrap_or_else(|| {
                SpeedHistory::default().feature_values(timestamp, fallback_log_speed)
            }),
    );
    features.extend(
        family_history
            .get(&row.family)
            .map(|history| history.feature_values(timestamp, fallback_log_speed))
            .unwrap_or_else(|| {
                SpeedHistory::default().feature_values(timestamp, fallback_log_speed)
            }),
    );
    features
}

fn build_speed_training_features(
    observations: &[SpeedObservation],
    fallback_log_speed: f64,
) -> Vec<Vec<f64>> {
    let mut result = Vec::with_capacity(observations.len());
    let mut exact = HashMap::<String, SpeedHistory>::new();
    let mut family = HashMap::<String, SpeedHistory>::new();
    let mut start = 0;
    while start < observations.len() {
        let timestamp = observations[start].observed_at;
        let mut end = start + 1;
        while end < observations.len() && observations[end].observed_at == timestamp {
            end += 1;
        }
        for row in &observations[start..end] {
            result.push(speed_features_for_observation(
                row,
                timestamp,
                &exact,
                &family,
                fallback_log_speed,
            ));
        }
        for row in &observations[start..end] {
            exact
                .entry(row.fingerprint.clone())
                .or_default()
                .record(row.speed_kbps.ln(), timestamp);
            family
                .entry(row.family.clone())
                .or_default()
                .record(row.speed_kbps.ln(), timestamp);
        }
        start = end;
    }
    result
}

fn median(values: &[f64]) -> Option<f64> {
    let mut values = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    })
}

fn speed_sample_weights(observations: &[SpeedObservation], now: u64) -> Vec<f32> {
    observations
        .iter()
        .map(|row| {
            let age = now.saturating_sub(row.observed_at) as f64;
            2.0_f64
                .powf(-age / SPEED_HISTORY_HALF_LIFE_SECS)
                .clamp(0.10, 1.0) as f32
        })
        .collect()
}

fn evaluate_speed_regressor(
    observations: &[SpeedObservation],
    feature_rows: &[Vec<f64>],
    now: u64,
) -> (bool, Value) {
    let unique_candidates = observations
        .iter()
        .map(|row| row.fingerprint.as_str())
        .collect::<HashSet<_>>()
        .len();
    let unique_timestamps = observations
        .iter()
        .map(|row| row.observed_at)
        .collect::<HashSet<_>>()
        .len();
    let base = serde_json::json!({
        "model": "LightGBM GBDT regression",
        "validation": "chronological_80_20_first_observation_of_unseen_candidates",
        "baseline": "training_median_log_stream_throughput",
        "accepted": false, "speed_samples": observations.len(),
        "unique_candidates": unique_candidates, "unique_timestamps": unique_timestamps,
        "train_rows": 0, "holdout_rows": 0, "baseline_log_mae": null,
        "model_log_mae": null, "relative_mae_improvement": null, "top_20pct_speed_lift": null,
        "minimum_relative_mae_improvement": 0.01,
        "minimum_top_20pct_speed_lift": MIN_TOP_QUINTILE_SPEED_LIFT
    });
    if observations.len() < MIN_SPEED_ROWS
        || unique_candidates < MIN_SPEED_CANDIDATES
        || unique_timestamps < MIN_SPEED_TIMESTAMPS
        || feature_rows.len() != observations.len()
        || feature_rows
            .iter()
            .any(|row| row.len() != SPEED_MODEL_FEATURE_COUNT)
    {
        let mut report = base;
        report["reason"] =
            Value::String("insufficient_speed_samples_or_temporal_coverage".to_string());
        return (false, report);
    }
    let mut timestamps = observations
        .iter()
        .map(|row| row.observed_at)
        .collect::<Vec<_>>();
    timestamps.sort_unstable();
    timestamps.dedup();
    if timestamps.len() < 2 {
        let mut report = base;
        report["reason"] = Value::String("insufficient_distinct_timestamps".to_string());
        return (false, report);
    }
    let cutoff = timestamps[(timestamps.len() * 4 / 5).clamp(1, timestamps.len() - 1)];
    let train_indices = observations
        .iter()
        .enumerate()
        .filter_map(|(i, row)| (row.observed_at < cutoff).then_some(i))
        .collect::<Vec<_>>();
    let train_ids = train_indices
        .iter()
        .map(|i| observations[*i].fingerprint.clone())
        .collect::<HashSet<_>>();
    let mut holdout_seen = HashSet::new();
    let holdout_indices = observations
        .iter()
        .enumerate()
        .filter_map(|(i, row)| {
            (row.observed_at >= cutoff
                && !train_ids.contains(&row.fingerprint)
                && holdout_seen.insert(row.fingerprint.clone()))
            .then_some(i)
        })
        .collect::<Vec<_>>();
    if train_indices.len() < MIN_SPEED_TRAIN_ROWS
        || holdout_indices.len() < MIN_SPEED_HOLDOUT_CANDIDATES
    {
        let mut report = base;
        report["train_rows"] = Value::from(train_indices.len());
        report["holdout_rows"] = Value::from(holdout_indices.len());
        report["cutoff_timestamp"] = Value::from(cutoff);
        report["reason"] = Value::String("insufficient_unseen_candidate_holdout".to_string());
        return (false, report);
    }
    let train_features = train_indices
        .iter()
        .map(|i| feature_rows[*i].clone())
        .collect::<Vec<_>>();
    let train_y = train_indices
        .iter()
        .map(|i| observations[*i].speed_kbps.ln() as f32)
        .collect::<Vec<_>>();
    let train_rows = train_indices
        .iter()
        .map(|i| observations[*i].clone())
        .collect::<Vec<_>>();
    let weights = speed_sample_weights(&train_rows, now);
    let model =
        match fit_speed_regression_booster(&train_features, &train_y, &weights, SPEED_MODEL_SEED) {
            Ok(model) => model,
            Err(error) => {
                let mut report = base;
                report["train_rows"] = Value::from(train_indices.len());
                report["holdout_rows"] = Value::from(holdout_indices.len());
                report["reason"] = Value::String("holdout_model_fit_failed".to_string());
                report["fit_error"] = Value::String(error);
                return (false, report);
            }
        };
    let exact_training_history = speed_history_by_key(&train_rows, |row| row.fingerprint.clone());
    let family_training_history = speed_history_by_key(&train_rows, |row| row.family.clone());
    let holdout_features = holdout_indices
        .iter()
        .map(|i| {
            let row = &observations[*i];
            speed_features_for_observation(
                row,
                row.observed_at,
                &exact_training_history,
                &family_training_history,
                10_000.0_f64.ln(),
            )
        })
        .collect::<Vec<_>>();
    let predictions = match predict_speed_regression(&model, &holdout_features) {
        Ok(predictions) => predictions,
        Err(error) => {
            let mut report = base;
            report["reason"] = Value::String("holdout_prediction_failed".to_string());
            report["prediction_error"] = Value::String(error);
            return (false, report);
        }
    };
    let actual_log = holdout_indices
        .iter()
        .map(|i| observations[*i].speed_kbps.ln())
        .collect::<Vec<_>>();
    let baseline_log = median(
        &train_indices
            .iter()
            .map(|i| observations[*i].speed_kbps.ln())
            .collect::<Vec<_>>(),
    )
    .unwrap_or_else(|| 10_000.0_f64.ln());
    let baseline_mae = mean_absolute_error(&vec![baseline_log; actual_log.len()], &actual_log);
    let model_mae = mean_absolute_error(&predictions, &actual_log);
    let improvement = if baseline_mae > f64::EPSILON {
        (baseline_mae - model_mae) / baseline_mae
    } else {
        0.0
    };
    let holdout_speeds = holdout_indices
        .iter()
        .map(|i| observations[*i].speed_kbps)
        .collect::<Vec<_>>();
    let top_lift = top_quintile_speed_lift(&predictions, &holdout_speeds);
    let accepted = model_mae.is_finite()
        && improvement >= 0.01
        && top_lift.is_finite()
        && top_lift >= MIN_TOP_QUINTILE_SPEED_LIFT;
    let report = serde_json::json!({
        "model": "LightGBM GBDT regression",
        "validation": "chronological_80_20_first_observation_of_unseen_candidates",
        "baseline": "training_median_log_stream_throughput",
        "accepted": accepted, "cutoff_timestamp": cutoff,
        "train_rows": train_indices.len(), "holdout_rows": holdout_indices.len(),
        "speed_samples": observations.len(), "unique_candidates": unique_candidates,
        "unique_timestamps": unique_timestamps, "baseline_log_mae": baseline_mae,
        "model_log_mae": model_mae, "relative_mae_improvement": improvement,
        "top_20pct_speed_lift": top_lift, "minimum_relative_mae_improvement": 0.01,
        "minimum_top_20pct_speed_lift": MIN_TOP_QUINTILE_SPEED_LIFT,
        "reason": if accepted { "promoted_after_temporal_speed_validation" }
            else if improvement < 0.01 { "model_did_not_beat_temporal_speed_baseline" }
            else { "top_20pct_speed_lift_below_minimum" }
    });
    (accepted, report)
}

fn mean_absolute_error(predictions: &[f64], actual: &[f64]) -> f64 {
    if predictions.is_empty() || predictions.len() != actual.len() {
        return f64::INFINITY;
    }
    predictions
        .iter()
        .zip(actual)
        .map(|(p, a)| (p - a).abs())
        .sum::<f64>()
        / predictions.len() as f64
}

fn top_quintile_speed_lift(predictions: &[f64], speeds: &[f64]) -> f64 {
    if predictions.is_empty()
        || predictions.len() != speeds.len()
        || speeds
            .iter()
            .any(|speed| !speed.is_finite() || *speed <= 0.0)
    {
        return f64::NAN;
    }
    let overall = speeds.iter().sum::<f64>() / speeds.len() as f64;
    if overall <= 0.0 {
        return f64::NAN;
    }
    let mut indices = (0..predictions.len()).collect::<Vec<_>>();
    indices.sort_unstable_by(|a, b| predictions[*b].total_cmp(&predictions[*a]));
    let count = predictions.len().div_ceil(5);
    indices
        .iter()
        .take(count.max(1))
        .map(|i| speeds[*i])
        .sum::<f64>()
        / count.max(1) as f64
        / overall
}

fn fit_speed_regression_booster(
    features: &[Vec<f64>],
    targets: &[f32],
    weights: &[f32],
    seed: i32,
) -> Result<Booster, String> {
    if features.is_empty()
        || features.len() != targets.len()
        || features.len() != weights.len()
        || features
            .iter()
            .any(|row| row.len() != SPEED_MODEL_FEATURE_COUNT)
        || targets.iter().any(|target| !target.is_finite())
    {
        return Err("speed-regression rows are empty, invalid, or mismatched".to_string());
    }
    let matrix = MatBuf::from_rows_non_empty(features)
        .map_err(|error| format!("failed to build speed-regression matrix: {error}"))?
        .ok_or_else(|| "speed-regression matrix is empty".to_string())?;
    let mut parameters = Parameters::new();
    parameters.push("objective", Objective::Regression);
    parameters.push("boosting", Boosting::Gbdt);
    parameters.push("verbosity", Verbosity::Fatal);
    parameters.push("learning_rate", 0.05);
    parameters.push("num_leaves", 15i32);
    parameters.push("min_data_in_leaf", 10i32);
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
    let mut dataset = Dataset::from_mat(&matrix, None, &parameters)
        .map_err(|error| format!("failed to create regression dataset: {error}"))?;
    dataset
        .set_field(Field::LABEL, targets)
        .map_err(|error| format!("failed to attach regression targets: {error}"))?;
    dataset
        .set_field(Field::WEIGHT, weights)
        .map_err(|error| format!("failed to attach regression weights: {error}"))?;
    let mut booster = Booster::new(Arc::new(dataset), &parameters)
        .map_err(|error| format!("failed to create regression booster: {error}"))?;
    for _ in 0..TRAINING_ITERATIONS {
        if booster
            .update_one_iter()
            .map_err(|error| format!("regression training failed: {error}"))?
        {
            break;
        }
    }
    Ok(booster)
}

fn predict_speed_regression(booster: &Booster, features: &[Vec<f64>]) -> Result<Vec<f64>, String> {
    if features.is_empty() {
        return Ok(Vec::new());
    }
    if features
        .iter()
        .any(|row| row.len() != SPEED_MODEL_FEATURE_COUNT)
    {
        return Err("speed-regression prediction feature count mismatch".to_string());
    }
    let matrix = MatBuf::from_rows_non_empty(features)
        .map_err(|error| format!("failed to build prediction matrix: {error}"))?
        .ok_or_else(|| "prediction matrix is empty".to_string())?;
    let result = booster
        .predict_for_mat(&matrix, PredictType::Normal, 0, None, &Parameters::new())
        .map_err(|error| format!("speed prediction failed: {error}"))?;
    if result.values().len() != features.len() {
        return Err(format!(
            "LightGBM returned {} speed predictions for {} rows",
            result.values().len(),
            features.len()
        ));
    }
    Ok(result
        .values()
        .iter()
        .map(|value| {
            if value.is_finite() {
                value.clamp(0.0, 20.0)
            } else {
                0.0
            }
        })
        .collect())
}

fn historical_speed_prediction(
    config: &str,
    exact_history: &HashMap<String, SpeedHistory>,
    family_history: &HashMap<String, SpeedHistory>,
    fallback: f64,
) -> f64 {
    let structural = config_feature_vector(config);
    let exact = exact_history.get(&candidate_fingerprint(config));
    let family = family_history.get(&feature_signature(&structural));
    match (exact, family) {
        (Some(exact), Some(family)) if exact.count > 0 && family.count > 0 => {
            let exact_weight = exact.count as f64 / (exact.count as f64 + 3.0);
            exact_weight * exact.average_log_speed(fallback)
                + (1.0 - exact_weight) * family.average_log_speed(fallback)
        }
        (Some(exact), _) if exact.count > 0 => exact.average_log_speed(fallback),
        (_, Some(family)) if family.count >= 3 => family.average_log_speed(fallback),
        _ => fallback,
    }
}

fn speed_ranking_score(predicted_log_speed: f64, baseline_log_speed: f64) -> f64 {
    let difference = (predicted_log_speed - baseline_log_speed).clamp(-12.0, 12.0);
    1.0 / (1.0 + (-difference).exp())
}

fn feature_signature(features: &[f64]) -> String {
    features
        .iter()
        .map(|value| format!("{:016x}", value.to_bits()))
        .collect::<Vec<_>>()
        .join("")
}

fn fnv64(input: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for byte in input {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
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
        candidate_fingerprint, feature_signature, speed_features_for_observation,
        speed_history_by_key, speed_ranking_score, top_quintile_speed_lift, LightGbmScores,
        SpeedObservation, SPEED_MODEL_FEATURE_COUNT, STRUCTURAL_FEATURE_COUNT,
    };
    use serde_json::Value;
    use std::fs;

    fn temp_path() -> String {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        format!(
            "{}/proxyrift-lightgbm-score-{}-{nonce}.json",
            std::env::temp_dir().display(),
            std::process::id()
        )
    }

    #[test]
    fn stream_speed_rank_score_is_monotonic_and_bounded() {
        let baseline = 10_000.0_f64.ln();
        let slow = speed_ranking_score(5_000.0_f64.ln(), baseline);
        let median = speed_ranking_score(baseline, baseline);
        let fast = speed_ranking_score(40_000.0_f64.ln(), baseline);
        assert!(slow < median && median < fast);
        assert!((0.0..=1.0).contains(&slow) && (0.0..=1.0).contains(&fast));
        assert!((median - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn holdout_features_use_training_history_only() {
        let training = vec![SpeedObservation {
            observed_at: 100,
            fingerprint: "training".to_string(),
            family: "shared-family".to_string(),
            structural: vec![0.0; STRUCTURAL_FEATURE_COUNT],
            speed_kbps: 1_000.0,
        }];
        let exact_history = speed_history_by_key(&training, |row| row.fingerprint.clone());
        let family_history = speed_history_by_key(&training, |row| row.family.clone());
        let holdout = SpeedObservation {
            observed_at: 200,
            fingerprint: "holdout".to_string(),
            family: "shared-family".to_string(),
            structural: vec![0.0; STRUCTURAL_FEATURE_COUNT],
            speed_kbps: 1_000_000.0,
        };

        let features = speed_features_for_observation(
            &holdout,
            200,
            &exact_history,
            &family_history,
            10_000.0_f64.ln(),
        );

        assert_eq!(features.len(), SPEED_MODEL_FEATURE_COUNT);
        assert!((features[STRUCTURAL_FEATURE_COUNT + 4] - 1_000.0_f64.ln()).abs() < f64::EPSILON);
        assert!((features[STRUCTURAL_FEATURE_COUNT + 5] - 1_000.0_f64.ln()).abs() < f64::EPSILON);
    }

    #[test]
    fn top_quintile_speed_metric_rejects_an_inverted_ranking() {
        let speeds = vec![100.0, 200.0, 300.0, 400.0, 10_000.0];
        assert!(top_quintile_speed_lift(&[1.0, 2.0, 3.0, 4.0, 9.0], &speeds) > 1.0);
        assert!(top_quintile_speed_lift(&[9.0, 8.0, 7.0, 6.0, 1.0], &speeds) < 1.0);
    }

    #[test]
    fn scores_file_rejects_the_legacy_classifier_schema() {
        let path = temp_path();
        fs::write(&path, r#"{"version":4,"trained":true,"training_rows":200,"feature_count":66,"scores":{"candidate":0.99}}"#)
            .expect("write legacy score file");
        assert!(LightGbmScores::from_file(&path).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn scores_file_round_trips_only_versioned_speed_scores() {
        let path = temp_path();
        let scores = LightGbmScores {
            scores: std::collections::HashMap::from([("candidate".to_string(), 0.8)]),
            training_rows: 100,
            trained: true,
            model_report: serde_json::json!({
                "ranking_strategy":"stream_speed_regression",
                "candidate_count":1,
                "scored_candidate_count":1
            }),
        };
        scores.save(&path).expect("save score file");
        let loaded = LightGbmScores::from_file(&path).expect("load score file");
        assert!(loaded.trained());
        assert_eq!(loaded.training_rows(), 100);
        assert_eq!(loaded.score("candidate"), 0.8);
        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(value["version"].as_u64(), Some(5));
        assert_eq!(
            value["feature_count"].as_u64(),
            Some(SPEED_MODEL_FEATURE_COUNT as u64)
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn scores_file_rejects_incomplete_candidate_coverage() {
        let path = temp_path();
        fs::write(
            &path,
            r#"{"version":5,"trained":false,"training_rows":100,"feature_count":61,"scores":{"candidate":0.8},"model_report":{"ranking_strategy":"historical_stream_speed_prior_fallback","candidate_count":2,"scored_candidate_count":1}}"#,
        )
        .expect("write incomplete score file");
        assert!(LightGbmScores::from_file(&path)
            .expect_err("incomplete scores must be rejected")
            .contains("incomplete LightGBM candidate score coverage"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn scores_file_rejects_missing_candidate_coverage_metadata() {
        let path = temp_path();
        fs::write(
            &path,
            r#"{"version":5,"trained":false,"training_rows":100,"feature_count":61,"scores":{"candidate":0.8},"model_report":{"ranking_strategy":"historical_stream_speed_prior_fallback"}}"#,
        )
        .expect("write score file without coverage metadata");
        assert!(LightGbmScores::from_file(&path)
            .expect_err("score coverage metadata is mandatory")
            .contains("missing candidate score coverage metadata"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn neutral_fallback_writes_complete_candidate_scores() {
        let path = temp_path();
        let candidates = vec![
            "candidate-a".to_string(),
            "candidate-b".to_string(),
            "candidate-a".to_string(),
        ];
        let neutral = LightGbmScores::neutral_for_candidates_at_path(
            &candidates,
            "candidate_scoring_failed",
            &path,
        )
        .expect("neutral score file should be written");
        assert_eq!(neutral.len(), 2);
        assert_eq!(neutral.score("candidate-a"), DEFAULT_SCORE);
        assert_eq!(neutral.score("candidate-b"), DEFAULT_SCORE);
        assert_eq!(
            neutral.model_report()["candidate_count"].as_u64(),
            Some(2)
        );
        assert_eq!(
            neutral.model_report()["scored_candidate_count"].as_u64(),
            Some(2)
        );
        assert_eq!(
            neutral.model_report()["ranking_strategy"].as_str(),
            Some("neutral_default")
        );
        let loaded = LightGbmScores::from_file(&path).expect("neutral score file should load");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.score("candidate-a"), DEFAULT_SCORE);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn candidate_fingerprint_ignores_remarks() {
        assert_eq!(
            candidate_fingerprint("vless://uuid@example.com:443?security=tls#first"),
            candidate_fingerprint("vless://uuid@example.com:443?security=tls#second")
        );
        assert_ne!(
            feature_signature(&[1.0, 2.0]),
            feature_signature(&[1.0, 3.0])
        );
    }

    #[test]
    fn structural_feature_contract_is_stable() {
        assert_eq!(super::config_feature_vector(
            "vless://00000000-0000-0000-0000-000000000001@example.com:443?security=tls&type=ws&sni=example.com&path=%2F"
        ).len(), 53);
    }
}
