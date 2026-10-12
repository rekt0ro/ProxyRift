use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DATASET_VERSION: u64 = 3;
pub const FEATURE_EXTRACTION_VERSION: u64 = 4;
const STRICT_VALIDATION_CONTRACT_VERSION: u64 = 3;
pub const FEATURE_COUNT: usize = 19;

const MAX_ROWS: usize = 50_000;
const RETENTION_SECS: u64 = 3 * 24 * 60 * 60;
const MIN_ROWS_FOR_SPEED_MODEL: usize = 40;
const MIN_UNIQUE_RUNS_FOR_SPEED_MODEL: usize = 3;
const MIN_UNIQUE_CANDIDATES_FOR_SPEED_MODEL: usize = 25;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DatasetStats {
    pub rows: usize,
    pub new_rows: usize,
    pub unique_runs: usize,
    pub unique_candidates: usize,
    pub strict_passes: usize,
    pub strict_failures: usize,
    pub transfer_tests: usize,
    pub transfer_passes: usize,
    pub transfer_failures: usize,
    pub stream_tests: usize,
    pub stream_passes: usize,
    pub stream_failures: usize,
}

impl DatasetStats {
    pub fn speed_model_ready(&self) -> bool {
        self.rows >= MIN_ROWS_FOR_SPEED_MODEL
            && self.unique_runs >= MIN_UNIQUE_RUNS_FOR_SPEED_MODEL
            && self.unique_candidates >= MIN_UNIQUE_CANDIDATES_FOR_SPEED_MODEL
    }

    pub fn strict_model_ready(&self) -> bool {
        self.speed_model_ready()
    }

    pub fn end_to_end_model_ready(&self) -> bool {
        self.speed_model_ready()
    }
}

#[derive(Clone, Debug)]
pub struct TrainingRow {
    pub observed_at: u64,
    pub run_id: Option<String>,
    pub candidate_fingerprint: String,
    pub observation_id: String,
    pub features: BTreeMap<String, Value>,
    pub strict_pass: bool,
    pub strict_checks: u64,
    pub transfer_tested: bool,
    pub transfer_pass: Option<bool>,
    pub stream_tested: bool,
    pub stream_pass: Option<bool>,
    pub stream_throughput_kbps: f64,
    pub stream_duration_ms: f64,
}

pub fn persist(path: &str, rows: &[TrainingRow]) -> Result<DatasetStats, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();

    persist_at(path, rows, now)
}

fn parse_compatible_stored_row(
    line: &str,
    now: u64,
    enforce_retention: bool,
) -> Option<(String, Value)> {
    let mut value = serde_json::from_str::<Value>(line).ok()?;
    upgrade_stored_row(&mut value);
    if !valid_stored_row(&value) {
        return None;
    }
    if value
        .get("strict_validation_contract_version")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        < STRICT_VALIDATION_CONTRACT_VERSION
    {
        let backend = value
            .pointer("/features/backend")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if matches!(backend, "xray" | "fallback") {
            return None;
        }
    }

    let observed_at = value.get("observed_at").and_then(Value::as_u64)?;
    if enforce_retention && observed_at.saturating_add(RETENTION_SECS) < now {
        return None;
    }

    let observation_id = value.get("observation_id").and_then(Value::as_str)?;
    Some((observation_id.to_string(), value))
}

fn load_existing_rows(
    path: &str,
    now: u64,
    enforce_current_retention: bool,
) -> Result<BTreeMap<String, Value>, String> {
    let mut by_id = BTreeMap::<String, Value>::new();
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(by_id),
        Err(error) => return Err(error.to_string()),
    };

    for line in BufReader::new(file).lines() {
        let line = line.map_err(|error| error.to_string())?;
        if let Some((observation_id, value)) =
            parse_compatible_stored_row(&line, now, enforce_current_retention)
        {
            by_id.insert(observation_id, value);
        }
    }
    Ok(by_id)
}

fn ordered_stored_values(by_id: BTreeMap<String, Value>) -> Vec<Value> {
    let mut values = by_id.into_values().collect::<Vec<_>>();
    values.sort_unstable_by(|a, b| {
        b.get("observed_at")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .cmp(&a.get("observed_at").and_then(Value::as_u64).unwrap_or(0))
            .then_with(|| {
                a.get("observation_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .cmp(
                        b.get("observation_id")
                            .and_then(Value::as_str)
                            .unwrap_or(""),
                    )
            })
    });
    values.truncate(MAX_ROWS);
    values
}

pub(crate) fn load_compatible_rows_for_training(path: &str) -> Result<Vec<Value>, String> {
    File::open(path)
        .map_err(|error| format!("failed to open LightGBM training data {path}: {error}"))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    Ok(ordered_stored_values(load_existing_rows(path, now, true)?))
}

fn persist_at(path: &str, rows: &[TrainingRow], now: u64) -> Result<DatasetStats, String> {
    let mut by_id = load_existing_rows(path, now, true)?;

    let mut new_rows = 0usize;
    for row in rows {
        if row.observed_at.saturating_add(RETENTION_SECS) < now
            || !row.strict_pass
            || row.strict_checks == 0
            || !row.transfer_tested
            || row.transfer_pass != Some(true)
            || !row.stream_tested
            || row.stream_pass != Some(true)
            || !row.stream_throughput_kbps.is_finite()
            || row.stream_throughput_kbps <= 0.0
            || !row.stream_duration_ms.is_finite()
            || row.stream_duration_ms <= 0.0
        {
            continue;
        }

        let value = row.to_value();
        if by_id.insert(row.observation_id.clone(), value).is_none() {
            new_rows += 1;
        }
    }

    let values = ordered_stored_values(by_id);

    let mut strict_passes = 0usize;
    let mut transfer_tests = 0usize;
    let mut transfer_passes = 0usize;
    let mut stream_tests = 0usize;
    let mut stream_passes = 0usize;
    let mut run_ids = std::collections::BTreeSet::<String>::new();
    let mut candidate_fingerprints = std::collections::BTreeSet::<String>::new();
    for value in &values {
        if let Some(run_id) = value.get("run_id").and_then(Value::as_str) {
            if !run_id.trim().is_empty() {
                run_ids.insert(run_id.to_string());
            }
        }
        if let Some(fingerprint) = value.get("candidate_fingerprint").and_then(Value::as_str) {
            if !fingerprint.trim().is_empty() {
                candidate_fingerprints.insert(fingerprint.to_string());
            }
        }
        let label = value.get("label").and_then(Value::as_object);
        if label
            .and_then(|label| label.get("strict_pass"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            strict_passes += 1;
        }
        if label
            .and_then(|label| label.get("transfer_tested"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            transfer_tests += 1;
            if label
                .and_then(|label| label.get("transfer_pass"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                transfer_passes += 1;
            }
        }

        let stream_validation_version = label
            .and_then(|label| label.get("stream_validation_version"))
            .and_then(Value::as_u64)
            .unwrap_or(1);
        if stream_validation_version >= 2
            && label
                .and_then(|label| label.get("stream_tested"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
        {
            stream_tests += 1;
            if label
                .and_then(|label| label.get("stream_pass"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                stream_passes += 1;
            }
        }
    }

    let temporary = format!("{path}.tmp");
    let mut body = String::new();
    for value in &values {
        let line = serde_json::to_string(value).map_err(|error| error.to_string())?;
        body.push_str(&line);
        body.push('\n');
    }

    if let Err(error) = fs::write(&temporary, body) {
        let _ = fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error.to_string());
    }

    Ok(DatasetStats {
        rows: values.len(),
        new_rows,
        unique_runs: run_ids.len(),
        unique_candidates: candidate_fingerprints.len(),
        strict_passes,
        strict_failures: values.len().saturating_sub(strict_passes),
        transfer_tests,
        transfer_passes,
        transfer_failures: transfer_tests.saturating_sub(transfer_passes),
        stream_tests,
        stream_passes,
        stream_failures: stream_tests.saturating_sub(stream_passes),
    })
}

pub fn write_readiness_report(path: &str, stats: &DatasetStats) -> Result<(), String> {
    let document = serde_json::json!({
        "schema_version": DATASET_VERSION,
        "feature_extraction_version": FEATURE_EXTRACTION_VERSION,
        "training_objective": "validated_stream_throughput_regression",
        "retention_seconds": RETENTION_SECS,
        "dataset": {
            "speed_samples": stats.rows,
            "new_speed_samples": stats.new_rows,
            "unique_runs": stats.unique_runs,
            "unique_candidates": stats.unique_candidates,
            "strict_passes": stats.strict_passes,
            "transfer_passes": stats.transfer_passes,
            "stream_passes": stats.stream_passes,
        },
        "readiness": {
            "speed_model_ready": stats.speed_model_ready(),
            "minimum_speed_samples": MIN_ROWS_FOR_SPEED_MODEL,
            "minimum_unique_runs": MIN_UNIQUE_RUNS_FOR_SPEED_MODEL,
            "minimum_unique_candidates": MIN_UNIQUE_CANDIDATES_FOR_SPEED_MODEL,
        }
    });
    let body = serde_json::to_vec_pretty(&document).map_err(|error| error.to_string())?;
    let temporary = format!("{path}.tmp");
    std::fs::write(&temporary, body).map_err(|error| error.to_string())?;
    if let Err(error) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    Ok(())
}

pub(crate) fn upgrade_stored_row(value: &mut Value) {
    let schema_version = value.get("schema_version").and_then(Value::as_u64);
    if schema_version == Some(1) {
        if let Some(label) = value.get_mut("label").and_then(Value::as_object_mut) {
            label
                .entry("stream_tested".to_string())
                .or_insert(Value::Bool(false));
            label
                .entry("stream_pass".to_string())
                .or_insert(Value::Null);
        }

        if let Some(object) = value.as_object_mut() {
            object.insert("schema_version".to_string(), Value::from(DATASET_VERSION));
        }
        return;
    }

    if schema_version != Some(DATASET_VERSION) || value.get("feature_extraction_version").is_some()
    {
        return;
    }

    if let Some(object) = value.as_object_mut() {
        object.insert(
            "feature_extraction_version".to_string(),
            Value::from(FEATURE_EXTRACTION_VERSION),
        );
    }

    if !valid_stored_row(value) {
        if let Some(object) = value.as_object_mut() {
            object.remove("feature_extraction_version");
        }
    }
}

impl TrainingRow {
    fn to_value(&self) -> Value {
        let mut root = Map::new();
        root.insert("schema_version".to_string(), Value::from(DATASET_VERSION));
        root.insert(
            "feature_extraction_version".to_string(),
            Value::from(FEATURE_EXTRACTION_VERSION),
        );
        root.insert(
            "strict_validation_contract_version".to_string(),
            Value::from(STRICT_VALIDATION_CONTRACT_VERSION),
        );
        root.insert(
            "observation_id".to_string(),
            Value::from(self.observation_id.clone()),
        );
        root.insert("observed_at".to_string(), Value::from(self.observed_at));
        root.insert(
            "candidate_fingerprint".to_string(),
            Value::from(self.candidate_fingerprint.clone()),
        );

        if let Some(run_id) = &self.run_id {
            root.insert("run_id".to_string(), Value::from(run_id.clone()));
        }

        root.insert(
            "features".to_string(),
            Value::Object(
                self.features
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ),
        );
        root.insert(
            "stream_throughput_kbps".to_string(),
            Value::from(self.stream_throughput_kbps),
        );
        root.insert(
            "stream_duration_ms".to_string(),
            Value::from(self.stream_duration_ms),
        );

        let mut label = Map::new();
        label.insert("strict_pass".to_string(), Value::from(self.strict_pass));
        label.insert("strict_checks".to_string(), Value::from(self.strict_checks));
        label.insert(
            "transfer_tested".to_string(),
            Value::from(self.transfer_tested),
        );
        label.insert(
            "transfer_pass".to_string(),
            self.transfer_pass.map(Value::from).unwrap_or(Value::Null),
        );
        label.insert("stream_tested".to_string(), Value::from(self.stream_tested));
        label.insert(
            "stream_pass".to_string(),
            self.stream_pass.map(Value::from).unwrap_or(Value::Null),
        );
        label.insert("stream_validation_version".to_string(), Value::from(3));
        root.insert("label".to_string(), Value::Object(label));

        Value::Object(root)
    }
}

const FEATURE_NAMES: [&str; FEATURE_COUNT] = [
    "protocol",
    "backend",
    "transport",
    "security",
    "port",
    "query_parameter_count",
    "has_sni",
    "has_host",
    "has_path",
    "tls_enabled",
    "reality_enabled",
    "early_attempts",
    "early_success_rate",
    "early_median_ms",
    "early_min_ms",
    "early_jitter_ms",
    "early_throughput_kbps",
    "history_checks",
    "history_pass_rate",
];

fn finite_number(value: Option<&Value>) -> bool {
    value.and_then(Value::as_f64).is_some_and(f64::is_finite)
}

fn valid_stored_row(value: &Value) -> bool {
    if value.get("schema_version").and_then(Value::as_u64) != Some(DATASET_VERSION)
        || value
            .get("feature_extraction_version")
            .and_then(Value::as_u64)
            != Some(FEATURE_EXTRACTION_VERSION)
        || value
            .get("observation_id")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        || value
            .get("candidate_fingerprint")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        || value.get("observed_at").and_then(Value::as_u64).is_none()
        || !finite_number(value.get("stream_throughput_kbps"))
        || value
            .get("stream_throughput_kbps")
            .and_then(Value::as_f64)
            .is_none_or(|n| n <= 0.0)
        || !finite_number(value.get("stream_duration_ms"))
        || value
            .get("stream_duration_ms")
            .and_then(Value::as_f64)
            .is_none_or(|n| n <= 0.0)
    {
        return false;
    }

    let Some(features) = value.get("features").and_then(Value::as_object) else {
        return false;
    };

    if features.len() != FEATURE_COUNT
        || !FEATURE_NAMES
            .iter()
            .all(|name| features.contains_key(*name))
    {
        return false;
    }

    for name in ["protocol", "backend", "transport", "security"] {
        if features
            .get(name)
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return false;
        }
    }

    for name in [
        "port",
        "query_parameter_count",
        "early_attempts",
        "history_checks",
    ] {
        if features.get(name).and_then(Value::as_u64).is_none() {
            return false;
        }
    }

    for name in [
        "has_sni",
        "has_host",
        "has_path",
        "tls_enabled",
        "reality_enabled",
    ] {
        if features.get(name).and_then(Value::as_bool).is_none() {
            return false;
        }
    }

    for name in [
        "early_success_rate",
        "early_median_ms",
        "early_min_ms",
        "early_jitter_ms",
        "early_throughput_kbps",
        "history_pass_rate",
    ] {
        if !finite_number(features.get(name)) {
            return false;
        }
    }

    let Some(label) = value.get("label").and_then(Value::as_object) else {
        return false;
    };

    label.get("strict_pass").and_then(Value::as_bool) == Some(true)
        && label
            .get("strict_checks")
            .and_then(Value::as_u64)
            .is_some_and(|checks| checks > 0)
        && label.get("transfer_tested").and_then(Value::as_bool) == Some(true)
        && label.get("transfer_pass").and_then(Value::as_bool) == Some(true)
        && label.get("stream_tested").and_then(Value::as_bool) == Some(true)
        && label.get("stream_pass").and_then(Value::as_bool) == Some(true)
        && label
            .get("stream_validation_version")
            .and_then(Value::as_u64)
            == Some(3)
}

#[cfg(test)]
mod tests {
    use super::{
        persist_at, valid_stored_row, TrainingRow, DATASET_VERSION, FEATURE_COUNT,
        FEATURE_EXTRACTION_VERSION, RETENTION_SECS, STRICT_VALIDATION_CONTRACT_VERSION,
    };
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> String {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        format!(
            "{}/proxyrift-light-training-{}-{nonce}.jsonl",
            std::env::temp_dir().display(),
            std::process::id()
        )
    }

    fn row(id: &str, observed_at: u64) -> TrainingRow {
        TrainingRow {
            observed_at,
            run_id: Some("test-run".to_string()),
            candidate_fingerprint: format!("fp-{id}"),
            observation_id: id.to_string(),
            features: BTreeMap::from([
                ("protocol".to_string(), Value::from("vless")),
                ("backend".to_string(), Value::from("sing-box")),
                ("transport".to_string(), Value::from("raw")),
                ("security".to_string(), Value::from("tls")),
                ("port".to_string(), Value::from(443u64)),
                ("query_parameter_count".to_string(), Value::from(1u64)),
                ("has_sni".to_string(), Value::from(true)),
                ("has_host".to_string(), Value::from(false)),
                ("has_path".to_string(), Value::from(false)),
                ("tls_enabled".to_string(), Value::from(true)),
                ("reality_enabled".to_string(), Value::from(false)),
                ("early_attempts".to_string(), Value::from(3u64)),
                ("early_success_rate".to_string(), Value::from(0.66)),
                ("early_median_ms".to_string(), Value::from(100.0)),
                ("early_min_ms".to_string(), Value::from(80.0)),
                ("early_jitter_ms".to_string(), Value::from(5.0)),
                ("early_throughput_kbps".to_string(), Value::from(1000.0)),
                ("history_checks".to_string(), Value::from(2u64)),
                ("history_pass_rate".to_string(), Value::from(0.5)),
            ]),
            strict_pass: true,
            strict_checks: 1,
            transfer_tested: true,
            transfer_pass: Some(true),
            stream_tested: true,
            stream_pass: Some(true),
            stream_throughput_kbps: 42_000.0,
            stream_duration_ms: 1_800.0,
        }
    }

    #[test]
    fn stores_only_complete_successful_stream_samples() {
        let path = temp_path();
        let mut invalid = row("failed", 1_001);
        invalid.stream_pass = Some(false);
        let mut no_speed = row("no-speed", 1_002);
        no_speed.stream_throughput_kbps = 0.0;
        let stats = persist_at(&path, &[row("pass", 1_000), invalid, no_speed], 1_002)
            .expect("persist valid samples");

        assert_eq!(stats.rows, 1);
        assert_eq!(stats.new_rows, 1);
        assert_eq!(stats.unique_candidates, 1);
        assert_eq!(stats.strict_passes, 1);
        assert_eq!(stats.transfer_passes, 1);
        assert_eq!(stats.stream_passes, 1);
        assert!(stats.speed_model_ready() == false);

        let body = fs::read_to_string(&path).expect("read dataset");
        assert_eq!(body.lines().count(), 1);
        let value: Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(value["schema_version"].as_u64(), Some(DATASET_VERSION));
        assert_eq!(
            value["feature_extraction_version"].as_u64(),
            Some(FEATURE_EXTRACTION_VERSION)
        );
        assert_eq!(
            value["strict_validation_contract_version"].as_u64(),
            Some(STRICT_VALIDATION_CONTRACT_VERSION)
        );
        assert_eq!(value["label"]["strict_pass"].as_bool(), Some(true));
        assert_eq!(value["label"]["transfer_pass"].as_bool(), Some(true));
        assert_eq!(value["label"]["stream_pass"].as_bool(), Some(true));
        assert_eq!(value["stream_throughput_kbps"].as_f64(), Some(42_000.0));
        assert_eq!(value["stream_duration_ms"].as_f64(), Some(1_800.0));
        assert!(valid_stored_row(&value));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_legacy_classification_rows_after_the_clean_schema_reset() {
        let mut legacy = serde_json::json!({
            "schema_version": 2,
            "feature_extraction_version": 3,
            "observation_id": "old",
            "candidate_fingerprint": "old",
            "observed_at": 1000,
            "label": {"strict_pass": true, "strict_checks": 1, "transfer_tested": true, "transfer_pass": true, "stream_tested": true, "stream_pass": true},
            "features": {}
        });
        super::upgrade_stored_row(&mut legacy);
        assert!(!valid_stored_row(&legacy));
        let serialized = serde_json::to_string(&legacy).unwrap();
        assert!(super::parse_compatible_stored_row(&serialized, 1_000, false).is_none());
    }

    #[test]
    fn duplicate_observation_ids_are_idempotent() {
        let path = temp_path();
        let first = row("same", 1_000);
        let second = row("same", 1_001);
        persist_at(&path, &[first], 1_001).expect("initial persist");
        let stats = persist_at(&path, &[second], 1_001).expect("deduplicated persist");
        assert_eq!(stats.rows, 1);
        assert_eq!(stats.new_rows, 0);
        assert_eq!(stats.strict_passes, 1);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn three_day_retention_prunes_old_speed_observations() {
        let path = temp_path();
        let now = 400_000;
        let fresh = row("fresh", now);
        let old = row("old", now.saturating_sub(RETENTION_SECS + 1));
        let stats = persist_at(&path, &[fresh, old], now).expect("persist retained rows");
        assert_eq!(stats.rows, 1);
        let body = fs::read_to_string(&path).expect("read dataset");
        assert_eq!(body.lines().count(), 1);
        assert!(body.contains("fresh"));
        assert!(!body.contains("old"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn readiness_uses_speed_sample_coverage_not_negative_labels() {
        let mut stats = super::DatasetStats::default();
        stats.rows = 39;
        stats.unique_runs = 10;
        stats.unique_candidates = 40;
        assert!(!stats.speed_model_ready());

        stats.rows = 40;
        stats.unique_runs = 3;
        stats.unique_candidates = 25;
        assert!(stats.speed_model_ready());
        assert!(stats.strict_model_ready());
        assert!(stats.end_to_end_model_ready());
    }

    #[test]
    fn rejects_non_finite_or_incomplete_stream_metrics() {
        let mut invalid = row("bad", 1_000).to_value();
        invalid["stream_duration_ms"] = Value::from(0.0);
        assert!(!valid_stored_row(&invalid));

        let mut invalid = row("incomplete", 1_000).to_value();
        invalid["label"]["stream_pass"] = Value::Null;
        assert!(!valid_stored_row(&invalid));

        let mut invalid = row("zero-checks", 1_000).to_value();
        invalid["label"]["strict_checks"] = Value::from(0u64);
        assert!(!valid_stored_row(&invalid));
    }

    #[test]
    fn feature_count_contract_is_explicit() {
        assert_eq!(FEATURE_COUNT, 19);
    }
}
