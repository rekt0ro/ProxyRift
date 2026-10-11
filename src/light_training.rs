use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

pub const DATASET_VERSION: u64 = 2;
pub const FEATURE_EXTRACTION_VERSION: u64 = 3;
pub const FEATURE_COUNT: usize = 19;

const MAX_ROWS: usize = 50_000;
const HISTORICAL_DATASET_REVISION_SCAN_LIMIT: usize = 32;
const RETENTION_SECS: u64 = 45 * 24 * 60 * 60;
const MIN_ROWS_FOR_STRICT_MODEL: usize = 5_000;
const MIN_UNIQUE_RUNS_FOR_STRICT_MODEL: usize = 20;
const MIN_UNIQUE_CANDIDATES_FOR_STRICT_MODEL: usize = 1_000;
const MIN_STRICT_PASSES_FOR_STRICT_MODEL: usize = 500;
const MIN_STRICT_FAILURES_FOR_STRICT_MODEL: usize = 500;
const MIN_TRANSFER_TESTS_FOR_END_TO_END: usize = 1_000;
const MIN_TRANSFER_PASSES_FOR_END_TO_END: usize = 100;
const MIN_TRANSFER_FAILURES_FOR_END_TO_END: usize = 100;
const MIN_STREAM_TESTS_FOR_END_TO_END: usize = 500;
const MIN_STREAM_PASSES_FOR_END_TO_END: usize = 50;
const MIN_STREAM_FAILURES_FOR_END_TO_END: usize = 50;

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
    pub fn strict_model_ready(&self) -> bool {
        self.rows >= MIN_ROWS_FOR_STRICT_MODEL
            && self.unique_runs >= MIN_UNIQUE_RUNS_FOR_STRICT_MODEL
            && self.unique_candidates >= MIN_UNIQUE_CANDIDATES_FOR_STRICT_MODEL
            && self.strict_passes >= MIN_STRICT_PASSES_FOR_STRICT_MODEL
            && self.strict_failures >= MIN_STRICT_FAILURES_FOR_STRICT_MODEL
    }

    pub fn end_to_end_model_ready(&self) -> bool {
        self.strict_model_ready()
            && self.transfer_tests >= MIN_TRANSFER_TESTS_FOR_END_TO_END
            && self.transfer_passes >= MIN_TRANSFER_PASSES_FOR_END_TO_END
            && self.transfer_failures >= MIN_TRANSFER_FAILURES_FOR_END_TO_END
            && self.stream_tests >= MIN_STREAM_TESTS_FOR_END_TO_END
            && self.stream_passes >= MIN_STREAM_PASSES_FOR_END_TO_END
            && self.stream_failures >= MIN_STREAM_FAILURES_FOR_END_TO_END
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

    if let Ok(file) = File::open(path) {
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if let Some((observation_id, value)) =
                parse_compatible_stored_row(&line, now, enforce_current_retention)
            {
                by_id.insert(observation_id, value);
            }
        }
    }

    if by_id.len() < MIN_ROWS_FOR_STRICT_MODEL
        && Command::new("git")
            .args(["ls-files", "--error-unmatch", path])
            .output()
            .is_ok_and(|output| output.status.success())
    {
        if let Ok(history) = Command::new("git")
            .args(["log", "--format=%H", "--", path])
            .output()
        {
            if history.status.success() {
                for commit in String::from_utf8_lossy(&history.stdout)
                    .lines()
                    .take(HISTORICAL_DATASET_REVISION_SCAN_LIMIT)
                {
                    let revision_path = format!("{commit}:{path}");
                    let Ok(snapshot) = Command::new("git").args(["show", &revision_path]).output()
                    else {
                        continue;
                    };
                    if !snapshot.status.success() {
                        continue;
                    }

                    for line in String::from_utf8_lossy(&snapshot.stdout).lines() {
                        if let Some((observation_id, value)) =
                            parse_compatible_stored_row(line, now, true)
                        {
                            by_id.entry(observation_id).or_insert(value);
                        }
                        if by_id.len() >= MAX_ROWS {
                            break;
                        }
                    }
                    if by_id.len() >= MAX_ROWS {
                        break;
                    }
                }
            }
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
    Ok(ordered_stored_values(load_existing_rows(path, now, false)?))
}

fn persist_at(path: &str, rows: &[TrainingRow], now: u64) -> Result<DatasetStats, String> {
    let mut by_id = load_existing_rows(path, now, true)?;

    let mut new_rows = 0usize;
    for row in rows {
        if row.observed_at.saturating_add(RETENTION_SECS) < now {
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

        if label
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
        "dataset": {
            "rows": stats.rows,
            "new_rows": stats.new_rows,
            "unique_runs": stats.unique_runs,
            "unique_candidates": stats.unique_candidates,
            "strict_passes": stats.strict_passes,
            "strict_failures": stats.strict_failures,
            "transfer_tests": stats.transfer_tests,
            "transfer_passes": stats.transfer_passes,
            "transfer_failures": stats.transfer_failures,
            "stream_tests": stats.stream_tests,
            "stream_passes": stats.stream_passes,
            "stream_failures": stats.stream_failures,
        },
        "readiness": {
            "strict_model_ready": stats.strict_model_ready(),
            "end_to_end_model_ready": stats.end_to_end_model_ready(),
            "requirements": {
                "min_rows": MIN_ROWS_FOR_STRICT_MODEL,
                "min_unique_runs": MIN_UNIQUE_RUNS_FOR_STRICT_MODEL,
                "min_unique_candidates": MIN_UNIQUE_CANDIDATES_FOR_STRICT_MODEL,
                "min_strict_passes": MIN_STRICT_PASSES_FOR_STRICT_MODEL,
                "min_strict_failures": MIN_STRICT_FAILURES_FOR_STRICT_MODEL,
                "min_transfer_tests": MIN_TRANSFER_TESTS_FOR_END_TO_END,
                "min_transfer_passes": MIN_TRANSFER_PASSES_FOR_END_TO_END,
                "min_transfer_failures": MIN_TRANSFER_FAILURES_FOR_END_TO_END,
                "min_stream_tests": MIN_STREAM_TESTS_FOR_END_TO_END,
                "min_stream_passes": MIN_STREAM_PASSES_FOR_END_TO_END,
                "min_stream_failures": MIN_STREAM_FAILURES_FOR_END_TO_END,
            },
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
        // Version 2 labels the sustained 9 MiB continuation, not the old 3 MiB
        // standalone stream probe. Older records are preserved but excluded
        // from the current stream model target.
        label.insert("stream_validation_version".to_string(), Value::from(2));
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

    if label.get("strict_pass").and_then(Value::as_bool).is_none()
        || label.get("strict_checks").and_then(Value::as_u64).is_none()
        || label
            .get("transfer_tested")
            .and_then(Value::as_bool)
            .is_none()
    {
        return false;
    }

    if label
        .get("strict_checks")
        .and_then(Value::as_u64)
        .is_none_or(|checks| checks == 0)
    {
        return false;
    }

    let transfer_tested = label
        .get("transfer_tested")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    match label.get("transfer_pass") {
        Some(Value::Null) if !transfer_tested => true,
        Some(Value::Bool(_)) if transfer_tested => true,
        _ => false,
    };

    let stream_tested = label
        .get("stream_tested")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    match label.get("stream_pass") {
        Some(Value::Null) if !stream_tested => true,
        Some(Value::Bool(_)) if stream_tested => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        persist_at, TrainingRow, DATASET_VERSION, FEATURE_COUNT, FEATURE_EXTRACTION_VERSION,
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

    fn row(
        id: &str,
        observed_at: u64,
        strict_pass: bool,
        transfer_pass: Option<bool>,
    ) -> TrainingRow {
        TrainingRow {
            observed_at,
            run_id: Some("test-run".to_string()),
            candidate_fingerprint: format!("fp-{id}"),
            observation_id: id.to_string(),
            features: BTreeMap::from([
                ("protocol".to_string(), Value::from("vless")),
                ("backend".to_string(), Value::from("xray")),
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
            strict_pass,
            strict_checks: 1,
            transfer_tested: transfer_pass.is_some(),
            transfer_pass,
            stream_tested: false,
            stream_pass: None,
        }
    }

    #[test]
    fn schema_is_versioned_and_stats_are_reported() {
        let path = temp_path();
        let stats = persist_at(
            &path,
            &[
                row("one", 1_000, true, Some(true)),
                row("two", 1_001, false, Some(false)),
            ],
            1_001,
        )
        .expect("persist dataset");

        assert_eq!(stats.rows, 2);
        assert_eq!(stats.new_rows, 2);
        assert_eq!(stats.strict_passes, 1);
        assert_eq!(stats.strict_failures, 1);
        assert_eq!(stats.transfer_tests, 2);
        assert_eq!(stats.transfer_passes, 1);
        assert_eq!(stats.transfer_failures, 1);
        assert_eq!(stats.stream_tests, 0);
        assert_eq!(stats.unique_runs, 1);
        assert_eq!(stats.unique_candidates, 2);

        let body = fs::read_to_string(&path).expect("read dataset");
        let first = body.lines().next().expect("first row");
        let value: Value = serde_json::from_str(first).expect("parse row");
        assert_eq!(
            value.get("schema_version").and_then(Value::as_u64),
            Some(DATASET_VERSION)
        );
        assert_eq!(
            value
                .get("feature_extraction_version")
                .and_then(Value::as_u64),
            Some(FEATURE_EXTRACTION_VERSION)
        );
        assert_eq!(value["label"]["stream_tested"].as_bool(), Some(false));
        assert!(value["label"]["stream_pass"].is_null());
        assert_eq!(
            value["label"]["stream_validation_version"].as_u64(),
            Some(2)
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn readiness_requires_real_time_and_label_coverage() {
        let not_ready = super::DatasetStats {
            rows: 4_999,
            new_rows: 0,
            unique_runs: 20,
            unique_candidates: 1_000,
            strict_passes: 500,
            strict_failures: 4_499,
            transfer_tests: 1_000,
            transfer_passes: 100,
            transfer_failures: 900,
            stream_tests: 500,
            stream_passes: 50,
            stream_failures: 450,
        };
        assert!(!not_ready.strict_model_ready());

        let ready = super::DatasetStats {
            rows: 5_000,
            new_rows: 0,
            unique_runs: 20,
            unique_candidates: 1_000,
            strict_passes: 500,
            strict_failures: 4_500,
            transfer_tests: 1_000,
            transfer_passes: 100,
            transfer_failures: 900,
            stream_tests: 500,
            stream_passes: 50,
            stream_failures: 450,
        };
        assert!(ready.strict_model_ready());
        assert!(ready.end_to_end_model_ready());
    }

    #[test]
    fn v1_rows_are_upgraded_without_fabricating_stream_results() {
        let mut value = row("legacy", 1_000, true, None).to_value();
        value["schema_version"] = Value::from(1u64);
        value["label"]
            .as_object_mut()
            .expect("label")
            .remove("stream_tested");
        value["label"]
            .as_object_mut()
            .expect("label")
            .remove("stream_pass");

        super::upgrade_stored_row(&mut value);
        assert_eq!(
            value.get("schema_version").and_then(Value::as_u64),
            Some(DATASET_VERSION)
        );
        assert_eq!(value["label"]["stream_tested"].as_bool(), Some(false));
        assert!(value["label"]["stream_pass"].is_null());
        assert!(super::valid_stored_row(&value));
    }

    #[test]
    fn duplicate_observation_ids_are_idempotent() {
        let path = temp_path();
        let first = row("same", 1_000, true, None);
        let second = row("same", 1_001, false, None);

        persist_at(&path, &[first], 1_001).expect("initial persist");
        let stats = persist_at(&path, &[second], 1_001).expect("deduplicated persist");

        assert_eq!(stats.rows, 1);
        assert_eq!(stats.new_rows, 0);
        assert_eq!(stats.strict_passes, 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn old_rows_and_excess_rows_are_pruned() {
        let path = temp_path();
        let mut rows = vec![row("fresh", 1_000, true, None)];
        rows.push(row("old", 1, true, None));

        persist_at(&path, &rows, 1_000 + 45 * 24 * 60 * 60).expect("persist retained rows");

        let body = fs::read_to_string(&path).expect("read dataset");
        assert_eq!(body.lines().count(), 1);
        assert!(body.contains("\"fresh\""));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn persistence_prunes_rows_with_stale_feature_extraction_version() {
        let path = temp_path();
        let mut stale = row("stale", 1_000, true, None).to_value();
        stale["feature_extraction_version"] = Value::from(FEATURE_EXTRACTION_VERSION - 1);
        let stale_line = serde_json::to_string(&stale).expect("serialize stale row");
        fs::write(&path, stale_line + "\n").expect("write stale row");

        let stats = persist_at(&path, &[row("fresh", 1_001, true, None)], 1_001)
            .expect("persist fresh row");

        assert_eq!(stats.rows, 1);
        assert_eq!(stats.new_rows, 1);
        let body = fs::read_to_string(&path).expect("read persisted data");
        assert!(body.contains("\"fresh\""));
        assert!(!body.contains("\"stale\""));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn migrates_compatible_v2_rows_without_feature_version() {
        let path = temp_path();
        let mut legacy = row("legacy-v2", 1_000, true, None).to_value();
        legacy
            .as_object_mut()
            .expect("legacy row")
            .remove("feature_extraction_version");
        let legacy_line = serde_json::to_string(&legacy).expect("serialize legacy row");
        fs::write(&path, legacy_line + "\n").expect("write legacy row");

        let stats = persist_at(&path, &[row("fresh", 1_001, false, None)], 1_001)
            .expect("migrate compatible legacy row");
        assert_eq!(stats.rows, 2);

        let body = fs::read_to_string(&path).expect("read migrated dataset");
        let migrated = body
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("parse migrated row"))
            .find(|value| value.get("observation_id").and_then(Value::as_str) == Some("legacy-v2"))
            .expect("legacy row retained");
        assert_eq!(
            migrated
                .get("feature_extraction_version")
                .and_then(Value::as_u64),
            Some(FEATURE_EXTRACTION_VERSION)
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn does_not_migrate_v2_rows_with_incompatible_features() {
        let path = temp_path();
        let mut incompatible = row("incompatible-v2", 1_000, true, None).to_value();
        incompatible
            .as_object_mut()
            .expect("incompatible row")
            .remove("feature_extraction_version");
        incompatible["features"]
            .as_object_mut()
            .expect("features")
            .remove("history_pass_rate");
        let incompatible_line =
            serde_json::to_string(&incompatible).expect("serialize incompatible row");
        fs::write(&path, incompatible_line + "\n").expect("write incompatible row");

        let stats = persist_at(&path, &[row("fresh", 1_001, false, None)], 1_001)
            .expect("persist fresh row");
        assert_eq!(stats.rows, 1);

        let body = fs::read_to_string(&path).expect("read dataset");
        assert!(!body.contains("incompatible-v2"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_rows_with_wrong_feature_schema() {
        let path = temp_path();
        let mut value = row("invalid", 1_000, true, None).to_value();
        value["features"]
            .as_object_mut()
            .expect("features object")
            .remove("history_pass_rate");

        assert!(!super::valid_stored_row(&value));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_rows_with_stale_feature_extraction_version() {
        let mut value = row("stale-features", 1_000, true, None).to_value();
        value["feature_extraction_version"] = Value::from(FEATURE_EXTRACTION_VERSION - 1);

        assert!(!super::valid_stored_row(&value));
    }

    #[test]
    fn rejects_zero_strict_checks() {
        let mut value = row("invalid-checks", 1_000, true, None).to_value();
        value["label"]["strict_checks"] = Value::from(0u64);
        assert!(!super::valid_stored_row(&value));
    }

    #[test]
    fn feature_count_contract_is_explicit() {
        assert_eq!(FEATURE_COUNT, 19);
    }
}
