use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::process::Command;

const DEFAULT_SCORE: f64 = 0.5;
const SCRIPT_PATH: &str = "scripts/lightgbm_rank.py";
const DEFAULT_TRAINING_PATH: &str = "subscriptions/light-training.jsonl";
const DEFAULT_SCORE_PATH: &str = "/tmp/proxyrift/lightgbm-scores.json";
const MAX_SCORE: f64 = 1.0;
const MIN_SCORE: f64 = 0.0;

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

        fs::create_dir_all("/tmp/proxyrift")
            .map_err(|error| format!("failed to create LightGBM temp directory: {error}"))?;

        let candidate_path = "/tmp/proxyrift/lightgbm-candidates.txt";
        let body = candidates.join("\n") + "\n";
        fs::write(candidate_path, body)
            .map_err(|error| format!("failed to write LightGBM candidates: {error}"))?;

        let output = Command::new("python3")
            .arg(SCRIPT_PATH)
            .arg("--training")
            .arg(DEFAULT_TRAINING_PATH)
            .arg("--candidates")
            .arg(candidate_path)
            .arg("--output")
            .arg(DEFAULT_SCORE_PATH)
            .output()
            .map_err(|error| format!("failed to start LightGBM helper: {error}"))?;

        if !output.stdout.is_empty() {
            print!("{}", String::from_utf8_lossy(&output.stdout));
        }

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!(
                "LightGBM helper exited with {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        Self::from_file(DEFAULT_SCORE_PATH)
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

#[cfg(test)]
mod tests {
    use super::LightGbmScores;
    use std::collections::HashMap;

    #[test]
    fn missing_score_defaults_to_neutral_probability() {
        let scores = LightGbmScores {
            scores: HashMap::from([("vless://a@example.com:443".to_string(), 0.9)]),
            training_rows: 100,
            trained: true,
        };
        assert!((scores.score("vless://unknown@example.com:443") - 0.5).abs() < f64::EPSILON);
        assert!((scores.score("vless://a@example.com:443") - 0.9).abs() < f64::EPSILON);
    }
}
