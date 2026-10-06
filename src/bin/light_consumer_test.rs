use proxyrift::singbox::validate_candidates_with_consumer_targets as validate_singbox_consumer_targets;
use proxyrift::validator::{
    is_light_consumer_compatible, validate_candidates_with_consumer_targets, LIGHT_CONSUMER_TARGETS,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_INPUT: &str = "subscriptions/light.txt";
const DEFAULT_HISTORY: &str = "subscriptions/light-consumer-results.json";
const DEFAULT_WORKERS: usize = 8;
const DEFAULT_BATCH_SIZE: usize = 24;
const DEFAULT_TIMEOUT_SECONDS: f64 = 15.0;
const DEFAULT_MAX_LATENCY_MS: f64 = 800.0;
const DEFAULT_ROUNDS: usize = 1;
const MAX_STORED_ROUNDS: usize = 30;

#[derive(Clone, Debug)]
struct ConfigResult {
    config_hash: String,
    protocol: String,
    pass: bool,
    backend: Option<String>,
    attempts: usize,
    successes: usize,
    median_ms: Option<f64>,
    min_ms: Option<f64>,
    jitter_ms: Option<f64>,
    throughput_kbps: Option<f64>,
}

fn value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}

fn parse_usize(args: &[String], name: &str, default: usize) -> Result<usize, String> {
    value(args, name, &default.to_string())
        .parse::<usize>()
        .map_err(|_| format!("invalid {name}"))
        .and_then(|value| {
            if value == 0 {
                Err(format!("{name} must be greater than zero"))
            } else {
                Ok(value)
            }
        })
}

fn parse_f64(args: &[String], name: &str, default: f64) -> Result<f64, String> {
    value(args, name, &default.to_string())
        .parse::<f64>()
        .map_err(|_| format!("invalid {name}"))
        .and_then(|value| {
            if !value.is_finite() || value <= 0.0 {
                Err(format!("{name} must be positive and finite"))
            } else {
                Ok(value)
            }
        })
}

fn fnv64(value: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn config_hash(config: &str) -> String {
    let first = fnv64(config.as_bytes(), 0xcbf29ce484222325);
    let second = fnv64(config.as_bytes(), 0x84222325cbf29ce4d);
    format!("{first:016x}{second:016x}")
}

fn protocol(config: &str) -> String {
    config
        .split_once("://")
        .map(|(scheme, _)| scheme.to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string())
}

fn read_candidates(path: &str) -> Result<Vec<String>, String> {
    let content =
        fs::read_to_string(path).map_err(|error| format!("failed to read {path}: {error}"))?;
    let mut seen = HashSet::new();

    Ok(content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter(|line| seen.insert((*line).to_string()))
        .map(ToOwned::to_owned)
        .collect())
}

fn now_unix() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .map_err(|error| format!("system clock error: {error}"))
}

fn result_from_metrics(
    config: &str,
    backend: &str,
    metrics: Option<&proxyrift::validator::ProxyMetrics>,
) -> ConfigResult {
    let passed = metrics.is_some();
    ConfigResult {
        config_hash: config_hash(config),
        protocol: protocol(config),
        pass: passed,
        backend: passed.then(|| backend.to_string()),
        attempts: metrics.map(|value| value.attempts).unwrap_or(0),
        successes: metrics.map(|value| value.successes).unwrap_or(0),
        median_ms: metrics.map(|value| value.median_ms),
        min_ms: metrics.map(|value| value.min_ms),
        jitter_ms: metrics.map(|value| value.jitter_ms),
        throughput_kbps: metrics.map(|value| value.throughput_kbps),
    }
}

fn result_json(result: &ConfigResult) -> Value {
    json!({
        "config_hash": result.config_hash,
        "protocol": result.protocol,
        "pass": result.pass,
        "backend": result.backend,
        "attempts": result.attempts,
        "successes": result.successes,
        "median_ms": result.median_ms,
        "min_ms": result.min_ms,
        "jitter_ms": result.jitter_ms,
        "throughput_kbps": result.throughput_kbps,
    })
}

fn load_history(path: &str) -> Result<Vec<Value>, String> {
    if !std::path::Path::new(path).exists() {
        return Ok(Vec::new());
    }

    let content =
        fs::read_to_string(path).map_err(|error| format!("failed to read {path}: {error}"))?;
    let value: Value = serde_json::from_str(&content)
        .map_err(|error| format!("invalid consumer history {path}: {error}"))?;

    let rounds = value
        .get("rounds")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{path} is missing the rounds array"))?;

    Ok(rounds.iter().cloned().collect())
}

fn save_history(
    path: &str,
    rounds: &[Value],
    input_path: &str,
    targets: &[&str],
) -> Result<(), String> {
    let stored_rounds = rounds
        .iter()
        .rev()
        .take(MAX_STORED_ROUNDS)
        .cloned()
        .collect::<Vec<_>>();

    let mut stored_rounds = stored_rounds;
    stored_rounds.reverse();

    let output = json!({
        "schema_version": 1,
        "input": input_path,
        "targets": targets,
        "privacy": {
            "stores_raw_configs": false,
            "stores_country_or_isp": false
        },
        "rounds": stored_rounds
    });

    if let Some(parent) = std::path::Path::new(path).parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }

    let encoded = serde_json::to_vec_pretty(&output)
        .map_err(|error| format!("failed to encode consumer history: {error}"))?;
    fs::write(path, encoded).map_err(|error| format!("failed to write {path}: {error}"))
}

fn print_usage() {
    println!(
        "Usage: light_consumer_test [options]\n\
         \n\
         Options:\n\
           --input FILE          Light subscription input (default: subscriptions/light.txt)\n\
           --history FILE        Persistent result history (default: subscriptions/light-consumer-results.json)\n\
           --xray PATH           Xray binary (default: xray)\n\
           --singbox PATH        sing-box binary (default: sing-box)\n\
           --workers N           Validation workers (default: 8)\n\
           --batch-size N        Candidates per core batch (default: 24)\n\
           --timeout SECONDS     Per-request timeout (default: 15)\n\
           --max-latency-ms N    Maximum accepted latency (default: 800)\n\
           --rounds N            Consecutive rounds in one invocation (default: 1)\n\
           --help                Show this help\n\
         \n\
         The tester never writes raw proxy URLs to the history file."
    );
}

async fn validate_round(
    candidates: &[String],
    xray: &str,
    singbox: &str,
    workers: usize,
    batch_size: usize,
    timeout: f64,
    max_latency_ms: f64,
) -> Result<Vec<ConfigResult>, String> {
    let compatible = candidates
        .iter()
        .filter(|config| is_light_consumer_compatible(config))
        .cloned()
        .collect::<Vec<_>>();

    let skipped = candidates.len().saturating_sub(compatible.len());
    if skipped > 0 {
        println!("[INFO] 🧹 Consumer prefilter skipped {skipped} locally unsupported candidates");
    }

    let mut results = Vec::with_capacity(compatible.len());
    let mut unresolved = Vec::new();

    if !compatible.is_empty() {
        println!(
            "[INFO] 🧪 Consumer round | {} compatible candidates | {} target URLs",
            compatible.len(),
            LIGHT_CONSUMER_TARGETS.len()
        );

        match validate_singbox_consumer_targets(
            singbox,
            &compatible,
            LIGHT_CONSUMER_TARGETS,
            workers.clamp(1, 40),
            batch_size.max(1),
            timeout,
            max_latency_ms,
        )
        .await
        {
            Ok(verified) => {
                let verified_set = verified.keys().cloned().collect::<HashSet<_>>();
                for config in &compatible {
                    if let Some(metrics) = verified.get(config) {
                        results.push(result_from_metrics(config, "sing-box", Some(metrics)));
                    } else {
                        unresolved.push(config.clone());
                    }
                }
                println!(
                    "[INFO] ✅ [Consumer/sing-box] {}/{} passed",
                    verified_set.len(),
                    compatible.len()
                );
            }
            Err(error) => {
                println!("[WARN] ⚠️ [Consumer/sing-box] unavailable | {error} | Trying Xray");
                unresolved.extend(compatible.iter().cloned());
            }
        }
    }

    if !unresolved.is_empty() {
        match validate_candidates_with_consumer_targets(
            xray,
            &unresolved,
            LIGHT_CONSUMER_TARGETS,
            workers.clamp(1, 40),
            batch_size.max(1),
            timeout,
            max_latency_ms,
        )
        .await
        {
            Ok(verified) => {
                let verified_set = verified.keys().cloned().collect::<HashSet<_>>();
                for config in &unresolved {
                    let metrics = verified.get(config);
                    results.push(result_from_metrics(config, "xray", metrics));
                }
                println!(
                    "[INFO] ✅ [Consumer/Xray] {}/{} unresolved candidates passed",
                    verified_set.len(),
                    unresolved.len()
                );
            }
            Err(error) => {
                return Err(format!("Xray consumer validation failed: {error}"));
            }
        }
    }

    let mut seen = HashSet::new();
    results.retain(|result| seen.insert(result.config_hash.clone()));

    for config in candidates {
        let hash = config_hash(config);
        if !seen.contains(&hash) {
            results.push(ConfigResult {
                config_hash: hash,
                protocol: protocol(config),
                pass: false,
                backend: None,
                attempts: 0,
                successes: 0,
                median_ms: None,
                min_ms: None,
                jitter_ms: None,
                throughput_kbps: None,
            });
        }
    }

    results.sort_by(|left, right| {
        right
            .pass
            .cmp(&left.pass)
            .then_with(|| left.protocol.cmp(&right.protocol))
            .then_with(|| left.config_hash.cmp(&right.config_hash))
    });

    Ok(results)
}

fn aggregate(rounds: &[Value]) -> BTreeMap<String, (String, usize, usize, f64)> {
    let mut data = BTreeMap::<String, (String, usize, usize, f64)>::new();

    for round in rounds {
        let Some(results) = round.get("results").and_then(Value::as_array) else {
            continue;
        };

        for result in results {
            let Some(hash) = result.get("config_hash").and_then(Value::as_str) else {
                continue;
            };
            let protocol = result
                .get("protocol")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let passed = result.get("pass").and_then(Value::as_bool).unwrap_or(false);
            let latency = result
                .get("median_ms")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);

            let entry = data
                .entry(hash.to_string())
                .or_insert((protocol, 0, 0, 0.0));
            entry.1 += 1;
            if passed {
                entry.2 += 1;
                if latency > 0.0 {
                    entry.3 += latency;
                }
            }
        }
    }

    data
}

fn print_summary(rounds: &[Value], latest: &[ConfigResult]) {
    let latest_passes = latest.iter().filter(|result| result.pass).count();
    println!();
    println!(
        "[SUMMARY] Latest consumer round: {}/{} passed",
        latest_passes,
        latest.len()
    );

    let aggregates = aggregate(rounds);
    let mut stable = aggregates
        .iter()
        .filter_map(|(hash, (protocol, observations, passes, latency_sum))| {
            (*observations >= 2 && *passes * 100 >= *observations * 70).then(|| {
                let avg_latency = if *passes > 0 {
                    latency_sum / *passes as f64
                } else {
                    0.0
                };
                (
                    hash.clone(),
                    protocol.clone(),
                    *observations,
                    *passes,
                    avg_latency,
                )
            })
        })
        .collect::<Vec<_>>();

    stable.sort_by(|left, right| {
        right
            .3
            .cmp(&left.3)
            .then_with(|| left.4.total_cmp(&right.4))
    });

    println!(
        "[SUMMARY] Historical observations: {} configs | {} stored rounds | {} stable >=70%",
        aggregates.len(),
        rounds.len(),
        stable.len()
    );

    for (index, (hash, protocol, observations, passes, avg_latency)) in
        stable.into_iter().take(10).enumerate()
    {
        println!(
            "[SUMMARY] #{:02} {} {} | {}/{} passes | avg median {:.0} ms",
            index + 1,
            protocol,
            &hash[..12],
            passes,
            observations,
            avg_latency
        );
    }

    if latest_passes == 0 {
        println!("[WARN] ⚠️ No consumer-passing configs were found in the latest round.");
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = env::args().collect::<Vec<_>>();
    if has_flag(&args, "--help") || has_flag(&args, "-h") {
        print_usage();
        return Ok(());
    }

    let input_path = value(&args, "--input", DEFAULT_INPUT);
    let history_path = value(&args, "--history", DEFAULT_HISTORY);
    let xray = value(&args, "--xray", "xray");
    let singbox = value(&args, "--singbox", "sing-box");
    let workers = parse_usize(&args, "--workers", DEFAULT_WORKERS)?;
    let batch_size = parse_usize(&args, "--batch-size", DEFAULT_BATCH_SIZE)?;
    let rounds = parse_usize(&args, "--rounds", DEFAULT_ROUNDS)?;
    let timeout = parse_f64(&args, "--timeout", DEFAULT_TIMEOUT_SECONDS)?;
    let max_latency_ms = parse_f64(&args, "--max-latency-ms", DEFAULT_MAX_LATENCY_MS)?;

    let candidates = read_candidates(&input_path)?;
    if candidates.is_empty() {
        return Err(format!("no candidates found in {input_path}"));
    }

    let mut history = load_history(&history_path)?;

    println!(
        "[INFO] 🧪 Light consumer validation | {} candidates | {} rounds",
        candidates.len(),
        rounds
    );
    println!("[INFO] 🎯 Targets: {}", LIGHT_CONSUMER_TARGETS.join(", "));
    println!("[INFO] 🔐 History stores hashes only | Raw configs are never persisted");

    let mut latest_round = Vec::new();

    for round_index in 0..rounds {
        let started = std::time::Instant::now();
        let latest = validate_round(
            &candidates,
            &xray,
            &singbox,
            workers,
            batch_size,
            timeout,
            max_latency_ms,
        )
        .await?;

        let observed_at = now_unix()?;
        history.push(json!({
            "observed_at": observed_at,
            "results": latest.iter().map(result_json).collect::<Vec<_>>()
        }));

        save_history(&history_path, &history, &input_path, LIGHT_CONSUMER_TARGETS)?;

        latest_round = latest.clone();
        let passes = latest.iter().filter(|result| result.pass).count();
        println!(
            "[INFO] 📊 Consumer round {}/{} complete | {}/{} passed | {:.1}s",
            round_index + 1,
            rounds,
            passes,
            latest.len(),
            started.elapsed().as_secs_f64()
        );
    }

    print_summary(&history, &latest_round);

    Ok(())
}
