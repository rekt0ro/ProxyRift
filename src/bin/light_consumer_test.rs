use proxyrift::consumer_history::{archetype_hash, config_hash, family_hash, now_unix, protocol};
use proxyrift::singbox::validate_candidates_with_consumer_targets as validate_singbox_consumer_targets;
use proxyrift::validator::{
    is_light_consumer_compatible, validate_candidates_with_consumer_targets, LIGHT_CONSUMER_TARGETS,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;

const DEFAULT_INPUT: &str = "subscriptions/light.txt";
const DEFAULT_HISTORY: &str = "subscriptions/light-consumer-results.json";
const DEFAULT_WORKERS: usize = 16;
const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_TIMEOUT_SECONDS: f64 = 6.0;
const DEFAULT_XRAY_TIMEOUT_SECONDS: f64 = 3.0;
const DEFAULT_MAX_LATENCY_MS: f64 = 800.0;
const DEFAULT_ROUNDS: usize = 1;
const DEFAULT_MAX_CANDIDATES: usize = 96;
const DEFAULT_DEEP_CANDIDATES: usize = 64;
const DEFAULT_DEEP_ROUNDS: usize = 3;
const RECENT_CONSUMER_WINDOW_SECS: u64 = 7 * 24 * 60 * 60;
const MAX_STORED_ROUNDS: usize = 30;

#[derive(Clone, Debug)]
struct ConfigResult {
    config_hash: String,
    family_hash: String,
    archetype_hash: String,
    protocol: String,
    compatible: bool,
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

fn result_from_metrics(
    config: &str,
    backend: &str,
    metrics: Option<&proxyrift::validator::ProxyMetrics>,
) -> ConfigResult {
    let passed = metrics.is_some();
    ConfigResult {
        config_hash: config_hash(config),
        family_hash: family_hash(config),
        archetype_hash: archetype_hash(config),
        protocol: protocol(config),
        compatible: true,
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
        "family_hash": result.family_hash,
        "archetype_hash": result.archetype_hash,
        "protocol": result.protocol,
        "compatible": result.compatible,
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

    if value
        .get("schema_version")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        != 2
    {
        println!("[INFO] 🧹 Ignoring legacy consumer history; starting structural history v2");
        return Ok(Vec::new());
    }

    let rounds = value
        .get("rounds")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{path} is missing the rounds array"))?;

    Ok(rounds.to_vec())
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
        "schema_version": 2,
        "input": input_path,
        "targets": targets,
        "privacy": {
            "stores_raw_configs": false,
            "stores_exact_config_hashes": true,
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
        "Usage: light_consumer_test [options]\n         \n         Options:\n           --input FILE          Light subscription input (default: subscriptions/light.txt)\n           --history FILE        Persistent result history (default: subscriptions/light-consumer-results.json)\n           --xray PATH           Xray binary (default: xray)\n           --singbox PATH        sing-box binary (default: sing-box)\n           --workers N           Validation workers (default: 16)\n           --batch-size N        Candidates per core batch (default: 64)\n           --timeout SECONDS     sing-box request timeout (default: 6)\n           --xray-timeout SECONDS Xray fallback request timeout (default: 3)\n           --max-latency-ms N    Maximum accepted latency (default: 800)\n           --rounds N            Full rounds in standard mode (default: 1)\n           --adaptive            Select a history-aware test pool\n           --max-candidates N    Adaptive pool size (default: 96)\n           --deep-candidates N   Adaptive candidates that get deep rounds (default: 64)\n           --deep-rounds N       Rounds for unseen/stale candidates (default: 3)\n           --help                Show this help\n         \n         The history stores exact + structural hashes only. Raw proxy URLs are never persisted.\n         The evidence file stores only structural aggregates for reuse by ranking."
    );
}

#[derive(Clone, Copy, Debug)]
struct HistoryStatus {
    observed_at: u64,
    pass: bool,
}

#[derive(Clone, Debug)]
struct AdaptivePlan {
    deep: Vec<String>,
    quick: Vec<String>,
}

fn latest_history_by_hash(rounds: &[Value]) -> HashMap<String, HistoryStatus> {
    let mut latest = HashMap::new();

    for round in rounds {
        let observed_at = round
            .get("observed_at")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let Some(results) = round.get("results").and_then(Value::as_array) else {
            continue;
        };

        for result in results {
            let Some(config_hash) = result.get("config_hash").and_then(Value::as_str) else {
                continue;
            };
            let status = HistoryStatus {
                observed_at,
                pass: result.get("pass").and_then(Value::as_bool).unwrap_or(false),
            };
            let replace = latest
                .get(config_hash)
                .map(|previous: &HistoryStatus| observed_at >= previous.observed_at)
                .unwrap_or(true);
            if replace {
                latest.insert(config_hash.to_string(), status);
            }
        }
    }

    latest
}

fn sort_adaptive_candidates(candidates: &mut [(String, u64, bool)], failures_first: bool) {
    candidates.sort_unstable_by(|left, right| {
        if failures_first {
            right.2.cmp(&left.2).then_with(|| left.1.cmp(&right.1))
        } else {
            left.1.cmp(&right.1)
        }
        .then_with(|| family_hash(&left.0).cmp(&family_hash(&right.0)))
        .then_with(|| config_hash(&left.0).cmp(&config_hash(&right.0)))
    });
}

fn take_family_diverse(candidates: &[(String, u64, bool)], limit: usize) -> Vec<String> {
    if limit == 0 {
        return Vec::new();
    }

    let mut selected = Vec::with_capacity(limit.min(candidates.len()));
    let mut selected_hashes = HashSet::new();
    let mut families = HashSet::new();

    for (config, _, _) in candidates {
        if selected.len() >= limit {
            break;
        }
        if !families.insert(family_hash(config)) {
            continue;
        }
        if selected_hashes.insert(config_hash(config)) {
            selected.push(config.clone());
        }
    }

    if selected.len() < limit {
        for (config, _, _) in candidates {
            if selected.len() >= limit {
                break;
            }
            if selected_hashes.insert(config_hash(config)) {
                selected.push(config.clone());
            }
        }
    }

    selected
}

fn build_adaptive_plan(
    candidates: &[String],
    history: &[Value],
    now: u64,
    max_candidates: usize,
    deep_candidates: usize,
) -> AdaptivePlan {
    let latest = latest_history_by_hash(history);
    let mut deep_pool = Vec::<(String, u64, bool)>::new();
    let mut quick_pool = Vec::<(String, u64, bool)>::new();

    for config in candidates {
        match latest.get(&config_hash(config)).copied() {
            None => deep_pool.push((config.clone(), 0, false)),
            Some(status)
                if now.saturating_sub(status.observed_at) >= RECENT_CONSUMER_WINDOW_SECS =>
            {
                deep_pool.push((config.clone(), status.observed_at, status.pass));
            }
            Some(status) => quick_pool.push((config.clone(), status.observed_at, status.pass)),
        }
    }

    sort_adaptive_candidates(&mut deep_pool, false);
    sort_adaptive_candidates(&mut quick_pool, true);

    let deep = take_family_diverse(&deep_pool, deep_candidates.min(max_candidates));
    let quick = take_family_diverse(&quick_pool, max_candidates.saturating_sub(deep.len()));

    AdaptivePlan { deep, quick }
}

#[allow(clippy::too_many_arguments)]
async fn validate_round(
    candidates: &[String],
    xray: &str,
    singbox: &str,
    workers: usize,
    batch_size: usize,
    timeout: f64,
    xray_timeout: f64,
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

        let request_timeout = std::time::Duration::try_from_secs_f64(timeout)
            .map_err(|_| "invalid consumer timeout".to_string())?;

        match validate_singbox_consumer_targets(
            singbox,
            &compatible,
            LIGHT_CONSUMER_TARGETS,
            workers.clamp(1, 40),
            request_timeout,
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
            xray_timeout,
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
                family_hash: family_hash(config),
                archetype_hash: archetype_hash(config),
                protocol: protocol(config),
                compatible: is_light_consumer_compatible(config),
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

fn aggregate_families(rounds: &[Value]) -> BTreeMap<String, (String, usize, usize)> {
    let mut data = BTreeMap::<String, (String, usize, usize)>::new();

    for round in rounds {
        let Some(results) = round.get("results").and_then(Value::as_array) else {
            continue;
        };

        for result in results {
            if !result
                .get("compatible")
                .and_then(Value::as_bool)
                .unwrap_or(true)
            {
                continue;
            }
            let Some(hash) = result.get("family_hash").and_then(Value::as_str) else {
                continue;
            };
            let protocol = result
                .get("protocol")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let passed = result.get("pass").and_then(Value::as_bool).unwrap_or(false);
            let entry = data.entry(hash.to_string()).or_insert((protocol, 0, 0));
            entry.1 += 1;
            if passed {
                entry.2 += 1;
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

    let aggregates = aggregate_families(rounds);
    let mut stable = aggregates
        .iter()
        .filter(|&(_, (_, observations, passes))| {
            *observations >= 2 && *passes * 100 >= *observations * 70
        })
        .map(|(hash, (protocol, observations, passes))| {
            (hash.clone(), protocol.clone(), *observations, *passes)
        })
        .collect::<Vec<_>>();

    stable.sort_by(|left, right| right.3.cmp(&left.3).then_with(|| right.2.cmp(&left.2)));

    println!(
        "[SUMMARY] Structural history: {} families | {} stored rounds | {} stable >=70%",
        aggregates.len(),
        rounds.len(),
        stable.len()
    );

    for (index, (hash, protocol, observations, passes)) in stable.into_iter().take(10).enumerate() {
        println!(
            "[SUMMARY] #{:02} {} {} | {}/{} passes",
            index + 1,
            protocol,
            &hash[..12],
            passes,
            observations
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
    let deep_rounds = parse_usize(&args, "--deep-rounds", DEFAULT_DEEP_ROUNDS)?;
    let max_candidates = parse_usize(&args, "--max-candidates", DEFAULT_MAX_CANDIDATES)?;
    let deep_candidates = parse_usize(&args, "--deep-candidates", DEFAULT_DEEP_CANDIDATES)?;
    let timeout = parse_f64(&args, "--timeout", DEFAULT_TIMEOUT_SECONDS)?;
    let xray_timeout = parse_f64(&args, "--xray-timeout", DEFAULT_XRAY_TIMEOUT_SECONDS)?;
    let max_latency_ms = parse_f64(&args, "--max-latency-ms", DEFAULT_MAX_LATENCY_MS)?;

    let candidates = read_candidates(&input_path)?;
    if candidates.is_empty() {
        return Err(format!("no candidates found in {input_path}"));
    }

    let mut history = load_history(&history_path)?;

    println!(
        "[INFO] 🧪 Light consumer validation | {} candidates",
        candidates.len()
    );
    println!("[INFO] 🎯 Targets: {}", LIGHT_CONSUMER_TARGETS.join(", "));
    println!("[INFO] ⏱️ Timeouts | sing-box: {timeout:.1}s | Xray fallback: {xray_timeout:.1}s");
    println!(
        "[INFO] 🔐 History stores exact + structural hashes | Raw proxy URLs are never persisted"
    );
    println!(
        "[INFO] 🧠 Full pass/fail consumer history is authoritative and is reused by update-run ranking"
    );

    let mut latest_round = Vec::new();

    if has_flag(&args, "--adaptive") {
        let now = now_unix()?;
        let plan = build_adaptive_plan(&candidates, &history, now, max_candidates, deep_candidates);
        let mut selected = plan.deep.clone();
        selected.extend(plan.quick.iter().cloned());

        if selected.is_empty() {
            return Err("adaptive consumer plan selected no candidates".to_string());
        }

        println!(
            "[INFO] 🧠 Adaptive pool | {} selected = {} deep + {} quick | Deep rounds: {}",
            selected.len(),
            plan.deep.len(),
            plan.quick.len(),
            deep_rounds
        );

        let started = std::time::Instant::now();
        latest_round = validate_round(
            &selected,
            &xray,
            &singbox,
            workers,
            batch_size,
            timeout,
            xray_timeout,
            max_latency_ms,
        )
        .await?;

        let observed_at = now_unix()?;
        history.push(json!({
            "observed_at": observed_at,
            "results": latest_round.iter().map(result_json).collect::<Vec<_>>()
        }));
        save_history(&history_path, &history, &input_path, LIGHT_CONSUMER_TARGETS)?;

        let passes = latest_round.iter().filter(|result| result.pass).count();
        println!(
            "[INFO] 📊 Adaptive quick round | {}/{} passed | {:.1}s",
            passes,
            latest_round.len(),
            started.elapsed().as_secs_f64()
        );

        for round_index in 1..deep_rounds {
            if plan.deep.is_empty() {
                break;
            }

            let started = std::time::Instant::now();
            latest_round = validate_round(
                &plan.deep,
                &xray,
                &singbox,
                workers,
                batch_size,
                timeout,
                xray_timeout,
                max_latency_ms,
            )
            .await?;

            let observed_at = now_unix()?;
            history.push(json!({
                "observed_at": observed_at,
                "results": latest_round.iter().map(result_json).collect::<Vec<_>>()
            }));
            save_history(&history_path, &history, &input_path, LIGHT_CONSUMER_TARGETS)?;

            let passes = latest_round.iter().filter(|result| result.pass).count();
            println!(
                "[INFO] 📊 Adaptive deep round {}/{} | {}/{} passed | {:.1}s",
                round_index + 1,
                deep_rounds,
                passes,
                latest_round.len(),
                started.elapsed().as_secs_f64()
            );
        }
    } else {
        println!(
            "[INFO] 🧪 Standard consumer validation | {} candidates | {} rounds",
            candidates.len(),
            rounds
        );

        for round_index in 0..rounds {
            let started = std::time::Instant::now();
            let latest = validate_round(
                &candidates,
                &xray,
                &singbox,
                workers,
                batch_size,
                timeout,
                xray_timeout,
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
    }

    print_summary(&history, &latest_round);

    Ok(())
}

#[cfg(test)]
mod adaptive_tests {
    use super::{
        archetype_hash, build_adaptive_plan, config_hash, family_hash, RECENT_CONSUMER_WINDOW_SECS,
    };
    use serde_json::json;

    fn round(observed_at: u64, configs: &[(&str, bool)]) -> serde_json::Value {
        json!({
            "observed_at": observed_at,
            "results": configs.iter().map(|(config, pass)| json!({
                "config_hash": config_hash(config),
                "family_hash": family_hash(config),
                "archetype_hash": archetype_hash(config),
                "protocol": "vless",
                "compatible": true,
                "pass": pass
            })).collect::<Vec<_>>()
        })
    }

    #[test]
    fn adaptive_plan_puts_unseen_and_stale_into_deep_pool() {
        let now = 1_000_000;
        let recent = "vless://recent@example.com:443?type=tcp";
        let stale = "vless://stale@example.com:443?type=tcp&sni=stale.example";
        let unseen = "vless://unseen@example.com:443?type=ws&sni=unseen.example";
        let history = vec![
            round(now - RECENT_CONSUMER_WINDOW_SECS - 1, &[(stale, true)]),
            round(now - 10, &[(recent, true)]),
        ];

        let plan = build_adaptive_plan(
            &[recent.to_string(), stale.to_string(), unseen.to_string()],
            &history,
            now,
            3,
            2,
        );

        assert!(plan.deep.contains(&stale.to_string()));
        assert!(plan.deep.contains(&unseen.to_string()));
        assert!(plan.quick.contains(&recent.to_string()));
    }

    #[test]
    fn adaptive_plan_caps_pool_and_keeps_family_diversity() {
        let configs = [
            "vless://one@example.com:443?type=tcp&sni=one.example",
            "vless://two@example.com:443?type=tcp&sni=two.example",
            "vless://three@example.com:443?type=ws&sni=three.example",
            "trojan://four@example.com:443?type=tcp&sni=four.example",
            "ss://five@example.com:8388?type=tcp",
        ];
        let candidate_strings = configs
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let plan = build_adaptive_plan(&candidate_strings, &[], 1_000_000, 3, 2);

        assert_eq!(plan.deep.len(), 2);
        assert_eq!(plan.quick.len(), 0);
        assert_eq!(plan.deep.iter().chain(plan.quick.iter()).count(), 2);
        assert_ne!(family_hash(&plan.deep[0]), family_hash(&plan.deep[1]));
    }
}
