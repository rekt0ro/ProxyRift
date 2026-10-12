use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use proxyrift::light_gbm::{backend_for_config, LightGbmScores};
use proxyrift::light_training::{
    persist as persist_light_training, write_readiness_report, DatasetStats, TrainingRow,
};
use proxyrift::singbox::{
    validate_candidates_with_consumer_targets as validate_singbox_consumer_targets,
    validate_candidates_with_ordered_target_pool_once_with_minimum_body as validate_singbox_target_pool_once_with_minimum_body,
    validate_candidates_with_target_once as validate_singbox_target_once,
    validate_candidates_with_target_pool_once_with_sustained_stream as validate_singbox_target_pool_once_with_sustained_stream,
};
use proxyrift::validator::{
    endpoint, is_light_consumer_compatible, read_lines, target_is_rate_limited,
    target_rate_limit_events, validate_candidates_with_consumer_targets,
    validate_candidates_with_ordered_target_pool_once_with_minimum_body,
    validate_candidates_with_target_once,
    validate_candidates_with_target_pool_once_with_sustained_stream, write_lines, ProxyMetrics,
    LIGHT_CONSUMER_TARGETS, LIGHT_TRANSFER_MINIMUM_TARGETS, LIGHT_TRANSFER_STABILITY_BYTES,
    LIGHT_TRANSFER_STABILITY_TARGETS, PRIMARY_TARGET,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::env;
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use url::Url;

const DISCOVERY_BATCH_MIN: usize = 24;
const DISCOVERY_BATCH_MAX: usize = 300;
const DISCOVERY_BATCH_HARD_MAX: usize = 600;
const DISCOVERY_STALL_BATCH_SIZE: usize = DISCOVERY_BATCH_HARD_MAX;
const DISCOVERY_STAGNATION_WAVES: usize = 4;
const MAX_DISCOVERY_CANDIDATES: usize = 15_000;
const DISCOVERY_SAFETY_FACTOR: f64 = 1.15;
const TRANSFER_RESERVE_DEFAULT_PASS_RATE: f64 = 0.80;
const FINAL_RECHECK_LIMIT: usize = 350;
const DEFAULT_SELECTION_LIMIT: usize = 200;
const DEFAULT_MAX_PER_ENDPOINT: usize = 1;
const DEFAULT_MAX_PER_FAMILY: usize = 3;
const STRICT_VALIDATION_RESERVE_PERCENT: usize = 20;
const STRICT_VALIDATION_RESERVE_MAX: usize = 64;
const STRICT_ADAPTIVE_MIN_TRANSFER_SAMPLES: usize = 32;
const STRICT_ADAPTIVE_MIN_STREAM_SAMPLES: usize = 16;
const STRICT_ADAPTIVE_TARGET_GROWTH_MAX: usize = 64;
const RECHECK_FAMILY_DIVERSITY: usize = 3;
const RECHECK_MAX_PER_ENDPOINT: usize = 2;
const RECHECK_EXPLORATION_PERCENT: usize = 15;
const MAX_RECHECK_EXPLORATION: usize = 64;
const MAX_FINAL_RECHECK_ATTEMPTS: usize = 2;
const FINAL_TRANSFER_BATCH_SIZE: usize = 32;
const FINAL_TRANSFER_WORKERS: usize = 12;
const FINAL_TRANSFER_INITIAL_WORKERS: usize = 10;
const FINAL_TRANSFER_MIN_WORKERS: usize = 2;
const FINAL_TRANSFER_QUEUE_MULTIPLIER: usize = 3;
const FINAL_TRANSFER_CLEAN_BATCHES_TO_RAMP: usize = 2;
const FINAL_TRANSFER_TEST_LIMIT: usize = 450;
const STABILITY_TRANSFER_TEST_LIMIT: usize = 450;
const STABILITY_TRANSFER_BATCH_SIZE: usize = 32;
const STABILITY_TRANSFER_WORKERS: usize = 16;
const STABILITY_TEST_MAX_PER_ENDPOINT: usize = 2;
const STABILITY_TEST_MAX_PER_FAMILY: usize = 6;
const STABILITY_TRANSFER_MAX_LATENCY_MS: f64 = 15000.0;
const STABILITY_TARGET_SAFETY_FACTOR: f64 = 1.15;
const STABILITY_TARGET_MIN_RESERVE: usize = 24;
const STABILITY_TRANSFER_DEFAULT_PASS_RATE: f64 = 0.68;
const STABILITY_COMPLETION_GRACE_REMAINING: usize = 24;
const STABILITY_COMPLETION_BATCH_SIZE: usize = 8;
const STREAM_CONTINUITY_RESERVE_PERCENT: usize = 5;
const STREAM_CONTINUITY_RESERVE_MAX: usize = 16;
const STREAM_CONTINUITY_WORKERS: usize = 24;
const STREAM_CONTINUITY_SEGMENTS: usize = 9;
const STREAM_CONTINUITY_SEGMENT_BYTES: usize = 1_048_576;
const STREAM_CONTINUITY_MAX_IDLE_SECS: u64 = 4;
const FINAL_TRANSFER_TIMEOUT_SECS: f64 = 15.0;
const FINAL_TRANSFER_LATENCY_LIMIT_MS: f64 = 15000.0;
const HISTORY_MAX_ENTRIES: usize = 10000;
const HISTORY_RETENTION_SECS: u64 = 45 * 24 * 60 * 60;
const LIGHT_TRAINING_PATH: &str = "subscriptions/light-training.jsonl";
const LIGHT_TRAINING_STATS_PATH: &str = "subscriptions/light-training-stats.json";
const LIGHT_SUBSCRIPTION_PATH: &str = "subscriptions/light.txt";
const LIGHT_CANDIDATE_LEDGER_PATH: &str = "subscriptions/light-candidate-ledger.json";
const REVALIDATION_WINDOW_SECS: u64 = 24 * 60 * 60;
const REVALIDATED_CANDIDATE_PERCENT: usize = 20;
const MAX_CANDIDATE_LEDGER_ENTRIES: usize = 10_000;
const HISTORICAL_LIGHT_COHORTS: usize = 7;
const FRESH_FAMILY_TARGET_PERCENT: usize = 70;

fn final_publication_limit(selection_target: usize) -> usize {
    selection_target
}

fn transfer_validation_target(selection_limit: usize) -> usize {
    if selection_limit == 0 {
        return 0;
    }

    let reserve = selection_limit
        .saturating_mul(STREAM_CONTINUITY_RESERVE_PERCENT)
        .div_ceil(100)
        .clamp(1, STREAM_CONTINUITY_RESERVE_MAX);
    selection_limit.saturating_add(reserve)
}

fn strict_validation_target(selection_limit: usize) -> usize {
    if selection_limit == 0 {
        return 0;
    }

    let reserve = selection_limit
        .saturating_mul(STRICT_VALIDATION_RESERVE_PERCENT)
        .div_ceil(100)
        .clamp(1, STRICT_VALIDATION_RESERVE_MAX);

    selection_limit.saturating_add(reserve)
}

fn strict_validation_batch_limit(
    desired_target: usize,
    strict_verified: usize,
    adaptive_attempt_limit: usize,
) -> usize {
    desired_target
        .saturating_sub(strict_verified)
        .min(adaptive_attempt_limit)
}

fn adaptive_recheck_limit(
    remaining: usize,
    configured_limit: usize,
    checked_candidates: usize,
    strict_verified: usize,
) -> usize {
    if remaining == 0 || configured_limit == 0 {
        return 0;
    }

    let observed_rate = if checked_candidates < 20 {
        0.20
    } else {
        ((strict_verified as f64 + 2.0) / (checked_candidates as f64 + 4.0)).clamp(0.05, 1.0)
    };

    let estimated = ((remaining as f64 / observed_rate) * 1.25).ceil() as usize;
    let exploration_floor = if checked_candidates == 0 {
        remaining.saturating_mul(4).saturating_add(50)
    } else {
        remaining.saturating_mul(2).saturating_add(20)
    };

    estimated.max(exploration_floor).min(configured_limit)
}

#[derive(Clone, Copy, Debug, Default)]
struct AdaptiveStrictValidationInputs {
    selection_limit: usize,
    strict_verified: usize,
    strict_selectable: usize,
    transfer_tested: usize,
    transfer_passed: usize,
    stream_tested: usize,
    stream_passed: usize,
    reachable_candidates: usize,
}

fn adaptive_strict_validation_target(inputs: AdaptiveStrictValidationInputs) -> usize {
    let AdaptiveStrictValidationInputs {
        selection_limit,
        strict_verified,
        strict_selectable,
        transfer_tested,
        transfer_passed,
        stream_tested,
        stream_passed,
        reachable_candidates,
    } = inputs;

    if selection_limit == 0 {
        return 0;
    }

    let selection_rate = if strict_verified < 32 {
        1.0
    } else {
        (strict_selectable as f64 / strict_verified as f64).clamp(0.25, 1.0)
    };
    let minimum_target =
        ((strict_validation_target(selection_limit) as f64 / selection_rate).ceil() as usize)
            .max(strict_validation_target(selection_limit));
    let reachable_target = strict_verified
        .saturating_add(reachable_candidates)
        .max(strict_verified);

    if transfer_tested < STRICT_ADAPTIVE_MIN_TRANSFER_SAMPLES
        || stream_tested < STRICT_ADAPTIVE_MIN_STREAM_SAMPLES
    {
        return minimum_target.max(strict_verified).min(reachable_target);
    }

    let transfer_rate =
        ((transfer_passed as f64 + 2.0) / (transfer_tested as f64 + 4.0)).clamp(0.35, 0.95);
    let stream_rate =
        ((stream_passed as f64 + 2.0) / (stream_tested as f64 + 4.0)).clamp(0.50, 0.98);

    let downstream_rate = (transfer_rate * stream_rate).clamp(0.25, 0.95);
    let projected_selectable =
        ((selection_limit as f64 / downstream_rate) * DISCOVERY_SAFETY_FACTOR).ceil() as usize;
    let projected_target =
        ((projected_selectable as f64 / selection_rate).ceil() as usize).max(minimum_target);
    let growth_ceiling = strict_verified.saturating_add(STRICT_ADAPTIVE_TARGET_GROWTH_MAX);

    projected_target.min(growth_ceiling).min(reachable_target)
}

fn recheck_exploration_limit(total_limit: usize) -> usize {
    if total_limit == 0 {
        return 0;
    }

    total_limit
        .saturating_mul(RECHECK_EXPLORATION_PERCENT)
        .div_ceil(100)
        .clamp(1, MAX_RECHECK_EXPLORATION)
        .min(total_limit)
}

fn recheck_exploration_seed(wave: usize) -> u64 {
    let base = env::var("GITHUB_RUN_ID")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or_default()
        });

    base ^ (wave as u64).wrapping_mul(0x9e3779b97f4a7c15)
}

fn exploration_sort_key(config: &str, seed: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325u64 ^ seed;

    for byte in config.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    hash
}

fn select_recheck_candidates(
    model_ranked: &[String],
    untested: &[String],
    limit: usize,
    max_family: usize,
    exploration_limit: usize,
    seed: u64,
) -> (Vec<String>, usize) {
    if limit == 0 || model_ranked.is_empty() {
        return (Vec::new(), 0);
    }

    let mut selected = Vec::with_capacity(limit.min(model_ranked.len()));
    let mut selected_set = HashSet::new();
    let mut endpoint_counts = HashMap::<(String, u16), usize>::new();
    let mut family_counts = HashMap::<String, usize>::new();

    let try_add = |config: &String,
                   selected: &mut Vec<String>,
                   selected_set: &mut HashSet<String>,
                   endpoint_counts: &mut HashMap<(String, u16), usize>,
                   family_counts: &mut HashMap<String, usize>|
     -> bool {
        if selected.len() >= limit || !selected_set.insert(config.clone()) {
            return false;
        }

        let family = family_key(config);
        if family_counts.get(&family).copied().unwrap_or(0) >= max_family {
            selected_set.remove(config);
            return false;
        }

        if let Some(ep) = endpoint(config) {
            if endpoint_counts.get(&ep).copied().unwrap_or(0) >= RECHECK_MAX_PER_ENDPOINT {
                selected_set.remove(config);
                return false;
            }
            *endpoint_counts.entry(ep).or_default() += 1;
        }

        *family_counts.entry(family).or_default() += 1;
        selected.push(config.clone());
        true
    };

    let mut exploration_ranked = untested.to_vec();
    exploration_ranked.sort_unstable_by_key(|config| exploration_sort_key(config, seed));

    let mut exploration_selected = 0usize;
    for config in exploration_ranked {
        if selected.len() >= limit || exploration_selected >= exploration_limit {
            break;
        }

        if try_add(
            &config,
            &mut selected,
            &mut selected_set,
            &mut endpoint_counts,
            &mut family_counts,
        ) {
            exploration_selected += 1;
        }
    }

    for config in model_ranked {
        if selected.len() >= limit {
            break;
        }

        let _ = try_add(
            config,
            &mut selected,
            &mut selected_set,
            &mut endpoint_counts,
            &mut family_counts,
        );
    }

    (selected, exploration_selected)
}

fn select_stability_test_batch(
    ranked: &[String],
    stable_selected: &[String],
    limit: usize,
    selection_target: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> Vec<String> {
    if limit == 0 || selection_target == 0 || stable_selected.len() >= selection_target {
        return Vec::new();
    }

    let mut selected_for_capacity = stable_selected.to_vec();
    let mut batch = Vec::with_capacity(limit.min(ranked.len()));

    for config in ranked {
        if batch.len() >= limit {
            break;
        }

        if selection_additional_potential_count(
            &selected_for_capacity,
            std::slice::from_ref(config),
            selection_target,
            max_per_endpoint,
            max_per_family,
        ) == 0
        {
            continue;
        }

        selected_for_capacity.push(config.clone());
        batch.push(config.clone());
    }

    batch
}

#[cfg(test)]
fn selection_eligible_count(
    configs: &[String],
    max_per_endpoint: usize,
    max_per_family: usize,
) -> usize {
    select_verified_configs(configs, configs.len(), max_per_endpoint, max_per_family).len()
}

fn selection_rejection_counts(
    configs: &[String],
    limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> (usize, usize, usize) {
    let mut endpoint_counts = HashMap::<(String, u16), usize>::new();
    let mut family_counts = HashMap::<String, usize>::new();
    let mut selected = 0usize;
    let mut endpoint_rejected = 0usize;
    let mut family_rejected = 0usize;

    for config in configs {
        if selected >= limit {
            break;
        }

        if let Some(endpoint) = endpoint(config) {
            if endpoint_counts.get(&endpoint).copied().unwrap_or(0) >= max_per_endpoint {
                endpoint_rejected += 1;
                continue;
            }

            let family = family_key(config);
            if family_counts.get(&family).copied().unwrap_or(0) >= max_per_family {
                family_rejected += 1;
                continue;
            }

            *endpoint_counts.entry(endpoint).or_insert(0) += 1;
            *family_counts.entry(family).or_insert(0) += 1;
            selected += 1;
        } else {
            let family = family_key(config);
            if family_counts.get(&family).copied().unwrap_or(0) >= max_per_family {
                family_rejected += 1;
                continue;
            }

            *family_counts.entry(family).or_insert(0) += 1;
            selected += 1;
        }
    }

    (selected, endpoint_rejected, family_rejected)
}

#[cfg(test)]
fn selection_potential_count(
    transfer_ranked: &[String],
    untested_strict: &[String],
    max_per_endpoint: usize,
    max_per_family: usize,
) -> usize {
    let mut combined = Vec::with_capacity(transfer_ranked.len() + untested_strict.len());
    combined.extend_from_slice(transfer_ranked);
    combined.extend_from_slice(untested_strict);
    selection_eligible_count(&combined, max_per_endpoint, max_per_family)
}

fn selection_additional_potential_count(
    selected: &[String],
    candidates: &[String],
    selection_limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> usize {
    if selected.len() >= selection_limit {
        return 0;
    }

    let mut combined = Vec::with_capacity(selected.len() + candidates.len());
    combined.extend_from_slice(selected);
    combined.extend_from_slice(candidates);

    select_verified_configs(&combined, selection_limit, max_per_endpoint, max_per_family)
        .len()
        .saturating_sub(selected.len())
}

fn adaptive_transfer_test_limit(
    selection_limit: usize,
    selected_len: usize,
    transfer_tested: usize,
    transfer_passed: usize,
    eligible_remaining: usize,
) -> usize {
    if selected_len >= selection_limit || eligible_remaining == 0 {
        return transfer_tested;
    }

    let remaining = selection_limit.saturating_sub(selected_len);
    let observed_rate = if transfer_tested < 16 {
        TRANSFER_RESERVE_DEFAULT_PASS_RATE
    } else {
        ((transfer_passed as f64 + 2.0) / (transfer_tested as f64 + 4.0)).clamp(0.35, 0.95)
    };

    let estimated_additional = ((remaining as f64 / observed_rate) * 1.20).ceil() as usize;
    let exploration_floor = remaining.saturating_add(8);
    let additional_budget = estimated_additional
        .max(exploration_floor)
        .min(eligible_remaining);

    transfer_tested
        .saturating_add(additional_budget)
        .min(FINAL_TRANSFER_TEST_LIMIT)
}

fn discovery_downstream_progressed(
    transfer_selected_before: usize,
    transfer_selected: usize,
    publishable_selected_before: usize,
    publishable_selected: usize,
) -> bool {
    transfer_selected > transfer_selected_before
        || publishable_selected > publishable_selected_before
}

#[allow(clippy::too_many_arguments)]
fn write_light_stats(
    path: &str,
    input_candidates: usize,
    security_rejected: usize,
    consumer_rejected: usize,
    strict_verified: usize,
    strict_attempts: usize,
    transfer_stability_tested: usize,
    transfer_stability_passed: usize,
    transfer_tested: usize,
    transfer_passed: usize,
    stream_continuity_tested: usize,
    stream_continuity_passed: usize,
    selection_target: usize,
    strict_selectable: usize,
    stability_selectable: usize,
    transfer_selectable: usize,
    stream_selectable: usize,
    published: usize,
    historical_subscription_revisions: usize,
    recent_family_keys: usize,
    fresh_family_target_percent: usize,
    score_independent_exploration_percent: usize,
    published_from_fresh_families: usize,
) -> Result<(), String> {
    if path.is_empty() {
        return Ok(());
    }

    let stats = serde_json::json!({
        "input_candidates": input_candidates,
        "security_rejected": security_rejected,
        "consumer_rejected": consumer_rejected,
        "strict_verified": strict_verified,
        "strict_attempts": strict_attempts,
        "transfer_stability_tested": transfer_stability_tested,
        "transfer_stability_passed": transfer_stability_passed,
        "transfer_tested": transfer_tested,
        "transfer_passed": transfer_passed,
        "stream_continuity_tested": stream_continuity_tested,
        "stream_continuity_passed": stream_continuity_passed,
        "selection_target": selection_target,
        "strict_selectable": strict_selectable,
        "stability_selectable": stability_selectable,
        "transfer_selectable": transfer_selectable,
        "stream_selectable": stream_selectable,
        "selection_shortfall": selection_target.saturating_sub(published),
        "published": published,
        "historical_subscription_revisions": historical_subscription_revisions,
        "recent_family_keys": recent_family_keys,
        "fresh_family_target_percent": fresh_family_target_percent,
        "score_independent_exploration_percent": score_independent_exploration_percent,
        "published_from_fresh_families": published_from_fresh_families,
        "published_from_recent_families": published.saturating_sub(published_from_fresh_families),
    });
    let body = serde_json::to_vec_pretty(&stats).map_err(|error| error.to_string())?;
    std::fs::write(path, body).map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
fn persist_light_result(
    output: &str,
    selected: &[String],
    history_path: &str,
    history: &HashMap<String, HistoryEntry>,
    final_attempts: &HashMap<String, usize>,
    final_metadata: &HashMap<String, ProxyMetrics>,
    global_metadata: &HashMap<String, ProxyMetrics>,
    transfer_tested: &HashSet<String>,
    transfer_verified: &HashMap<String, ProxyMetrics>,
    stream_tested: &HashSet<String>,
    stream_verified: &HashMap<String, ProxyMetrics>,
) -> Result<(), String> {
    write_light_lines(output, selected)?;
    persist_light_training_data(
        history,
        final_attempts,
        final_metadata,
        global_metadata,
        transfer_tested,
        transfer_verified,
        stream_tested,
        stream_verified,
    )?;

    let strict_tested = final_attempts.keys().cloned().collect::<HashSet<_>>();
    let strict_passed = final_metadata.keys().cloned().collect::<HashSet<_>>();
    let transfer_passed = transfer_verified.keys().cloned().collect::<HashSet<_>>();
    if let Err(error) = proxyrift::source_discovery::record_light_results(
        &strict_tested,
        &strict_passed,
        transfer_tested,
        &transfer_passed,
    ) {
        println!("[WARN] ⚠️ [Source quality] Failed to persist Light feedback: {error}");
    }

    persist_history(history_path, history, final_attempts, final_metadata)
}

fn value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn required(args: &[String], name: &str) -> Result<String, String> {
    let result = value(args, name, "");
    if result.is_empty() {
        Err(format!("missing required argument {name}"))
    } else {
        Ok(result)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct HistoryEntry {
    checks: u64,
    passes: u64,
    last_seen: u64,
}

fn history_identity(config: &str) -> String {
    let cleaned = config.split('#').next().unwrap_or(config);

    if cleaned
        .split_once("://")
        .map(|(scheme, _)| scheme.eq_ignore_ascii_case("vmess"))
        .unwrap_or(false)
    {
        if let Some(payload) = cleaned.split_once("://").map(|(_, rest)| rest) {
            let payload = payload.split('#').next().unwrap_or("").trim();
            let mut padded = payload.to_string();
            while !padded.len().is_multiple_of(4) {
                padded.push('=');
            }

            for candidate in [payload, padded.as_str()] {
                for bytes in [
                    STANDARD.decode(candidate),
                    URL_SAFE.decode(candidate),
                    URL_SAFE_NO_PAD.decode(candidate),
                ]
                .into_iter()
                .flatten()
                {
                    if let Ok(mut value) = serde_json::from_slice::<Value>(&bytes) {
                        if let Value::Object(object) = &mut value {
                            object.remove("ps");
                        }
                        if let Ok(canonical) = serde_json::to_string(&value) {
                            return canonical;
                        }
                    }
                }
            }
        }
    }

    cleaned.to_string()
}

fn fnv64(input: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for byte in input {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn history_fingerprint(config: &str) -> String {
    let identity = history_identity(config);
    let first = fnv64(identity.as_bytes(), 0xcbf29ce484222325);
    let second = fnv64(identity.as_bytes(), 0x9e3779b97f4a7c15);
    format!("{first:016x}{second:016x}")
}

fn observation_fingerprint(config: &str) -> String {
    let first = fnv64(config.as_bytes(), 0xcbf29ce484222325);
    let second = fnv64(config.as_bytes(), 0x9e3779b97f4a7c15);
    format!("{first:016x}{second:016x}")
}

fn load_candidate_ledger(path: &str, now: u64) -> HashMap<String, u64> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
    let Ok(document) = serde_json::from_str::<Value>(&content) else {
        println!("[WARN] [Light reuse] Candidate ledger is invalid; all candidates will be treated as new");
        return HashMap::new();
    };
    if document.get("version").and_then(Value::as_u64) != Some(1) {
        println!("[WARN] [Light reuse] Candidate ledger version is unsupported; all candidates will be treated as new");
        return HashMap::new();
    }

    document
        .get("entries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let fingerprint = entry.get("config_id")?.as_str()?;
            let validated_at = entry.get("validated_at")?.as_u64()?;
            if fingerprint.len() != 32
                || validated_at > now
                || now.saturating_sub(validated_at) >= REVALIDATION_WINDOW_SECS
            {
                return None;
            }
            Some((fingerprint.to_string(), validated_at))
        })
        .collect()
}

fn active_revalidated_fingerprints(
    ledger: &HashMap<String, u64>,
    active_fingerprints: &HashSet<String>,
    now: u64,
) -> HashSet<String> {
    ledger
        .iter()
        .filter_map(|(fingerprint, validated_at)| {
            (active_fingerprints.contains(fingerprint)
                && *validated_at <= now
                && now.saturating_sub(*validated_at) < REVALIDATION_WINDOW_SECS)
                .then_some(fingerprint.clone())
        })
        .collect()
}

fn interleave_repeat_candidates(
    candidates: &[String],
    repeat_fingerprints: &HashSet<String>,
) -> (Vec<String>, usize) {
    let mut fresh = VecDeque::new();
    let mut repeats = VecDeque::new();
    for candidate in candidates {
        if repeat_fingerprints.contains(&history_fingerprint(candidate)) {
            repeats.push_back(candidate.clone());
        } else {
            fresh.push_back(candidate.clone());
        }
    }

    // For 20% repeats, each repeat must be backed by at least four non-repeat candidates.
    let repeat_budget = fresh.len() / 4;
    let mut ordered = Vec::with_capacity(candidates.len());
    let mut used_repeats = 0usize;
    while !fresh.is_empty() {
        for _ in 0..4 {
            if let Some(candidate) = fresh.pop_front() {
                ordered.push(candidate);
            } else {
                break;
            }
        }
        if used_repeats < repeat_budget {
            if let Some(candidate) = repeats.pop_front() {
                ordered.push(candidate);
                used_repeats += 1;
            }
        }
    }
    (ordered, used_repeats)
}

fn persist_candidate_ledger(
    path: &str,
    ledger: &mut HashMap<String, u64>,
    fully_validated: &HashSet<String>,
    now: u64,
) -> Result<(), String> {
    ledger.retain(|_, validated_at| {
        *validated_at <= now
            && now.saturating_sub(*validated_at) < REVALIDATION_WINDOW_SECS
    });
    for config in fully_validated {
        ledger.insert(history_fingerprint(config), now);
    }

    if ledger.len() > MAX_CANDIDATE_LEDGER_ENTRIES {
        let mut entries = ledger.iter().map(|(key, value)| (key.clone(), *value)).collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| right.1.cmp(&left.1));
        entries.truncate(MAX_CANDIDATE_LEDGER_ENTRIES);
        ledger.clear();
        ledger.extend(entries);
    }

    let entries = ledger
        .iter()
        .map(|(config_id, validated_at)| {
            serde_json::json!({"config_id": config_id, "validated_at": validated_at})
        })
        .collect::<Vec<_>>();
    let document = serde_json::json!({
        "version": 1,
        "retention_seconds": REVALIDATION_WINDOW_SECS,
        "updated_at": now,
        "entries": entries,
    });
    let body = serde_json::to_vec_pretty(&document).map_err(|error| error.to_string())?;
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary = format!("{path}.tmp");
    std::fs::write(&temporary, body).map_err(|error| error.to_string())?;
    if let Err(error) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    Ok(())
}

fn load_light_cohorts(path: &str) -> Result<Vec<Vec<String>>, String> {
    let current = read_lines(path)
        .unwrap_or_default()
        .into_iter()
        .filter(|line| !line.starts_with('#'))
        .collect::<Vec<_>>();

    let mut cohorts = vec![current];
    let history_commits = Command::new("git")
        .args(["log", "--format=%H", "--", path])
        .output();

    let Ok(history_commits) = history_commits else {
        return Ok(cohorts);
    };
    if !history_commits.status.success() {
        return Ok(cohorts);
    }

    for commit in String::from_utf8_lossy(&history_commits.stdout)
        .lines()
        .skip(1)
        .take(HISTORICAL_LIGHT_COHORTS)
    {
        let output = Command::new("git")
            .args(["show", &format!("{commit}:{path}")])
            .output();

        let Ok(output) = output else {
            continue;
        };
        if !output.status.success() {
            continue;
        }

        let values = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if !values.is_empty() {
            cohorts.push(values);
        }
    }

    Ok(cohorts)
}

fn load_history(path: &str) -> Result<HashMap<String, HistoryEntry>, String> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return Ok(HashMap::new());
    };

    let value = match serde_json::from_str::<Value>(&content) {
        Ok(value) => value,
        Err(error) => {
            println!("[WARN] ⚠️ Ignoring invalid Light history: {error}");
            return Ok(HashMap::new());
        }
    };

    let Some(entries) = value.get("entries").and_then(Value::as_object) else {
        return Ok(HashMap::new());
    };

    let mut history = HashMap::new();
    for (fingerprint, entry) in entries {
        let checks = entry.get("checks").and_then(Value::as_u64).unwrap_or(0);
        let passes = entry.get("passes").and_then(Value::as_u64).unwrap_or(0);
        let last_seen = entry.get("last_seen").and_then(Value::as_u64).unwrap_or(0);
        if checks == 0 && last_seen == 0 {
            continue;
        }
        history.insert(
            fingerprint.clone(),
            HistoryEntry {
                checks,
                passes: passes.min(checks),
                last_seen,
            },
        );
    }

    Ok(history)
}

fn persist_history(
    path: &str,
    history: &HashMap<String, HistoryEntry>,
    final_attempts: &HashMap<String, usize>,
    final_metadata: &HashMap<String, ProxyMetrics>,
) -> Result<(), String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();

    let mut updated = history.clone();
    for config in final_attempts.keys() {
        let fingerprint = history_fingerprint(config);
        let entry = updated.entry(fingerprint).or_default();
        entry.checks = entry.checks.saturating_add(1);
        entry.passes = entry
            .passes
            .saturating_add(u64::from(final_metadata.contains_key(config)));
        entry.last_seen = now;
    }

    updated.retain(|_, entry| entry.last_seen.saturating_add(HISTORY_RETENTION_SECS) >= now);

    if updated.len() > HISTORY_MAX_ENTRIES {
        let mut entries = updated.into_iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(_, entry)| std::cmp::Reverse(entry.last_seen));
        entries.truncate(HISTORY_MAX_ENTRIES);
        updated = entries.into_iter().collect();
    }

    let mut entries = BTreeMap::new();
    for (fingerprint, entry) in updated {
        entries.insert(
            fingerprint,
            serde_json::json!({
                "checks": entry.checks,
                "passes": entry.passes.min(entry.checks),
                "last_seen": entry.last_seen,
            }),
        );
    }

    let document = serde_json::json!({
        "version": 1,
        "entries": entries,
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

fn historical_score(config: &str, history: &HashMap<String, HistoryEntry>) -> (f64, u64) {
    history
        .get(&history_fingerprint(config))
        .map(|entry| {
            (
                (entry.passes as f64 + 2.0) / (entry.checks as f64 + 4.0),
                entry.checks,
            )
        })
        .unwrap_or((0.5, 0))
}

fn sort_ranked(
    configs: &mut [String],
    metadata: &HashMap<String, ProxyMetrics>,
    positions: &HashMap<String, usize>,
    history: &HashMap<String, HistoryEntry>,
) {
    configs.sort_unstable_by(|a, b| {
        let ma = metadata.get(a);
        let mb = metadata.get(b);

        let a_successes = ma.map(|m| m.successes).unwrap_or(0);
        let b_successes = mb.map(|m| m.successes).unwrap_or(0);
        let a_attempts = ma.map(|m| m.attempts.max(1)).unwrap_or(1);
        let b_attempts = mb.map(|m| m.attempts.max(1)).unwrap_or(1);
        let a_success_rate = a_successes as f64 / a_attempts as f64;
        let b_success_rate = b_successes as f64 / b_attempts as f64;
        let a_median = ma.map(|m| m.median_ms).unwrap_or(f64::INFINITY);
        let b_median = mb.map(|m| m.median_ms).unwrap_or(f64::INFINITY);
        let a_jitter = ma.map(|m| m.jitter_ms).unwrap_or(f64::INFINITY);
        let b_jitter = mb.map(|m| m.jitter_ms).unwrap_or(f64::INFINITY);
        let a_throughput = ma.map(|m| m.throughput_kbps).unwrap_or(0.0);
        let b_throughput = mb.map(|m| m.throughput_kbps).unwrap_or(0.0);
        let a_min = ma.map(|m| m.min_ms).unwrap_or(f64::INFINITY);
        let b_min = mb.map(|m| m.min_ms).unwrap_or(f64::INFINITY);
        let (a_history_rate, a_history_checks) = historical_score(a, history);
        let (b_history_rate, b_history_checks) = historical_score(b, history);

        b_success_rate
            .total_cmp(&a_success_rate)
            .then_with(|| b_successes.cmp(&a_successes))
            .then_with(|| a_median.total_cmp(&b_median))
            .then_with(|| a_jitter.total_cmp(&b_jitter))
            .then_with(|| b_throughput.total_cmp(&a_throughput))
            .then_with(|| a_history_rate.total_cmp(&b_history_rate))
            .then_with(|| b_history_checks.cmp(&a_history_checks))
            .then_with(|| a_min.total_cmp(&b_min))
            .then_with(|| {
                positions
                    .get(a)
                    .copied()
                    .unwrap_or(usize::MAX)
                    .cmp(&positions.get(b).copied().unwrap_or(usize::MAX))
            })
            .then_with(|| a.cmp(b))
    });
}

fn rank_discovery_candidates(
    candidates: &[String],
    light_gbm_scores: &LightGbmScores,
    seed: u64,
    recent_families: &HashSet<String>,
) -> Vec<String> {
    if candidates.is_empty() {
        return Vec::new();
    }

    let mut fresh_candidates = Vec::new();
    let mut recent_candidates = Vec::new();
    for config in candidates {
        if recent_families.contains(&family_key(config)) {
            recent_candidates.push(config.clone());
        } else {
            fresh_candidates.push(config.clone());
        }
    }
    if fresh_candidates.is_empty() {
        fresh_candidates = std::mem::take(&mut recent_candidates);
    }

    let mut ranked = fresh_candidates.clone();
    ranked.sort_unstable_by(|a, b| {
        light_gbm_scores
            .score(b)
            .total_cmp(&light_gbm_scores.score(a))
            .then_with(|| a.cmp(b))
    });

    let mut exploration = fresh_candidates;
    exploration.sort_unstable_by_key(|config| exploration_sort_key(config, seed));

    let mut ordered = Vec::with_capacity(candidates.len());
    if !ranked.is_empty() {
        let exploration_target = ranked
            .len()
            .saturating_mul(RECHECK_EXPLORATION_PERCENT)
            .div_ceil(100)
            .clamp(1, ranked.len());

        let mut selected = HashSet::with_capacity(ranked.len());
        let mut ranked_index = 0usize;
        let mut exploration_index = 0usize;
        let mut exploration_selected = 0usize;

        for position in 0..ranked.len() {
            let exploration_due = ((position + 1) * exploration_target) / ranked.len()
                > (position * exploration_target) / ranked.len();

            if exploration_due && exploration_selected < exploration_target {
                while exploration_index < exploration.len()
                    && selected.contains(&exploration[exploration_index])
                {
                    exploration_index += 1;
                }

                if let Some(config) = exploration.get(exploration_index) {
                    selected.insert(config.clone());
                    ordered.push(config.clone());
                    exploration_selected += 1;
                    exploration_index += 1;
                    continue;
                }
            }

            while ranked_index < ranked.len() && selected.contains(&ranked[ranked_index]) {
                ranked_index += 1;
            }

            if let Some(config) = ranked.get(ranked_index) {
                selected.insert(config.clone());
                ordered.push(config.clone());
                ranked_index += 1;
            }
        }
    }

    recent_candidates.sort_unstable_by(|a, b| {
        light_gbm_scores
            .score(b)
            .total_cmp(&light_gbm_scores.score(a))
            .then_with(|| a.cmp(b))
    });
    ordered.extend(recent_candidates);
    ordered
}

fn rank_strict_recheck_candidates(
    candidates: &[String],
    light_gbm_scores: &LightGbmScores,
    positions: &HashMap<String, usize>,
) -> Vec<String> {
    let mut ranked = candidates.to_vec();
    ranked.sort_unstable_by(|a, b| {
        light_gbm_scores
            .score(b)
            .total_cmp(&light_gbm_scores.score(a))
            .then_with(|| {
                positions
                    .get(a)
                    .copied()
                    .unwrap_or(usize::MAX)
                    .cmp(&positions.get(b).copied().unwrap_or(usize::MAX))
            })
            .then_with(|| a.cmp(b))
    });
    ranked
}

#[allow(clippy::too_many_arguments)]
fn adaptive_discovery_batch_size(
    selection_limit: usize,
    publishable_selected: usize,
    stability_tested: usize,
    stability_passed: usize,
    transfer_tested: usize,
    transfer_passed: usize,
    stream_tested: usize,
    stream_passed: usize,
    available_candidates: usize,
) -> usize {
    if selection_limit == 0 || available_candidates == 0 {
        return 0;
    }

    let remaining = selection_limit.saturating_sub(publishable_selected);
    if remaining == 0 {
        return 0;
    }

    let stability_rate = if stability_tested < 16 {
        0.75
    } else {
        ((stability_passed as f64 + 2.0) / (stability_tested as f64 + 4.0)).clamp(0.50, 0.95)
    };
    let transfer_rate = if transfer_tested < 16 {
        0.80
    } else {
        ((transfer_passed as f64 + 2.0) / (transfer_tested as f64 + 4.0)).clamp(0.35, 0.95)
    };
    let stream_rate = if stream_tested < 16 {
        1.0
    } else {
        ((stream_passed as f64 + 2.0) / (stream_tested as f64 + 4.0)).clamp(0.50, 0.98)
    };

    let observed_funnel_rate = (stability_rate * transfer_rate * stream_rate).clamp(0.08, 0.95);
    let estimated =
        ((remaining as f64 / observed_funnel_rate) * DISCOVERY_SAFETY_FACTOR).ceil() as usize;

    estimated
        .clamp(DISCOVERY_BATCH_MIN, DISCOVERY_BATCH_MAX)
        .min(available_candidates)
}

fn adjust_discovery_batch_for_yield(
    suggested: usize,
    current_batch: usize,
    newly_selectable: usize,
    tested_candidates: usize,
    available_candidates: usize,
) -> usize {
    if available_candidates == 0 {
        return 0;
    }

    let suggested = suggested.clamp(1, DISCOVERY_BATCH_MAX);
    let low_yield =
        tested_candidates > 0 && newly_selectable.saturating_mul(100) < tested_candidates;
    if !low_yield {
        return suggested.min(available_candidates);
    }

    suggested
        .max(
            current_batch
                .saturating_mul(2)
                .min(DISCOVERY_BATCH_HARD_MAX),
        )
        .min(DISCOVERY_BATCH_HARD_MAX)
        .min(available_candidates)
}

fn family_key(config: &str) -> String {
    let cleaned = config.split('#').next().unwrap_or(config);
    let Ok(url) = Url::parse(cleaned) else {
        return cleaned.to_string();
    };
    let scheme = url.scheme().to_ascii_lowercase();

    if scheme == "vmess" {
        if let Some(payload) = cleaned.split_once("://").map(|(_, value)| value) {
            let mut padded = payload.to_string();
            while !padded.len().is_multiple_of(4) {
                padded.push('=');
            }
            for encoded in [payload, padded.as_str()] {
                for bytes in [
                    STANDARD.decode(encoded),
                    URL_SAFE.decode(encoded),
                    URL_SAFE_NO_PAD.decode(encoded),
                ]
                .into_iter()
                .flatten()
                {
                    if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                        let fields = [
                            "id", "aid", "scy", "net", "tls", "sni", "host", "path", "type",
                        ];
                        let mut key = String::from("vmess|");
                        for field in fields {
                            if let Some(value) = value.get(field) {
                                key.push_str(field);
                                key.push('=');
                                key.push_str(&value.to_string());
                                key.push('|');
                            }
                        }
                        return key;
                    }
                }
            }
        }
    }

    let mut pairs = url
        .query_pairs()
        .map(|(key, value)| (key.to_ascii_lowercase(), value.into_owned()))
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "ps" | "name" | "remark" | "remarks" | "test_name" | "telegram"
            )
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();

    let mut key = format!(
        "{}|{}|{}",
        scheme,
        url.username(),
        url.password().unwrap_or("")
    );
    for (name, value) in pairs {
        key.push('|');
        key.push_str(&name);
        key.push('=');
        key.push_str(&value);
    }
    key
}

fn diversify_recheck_candidates(
    configs: &[String],
    limit: usize,
    max_family: usize,
) -> Vec<String> {
    let mut selected = Vec::new();
    let mut seen_endpoints = HashSet::new();
    let mut family_counts = HashMap::<String, usize>::new();

    for config in configs {
        if selected.len() >= limit {
            break;
        }

        let family = family_key(config);
        let family_available = family_counts.get(&family).copied().unwrap_or(0) < max_family;
        let endpoint_available = endpoint(config)
            .map(|ep| !seen_endpoints.contains(&ep))
            .unwrap_or(true);

        if !family_available || !endpoint_available {
            continue;
        }

        *family_counts.entry(family).or_default() += 1;
        if let Some(ep) = endpoint(config) {
            seen_endpoints.insert(ep);
        }
        selected.push(config.clone());
    }

    selected
}

fn normalize_light_config(config: &str) -> String {
    if !config
        .split_once("://")
        .map(|(scheme, _)| scheme.eq_ignore_ascii_case("trojan"))
        .unwrap_or(false)
    {
        return config.to_string();
    }

    let fragment_index = config.find('#').unwrap_or(config.len());
    let base = &config[..fragment_index];
    let fragment = &config[fragment_index..];

    let Ok(url) = Url::parse(base) else {
        return config.to_string();
    };

    if url
        .query_pairs()
        .any(|(key, value)| key.eq_ignore_ascii_case("security") && !value.trim().is_empty())
    {
        return config.to_string();
    }

    let query_parts = url
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
        .filter(|part| {
            let mut pairs = url::form_urlencoded::parse(part.as_bytes());
            !matches!(
                pairs.next(),
                Some((key, value))
                    if key.eq_ignore_ascii_case("security") && value.trim().is_empty()
            )
        })
        .collect::<Vec<_>>();

    let path = base.split_once('?').map(|(path, _)| path).unwrap_or(base);
    if query_parts.is_empty() {
        format!("{path}?security=tls{fragment}")
    } else {
        format!("{path}?{}&security=tls{fragment}", query_parts.join("&"))
    }
}

fn write_light_lines(output: &str, values: &[String]) -> Result<(), String> {
    let normalized = values
        .iter()
        .map(|config| normalize_light_config(config))
        .collect::<Vec<_>>();
    write_lines(output, &normalized)
}

#[allow(clippy::too_many_arguments)]
async fn validate_light_transfer_batch(
    xray: &str,
    singbox: &str,
    candidates: &[String],
    workers: usize,
    target: &str,
) -> Result<HashMap<String, ProxyMetrics>, String> {
    let mut singbox_candidates = Vec::new();
    let mut xray_candidates = Vec::new();
    let mut fallback_candidates = Vec::new();

    for config in candidates {
        match light_backend(config) {
            LightBackend::SingBox => singbox_candidates.push(config.clone()),
            LightBackend::Xray => xray_candidates.push(config.clone()),
            LightBackend::Fallback => fallback_candidates.push(config.clone()),
        }
    }

    let targets = [target];
    let request_timeout = std::time::Duration::from_secs_f64(FINAL_TRANSFER_TIMEOUT_SECS);
    let max_idle_gap = std::time::Duration::from_secs(STREAM_CONTINUITY_MAX_IDLE_SECS);
    let transfer_workers = workers.clamp(1, STREAM_CONTINUITY_WORKERS);

    let mut singbox_validation_candidates = singbox_candidates;
    singbox_validation_candidates.extend(fallback_candidates.iter().cloned());

    let singbox_future = async {
        if singbox_validation_candidates.is_empty() {
            Ok(HashMap::new())
        } else {
            validate_singbox_target_pool_once_with_sustained_stream(
                singbox,
                &singbox_validation_candidates,
                &targets,
                transfer_workers,
                request_timeout,
                FINAL_TRANSFER_LATENCY_LIMIT_MS,
                STREAM_CONTINUITY_SEGMENTS,
                STREAM_CONTINUITY_SEGMENT_BYTES,
                max_idle_gap,
                1,
            )
            .await
        }
    };

    let xray_future = async {
        if xray_candidates.is_empty() {
            Ok(HashMap::new())
        } else {
            validate_candidates_with_target_pool_once_with_sustained_stream(
                xray,
                &xray_candidates,
                &targets,
                transfer_workers,
                FINAL_TRANSFER_BATCH_SIZE,
                FINAL_TRANSFER_TIMEOUT_SECS,
                FINAL_TRANSFER_LATENCY_LIMIT_MS,
                STREAM_CONTINUITY_SEGMENTS,
                STREAM_CONTINUITY_SEGMENT_BYTES,
                max_idle_gap,
                1,
            )
            .await
        }
    };

    let (singbox_result, xray_result) = tokio::join!(singbox_future, xray_future);

    let singbox_metadata = match singbox_result {
        Ok(metadata) => metadata,
        Err(error) => {
            println!("[WARN] ⚠️ Sing-box sustained transfer failed for this batch; preserving Xray results: {error}");
            HashMap::new()
        }
    };

    let mut xray_metadata = match xray_result {
        Ok(metadata) => metadata,
        Err(error) => {
            println!("[WARN] ⚠️ Xray sustained transfer failed for this batch; preserving sing-box results: {error}");
            HashMap::new()
        }
    };

    let fallback_retry = fallback_candidates
        .into_iter()
        .filter(|config| !singbox_metadata.contains_key(config))
        .collect::<Vec<_>>();

    if !fallback_retry.is_empty() {
        match validate_candidates_with_target_pool_once_with_sustained_stream(
            xray,
            &fallback_retry,
            &targets,
            transfer_workers,
            FINAL_TRANSFER_BATCH_SIZE,
            FINAL_TRANSFER_TIMEOUT_SECS,
            FINAL_TRANSFER_LATENCY_LIMIT_MS,
            STREAM_CONTINUITY_SEGMENTS,
            STREAM_CONTINUITY_SEGMENT_BYTES,
            max_idle_gap,
            1,
        )
        .await
        {
            Ok(fallback_xray) => xray_metadata.extend(fallback_xray),
            Err(error) => println!(
                "[WARN] ⚠️ Xray sustained fallback failed for {} candidates: {error}",
                fallback_retry.len()
            ),
        }
    }

    Ok(singbox_qualified_metadata(xray_metadata, singbox_metadata))
}

#[allow(clippy::too_many_arguments)]
async fn validate_light_transfer_stability_batch(
    xray: &str,
    singbox: &str,
    candidates: &[String],
    workers: usize,
) -> Result<HashMap<String, ProxyMetrics>, String> {
    if candidates.is_empty() {
        return Ok(HashMap::new());
    }

    let mut singbox_candidates = Vec::new();
    let mut xray_candidates = Vec::new();
    let mut fallback_candidates = Vec::new();

    for config in candidates {
        match light_backend(config) {
            LightBackend::SingBox => singbox_candidates.push(config.clone()),
            LightBackend::Xray => xray_candidates.push(config.clone()),
            LightBackend::Fallback => fallback_candidates.push(config.clone()),
        }
    }

    let request_timeout = std::time::Duration::from_secs_f64(FINAL_TRANSFER_TIMEOUT_SECS);

    let mut singbox_validation_candidates = singbox_candidates;
    singbox_validation_candidates.extend(fallback_candidates.iter().cloned());

    let singbox_future = async {
        if singbox_validation_candidates.is_empty() {
            Ok(HashMap::new())
        } else {
            validate_singbox_target_pool_once_with_minimum_body(
                singbox,
                &singbox_validation_candidates,
                LIGHT_TRANSFER_STABILITY_TARGETS,
                workers.clamp(1, 40),
                request_timeout,
                STABILITY_TRANSFER_MAX_LATENCY_MS,
                LIGHT_TRANSFER_STABILITY_BYTES,
                LIGHT_TRANSFER_MINIMUM_TARGETS,
            )
            .await
        }
    };

    let xray_future = async {
        if xray_candidates.is_empty() {
            Ok(HashMap::new())
        } else {
            validate_candidates_with_ordered_target_pool_once_with_minimum_body(
                xray,
                &xray_candidates,
                LIGHT_TRANSFER_STABILITY_TARGETS,
                workers.max(1),
                STABILITY_TRANSFER_BATCH_SIZE,
                FINAL_TRANSFER_TIMEOUT_SECS,
                STABILITY_TRANSFER_MAX_LATENCY_MS,
                LIGHT_TRANSFER_STABILITY_BYTES,
                LIGHT_TRANSFER_MINIMUM_TARGETS,
            )
            .await
        }
    };

    let (singbox_result, xray_result) = tokio::join!(singbox_future, xray_future);
    let singbox_metadata = singbox_result?;
    let mut xray_metadata = xray_result?;

    let fallback_retry = fallback_candidates
        .into_iter()
        .filter(|config| !singbox_metadata.contains_key(config))
        .collect::<Vec<_>>();

    if !fallback_retry.is_empty() {
        let fallback_xray = validate_candidates_with_ordered_target_pool_once_with_minimum_body(
            xray,
            &fallback_retry,
            LIGHT_TRANSFER_STABILITY_TARGETS,
            workers.max(1),
            STABILITY_TRANSFER_BATCH_SIZE,
            FINAL_TRANSFER_TIMEOUT_SECS,
            STABILITY_TRANSFER_MAX_LATENCY_MS,
            LIGHT_TRANSFER_STABILITY_BYTES,
            LIGHT_TRANSFER_MINIMUM_TARGETS,
        )
        .await?;
        xray_metadata.extend(fallback_xray);
    }

    Ok(singbox_qualified_metadata(xray_metadata, singbox_metadata))
}

const FINAL_TRANSFER_RATE_LIMIT_TOLERANCE_PERCENT: u64 = 25;
const FINAL_TRANSFER_RATE_LIMIT_SEVERE_PERCENT: u64 = 50;
const PRIMARY_TRANSFER_TARGET_COUNT: usize = 2;
const FINAL_TRANSFER_TARGET_MIN_TESTS: usize = 8;
const FINAL_TRANSFER_TARGET_RATE_LIMIT_PENALTY_PERCENT: u64 = 75;

#[derive(Clone, Copy, Debug, Default)]
struct TransferTargetState {
    tested: usize,
    passed: usize,
    rate_limits: u64,
    elapsed_secs: u64,
    batches: usize,
    quarantined: bool,
}

fn transfer_target_pass_rate(state: &TransferTargetState) -> f64 {
    if state.tested == 0 {
        0.0
    } else {
        state.passed as f64 / state.tested as f64
    }
}

fn transfer_target_score(state: &TransferTargetState) -> f64 {
    if state.batches == 0 {
        return 10.0;
    }

    let pass_rate = ((state.passed as f64 + 2.0) / (state.tested as f64 + 4.0)).clamp(0.05, 0.95);
    let rate_limit_rate = if state.tested == 0 {
        0.0
    } else {
        (state.rate_limits as f64 / state.tested as f64).clamp(0.0, 1.0)
    };
    let average_batch_secs = (state.elapsed_secs as f64 / state.batches.max(1) as f64).max(1.0);
    let speed_factor = 30.0 / (30.0 + average_batch_secs);

    pass_rate * speed_factor * (1.0 - rate_limit_rate.min(0.90))
}

fn should_quarantine_transfer_target(state: &TransferTargetState) -> bool {
    if state.tested < FINAL_TRANSFER_TARGET_MIN_TESTS || state.batches == 0 {
        return false;
    }

    let pass_rate = transfer_target_pass_rate(state);
    let rate_limit_percent = state
        .rate_limits
        .saturating_mul(100)
        .div_ceil(state.tested as u64);

    if pass_rate == 0.0 {
        return true;
    }

    if state.batches >= 2 && pass_rate < 0.15 {
        return true;
    }

    rate_limit_percent >= FINAL_TRANSFER_TARGET_RATE_LIMIT_PENALTY_PERCENT && pass_rate < 0.25
}

fn select_transfer_target(
    states: &[TransferTargetState],
    next_preferred_primary: usize,
) -> Option<usize> {
    if states.is_empty() {
        return None;
    }

    let primary_count = states.len().min(PRIMARY_TRANSFER_TARGET_COUNT);
    let healthy_primary = (0..primary_count)
        .filter(|&index| !states[index].quarantined)
        .collect::<Vec<_>>();
    let has_healthy_primary = !healthy_primary.is_empty();
    let both_primaries_healthy = primary_count == PRIMARY_TRANSFER_TARGET_COUNT
        && healthy_primary.len() == PRIMARY_TRANSFER_TARGET_COUNT;
    let healthy = if has_healthy_primary {
        healthy_primary
    } else {
        states
            .iter()
            .enumerate()
            .filter(|(_, state)| !state.quarantined)
            .map(|(index, _)| index)
            .collect::<Vec<_>>()
    };

    let minimum_tested = healthy.iter().map(|&index| states[index].tested).min()?;
    let mut least_tested = healthy
        .into_iter()
        .filter(|&index| states[index].tested == minimum_tested)
        .collect::<Vec<_>>();

    if least_tested.len() == 1 {
        return least_tested.pop();
    }

    if both_primaries_healthy && least_tested.contains(&next_preferred_primary) {
        return Some(next_preferred_primary);
    }

    least_tested.into_iter().min_by(|left, right| {
        transfer_target_score(&states[*right])
            .total_cmp(&transfer_target_score(&states[*left]))
            .then_with(|| left.cmp(right))
    })
}

fn update_transfer_target_state(
    state: &mut TransferTargetState,
    tested: usize,
    passed: usize,
    rate_limits: u64,
    elapsed_secs: u64,
) {
    state.tested = state.tested.saturating_add(tested);
    state.passed = state.passed.saturating_add(passed.min(tested));
    state.rate_limits = state.rate_limits.saturating_add(rate_limits);
    state.elapsed_secs = state.elapsed_secs.saturating_add(elapsed_secs.max(1));
    state.batches = state.batches.saturating_add(1);
    state.quarantined = state.quarantined || should_quarantine_transfer_target(state);
}

fn adjust_transfer_workers(
    current: usize,
    rate_limits: u64,
    batch_size: usize,
    clean_batches: usize,
) -> (usize, usize) {
    let rate_limit_percent = if batch_size == 0 {
        0
    } else {
        rate_limits.saturating_mul(100).div_ceil(batch_size as u64)
    };

    if rate_limit_percent >= FINAL_TRANSFER_RATE_LIMIT_SEVERE_PERCENT {
        (current.saturating_sub(2).max(FINAL_TRANSFER_MIN_WORKERS), 0)
    } else if rate_limit_percent >= FINAL_TRANSFER_RATE_LIMIT_TOLERANCE_PERCENT {
        (current.saturating_sub(1).max(FINAL_TRANSFER_MIN_WORKERS), 0)
    } else if clean_batches.saturating_add(1) >= FINAL_TRANSFER_CLEAN_BATCHES_TO_RAMP {
        ((current + 1).min(FINAL_TRANSFER_WORKERS), 0)
    } else {
        (current, clean_batches.saturating_add(1))
    }
}

#[derive(Clone, Copy, Debug)]
struct TransferConcurrencyState {
    workers: usize,
    clean_batches: usize,
}

impl Default for TransferConcurrencyState {
    fn default() -> Self {
        Self {
            workers: FINAL_TRANSFER_INITIAL_WORKERS,
            clean_batches: 0,
        }
    }
}

impl TransferConcurrencyState {
    fn observe(&mut self, rate_limits: u64, batch_size: usize) -> (usize, usize) {
        let previous_workers = self.workers;
        let previous_clean_batches = self.clean_batches;
        (self.workers, self.clean_batches) =
            adjust_transfer_workers(self.workers, rate_limits, batch_size, self.clean_batches);
        (previous_workers, previous_clean_batches)
    }
}

const FINAL_TRANSFER_TARGET_PROBE_SIZE: usize = 12;

fn transfer_target_batch_limit(state: &TransferTargetState, requested: usize) -> usize {
    if state.batches == 0 {
        requested.min(FINAL_TRANSFER_TARGET_PROBE_SIZE)
    } else {
        requested
    }
}

fn adaptive_stability_pool_target(
    selection_limit: usize,
    transfer_tested: usize,
    transfer_passed: usize,
) -> usize {
    let transfer_target =
        transfer_validation_target(selection_limit).min(STABILITY_TRANSFER_TEST_LIMIT);
    if transfer_target == 0 {
        return 0;
    }

    let observed_rate = if transfer_tested < 16 {
        STABILITY_TRANSFER_DEFAULT_PASS_RATE
    } else {
        ((transfer_passed as f64 + 2.0) / (transfer_tested as f64 + 4.0)).clamp(0.25, 0.95)
    };
    let estimated =
        ((transfer_target as f64 / observed_rate) * STABILITY_TARGET_SAFETY_FACTOR).ceil() as usize;

    estimated
        .max(selection_limit.saturating_add(STABILITY_TARGET_MIN_RESERVE))
        .min(STABILITY_TRANSFER_TEST_LIMIT)
}

fn adaptive_stability_target(
    selection_limit: usize,
    transfer_tested: usize,
    transfer_passed: usize,
    available_candidates: usize,
) -> usize {
    if available_candidates == 0 {
        return 0;
    }

    adaptive_stability_pool_target(selection_limit, transfer_tested, transfer_passed)
        .min(available_candidates.min(STABILITY_TRANSFER_TEST_LIMIT))
}

#[allow(clippy::too_many_arguments)]
async fn fill_transfer_stability_gate(
    xray: &str,
    singbox: &str,
    final_verified: &[String],
    final_metadata: &HashMap<String, ProxyMetrics>,
    mut stability_verified: HashMap<String, ProxyMetrics>,
    mut stability_tested: HashSet<String>,
    stability_sender: tokio::sync::mpsc::UnboundedSender<Vec<String>>,
    transfer_done: Arc<AtomicBool>,
    global_positions: &HashMap<String, usize>,
    history: &HashMap<String, HistoryEntry>,
    selection_limit: usize,
    transfer_tested: Arc<AtomicUsize>,
    transfer_passed: Arc<AtomicUsize>,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> Result<(usize, HashMap<String, ProxyMetrics>, HashSet<String>), String> {
    if !stability_verified.is_empty() {
        let initial = stability_verified.keys().cloned().collect::<Vec<_>>();
        if !initial.is_empty() {
            let _ = stability_sender.send(initial);
        }
    }

    loop {
        if transfer_done.load(Ordering::Relaxed) {
            break;
        }

        let stability_target = adaptive_stability_target(
            selection_limit,
            transfer_tested.load(Ordering::Relaxed),
            transfer_passed.load(Ordering::Relaxed),
            final_verified.len(),
        );

        let mut stable_ranked = stability_verified.keys().cloned().collect::<Vec<_>>();
        sort_ranked(
            &mut stable_ranked,
            &stability_verified,
            global_positions,
            history,
        );
        let stable_selected = select_verified_configs(
            &stable_ranked,
            stability_target,
            max_per_endpoint,
            max_per_family,
        );

        if stability_tested.len() >= STABILITY_TRANSFER_TEST_LIMIT {
            break;
        }

        let mut ranked = final_verified.to_vec();
        sort_ranked(&mut ranked, final_metadata, global_positions, history);

        let untested = ranked
            .into_iter()
            .filter(|config| !stability_tested.contains(config))
            .collect::<Vec<_>>();

        if untested.is_empty() {
            break;
        }

        let completion_mode = stable_selected.len() < stability_target
            && stable_selected
                .len()
                .saturating_add(STABILITY_COMPLETION_GRACE_REMAINING)
                >= stability_target;
        let remaining_budget = STABILITY_TRANSFER_TEST_LIMIT.saturating_sub(stability_tested.len());
        let batch_limit = if completion_mode {
            remaining_budget.clamp(1, STABILITY_COMPLETION_BATCH_SIZE)
        } else {
            remaining_budget.clamp(1, STABILITY_TRANSFER_BATCH_SIZE)
        };
        let batch = select_stability_test_batch(
            &untested,
            &stable_selected,
            batch_limit,
            STABILITY_TRANSFER_TEST_LIMIT,
            STABILITY_TEST_MAX_PER_ENDPOINT,
            STABILITY_TEST_MAX_PER_FAMILY,
        );

        if batch.is_empty() {
            break;
        }

        stability_tested.extend(batch.iter().cloned());

        if completion_mode {
            proxyrift::emit_log_if!(proxyrift::should_emit_compact_progress(stability_tested.len(), batch.len(), 100, stable_selected.len() >= stability_target);
                "[INFO] 🎯 [1 MiB] Completion mode | Stable: {} | Need: {} | Prioritizing {} highest-ranked untested candidates | Test caps: {}/{} endpoint/family | Tested: {}/{} | Pipeline: 10 MiB consuming concurrently",
                stable_selected.len(),
                stability_target.saturating_sub(stable_selected.len()),
                batch.len(),
                STABILITY_TEST_MAX_PER_ENDPOINT,
                STABILITY_TEST_MAX_PER_FAMILY,
                stability_tested.len(),
                STABILITY_TRANSFER_TEST_LIMIT
            );
        } else {
            proxyrift::emit_log_if!(proxyrift::should_emit_compact_progress(stability_tested.len(), batch.len(), 100, stable_selected.len() >= stability_target);
                "[INFO] 📥 [1 MiB] Stable pool: {}/{} | Testing {} candidates | Test caps: {}/{} endpoint/family | Tested: {}/{} | Pipeline: 10 MiB consuming concurrently",
                stable_selected.len(),
                stability_target,
                batch.len(),
                STABILITY_TEST_MAX_PER_ENDPOINT,
                STABILITY_TEST_MAX_PER_FAMILY,
                stability_tested.len(),
                STABILITY_TRANSFER_TEST_LIMIT
            );
        }

        let batch_started = Instant::now();
        let metadata = validate_light_transfer_stability_batch(
            xray,
            singbox,
            &batch,
            STABILITY_TRANSFER_WORKERS,
        )
        .await?;
        let batch_elapsed = batch_started.elapsed().as_secs();
        let batch_passed = metadata.len();
        let passed_configs = metadata.keys().cloned().collect::<Vec<_>>();
        stability_verified.extend(metadata);

        if !passed_configs.is_empty() {
            let _ = stability_sender.send(passed_configs);
        }

        proxyrift::emit_log_if!(proxyrift::should_emit_compact_progress(stability_tested.len(), batch.len(), 100, stability_verified.len() >= stability_target);
            "[INFO] ✅ [1 MiB] {}/{} Passed the initial transfer probe | Batch: {}s | Stable pool: {} | Transfer pipeline: active",
            batch_passed,
            batch.len(),
            batch_elapsed,
            stability_verified.len()
        );
    }

    Ok((
        stability_verified.len(),
        stability_verified,
        stability_tested,
    ))
}

fn mirror_sustained_stream_results(
    transfer_tested: &HashSet<String>,
    transfer_verified: &HashMap<String, ProxyMetrics>,
    stream_tested: &mut HashSet<String>,
    stream_verified: &mut HashMap<String, ProxyMetrics>,
) -> usize {
    stream_tested.clone_from(transfer_tested);
    stream_verified.clone_from(transfer_verified);
    stream_verified.len()
}

#[allow(clippy::too_many_arguments)]
async fn run_transfer_gate_consumer(
    xray: &str,
    singbox: &str,
    mut stability_receiver: tokio::sync::mpsc::UnboundedReceiver<Vec<String>>,
    transfer_verified: &mut HashMap<String, ProxyMetrics>,
    transfer_tested: &mut HashSet<String>,
    global_positions: &HashMap<String, usize>,
    history: &HashMap<String, HistoryEntry>,
    selection_limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
    transfer_tested_count: Arc<AtomicUsize>,
    transfer_passed_count: Arc<AtomicUsize>,
    transfer_done: Arc<AtomicBool>,
) -> Result<usize, String> {
    let transfer_target = transfer_validation_target(selection_limit);

    let target_count = proxyrift::validator::STRICT_THROUGHPUT_TARGETS.len();
    let mut target_concurrency = vec![TransferConcurrencyState::default(); target_count];
    let mut target_states = vec![TransferTargetState::default(); target_count];
    let mut target_tested_candidates =
        vec![HashSet::<String>::new(); proxyrift::validator::STRICT_THROUGHPUT_TARGETS.len()];
    let mut pending = VecDeque::<String>::new();
    let mut receiver_closed = false;
    let mut next_preferred_primary = 0usize;
    loop {
        if transfer_done.load(Ordering::Relaxed) {
            break;
        }

        let mut transfer_ranked = transfer_verified.keys().cloned().collect::<Vec<_>>();
        sort_ranked(
            &mut transfer_ranked,
            transfer_verified,
            global_positions,
            history,
        );

        let selected = select_verified_configs(
            &transfer_ranked,
            transfer_target,
            max_per_endpoint,
            max_per_family,
        );

        if selected.len() >= transfer_target {
            transfer_done.store(true, Ordering::Relaxed);
            return Ok(selected.len());
        }

        let remaining = transfer_target.saturating_sub(selected.len());
        if pending.is_empty() {
            if receiver_closed {
                break;
            }

            match stability_receiver.recv().await {
                Some(batch) => pending.extend(batch),
                None => receiver_closed = true,
            }

            if receiver_closed && pending.is_empty() {
                continue;
            }
        }
        let dynamic_test_limit = adaptive_transfer_test_limit(
            transfer_target,
            selected.len(),
            transfer_tested.len(),
            transfer_verified.len(),
            pending.len(),
        );

        if transfer_tested.len() >= dynamic_test_limit {
            if receiver_closed {
                break;
            }

            match stability_receiver.recv().await {
                Some(batch) => pending.extend(batch),
                None => receiver_closed = true,
            }
            continue;
        }

        let mut eligible_untested = Vec::new();
        while let Some(config) = pending.pop_front() {
            if transfer_tested.contains(&config) {
                continue;
            }

            if selection_additional_potential_count(
                &selected,
                std::slice::from_ref(&config),
                transfer_target,
                max_per_endpoint,
                max_per_family,
            ) > 0
            {
                eligible_untested.push(config);
            }
        }

        if eligible_untested.is_empty() {
            if receiver_closed {
                break;
            }

            continue;
        }

        let eligible_remaining = selection_additional_potential_count(
            &selected,
            &eligible_untested,
            transfer_target,
            max_per_endpoint,
            max_per_family,
        );
        if eligible_remaining == 0 {
            if receiver_closed {
                break;
            }
            continue;
        }

        for (index, state) in target_states.iter_mut().enumerate() {
            let target = proxyrift::validator::STRICT_THROUGHPUT_TARGETS[index];
            if target_is_rate_limited(target) {
                state.quarantined = true;
            }
        }
        let Some(target_index) = select_transfer_target(&target_states, next_preferred_primary)
        else {
            println!(
                "[WARN] ⛔ [9 MiB sustained] All download hosts are cooling down or quarantined | Tested: {} | Passed: {} | Preserving only quality-verified candidates",
                transfer_tested.len(),
                transfer_verified.len()
            );
            transfer_done.store(true, Ordering::Relaxed);
            break;
        };
        let target = proxyrift::validator::STRICT_THROUGHPUT_TARGETS[target_index];
        let target_state_before = target_states[target_index];
        let transfer_workers = target_concurrency[target_index].workers;

        let queue_window = transfer_workers
            .saturating_mul(FINAL_TRANSFER_QUEUE_MULTIPLIER)
            .max(8);
        let requested_batch_limit = dynamic_test_limit
            .saturating_sub(transfer_tested.len())
            .min(FINAL_TRANSFER_BATCH_SIZE)
            .min(queue_window)
            .min(eligible_untested.len())
            .max(1);
        let batch_limit = transfer_target_batch_limit(&target_state_before, requested_batch_limit);

        let batch = diversify_recheck_candidates(&eligible_untested, batch_limit, 1);
        let batch_set = batch.iter().cloned().collect::<HashSet<_>>();

        for config in eligible_untested {
            if !batch_set.contains(&config) {
                pending.push_back(config);
            }
        }

        if batch.is_empty() {
            if receiver_closed {
                break;
            }
            continue;
        }

        transfer_tested.extend(batch.iter().cloned());

        target_tested_candidates[target_index].extend(batch.iter().cloned());

        proxyrift::emit_log_if!(proxyrift::should_emit_compact_progress(transfer_tested.len(), batch.len(), 100, transfer_tested.len() >= dynamic_test_limit);
            "[INFO] 📥 [9 MiB sustained] {} Validation slots remaining | Testing {} candidates | Adaptive max tests: {} | Target: {} | Score: {:.3} | Quarantined: {} | Pipeline: 1 MiB producer active",
            remaining,
            batch.len(),
            dynamic_test_limit,
            target,
            transfer_target_score(&target_state_before),
            target_state_before.quarantined
        );

        let rate_limits_before = target_rate_limit_events(target);
        let batch_started = Instant::now();
        let metadata =
            match validate_light_transfer_batch(xray, singbox, &batch, transfer_workers, target)
                .await
            {
                Ok(metadata) => metadata,
                Err(error) => {
                    transfer_done.store(true, Ordering::Relaxed);
                    return Err(error);
                }
            };
        let batch_elapsed = batch_started.elapsed().as_secs();
        let batch_passed = metadata.len();
        transfer_verified.extend(metadata);
        transfer_tested_count.store(transfer_tested.len(), Ordering::Relaxed);
        transfer_passed_count.store(transfer_verified.len(), Ordering::Relaxed);

        let rate_limits = target_rate_limit_events(target)
            .saturating_sub(rate_limits_before)
            .min(batch.len() as u64);

        let target_state_before = target_states[target_index];
        update_transfer_target_state(
            &mut target_states[target_index],
            batch.len(),
            batch_passed,
            rate_limits,
            batch_elapsed,
        );
        if target_index < PRIMARY_TRANSFER_TARGET_COUNT {
            next_preferred_primary = (target_index + 1) % PRIMARY_TRANSFER_TARGET_COUNT;
        }
        if target_is_rate_limited(target) {
            target_states[target_index].quarantined = true;
        }
        if target_states[target_index].quarantined && !target_state_before.quarantined {
            let alternative_target_available = target_states
                .iter()
                .enumerate()
                .any(|(index, state)| index != target_index && !state.quarantined);

            println!(
                "[WARN] ⚠️ [9 MiB sustained] Quarantining target for this run | Target: {} | Tested: {} | Passed: {} | Pass rate: {:.1}% | Rate limits: {}",
                target,
                target_states[target_index].tested,
                target_states[target_index].passed,
                transfer_target_pass_rate(&target_states[target_index]) * 100.0,
                target_states[target_index].rate_limits
            );

            if alternative_target_available {
                let mut requeued = 0usize;
                for config in &target_tested_candidates[target_index] {
                    if !transfer_verified.contains_key(config) && transfer_tested.remove(config) {
                        if !pending.iter().any(|queued| queued == config) {
                            pending.push_back(config.clone());
                        }
                        requeued += 1;
                    }
                }

                if requeued > 0 {
                    println!(
                        "[INFO] ↪️ [9 MiB sustained] Requeued {} failed candidates after target quarantine | Target: {}",
                        requeued, target
                    );
                }
            } else {
                println!(
                    "[WARN] ⚠️ [9 MiB sustained] All transfer targets are quarantined | Keeping failed candidates closed to avoid retry loop"
                );
            }
        }
        transfer_tested_count.store(transfer_tested.len(), Ordering::Relaxed);
        transfer_passed_count.store(transfer_verified.len(), Ordering::Relaxed);

        let (previous_workers, previous_clean_batches) =
            target_concurrency[target_index].observe(rate_limits, batch.len());
        if rate_limits > 0 {
            let rate_limit_percent = if batch.is_empty() {
                0
            } else {
                rate_limits.saturating_mul(100).div_ceil(batch.len() as u64)
            };

            if target_concurrency[target_index].workers < previous_workers {
                println!(
                    "[WARN] ⚠️ Light transfer: {} rate-limit responses ({rate_limit_percent}%) at {} | Reducing workers {} -> {}",
                    rate_limits,
                    target,
                    previous_workers,
                    target_concurrency[target_index].workers
                );
            } else {
                println!(
                    "[WARN] ⚠️ Light transfer: {} rate-limit responses ({rate_limit_percent}%) at {} | Within tolerance, keeping workers at {}",
                    rate_limits,
                    target,
                    previous_workers
                );
            }
        } else if target_concurrency[target_index].workers > previous_workers {
            println!(
                "[INFO] 📈 Light transfer: {} clean batches; increasing workers {} -> {}",
                previous_clean_batches + 1,
                previous_workers,
                target_concurrency[target_index].workers
            );
        }

        proxyrift::emit_log_if!(proxyrift::should_emit_compact_progress(transfer_tested.len(), batch.len(), 100, transfer_verified.len() >= transfer_target || (receiver_closed && pending.is_empty()));
            "[INFO] ✅ [9 MiB sustained] {}/{} Passed in {}s | Total passed: {} | Validation slots remaining: {}",
            batch_passed,
            batch.len(),
            batch_elapsed,
            transfer_verified.len(),
            transfer_target.saturating_sub(
                select_verified_configs(
                    &transfer_verified.keys().cloned().collect::<Vec<_>>(),
                    transfer_target,
                    max_per_endpoint,
                    max_per_family,
                )
                .len(),
            ),
        );
    }

    transfer_done.store(true, Ordering::Relaxed);
    Ok(select_verified_configs(
        &transfer_verified.keys().cloned().collect::<Vec<_>>(),
        transfer_target,
        max_per_endpoint,
        max_per_family,
    )
    .len())
}

struct TransferGateSnapshot {
    stability_verified: HashMap<String, ProxyMetrics>,
    stability_tested: HashSet<String>,
    transfer_verified: HashMap<String, ProxyMetrics>,
    transfer_tested: HashSet<String>,
}

struct TransferGateRequest {
    xray: String,
    singbox: String,
    final_verified: Vec<String>,
    final_metadata: HashMap<String, ProxyMetrics>,
    stability_verified: HashMap<String, ProxyMetrics>,
    stability_tested: HashSet<String>,
    transfer_verified: HashMap<String, ProxyMetrics>,
    transfer_tested: HashSet<String>,
    global_positions: HashMap<String, usize>,
    history: HashMap<String, HistoryEntry>,
    selection_limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
}

type TransferGateTask = tokio::task::JoinHandle<Result<TransferGateSnapshot, String>>;

fn spawn_transfer_gate(request: TransferGateRequest) -> TransferGateTask {
    tokio::spawn(async move {
        let mut stability_verified = request.stability_verified;
        let mut stability_tested = request.stability_tested;
        let mut transfer_verified = request.transfer_verified;
        let mut transfer_tested = request.transfer_tested;

        let _ = fill_transfer_gate(
            &request.xray,
            &request.singbox,
            &request.final_verified,
            &request.final_metadata,
            &mut stability_verified,
            &mut stability_tested,
            &mut transfer_verified,
            &mut transfer_tested,
            &request.global_positions,
            &request.history,
            request.selection_limit,
            request.max_per_endpoint,
            request.max_per_family,
        )
        .await?;

        Ok(TransferGateSnapshot {
            stability_verified,
            stability_tested,
            transfer_verified,
            transfer_tested,
        })
    })
}

fn apply_transfer_gate_snapshot(
    snapshot: TransferGateSnapshot,
    stability_verified: &mut HashMap<String, ProxyMetrics>,
    stability_tested: &mut HashSet<String>,
    transfer_verified: &mut HashMap<String, ProxyMetrics>,
    transfer_tested: &mut HashSet<String>,
) {
    *stability_verified = snapshot.stability_verified;
    *stability_tested = snapshot.stability_tested;
    *transfer_verified = snapshot.transfer_verified;
    *transfer_tested = snapshot.transfer_tested;
}

#[allow(clippy::too_many_arguments)]
async fn fill_transfer_gate(
    xray: &str,
    singbox: &str,
    final_verified: &[String],
    final_metadata: &HashMap<String, ProxyMetrics>,
    stability_verified: &mut HashMap<String, ProxyMetrics>,
    stability_tested: &mut HashSet<String>,
    transfer_verified: &mut HashMap<String, ProxyMetrics>,
    transfer_tested: &mut HashSet<String>,
    global_positions: &HashMap<String, usize>,
    history: &HashMap<String, HistoryEntry>,
    selection_limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> Result<usize, String> {
    let transfer_target = transfer_validation_target(selection_limit);

    let mut existing_ranked = transfer_verified.keys().cloned().collect::<Vec<_>>();
    sort_ranked(
        &mut existing_ranked,
        transfer_verified,
        global_positions,
        history,
    );
    let existing_selected = select_verified_configs(
        &existing_ranked,
        transfer_target,
        max_per_endpoint,
        max_per_family,
    );
    if existing_selected.len() >= transfer_target {
        return Ok(existing_selected.len());
    }

    let transfer_done = Arc::new(AtomicBool::new(false));
    let transfer_tested_count = Arc::new(AtomicUsize::new(transfer_tested.len()));
    let transfer_passed_count = Arc::new(AtomicUsize::new(transfer_verified.len()));
    let (stability_sender, stability_receiver) = tokio::sync::mpsc::unbounded_channel();

    let stability_future = fill_transfer_stability_gate(
        xray,
        singbox,
        final_verified,
        final_metadata,
        std::mem::take(stability_verified),
        std::mem::take(stability_tested),
        stability_sender,
        transfer_done.clone(),
        global_positions,
        history,
        selection_limit,
        transfer_tested_count.clone(),
        transfer_passed_count.clone(),
        max_per_endpoint,
        max_per_family,
    );

    let mut transfer_verified_state = std::mem::take(transfer_verified);
    let mut transfer_tested_state = std::mem::take(transfer_tested);
    let transfer_future = run_transfer_gate_consumer(
        xray,
        singbox,
        stability_receiver,
        &mut transfer_verified_state,
        &mut transfer_tested_state,
        global_positions,
        history,
        selection_limit,
        max_per_endpoint,
        max_per_family,
        transfer_tested_count,
        transfer_passed_count,
        transfer_done.clone(),
    );

    let (stability_result, transfer_result) = tokio::join!(stability_future, transfer_future);

    let (_, completed_stability_verified, completed_stability_tested) = stability_result?;
    *stability_verified = completed_stability_verified;
    *stability_tested = completed_stability_tested;

    let transfer_selected = transfer_result?;
    *transfer_verified = transfer_verified_state;
    *transfer_tested = transfer_tested_state;

    Ok(transfer_selected)
}

#[allow(clippy::too_many_arguments)]
fn try_add_verified_config(
    config: &String,
    selected: &mut Vec<String>,
    selected_set: &mut HashSet<String>,
    endpoint_counts: &mut HashMap<(String, u16), usize>,
    family_counts: &mut HashMap<String, usize>,
    limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> bool {
    if selected.len() >= limit || !selected_set.insert(config.clone()) {
        return false;
    }

    let family = family_key(config);
    if family_counts.get(&family).copied().unwrap_or(0) >= max_per_family {
        selected_set.remove(config);
        return false;
    }

    if let Some(ep) = endpoint(config) {
        if endpoint_counts.get(&ep).copied().unwrap_or(0) >= max_per_endpoint {
            selected_set.remove(config);
            return false;
        }
        *endpoint_counts.entry(ep).or_default() += 1;
    }

    *family_counts.entry(family).or_default() += 1;
    selected.push(config.clone());
    true
}

fn select_verified_configs_with_freshness_quota(
    configs: &[String],
    recent_families: &HashSet<String>,
    limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> (Vec<String>, usize) {
    if limit == 0 || configs.is_empty() {
        return (Vec::new(), 0);
    }

    let fresh_target = limit
        .saturating_mul(FRESH_FAMILY_TARGET_PERCENT)
        .div_ceil(100)
        .min(limit);
    let mut endpoint_counts = HashMap::<(String, u16), usize>::new();
    let mut family_counts = HashMap::<String, usize>::new();
    let mut selected = Vec::with_capacity(limit.min(configs.len()));
    let mut selected_set = HashSet::new();

    for config in configs {
        if selected.len() >= fresh_target {
            break;
        }
        if recent_families.contains(&family_key(config)) {
            continue;
        }
        let _ = try_add_verified_config(
            config,
            &mut selected,
            &mut selected_set,
            &mut endpoint_counts,
            &mut family_counts,
            fresh_target,
            max_per_endpoint,
            max_per_family,
        );
    }

    for config in configs {
        if selected.len() >= limit {
            break;
        }
        let _ = try_add_verified_config(
            config,
            &mut selected,
            &mut selected_set,
            &mut endpoint_counts,
            &mut family_counts,
            limit,
            max_per_endpoint,
            max_per_family,
        );
    }

    let fresh_selected = selected
        .iter()
        .filter(|config| !recent_families.contains(&family_key(config)))
        .count();

    (selected, fresh_selected)
}

fn select_verified_configs_with_repeat_cap(
    configs: &[String],
    repeat_fingerprints: &HashSet<String>,
    limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> (Vec<String>, usize) {
    let fresh = configs
        .iter()
        .filter(|config| !repeat_fingerprints.contains(&history_fingerprint(config)))
        .cloned()
        .collect::<Vec<_>>();
    let repeated = configs
        .iter()
        .filter(|config| repeat_fingerprints.contains(&history_fingerprint(config)))
        .cloned()
        .collect::<Vec<_>>();

    let mut selected = select_verified_configs(&fresh, limit, max_per_endpoint, max_per_family);
    let selected_fresh = selected.len();
    if selected.len() >= limit || limit == 0 {
        return (selected, selected_fresh);
    }

    let repeat_limit = limit.saturating_mul(REVALIDATED_CANDIDATE_PERCENT) / 100;
    let mut endpoint_counts = HashMap::<(String, u16), usize>::new();
    let mut family_counts = HashMap::<String, usize>::new();
    for config in &selected {
        if let Some(endpoint) = endpoint(config) {
            *endpoint_counts.entry(endpoint).or_insert(0) += 1;
        }
        *family_counts.entry(family_key(config)).or_insert(0) += 1;
    }

    let mut selected_repeats = 0usize;
    for config in repeated {
        if selected.len() >= limit || selected_repeats >= repeat_limit {
            break;
        }
        let endpoint = endpoint(&config);
        if endpoint
            .as_ref()
            .is_some_and(|key| endpoint_counts.get(key).copied().unwrap_or(0) >= max_per_endpoint)
        {
            continue;
        }
        let family = family_key(&config);
        if family_counts.get(&family).copied().unwrap_or(0) >= max_per_family {
            continue;
        }
        if let Some(endpoint) = endpoint {
            *endpoint_counts.entry(endpoint).or_insert(0) += 1;
        }
        *family_counts.entry(family).or_insert(0) += 1;
        selected.push(config);
        selected_repeats += 1;
    }

    (selected, selected_fresh)
}

fn select_verified_configs(
    configs: &[String],
    limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> Vec<String> {
    let mut endpoint_counts = HashMap::<(String, u16), usize>::new();
    let mut family_counts = HashMap::<String, usize>::new();
    let mut result = Vec::with_capacity(limit.min(configs.len()));

    for config in configs {
        if result.len() >= limit {
            break;
        }

        if let Some(endpoint) = endpoint(config) {
            if endpoint_counts.get(&endpoint).copied().unwrap_or(0) >= max_per_endpoint {
                continue;
            }

            let family = family_key(config);
            if family_counts.get(&family).copied().unwrap_or(0) >= max_per_family {
                continue;
            }

            *endpoint_counts.entry(endpoint).or_insert(0) += 1;
            *family_counts.entry(family).or_insert(0) += 1;
            result.push(config.clone());
        } else {
            let family = family_key(config);
            if family_counts.get(&family).copied().unwrap_or(0) >= max_per_family {
                continue;
            }

            *family_counts.entry(family).or_insert(0) += 1;
            result.push(config.clone());
        }
    }

    result
}

fn stage_selectable_count(
    metadata: &HashMap<String, ProxyMetrics>,
    global_positions: &HashMap<String, usize>,
    history: &HashMap<String, HistoryEntry>,
    selection_limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> usize {
    let mut ranked = metadata.keys().cloned().collect::<Vec<_>>();
    sort_ranked(&mut ranked, metadata, global_positions, history);
    select_verified_configs(&ranked, selection_limit, max_per_endpoint, max_per_family).len()
}

#[cfg(test)]
fn stream_selection_count(
    stream_verified: &HashMap<String, ProxyMetrics>,
    recent_families: &HashSet<String>,
    global_positions: &HashMap<String, usize>,
    history: &HashMap<String, HistoryEntry>,
    selection_limit: usize,
    max_per_endpoint: usize,
    max_per_family: usize,
) -> usize {
    if selection_limit == 0 || stream_verified.is_empty() {
        return 0;
    }

    let mut ranked = stream_verified.keys().cloned().collect::<Vec<_>>();
    sort_ranked(&mut ranked, stream_verified, global_positions, history);
    select_verified_configs_with_freshness_quota(
        &ranked,
        recent_families,
        selection_limit,
        max_per_endpoint,
        max_per_family,
    )
    .0
    .len()
}

#[derive(Clone, Copy, Debug)]
struct ValidationSettings {
    workers: usize,
    batch_size: usize,
    timeout_seconds: f64,
    strict: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LightBackend {
    SingBox,
    Xray,
    Fallback,
}

fn query_value(url: &Url, names: &[&str]) -> String {
    url.query_pairs()
        .find(|(key, value)| {
            names.iter().any(|name| key.eq_ignore_ascii_case(name)) && !value.is_empty()
        })
        .map(|(_, value)| value.into_owned())
        .unwrap_or_default()
}

fn has_query_key(url: &Url, names: &[&str]) -> bool {
    url.query_pairs()
        .any(|(key, _)| names.iter().any(|name| key.eq_ignore_ascii_case(name)))
}

fn value_boolish(value: &Value) -> bool {
    match value {
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_u64().unwrap_or(0) != 0,
        Value::String(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        _ => false,
    }
}

fn has_disabled_tls_verification(config: &str) -> bool {
    let cleaned = config.split('#').next().unwrap_or(config);
    let Ok(url) = Url::parse(cleaned) else {
        return true;
    };

    let scheme = url.scheme().to_ascii_lowercase();

    if scheme == "vmess" {
        let Some(encoded) = cleaned.split_once("://").map(|(_, rest)| rest) else {
            return true;
        };
        let payload = encoded.trim();
        let mut padded = payload.to_string();
        while !padded.len().is_multiple_of(4) {
            padded.push('=');
        }

        for candidate in [payload, padded.as_str()] {
            for bytes in [
                STANDARD.decode(candidate),
                URL_SAFE.decode(candidate),
                URL_SAFE_NO_PAD.decode(candidate),
            ]
            .into_iter()
            .flatten()
            {
                if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                    let tls_enabled = match value.get("tls") {
                        Some(Value::Bool(value)) => *value,
                        Some(Value::Number(value)) => value.as_u64().unwrap_or(0) != 0,
                        Some(Value::String(value)) => !matches!(
                            value.trim().to_ascii_lowercase().as_str(),
                            "" | "0" | "false" | "none" | "off"
                        ),
                        _ => false,
                    };
                    let insecure = value.get("allowInsecure").is_some_and(value_boolish);
                    if tls_enabled && insecure {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    url.query_pairs().any(|(key, value)| {
        matches!(
            key.to_ascii_lowercase().as_str(),
            "insecure" | "allowinsecure"
        ) && matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn light_backend(config: &str) -> LightBackend {
    match backend_for_config(config) {
        "xray" => LightBackend::Xray,
        "fallback" => LightBackend::Fallback,
        _ => LightBackend::SingBox,
    }
}

fn vmess_payload(config: &str) -> Option<Value> {
    let encoded = config.split_once("://").map(|(_, rest)| rest)?;
    let payload = encoded.split('#').next().unwrap_or("").trim();
    let mut padded = payload.to_string();
    while !padded.len().is_multiple_of(4) {
        padded.push('=');
    }

    for candidate in [payload, padded.as_str()] {
        for bytes in [
            STANDARD.decode(candidate),
            URL_SAFE.decode(candidate),
            URL_SAFE_NO_PAD.decode(candidate),
        ]
        .into_iter()
        .flatten()
        {
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                return Some(value);
            }
        }
    }

    None
}

fn json_u64(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Number(number)) => number.as_u64().unwrap_or(0),
        Some(Value::String(string)) => string.trim().parse::<u64>().unwrap_or(0),
        _ => 0,
    }
}

fn training_number(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn light_training_features(
    config: &str,
    early_metadata: Option<&ProxyMetrics>,
    history_rate: f64,
    history_checks: u64,
) -> BTreeMap<String, Value> {
    let cleaned = config.split('#').next().unwrap_or(config);
    let url = Url::parse(cleaned).ok();
    let protocol = url
        .as_ref()
        .map(|value| value.scheme().to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string());
    let vmess = if protocol == "vmess" {
        vmess_payload(config)
    } else {
        None
    };

    let transport = if let Some(payload) = vmess.as_ref() {
        payload
            .get("net")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "vmess-default".to_string())
    } else {
        url.as_ref()
            .map(|value| query_value(value, &["type", "network"]).to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "default".to_string())
    };

    let security = if let Some(payload) = vmess.as_ref() {
        let tls_enabled = match payload.get("tls") {
            Some(Value::String(value)) => matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "tls" | "1" | "true" | "yes" | "on"
            ),
            Some(value) => value_boolish(value),
            None => false,
        };
        if tls_enabled {
            "tls".to_string()
        } else {
            "none".to_string()
        }
    } else {
        url.as_ref()
            .map(|value| query_value(value, &["security"]).to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "default".to_string())
    };

    let port = if let Some(payload) = vmess.as_ref() {
        json_u64(payload.get("port"))
    } else {
        url.as_ref().and_then(Url::port).map(u64::from).unwrap_or(0)
    };

    let query_parameter_count = url
        .as_ref()
        .map(|value| value.query_pairs().count() as u64)
        .unwrap_or(0);

    let has_sni = if let Some(payload) = vmess.as_ref() {
        ["sni", "serverName", "servername"].iter().any(|key| {
            payload
                .get(*key)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        })
    } else {
        url.as_ref()
            .is_some_and(|value| has_query_key(value, &["sni", "serverName", "servername"]))
    };

    let has_host = if let Some(payload) = vmess.as_ref() {
        ["host", "Host"].iter().any(|key| {
            payload
                .get(*key)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        })
    } else {
        url.as_ref()
            .is_some_and(|value| has_query_key(value, &["host", "authority"]))
    };

    let has_path = if let Some(payload) = vmess.as_ref() {
        payload
            .get("path")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
    } else {
        url.as_ref()
            .is_some_and(|value| has_query_key(value, &["path"]))
    };

    let tls_enabled = if vmess.is_some() {
        security == "tls"
    } else {
        matches!(security.as_str(), "tls" | "reality" | "xtls")
    };
    let reality_enabled = security == "reality";

    let early_attempts = early_metadata.map(|metrics| metrics.attempts).unwrap_or(0);
    let early_success_rate = early_metadata
        .filter(|metrics| metrics.attempts > 0)
        .map(|metrics| metrics.successes as f64 / metrics.attempts as f64)
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);

    let mut features = BTreeMap::new();
    features.insert("protocol".to_string(), Value::String(protocol));
    features.insert(
        "backend".to_string(),
        Value::String(
            match light_backend(config) {
                LightBackend::SingBox => "sing-box",
                LightBackend::Xray => "xray",
                LightBackend::Fallback => "fallback",
            }
            .to_string(),
        ),
    );
    features.insert("transport".to_string(), Value::String(transport));
    features.insert("security".to_string(), Value::String(security));
    features.insert("port".to_string(), Value::from(port));
    features.insert(
        "query_parameter_count".to_string(),
        Value::from(query_parameter_count),
    );
    features.insert("has_sni".to_string(), Value::Bool(has_sni));
    features.insert("has_host".to_string(), Value::Bool(has_host));
    features.insert("has_path".to_string(), Value::Bool(has_path));
    features.insert("tls_enabled".to_string(), Value::Bool(tls_enabled));
    features.insert("reality_enabled".to_string(), Value::Bool(reality_enabled));
    features.insert("early_attempts".to_string(), Value::from(early_attempts));
    features.insert(
        "early_success_rate".to_string(),
        training_number(early_success_rate),
    );

    let metrics = early_metadata;
    features.insert(
        "early_median_ms".to_string(),
        training_number(metrics.map(|value| value.median_ms).unwrap_or(0.0)),
    );
    features.insert(
        "early_min_ms".to_string(),
        training_number(metrics.map(|value| value.min_ms).unwrap_or(0.0)),
    );
    features.insert(
        "early_jitter_ms".to_string(),
        training_number(metrics.map(|value| value.jitter_ms).unwrap_or(0.0)),
    );
    features.insert(
        "early_throughput_kbps".to_string(),
        training_number(metrics.map(|value| value.throughput_kbps).unwrap_or(0.0)),
    );
    features.insert("history_checks".to_string(), Value::from(history_checks));
    features.insert(
        "history_pass_rate".to_string(),
        training_number(history_rate.clamp(0.0, 1.0)),
    );

    features
}

#[allow(clippy::too_many_arguments)]
fn write_light_validation_report(
    selected: &[String],
    stability_verified: &HashMap<String, ProxyMetrics>,
    transfer_verified: &HashMap<String, ProxyMetrics>,
    light_gbm_scores: &LightGbmScores,
) -> Result<String, String> {
    let output_dir = env::var("PROXYRIFT_TMP_DIR").unwrap_or_else(|_| "/tmp/proxyrift".to_string());
    std::fs::create_dir_all(&output_dir)
        .map_err(|error| format!("failed to create Light validation report directory: {error}"))?;

    let configurations = selected
        .iter()
        .map(|config| {
            let stability = stability_verified.get(config).ok_or_else(|| {
                format!(
                    "published config {} has no 1 MiB validation record",
                    observation_fingerprint(config)
                )
            })?;
            let transfer = transfer_verified.get(config).ok_or_else(|| {
                format!(
                    "published config {} has no 9 MiB validation record",
                    observation_fingerprint(config)
                )
            })?;
            if stability.successful_targets.is_empty() || transfer.successful_targets.is_empty() {
                return Err(format!(
                    "published config {} is missing a successful download destination",
                    observation_fingerprint(config)
                ));
            }

            let protocol = config
                .split_once("://")
                .map(|(scheme, _)| scheme.to_ascii_lowercase())
                .unwrap_or_else(|| "unknown".to_string());
            Ok(serde_json::json!({
                "config_id": observation_fingerprint(config),
                "protocol": protocol,
                "validated_core": "sing-box",
                "one_mib": {
                    "passed": true,
                    "bytes": LIGHT_TRANSFER_STABILITY_BYTES,
                    "successful_targets": stability.successful_targets.clone(),
                    "duration_ms": stability.median_ms,
                },
                "nine_mib_sustained": {
                    "passed": true,
                    "bytes": STREAM_CONTINUITY_SEGMENTS * STREAM_CONTINUITY_SEGMENT_BYTES,
                    "successful_targets": transfer.successful_targets.clone(),
                    "duration_ms": transfer.median_ms,
                    "throughput_kbps": transfer.throughput_kbps,
                    "throughput_mbps": transfer.throughput_kbps / 1000.0,
                    "timeout_limit_ms": FINAL_TRANSFER_LATENCY_LIMIT_MS,
                    "max_inter_chunk_idle_secs": STREAM_CONTINUITY_MAX_IDLE_SECS,
                },
                "total_download_bytes": LIGHT_TRANSFER_STABILITY_BYTES
                    + STREAM_CONTINUITY_SEGMENTS * STREAM_CONTINUITY_SEGMENT_BYTES,
                "lightgbm_score": light_gbm_scores.score(config),
                "published": true
            }))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let report = serde_json::json!({
        "version": 2,
        "training_objective": "validated_stream_throughput_regression",
        "validated_core": "sing-box",
        "primary_targets": [
            proxyrift::validator::LIGHT_TRANSFER_STABILITY_TARGETS[0],
            proxyrift::validator::LIGHT_TRANSFER_STABILITY_TARGETS[1]
        ],
        "backup_targets": [
            proxyrift::validator::LIGHT_TRANSFER_STABILITY_TARGETS[2],
            proxyrift::validator::LIGHT_TRANSFER_STABILITY_TARGETS[3]
        ],
        "required_download_bytes": LIGHT_TRANSFER_STABILITY_BYTES
            + STREAM_CONTINUITY_SEGMENTS * STREAM_CONTINUITY_SEGMENT_BYTES,
        "configurations": configurations
    });
    let path = std::path::Path::new(&output_dir).join("light-validation-report.json");
    let temporary = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(&report)
        .map_err(|error| format!("failed to serialize Light validation report: {error}"))?;
    std::fs::write(&temporary, body)
        .map_err(|error| format!("failed to write Light validation report: {error}"))?;
    std::fs::rename(&temporary, &path)
        .map_err(|error| format!("failed to publish Light validation report: {error}"))?;
    Ok(path.display().to_string())
}

#[allow(clippy::too_many_arguments)]
fn persist_light_training_data(
    history: &HashMap<String, HistoryEntry>,
    final_attempts: &HashMap<String, usize>,
    final_metadata: &HashMap<String, ProxyMetrics>,
    global_metadata: &HashMap<String, ProxyMetrics>,
    transfer_tested: &HashSet<String>,
    transfer_verified: &HashMap<String, ProxyMetrics>,
    stream_tested: &HashSet<String>,
    stream_verified: &HashMap<String, ProxyMetrics>,
) -> Result<DatasetStats, String> {
    let observed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    let run_id = env::var("GITHUB_RUN_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let mut rows = Vec::with_capacity(final_attempts.len());
    for (config, attempts) in final_attempts {
        let (history_rate, history_checks) = historical_score(config, history);
        let candidate_fingerprint = history_fingerprint(config);
        let observation_fingerprint = observation_fingerprint(config);
        let observation_id = if let Some(run_id) = run_id.as_deref() {
            format!("{run_id}:{candidate_fingerprint}:{observation_fingerprint}")
        } else {
            format!("local:{observed_at}:{candidate_fingerprint}:{observation_fingerprint}")
        };
        let transfer_was_tested = transfer_tested.contains(config);
        let stream_was_tested = stream_tested.contains(config);

        let stream_metrics = stream_verified.get(config);
        rows.push(TrainingRow {
            observed_at,
            run_id: run_id.clone(),
            candidate_fingerprint,
            observation_id,
            features: light_training_features(
                config,
                global_metadata.get(config),
                history_rate,
                history_checks,
            ),
            strict_pass: final_metadata.contains_key(config),
            strict_checks: *attempts as u64,
            transfer_tested: transfer_was_tested,
            transfer_pass: transfer_was_tested.then(|| transfer_verified.contains_key(config)),
            stream_tested: stream_was_tested,
            stream_pass: stream_was_tested.then(|| stream_verified.contains_key(config)),
            stream_throughput_kbps: stream_metrics
                .map(|metrics| metrics.throughput_kbps)
                .filter(|speed| speed.is_finite() && *speed > 0.0)
                .unwrap_or(0.0),
            stream_duration_ms: stream_metrics
                .map(|metrics| metrics.median_ms)
                .filter(|duration| duration.is_finite() && *duration > 0.0)
                .unwrap_or(0.0),
        });
    }

    let stats = persist_light_training(LIGHT_TRAINING_PATH, &rows)?;
    write_readiness_report(LIGHT_TRAINING_STATS_PATH, &stats)?;

    println!(
        "[INFO] 🧠 [Light ml data] +{} speed samples | Retained: {} / 72h | Runs: {} | Candidates: {} | Speed model ready: {}",
        stats.new_rows,
        stats.rows,
        stats.unique_runs,
        stats.unique_candidates,
        stats.speed_model_ready()
    );

    Ok(stats)
}

fn singbox_qualified_metadata(
    xray_metadata: HashMap<String, ProxyMetrics>,
    singbox_metadata: HashMap<String, ProxyMetrics>,
) -> HashMap<String, ProxyMetrics> {
    let xray_only = xray_metadata
        .keys()
        .filter(|config| !singbox_metadata.contains_key(*config))
        .count();
    if xray_only > 0 {
        println!(
            "[INFO] 🧹 [Light compatibility] Excluding {} configs verified only by Xray from sing-box publication",
            xray_only
        );
    }
    singbox_metadata
}

async fn validate_light_batch(
    xray: &str,
    singbox: &str,
    candidates: &[String],
    targets: &[&str],
    settings: ValidationSettings,
) -> Result<HashMap<String, ProxyMetrics>, String> {
    let mut singbox_candidates = Vec::new();
    let mut xray_candidates = Vec::new();
    let mut fallback_candidates = Vec::new();

    for config in candidates {
        match light_backend(config) {
            LightBackend::SingBox => singbox_candidates.push(config.clone()),
            LightBackend::Xray => xray_candidates.push(config.clone()),
            LightBackend::Fallback => fallback_candidates.push(config.clone()),
        }
    }

    let mut singbox_validation_candidates = singbox_candidates;
    singbox_validation_candidates.extend(fallback_candidates.iter().cloned());
    let xray_validation_candidates = xray_candidates;

    let request_timeout = std::time::Duration::try_from_secs_f64(settings.timeout_seconds)
        .map_err(|_| "invalid validation timeout: value overflows Duration".to_string())?;
    let prefilter_target = *targets
        .first()
        .ok_or_else(|| "Light validation requires at least one target".to_string())?;

    let singbox_future = async {
        if singbox_validation_candidates.is_empty() {
            Ok(HashMap::new())
        } else if settings.strict {
            validate_singbox_consumer_targets(
                singbox,
                &singbox_validation_candidates,
                targets,
                settings.workers.clamp(1, 40),
                request_timeout,
                settings.timeout_seconds * 1000.0,
            )
            .await
        } else {
            validate_singbox_target_once(
                singbox,
                &singbox_validation_candidates,
                prefilter_target,
                settings.workers.clamp(1, 40),
                request_timeout,
                settings.timeout_seconds * 1000.0,
            )
            .await
        }
    };

    let xray_future = async {
        if xray_validation_candidates.is_empty() {
            Ok(HashMap::new())
        } else if settings.strict {
            validate_candidates_with_consumer_targets(
                xray,
                &xray_validation_candidates,
                targets,
                settings.workers.max(1),
                settings.batch_size,
                settings.timeout_seconds,
                settings.timeout_seconds * 1000.0,
            )
            .await
        } else {
            validate_candidates_with_target_once(
                xray,
                &xray_validation_candidates,
                prefilter_target,
                settings.workers.max(1),
                settings.batch_size,
                settings.timeout_seconds,
                settings.timeout_seconds * 1000.0,
            )
            .await
        }
    };

    let (singbox_result, xray_result) = tokio::join!(singbox_future, xray_future);
    let singbox_metadata = singbox_result?;
    let mut xray_metadata = xray_result?;

    let fallback_retry = fallback_candidates
        .into_iter()
        .filter(|config| !singbox_metadata.contains_key(config))
        .collect::<Vec<_>>();

    if !fallback_retry.is_empty() {
        println!(
            "[INFO] 🔄 [Light fallback] Retrying {} candidates with xray",
            fallback_retry.len()
        );
        let fallback_xray = if settings.strict {
            validate_candidates_with_consumer_targets(
                xray,
                &fallback_retry,
                targets,
                settings.workers.max(1),
                settings.batch_size,
                settings.timeout_seconds,
                settings.timeout_seconds * 1000.0,
            )
            .await?
        } else {
            validate_candidates_with_target_once(
                xray,
                &fallback_retry,
                prefilter_target,
                settings.workers.max(1),
                settings.batch_size,
                settings.timeout_seconds,
                settings.timeout_seconds * 1000.0,
            )
            .await?
        };
        xray_metadata.extend(fallback_xray);
    }

    let verified = singbox_qualified_metadata(xray_metadata, singbox_metadata);

    let stage = if settings.strict {
        "CONSUMER"
    } else {
        "PREFILTER"
    };
    println!(
        "[INFO] ✅ [Light {stage}] {}/{} Candidates verified | Targets: {}",
        verified.len(),
        candidates.len(),
        if settings.strict { targets.len() } else { 1 }
    );

    Ok(verified)
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();

    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: polish_light --candidates FILE --output FILE [--workers N]              [--batch-size N] [--timeout SECONDS] [--selected-recheck-limit N]              [--max-candidates N] [--selected-workers N] [--selected-batch-size N]              [--primary-target URL] [--selection-limit N] [--max-per-endpoint N]              [--max-per-family N]              [--xray PATH] [--singbox PATH] [--stats PATH]"
        );
        return Ok(());
    }

    let candidates_path = required(&args, "--candidates")?;
    let output = required(&args, "--output")?;
    let stats_path = value(&args, "--stats", "");
    let workers = value(&args, "--workers", "32")
        .parse::<usize>()
        .map_err(|_| "invalid --workers".to_string())?;
    let batch_size = value(&args, "--batch-size", "1000")
        .parse::<usize>()
        .map_err(|_| "invalid --batch-size".to_string())?;
    let timeout = value(&args, "--timeout", "5")
        .parse::<f64>()
        .map_err(|_| "invalid --timeout".to_string())?;
    if !timeout.is_finite() || timeout <= 0.0 {
        return Err("invalid --timeout: must be a positive finite number".to_string());
    }
    let final_recheck_limit = value(
        &args,
        "--selected-recheck-limit",
        FINAL_RECHECK_LIMIT.to_string().as_str(),
    )
    .parse::<usize>()
    .map_err(|_| "invalid --selected-recheck-limit".to_string())?;
    let max_candidates = value(
        &args,
        "--max-candidates",
        MAX_DISCOVERY_CANDIDATES.to_string().as_str(),
    )
    .parse::<usize>()
    .map_err(|_| "invalid --max-candidates".to_string())?
    .clamp(1, MAX_DISCOVERY_CANDIDATES);
    let final_workers = value(&args, "--selected-workers", "16")
        .parse::<usize>()
        .map_err(|_| "invalid --selected-workers".to_string())?;
    let final_batch_size = value(&args, "--selected-batch-size", "500")
        .parse::<usize>()
        .map_err(|_| "invalid --selected-batch-size".to_string())?;
    let primary_target = value(&args, "--primary-target", PRIMARY_TARGET);
    let early_targets = [primary_target.as_str()];
    let consumer_targets = {
        let mut targets = LIGHT_CONSUMER_TARGETS.to_vec();
        targets[0] = primary_target.as_str();
        targets
    };
    let xray = value(&args, "--xray", "xray");
    let selection_limit = value(
        &args,
        "--selection-limit",
        DEFAULT_SELECTION_LIMIT.to_string().as_str(),
    )
    .parse::<usize>()
    .map_err(|_| "invalid --selection-limit".to_string())?
    .max(1);
    let max_per_endpoint = value(
        &args,
        "--max-per-endpoint",
        DEFAULT_MAX_PER_ENDPOINT.to_string().as_str(),
    )
    .parse::<usize>()
    .map_err(|_| "invalid --max-per-endpoint".to_string())?
    .max(1);
    let max_per_family = value(
        &args,
        "--max-per-family",
        DEFAULT_MAX_PER_FAMILY.to_string().as_str(),
    )
    .parse::<usize>()
    .map_err(|_| "invalid --max-per-family".to_string())?
    .max(1);
    let singbox = value(&args, "--singbox", "sing-box");

    let raw_candidates = read_lines(&candidates_path)?;
    let input_candidate_count = raw_candidates.len().min(max_candidates);
    let current_candidates = raw_candidates
        .into_iter()
        .take(max_candidates)
        .filter(|config| !has_disabled_tls_verification(config))
        .collect::<Vec<_>>();
    let security_rejected = input_candidate_count.saturating_sub(current_candidates.len());

    let before_consumer_compatibility = current_candidates.len();
    let current_candidates = current_candidates
        .into_iter()
        .filter(|config| is_light_consumer_compatible(config))
        .collect::<Vec<_>>();
    let consumer_rejected = before_consumer_compatibility.saturating_sub(current_candidates.len());

    if consumer_rejected > 0 {
        println!(
            "[INFO] 🧹 [Light compatibility] Rejected {} candidates by conservative consumer contract",
            consumer_rejected
        );
    }

    let history_path = "subscriptions/light-history.json";
    let history = load_history(history_path)?;
    let run_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    let mut candidate_ledger = load_candidate_ledger(LIGHT_CANDIDATE_LEDGER_PATH, run_timestamp);
    let active_fingerprints = read_lines(LIGHT_SUBSCRIPTION_PATH)
        .unwrap_or_default()
        .into_iter()
        .filter(|line| !line.starts_with('#'))
        .map(|config| history_fingerprint(&config))
        .collect::<HashSet<_>>();
    let repeat_candidate_fingerprints = active_revalidated_fingerprints(
        &candidate_ledger,
        &active_fingerprints,
        run_timestamp,
    );
    let light_cohorts = load_light_cohorts(LIGHT_SUBSCRIPTION_PATH)?;

    let mut seen_candidates = HashSet::new();
    let mut candidates = Vec::with_capacity(
        current_candidates.len() + light_cohorts.iter().map(Vec::len).sum::<usize>(),
    );
    let mut cohort_configs_loaded = 0usize;

    for cohort in &light_cohorts {
        for config in cohort {
            if !seen_candidates.insert(config.clone()) {
                continue;
            }
            if !has_disabled_tls_verification(config) && is_light_consumer_compatible(config) {
                candidates.push(config.clone());
                cohort_configs_loaded += 1;
            }
        }
    }

    for config in current_candidates {
        if seen_candidates.insert(config.clone()) {
            candidates.push(config);
        }
    }

    println!(
        "[INFO] 🛡️ [Light retention] Cohorts loaded: {} | Cohort configs: {}",
        light_cohorts.len(),
        cohort_configs_loaded
    );

    let light_gbm_scores =
        LightGbmScores::from_file("/tmp/proxyrift/lightgbm-scores.json").unwrap_or_default();

    let ranking_strategy = light_gbm_scores
        .model_report()
        .get("ranking_strategy")
        .and_then(Value::as_str)
        .unwrap_or("legacy_stage_model_fallback");
    println!(
        "[INFO] 🧠 [LightGBM] Loaded collection scores | Ranking strategy: {}",
        ranking_strategy
    );

    if candidates.is_empty() {
        return Err("no Light candidates available".to_string());
    }

    let mut global_verified = Vec::<String>::new();
    let mut global_positions = HashMap::<String, usize>::new();
    let mut global_metadata = HashMap::<String, ProxyMetrics>::new();
    let mut final_verified = Vec::<String>::new();
    let mut final_attempts = HashMap::<String, usize>::new();
    let mut final_metadata = HashMap::<String, ProxyMetrics>::new();
    let mut stability_verified = HashMap::<String, ProxyMetrics>::new();
    let mut stability_tested = HashSet::<String>::new();
    let mut transfer_verified = HashMap::<String, ProxyMetrics>::new();
    let mut transfer_tested = HashSet::<String>::new();
    let mut stream_verified = HashMap::<String, ProxyMetrics>::new();
    let mut stream_tested = HashSet::<String>::new();

    let discovery_seed = recheck_exploration_seed(0);
    let recent_families = light_cohorts
        .iter()
        .flatten()
        .map(|config| family_key(config))
        .collect::<HashSet<_>>();
    println!(
        "[INFO] 🧭 [Light discovery] {} candidates | {} family keys from {} subscription revisions | Fresh families first | {}% score-independent exploration",
        candidates.len(),
        recent_families.len(),
        light_cohorts.len(),
        RECHECK_EXPLORATION_PERCENT
    );
    let ranked_discovery_candidates = rank_discovery_candidates(
        &candidates,
        &light_gbm_scores,
        discovery_seed,
        &recent_families,
    );
    let (discovery_candidates, scheduled_repeat_count) = interleave_repeat_candidates(
        &ranked_discovery_candidates,
        &repeat_candidate_fingerprints,
    );
    println!(
        "[INFO] 🔁 [Light reuse] Candidate test order | New/expired candidates: {} | Recently validated active candidates: {} (max 20%, within 24h)",
        discovery_candidates.len().saturating_sub(scheduled_repeat_count),
        scheduled_repeat_count
    );
    let mut discovery_cursor = 0usize;
    let mut wave = 0usize;
    let mut stagnant_waves = 0usize;
    let mut strict_pool_ready_logged = false;
    let mut downstream_gate: Option<TransferGateTask> = None;
    let mut downstream_gate_candidate_count = 0usize;
    let mut discovery_batch_floor = DISCOVERY_BATCH_MIN;
    let mut last_funnel_snapshot = None;

    println!(
        "[INFO] 🔬 [Light] Validation started | {} Candidates | Targets: {} | ML/history ranked with {}% exploration",
        discovery_candidates.len(),
        early_targets.len(),
        RECHECK_EXPLORATION_PERCENT
    );

    loop {
        if let Some(task) = downstream_gate.take() {
            if task.is_finished() {
                let snapshot = task
                    .await
                    .map_err(|error| format!("Light transfer gate task failed: {error}"))??;
                apply_transfer_gate_snapshot(
                    snapshot,
                    &mut stability_verified,
                    &mut stability_tested,
                    &mut transfer_verified,
                    &mut transfer_tested,
                );
                mirror_sustained_stream_results(
                    &transfer_tested,
                    &transfer_verified,
                    &mut stream_tested,
                    &mut stream_verified,
                );
            } else {
                downstream_gate = Some(task);
            }
        }

        let mut transfer_ranked = transfer_verified.keys().cloned().collect::<Vec<_>>();
        sort_ranked(
            &mut transfer_ranked,
            &transfer_verified,
            &global_positions,
            &history,
        );
        let transfer_selected = select_verified_configs(
            &transfer_ranked,
            selection_limit,
            max_per_endpoint,
            max_per_family,
        )
        .len();

        let publishable_selected = select_verified_configs(
            &stream_verified.keys().cloned().collect::<Vec<_>>(),
            selection_limit,
            max_per_endpoint,
            max_per_family,
        )
        .len();

        if publishable_selected >= selection_limit || discovery_cursor >= discovery_candidates.len()
        {
            break;
        }

        let candidates_remaining = discovery_candidates.len().saturating_sub(discovery_cursor);
        let mut discovery_batch_size = adaptive_discovery_batch_size(
            selection_limit,
            publishable_selected,
            stability_tested.len(),
            stability_verified.len(),
            transfer_tested.len(),
            transfer_verified.len(),
            stream_tested.len(),
            stream_verified.len(),
            candidates_remaining,
        )
        .max(discovery_batch_floor)
        .min(DISCOVERY_BATCH_HARD_MAX)
        .min(candidates_remaining);
        if discovery_batch_size == 0 {
            break;
        }

        if stagnant_waves > 0 && publishable_selected < selection_limit {
            let boosted_batch = discovery_batch_size
                .max(DISCOVERY_STALL_BATCH_SIZE)
                .min(discovery_candidates.len().saturating_sub(discovery_cursor));
            if boosted_batch > discovery_batch_size {
                println!(
                    "[INFO] 🚀 [Light discovery] Stalled funnel | No downstream growth for {} wave(s) | Expanding next batch: {} -> {}",
                    stagnant_waves,
                    discovery_batch_size,
                    boosted_batch
                );
                discovery_batch_size = boosted_batch;
            }
        }

        wave += 1;
        let batch_start = discovery_cursor;
        let batch_end = batch_start + discovery_batch_size;
        let transfer_selected_before = transfer_selected;
        let publishable_selected_before = publishable_selected;
        let strict_selectable_before = select_verified_configs(
            &final_verified,
            final_verified.len(),
            max_per_endpoint,
            max_per_family,
        )
        .len();
        discovery_cursor = batch_end;
        let chunk = &discovery_candidates[batch_start..batch_end];

        println!(
            "[INFO] 🔎 [Light discovery] Wave {wave} | Testing {} candidates | Ordered cursor: {}/{} | Transfer qualified: {} | Publishable: {}",
            chunk.len(),
            discovery_cursor,
            discovery_candidates.len(),
            transfer_selected,
            publishable_selected
        );

        let target = early_targets[0];
        let target_metadata = validate_light_batch(
            &xray,
            &singbox,
            chunk,
            &[target],
            ValidationSettings {
                workers,
                batch_size,
                timeout_seconds: timeout,
                strict: false,
            },
        )
        .await?;
        let chunk_verified_count = target_metadata.len();

        for (config, metrics) in target_metadata {
            if !global_positions.contains_key(&config) {
                let position = global_verified.len();
                global_verified.push(config.clone());
                global_positions.insert(config.clone(), position);
            }
            global_metadata.insert(config, metrics);
        }

        sort_ranked(
            &mut global_verified,
            &global_metadata,
            &global_positions,
            &history,
        );

        let available_for_strict = discovery_candidates.len().saturating_sub(discovery_cursor);
        let remaining_strict_candidates = global_verified
            .iter()
            .filter(|config| {
                !final_metadata.contains_key(*config)
                    && final_attempts.get(*config).copied().unwrap_or(0)
                        < MAX_FINAL_RECHECK_ATTEMPTS
            })
            .count();
        let reachable_strict_candidates =
            available_for_strict.saturating_add(remaining_strict_candidates);
        let desired_strict = adaptive_strict_validation_target(AdaptiveStrictValidationInputs {
            selection_limit,
            strict_verified: final_verified.len(),
            strict_selectable: strict_selectable_before,
            transfer_tested: transfer_tested.len(),
            transfer_passed: transfer_verified.len(),
            stream_tested: stream_tested.len(),
            stream_passed: stream_verified.len(),
            reachable_candidates: reachable_strict_candidates,
        });
        let remaining = desired_strict.saturating_sub(final_verified.len());
        let adaptive_attempt_limit = adaptive_recheck_limit(
            remaining,
            final_recheck_limit,
            final_attempts.len(),
            final_metadata.len(),
        );
        let strict_batch_limit = strict_validation_batch_limit(
            desired_strict,
            final_verified.len(),
            adaptive_attempt_limit,
        );

        let untested = global_verified
            .iter()
            .filter(|config| {
                !final_metadata.contains_key(*config)
                    && final_attempts.get(*config).copied().unwrap_or(0)
                        < MAX_FINAL_RECHECK_ATTEMPTS
            })
            .cloned()
            .collect::<Vec<_>>();

        let ai_ranked =
            rank_strict_recheck_candidates(&untested, &light_gbm_scores, &global_positions);

        let exploration_limit = recheck_exploration_limit(strict_batch_limit);
        let (final_candidates, exploration_selected) = select_recheck_candidates(
            &ai_ranked,
            &untested,
            strict_batch_limit,
            RECHECK_FAMILY_DIVERSITY,
            exploration_limit,
            recheck_exploration_seed(wave),
        );

        if !final_candidates.is_empty() {
            for config in &final_candidates {
                *final_attempts.entry(config.clone()).or_default() += 1;
            }

            println!(
                "[INFO] 🔎 [Light recheck] Wave {wave} | Testing {} candidates | Exploration: {}",
                final_candidates.len(),
                exploration_selected
            );

            let primary_metadata = validate_light_batch(
                &xray,
                &singbox,
                &final_candidates,
                &consumer_targets,
                ValidationSettings {
                    workers: final_workers,
                    batch_size: final_batch_size,
                    timeout_seconds: timeout,
                    strict: true,
                },
            )
            .await?;

            for (config, mut metrics) in primary_metadata {
                if metrics.throughput_kbps <= 0.0 {
                    if let Some(early) = global_metadata.get(&config) {
                        metrics.throughput_kbps = early.throughput_kbps;
                    }
                }
                if !final_metadata.contains_key(&config) {
                    final_verified.push(config.clone());
                }
                final_metadata.insert(config, metrics);
            }
        }

        sort_ranked(
            &mut final_verified,
            &final_metadata,
            &global_positions,
            &history,
        );
        let strict_selected = select_verified_configs(
            &final_verified,
            selection_limit,
            max_per_endpoint,
            max_per_family,
        );
        let strict_eligible = select_verified_configs(
            &final_verified,
            final_verified.len(),
            max_per_endpoint,
            max_per_family,
        )
        .len();

        println!(
            "[INFO] 🧭 [Light discovery] Wave {wave} complete | Prefilter verified: {} | Global verified: {} | Strict verified: {} | Publish-selectable strict: {}",
            chunk_verified_count,
            global_verified.len(),
            final_metadata.len(),
            strict_selected.len()
        );

        if strict_selected.len() >= selection_limit {
            if !strict_pool_ready_logged {
                println!(
                    "[INFO] 🚀 [Light] Strict pool reached {} | Starting downstream validation",
                    selection_limit
                );
                strict_pool_ready_logged = true;
            }

            if downstream_gate.is_none() && final_verified.len() > downstream_gate_candidate_count {
                downstream_gate = Some(spawn_transfer_gate(TransferGateRequest {
                    xray: xray.clone(),
                    singbox: singbox.clone(),
                    final_verified: final_verified.clone(),
                    final_metadata: final_metadata.clone(),
                    stability_verified: stability_verified.clone(),
                    stability_tested: stability_tested.clone(),
                    transfer_verified: transfer_verified.clone(),
                    transfer_tested: transfer_tested.clone(),
                    global_positions: global_positions.clone(),
                    history: history.clone(),
                    selection_limit,
                    max_per_endpoint,
                    max_per_family,
                }));
                downstream_gate_candidate_count = final_verified.len();
            }
            mirror_sustained_stream_results(
                &transfer_tested,
                &transfer_verified,
                &mut stream_tested,
                &mut stream_verified,
            );

            let mut transfer_ranked = transfer_verified.keys().cloned().collect::<Vec<_>>();
            sort_ranked(
                &mut transfer_ranked,
                &transfer_verified,
                &global_positions,
                &history,
            );
            let transfer_selected = select_verified_configs(
                &transfer_ranked,
                selection_limit,
                max_per_endpoint,
                max_per_family,
            )
            .len();

            let publishable_selected = select_verified_configs(
                &stream_verified.keys().cloned().collect::<Vec<_>>(),
                selection_limit,
                max_per_endpoint,
                max_per_family,
            )
            .len();

            let funnel_snapshot = (
                final_metadata.len(),
                stability_verified.len(),
                stability_tested.len(),
                transfer_verified.len(),
                transfer_tested.len(),
                stream_verified.len(),
                stream_tested.len(),
                publishable_selected,
            );
            if last_funnel_snapshot != Some(funnel_snapshot) {
                println!(
                    "[INFO] 📈 [Light adaptive funnel] Strict: {} | 1 MiB: {}/{} | 9 MiB sustained: {}/{} | Sub-light-ready: {}/{}",
                    final_metadata.len(),
                    stability_verified.len(),
                    stability_tested.len(),
                    transfer_verified.len(),
                    transfer_tested.len(),
                    publishable_selected,
                    selection_limit
                );
                last_funnel_snapshot = Some(funnel_snapshot);
            }

            if publishable_selected >= selection_limit {
                break;
            }

            let estimated_next_batch = adaptive_discovery_batch_size(
                selection_limit,
                select_verified_configs(
                    &stream_verified.keys().cloned().collect::<Vec<_>>(),
                    selection_limit,
                    max_per_endpoint,
                    max_per_family,
                )
                .len(),
                stability_tested.len(),
                stability_verified.len(),
                transfer_tested.len(),
                transfer_verified.len(),
                stream_tested.len(),
                stream_verified.len(),
                discovery_candidates.len().saturating_sub(discovery_cursor),
            );
            let newly_selectable = strict_eligible.saturating_sub(strict_selectable_before);
            let candidates_remaining = discovery_candidates.len().saturating_sub(discovery_cursor);
            let next_batch = adjust_discovery_batch_for_yield(
                estimated_next_batch,
                discovery_batch_size,
                newly_selectable,
                chunk.len(),
                candidates_remaining,
            );
            if next_batch > estimated_next_batch {
                println!(
                    "[INFO] 🚀 [Light discovery] Low selectable yield | New strict-selectable: {}/{} | Expanding next batch: {} -> {}",
                    newly_selectable,
                    chunk.len(),
                    estimated_next_batch,
                    next_batch
                );
            }
            println!(
                "[INFO] 🔁 [Light adaptive funnel] Downstream yield measured | Next discovery batch: {} | Candidates remaining: {}",
                next_batch,
                candidates_remaining
            );
            discovery_batch_floor = next_batch;

            let progressed = discovery_downstream_progressed(
                transfer_selected_before,
                transfer_selected,
                publishable_selected_before,
                publishable_selected,
            );

            if strict_selected.len() >= selection_limit {
                if downstream_gate.is_some() || progressed {
                    stagnant_waves = 0;
                } else {
                    stagnant_waves = stagnant_waves.saturating_add(1);
                }

                if stagnant_waves >= DISCOVERY_STAGNATION_WAVES {
                    println!(
                        "[INFO] 🛑 [Light discovery] Funnel stalled | No transfer/stream output growth for {} consecutive waves | Strict selectable: {} | Transfer selectable: {} | Stream passed: {} | Remaining candidates: {} | Stopping best-effort discovery",
                        stagnant_waves,
                        strict_selected.len(),
                        transfer_selected,
                        stream_verified.len(),
                        discovery_candidates.len().saturating_sub(discovery_cursor)
                    );
                    break;
                }
            } else {
                stagnant_waves = 0;
            }
        } else {
            let newly_selectable = strict_eligible.saturating_sub(strict_selectable_before);
            let candidates_remaining = discovery_candidates.len().saturating_sub(discovery_cursor);
            let next_floor = adjust_discovery_batch_for_yield(
                DISCOVERY_BATCH_MIN,
                discovery_batch_size,
                newly_selectable,
                chunk.len(),
                candidates_remaining,
            );
            if next_floor > discovery_batch_size {
                println!(
                    "[INFO] 🚀 [Light discovery] Low selectable yield | New strict-selectable: {}/{} | Expanding next batch: {} -> {}",
                    newly_selectable,
                    chunk.len(),
                    discovery_batch_size,
                    next_floor
                );
            }
            discovery_batch_floor = next_floor;
        }
    }

    sort_ranked(
        &mut final_verified,
        &final_metadata,
        &global_positions,
        &history,
    );
    if let Some(task) = downstream_gate.take() {
        let snapshot = task
            .await
            .map_err(|error| format!("Light transfer gate task failed: {error}"))??;
        apply_transfer_gate_snapshot(
            snapshot,
            &mut stability_verified,
            &mut stability_tested,
            &mut transfer_verified,
            &mut transfer_tested,
        );
        mirror_sustained_stream_results(
            &transfer_tested,
            &transfer_verified,
            &mut stream_tested,
            &mut stream_verified,
        );
    }

    let transfer_selected = fill_transfer_gate(
        &xray,
        &singbox,
        &final_verified,
        &final_metadata,
        &mut stability_verified,
        &mut stability_tested,
        &mut transfer_verified,
        &mut transfer_tested,
        &global_positions,
        &history,
        selection_limit,
        max_per_endpoint,
        max_per_family,
    )
    .await?;

    let stream_selected = mirror_sustained_stream_results(
        &transfer_tested,
        &transfer_verified,
        &mut stream_tested,
        &mut stream_verified,
    );

    let mut stream_ranked = stream_verified.keys().cloned().collect::<Vec<_>>();
    sort_ranked(
        &mut stream_ranked,
        &stream_verified,
        &global_positions,
        &history,
    );
    let publication_limit = final_publication_limit(selection_limit);
    let (selected, selected_new_count) = select_verified_configs_with_repeat_cap(
        &stream_ranked,
        &repeat_candidate_fingerprints,
        publication_limit,
        max_per_endpoint,
        max_per_family,
    );
    let fresh_family_selected = selected
        .iter()
        .filter(|config| !recent_families.contains(&family_key(config)))
        .count();
    let selected_revalidated_count = selected.len().saturating_sub(selected_new_count);
    println!(
        "[INFO] 🧭 [Light reuse] Final selection | New/expired: {} | Fully validated in previous 24h and still active: {} (max {}%) | Fresh families: {}",
        selected_new_count,
        selected_revalidated_count,
        REVALIDATED_CANDIDATE_PERCENT,
        fresh_family_selected
    );
    let (_, endpoint_rejected, family_rejected) = selection_rejection_counts(
        &stream_ranked,
        publication_limit,
        max_per_endpoint,
        max_per_family,
    );

    if selected.is_empty() {
        println!(
            "[WARN] ⚠️ [Light] No configs passed 10 MiB sustained validation | Output 0 | Workflow will preserve previous subscription | Transfer qualified: {} | Stream tested: {} | Stream passes: {}",
            transfer_selected,
            stream_tested.len(),
            stream_selected
        );
    } else if selected.len() < selection_limit {
        println!(
            "[WARN] ⚠️ [Light] Stream target not reached | Publishing {} configs that passed 10 MiB validation | Target/max: {} | Transfer target/max: {} | Stream tested: {} | Stream passes: {} | Endpoint cap exclusions: {} | Family cap exclusions: {}",
            selected.len(),
            selection_limit,
            transfer_selected,
            stream_tested.len(),
            stream_selected,
            endpoint_rejected,
            family_rejected
        );
    }

    let strict_selectable = stage_selectable_count(
        &final_metadata,
        &global_positions,
        &history,
        selection_limit,
        max_per_endpoint,
        max_per_family,
    );
    let stability_selectable = stage_selectable_count(
        &stability_verified,
        &global_positions,
        &history,
        selection_limit,
        max_per_endpoint,
        max_per_family,
    );
    let transfer_selectable = stage_selectable_count(
        &transfer_verified,
        &global_positions,
        &history,
        selection_limit,
        max_per_endpoint,
        max_per_family,
    );
    let stream_selectable = stage_selectable_count(
        &stream_verified,
        &global_positions,
        &history,
        publication_limit,
        max_per_endpoint,
        max_per_family,
    );
    println!(
        "[INFO] 📊 [Light feasibility] Target: {} | Strict selectable: {} | 1 MiB selectable: {} | 10 MiB selectable: {} | 9 MiB sustained selectable: {} | Selected: {} | Shortfall: {}",
        selection_limit,
        strict_selectable,
        stability_selectable,
        transfer_selectable,
        stream_selectable,
        selected.len(),
        selection_limit.saturating_sub(selected.len())
    );

    write_light_stats(
        &stats_path,
        input_candidate_count,
        security_rejected,
        consumer_rejected,
        final_metadata.len(),
        final_attempts.values().copied().sum::<usize>(),
        stability_tested.len(),
        stability_verified.len(),
        transfer_tested.len(),
        transfer_verified.len(),
        stream_tested.len(),
        stream_selected,
        selection_limit,
        strict_selectable,
        stability_selectable,
        transfer_selectable,
        stream_selectable,
        selected.len(),
        light_cohorts.len(),
        recent_families.len(),
        FRESH_FAMILY_TARGET_PERCENT,
        RECHECK_EXPLORATION_PERCENT,
        fresh_family_selected,
    )?;

    let validation_report_path = write_light_validation_report(
        &selected,
        &stability_verified,
        &transfer_verified,
        &light_gbm_scores,
    )?;
    println!(
        "[INFO] 📋 [Light] Per-config validation report written | Configs: {} | Path: {}",
        selected.len(),
        validation_report_path
    );

    let mut protocol_counts = BTreeMap::<String, usize>::new();
    let mut backend_counts = BTreeMap::<&str, usize>::new();
    for config in &selected {
        let scheme = config
            .split_once("://")
            .map(|(scheme, _)| scheme.to_ascii_lowercase())
            .unwrap_or_else(|| "unknown".to_string());
        *protocol_counts.entry(scheme).or_default() += 1;
        match light_backend(config) {
            LightBackend::SingBox => *backend_counts.entry("sing-box").or_default() += 1,
            LightBackend::Xray => *backend_counts.entry("xray").or_default() += 1,
            LightBackend::Fallback => *backend_counts.entry("fallback").or_default() += 1,
        }
    }
    for chunk in protocol_counts
        .iter()
        .map(|(scheme, count)| format!("{scheme} {count}"))
        .collect::<Vec<_>>()
        .chunks(4)
    {
        println!("[INFO] 📊 [Light protocols] {}", chunk.join(" | "));
    }

    let backend_summary = backend_counts
        .iter()
        .map(|(backend, count)| format!("{backend} {count}"))
        .collect::<Vec<_>>()
        .join(" | ");
    println!("[INFO] 📊 [Light backends] {backend_summary}");

    println!(
        "[INFO] 🎯 [Light selection] {} Configs ready | No protocol quota",
        selected.len()
    );

    let mut full_path_tested = transfer_tested.clone();
    full_path_tested.extend(
        stability_tested
            .iter()
            .filter(|config| !stability_verified.contains_key(*config))
            .cloned(),
    );

    persist_light_result(
        &output,
        &selected,
        history_path,
        &history,
        &final_attempts,
        &final_metadata,
        &global_metadata,
        &full_path_tested,
        &transfer_verified,
        &transfer_tested,
        &transfer_verified,
    )?;
    let mut fully_validated = HashSet::new();
    for config in stream_verified.keys() {
        if final_metadata.contains_key(config) && stability_verified.contains_key(config) {
            fully_validated.insert(config.clone());
        }
    }
    persist_candidate_ledger(
        LIGHT_CANDIDATE_LEDGER_PATH,
        &mut candidate_ledger,
        &fully_validated,
        run_timestamp,
    )?;
    println!(
        "[INFO] ✅ [Light] Published {} configs | Discovery: {} | Strict attempts: {} | Strictly verified: {} | Transfer tested: {} | Transfer passes: {} | Reuse ledger: {} recent fully validated identities",
        selected.len(),
        candidates.len(),
        final_attempts.values().copied().sum::<usize>(),
        final_metadata.len(),
        transfer_tested.len(),
        transfer_verified.len(),
        candidate_ledger.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        adaptive_discovery_batch_size, adaptive_recheck_limit, adaptive_stability_pool_target,
        adaptive_stability_target, adaptive_strict_validation_target, adaptive_transfer_test_limit,
        adjust_discovery_batch_for_yield, adjust_transfer_workers, discovery_downstream_progressed,
        final_publication_limit, has_disabled_tls_verification, history_fingerprint, light_backend,
        light_training_features, mirror_sustained_stream_results, normalize_light_config,
        observation_fingerprint, rank_discovery_candidates, recheck_exploration_limit,
        select_recheck_candidates, select_stability_test_batch, select_transfer_target,
        select_verified_configs, select_verified_configs_with_freshness_quota,
        active_revalidated_fingerprints, interleave_repeat_candidates,
        select_verified_configs_with_repeat_cap, persist_candidate_ledger, load_candidate_ledger,
        selection_additional_potential_count, selection_eligible_count, selection_potential_count,
        selection_rejection_counts, should_quarantine_transfer_target, singbox_qualified_metadata,
        stream_selection_count, strict_validation_batch_limit, strict_validation_target,
        transfer_target_batch_limit, transfer_validation_target, update_transfer_target_state,
        AdaptiveStrictValidationInputs, LightBackend, LightGbmScores, ProxyMetrics,
        TransferConcurrencyState, TransferTargetState, FINAL_TRANSFER_INITIAL_WORKERS,
        FINAL_TRANSFER_WORKERS, LIGHT_TRANSFER_STABILITY_BYTES, MAX_DISCOVERY_CANDIDATES,
        STREAM_CONTINUITY_SEGMENTS, STREAM_CONTINUITY_SEGMENT_BYTES,
    };
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn recently_revalidated_candidates_are_limited_to_twenty_percent_of_test_order() {
        let candidates = (0..20)
            .map(|index| format!("vless://uuid@node{index}.example:443?security=tls"))
            .collect::<Vec<_>>();
        let repeats = candidates.iter().take(6)
            .map(|config| history_fingerprint(config))
            .collect::<HashSet<_>>();
        let (ordered, repeats_scheduled) = interleave_repeat_candidates(&candidates, &repeats);
        assert_eq!(ordered.len(), 20);
        assert_eq!(repeats_scheduled, 4);
        assert_eq!(
            ordered.iter().filter(|config| repeats.contains(&history_fingerprint(config))).count(),
            4
        );
        for prefix_len in 1..=ordered.len() {
            let repeated = ordered[..prefix_len].iter()
                .filter(|config| repeats.contains(&history_fingerprint(config))).count();
            assert!(repeated <= prefix_len / 5, "repeat quota exceeded in first {prefix_len} tests");
        }
    }

    #[test]
    fn recent_reuse_requires_current_subscription_and_age_strictly_under_24_hours() {
        let now = 1_800_000_u64;
        let ledger = HashMap::from([
            ("active-recent".to_string(), now - 120),
            ("active-exactly-24h".to_string(), now - 86_400),
            ("not-active".to_string(), now - 120),
            ("future".to_string(), now + 1),
        ]);
        let active = HashSet::from([
            "active-recent".to_string(),
            "active-exactly-24h".to_string(),
            "future".to_string(),
        ]);
        assert_eq!(
            active_revalidated_fingerprints(&ledger, &active, now),
            HashSet::from(["active-recent".to_string()])
        );
    }

    #[test]
    fn publication_does_not_exceed_twenty_percent_revalidated_items() {
        let configs = (0..10)
            .map(|index| format!("vless://uuid@host{index}.example:443?security=tls"))
            .collect::<Vec<_>>();
        let repeats = configs.iter().take(5)
            .map(|config| history_fingerprint(config))
            .collect::<HashSet<_>>();
        let (selected, fresh_count) =
            select_verified_configs_with_repeat_cap(&configs, &repeats, 10, 1, 3);
        assert_eq!(selected.len(), 10);
        assert_eq!(fresh_count, 5);
        let revalidated = selected.iter()
            .filter(|config| repeats.contains(&history_fingerprint(config))).count();
        assert!(revalidated <= selected.len() / 5);
    }

    #[test]
    fn candidate_ledger_writes_only_recent_hashed_fully_validated_identities() {
        let path = format!("{}/proxyrift-ledger-{}.json", std::env::temp_dir().display(), std::process::id());
        let mut ledger = HashMap::from([("expired".to_string(), 1_u64)]);
        let now = 200_000_u64;
        let validated = HashSet::from([
            "vless://uuid@valid.example:443?security=tls".to_string(),
        ]);
        persist_candidate_ledger(&path, &mut ledger, &validated, now).expect("persist candidate ledger");
        let body = std::fs::read_to_string(&path).expect("read candidate ledger");
        let json: serde_json::Value = serde_json::from_str(&body).expect("parse ledger");
        assert_eq!(json["retention_seconds"].as_u64(), Some(86_400));
        assert_eq!(json["entries"].as_array().unwrap().len(), 1);
        assert_eq!(json["entries"][0]["validated_at"].as_u64(), Some(now));
        assert_eq!(json["entries"][0]["config_id"].as_str().unwrap().len(), 32);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn staged_light_transfer_requires_ten_mib_before_publication() {
        const MIB: usize = 1_048_576;
        assert_eq!(LIGHT_TRANSFER_STABILITY_BYTES, MIB);
        assert_eq!(STREAM_CONTINUITY_SEGMENTS, 9);
        assert_eq!(STREAM_CONTINUITY_SEGMENT_BYTES, MIB);
        assert_eq!(
            LIGHT_TRANSFER_STABILITY_BYTES
                + STREAM_CONTINUITY_SEGMENTS * STREAM_CONTINUITY_SEGMENT_BYTES,
            10 * MIB
        );
    }

    #[test]
    fn publishable_stream_results_match_the_sustained_transfer_gate() {
        let passing = "vless://ok@example.net:443".to_string();
        let failed = "vless://failed@example.org:443".to_string();
        let metrics = ProxyMetrics {
            successes: 1,
            attempts: 1,
            median_ms: 10.0,
            min_ms: 10.0,
            jitter_ms: 0.0,
            throughput_kbps: 1000.0,
            successful_targets: Vec::new(),
        };
        let transfer_tested = HashSet::from([passing.clone(), failed.clone()]);
        let transfer_verified = HashMap::from([(passing.clone(), metrics)]);
        let mut stream_tested = HashSet::new();
        let mut stream_verified = HashMap::new();

        let passed = mirror_sustained_stream_results(
            &transfer_tested,
            &transfer_verified,
            &mut stream_tested,
            &mut stream_verified,
        );

        assert_eq!(passed, 1);
        assert_eq!(stream_tested, transfer_tested);
        assert_eq!(
            stream_verified.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([passing])
        );
        assert!(!stream_verified.contains_key(&failed));
    }

    #[test]
    fn discovery_batch_expands_when_selectable_yield_stalls() {
        assert_eq!(
            adjust_discovery_batch_for_yield(300, 250, 0, 250, 8000),
            500
        );
        assert_eq!(
            adjust_discovery_batch_for_yield(300, 500, 0, 500, 8000),
            600
        );
        assert_eq!(
            adjust_discovery_batch_for_yield(300, 250, 3, 250, 8000),
            300
        );
        assert_eq!(adjust_discovery_batch_for_yield(300, 600, 0, 600, 120), 120);
        assert_eq!(adjust_discovery_batch_for_yield(300, 250, 0, 250, 0), 0);
    }

    #[test]
    fn fresh_families_reach_the_target_before_recent_families() {
        let configs = (0..10)
            .map(|index| format!("vless://id{index}@node{index}.example.com:443"))
            .collect::<Vec<_>>();
        let recent_families = configs[..3]
            .iter()
            .map(|config| super::family_key(config))
            .collect::<HashSet<_>>();

        let (selected, fresh_selected) =
            select_verified_configs_with_freshness_quota(&configs, &recent_families, 8, 1, 3);

        assert_eq!(selected.len(), 8);
        assert_eq!(fresh_selected, 6);
    }

    #[test]
    fn max_light_candidates_matches_update_workflow_limit() {
        assert_eq!(MAX_DISCOVERY_CANDIDATES, 15_000);
    }

    #[test]
    fn final_publication_respects_target_when_stream_batch_overshoots() {
        let configs = (0..143)
            .map(|index| format!("vless://id{index}@node{index}.example.com:443"))
            .collect::<Vec<_>>();
        let recent_families = HashSet::new();
        let publication_limit = final_publication_limit(130);

        assert_eq!(publication_limit, 130);
        let (selected, fresh_selected) = select_verified_configs_with_freshness_quota(
            &configs,
            &recent_families,
            publication_limit,
            1,
            3,
        );
        assert_eq!(selected.len(), 130);
        assert_eq!(fresh_selected, 130);
    }

    #[test]
    fn stream_pool_early_exit_uses_final_diversity_selection() {
        let configs = [
            "vless://a@example.com:443".to_string(),
            "vless://b@example.net:443".to_string(),
        ];
        let metrics = ProxyMetrics {
            successes: 3,
            attempts: 3,
            median_ms: 20.0,
            min_ms: 10.0,
            jitter_ms: 1.0,
            throughput_kbps: 100.0,
            successful_targets: Vec::new(),
        };
        let stream_verified = configs
            .iter()
            .cloned()
            .map(|config| (config, metrics.clone()))
            .collect::<HashMap<_, _>>();
        let recent_families = HashSet::new();
        let positions = HashMap::new();
        let history = HashMap::new();

        assert_eq!(
            stream_selection_count(
                &stream_verified,
                &recent_families,
                &positions,
                &history,
                2,
                1,
                3,
            ),
            2
        );
        assert_eq!(
            stream_selection_count(
                &stream_verified,
                &recent_families,
                &positions,
                &history,
                3,
                1,
                3,
            ),
            2
        );
    }

    #[test]
    fn transfer_target_selection_fails_over_across_four_external_hosts() {
        let mut states = vec![
            TransferTargetState::default();
            proxyrift::validator::STRICT_THROUGHPUT_TARGETS.len()
        ];
        assert_eq!(states.len(), 4);
        assert_eq!(select_transfer_target(&states, 0), Some(0));
        states[0].quarantined = true;
        assert_eq!(select_transfer_target(&states, 0), Some(1));
        states[1].quarantined = true;
        assert_eq!(select_transfer_target(&states, 0), Some(2));
        states[2].quarantined = true;
        assert_eq!(select_transfer_target(&states, 0), Some(3));
        states[3].quarantined = true;
        assert_eq!(select_transfer_target(&states, 0), None);

        let targets = proxyrift::validator::STRICT_THROUGHPUT_TARGETS;
        assert!(targets.contains(&proxyrift::validator::STRICT_THROUGHPUT_TARGET));
        assert!(targets.contains(&proxyrift::validator::HETZNER_SPEEDTEST_100MB_TARGET));
        assert!(targets.contains(&proxyrift::validator::TRUEFILESIZE_SPEEDTEST_100MB_TARGET));
        assert!(targets.contains(&proxyrift::validator::TELE2_SPEEDTEST_100MB_TARGET));
    }

    #[test]
    fn rate_limit_worker_adjustment_is_isolated_per_host() {
        let mut hosts = [
            TransferConcurrencyState::default(),
            TransferConcurrencyState::default(),
        ];

        let (previous_workers, _) = hosts[0].observe(2, 8);
        assert_eq!(previous_workers, FINAL_TRANSFER_INITIAL_WORKERS);
        assert_eq!(hosts[0].workers, FINAL_TRANSFER_INITIAL_WORKERS - 1);
        assert_eq!(hosts[1].workers, FINAL_TRANSFER_INITIAL_WORKERS);
    }

    #[test]
    fn strict_validation_target_keeps_a_bounded_recheck_reserve() {
        assert_eq!(strict_validation_target(0), 0);
        assert_eq!(strict_validation_target(1), 2);
        assert_eq!(strict_validation_target(200), 240);
        assert_eq!(strict_validation_target(1000), 1064);
    }

    #[test]
    fn transfer_validation_target_adds_small_stream_reserve() {
        assert_eq!(transfer_validation_target(0), 0);
        assert_eq!(transfer_validation_target(1), 2);
        assert_eq!(transfer_validation_target(200), 210);
    }

    #[test]
    fn stability_test_batch_can_recheck_endpoint_variants() {
        let configs = vec![
            "vless://00000000-0000-0000-0000-000000000001@a.example:443?path=%2Fa".to_string(),
            "vless://00000000-0000-0000-0000-000000000002@a.example:443?path=%2Fb".to_string(),
            "vless://00000000-0000-0000-0000-000000000003@b.example:443?path=%2Fc".to_string(),
        ];

        let batch = select_stability_test_batch(&configs, &[], 3, 3, 2, 6);

        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn selection_rejection_counts_explain_endpoint_and_family_caps() {
        let configs = vec![
            "vless://00000000-0000-0000-0000-000000000001@a.example:443".to_string(),
            "vless://00000000-0000-0000-0000-000000000002@a.example:443".to_string(),
            "vless://00000000-0000-0000-0000-000000000003@b.example:443".to_string(),
            "vless://00000000-0000-0000-0000-000000000004@c.example:443".to_string(),
        ];

        let (selected, endpoint_rejected, family_rejected) =
            selection_rejection_counts(&configs, 4, 1, 4);

        assert_eq!(selected, 3);
        assert_eq!(endpoint_rejected, 1);
        assert_eq!(family_rejected, 0);
    }

    #[test]
    fn selection_potential_excludes_duplicate_endpoint() {
        let selected =
            vec!["vless://00000000-0000-0000-0000-000000000001@a.example:443".to_string()];
        let duplicate_endpoint =
            "vless://00000000-0000-0000-0000-000000000002@a.example:443".to_string();
        let new_endpoint = "vless://00000000-0000-0000-0000-000000000003@b.example:443".to_string();

        assert_eq!(
            selection_additional_potential_count(&selected, &[duplicate_endpoint], 200, 1, 3),
            0
        );
        assert_eq!(
            selection_additional_potential_count(&selected, &[new_endpoint], 200, 1, 3),
            1
        );
    }

    #[test]
    fn rejects_explicit_tls_verification_bypass() {
        assert!(has_disabled_tls_verification(
            "trojan://password@example.com:443?security=tls&allowInsecure=1"
        ));
        assert!(has_disabled_tls_verification(
            "hysteria2://password@example.com:443?insecure=true"
        ));
        assert!(!has_disabled_tls_verification(
            "vless://uuid@example.com:443?security=tls&allowInsecure=0"
        ));
        assert!(!has_disabled_tls_verification(
            "vless://uuid@example.com:443?security=none"
        ));
    }

    #[test]
    fn rejects_vmess_tls_verification_bypass() {
        let payload = serde_json::json!({
            "add": "example.com",
            "port": 443,
            "id": "00000000-0000-0000-0000-000000000001",
            "tls": "tls",
            "allowInsecure": true
        });
        let encoded = STANDARD.encode(payload.to_string());
        assert!(has_disabled_tls_verification(&format!("vmess://{encoded}")));
    }

    #[test]
    fn allows_vmess_tls_with_certificate_verification() {
        let payload = serde_json::json!({
            "add": "example.com",
            "port": 443,
            "id": "00000000-0000-0000-0000-000000000001",
            "tls": "tls",
            "allowInsecure": false
        });
        let encoded = STANDARD.encode(payload.to_string());
        assert!(!has_disabled_tls_verification(&format!(
            "vmess://{encoded}"
        )));
    }

    #[test]
    fn adaptive_strict_target_uses_bounded_warmup_reserve() {
        assert_eq!(
            adaptive_strict_validation_target(AdaptiveStrictValidationInputs {
                selection_limit: 200,
                strict_verified: 0,
                strict_selectable: 0,
                transfer_tested: 0,
                transfer_passed: 0,
                stream_tested: 0,
                stream_passed: 0,
                reachable_candidates: 1000,
            }),
            240
        );
        assert_eq!(
            adaptive_strict_validation_target(AdaptiveStrictValidationInputs {
                selection_limit: 200,
                strict_verified: 200,
                strict_selectable: 200,
                transfer_tested: 15,
                transfer_passed: 12,
                stream_tested: 0,
                stream_passed: 0,
                reachable_candidates: 1000,
            }),
            240
        );
    }

    #[test]
    fn adaptive_strict_reserve_grows_in_bounded_steps_when_yield_is_low() {
        let target = adaptive_strict_validation_target(AdaptiveStrictValidationInputs {
            selection_limit: 200,
            strict_verified: 240,
            strict_selectable: 240,
            transfer_tested: 32,
            transfer_passed: 8,
            stream_tested: 16,
            stream_passed: 4,
            reachable_candidates: 1000,
        });
        assert_eq!(target, 304);
    }

    #[test]
    fn adaptive_strict_reserve_expands_for_non_selectable_strict_passes() {
        assert_eq!(
            adaptive_strict_validation_target(AdaptiveStrictValidationInputs {
                selection_limit: 200,
                strict_verified: 240,
                strict_selectable: 177,
                transfer_tested: 0,
                transfer_passed: 0,
                stream_tested: 0,
                stream_passed: 0,
                reachable_candidates: 1000,
            }),
            326
        );
    }

    #[test]
    fn adaptive_strict_reserve_counts_already_verified_candidates_as_reachable() {
        assert_eq!(
            adaptive_strict_validation_target(AdaptiveStrictValidationInputs {
                selection_limit: 200,
                strict_verified: 240,
                strict_selectable: 177,
                transfer_tested: 0,
                transfer_passed: 0,
                stream_tested: 0,
                stream_passed: 0,
                reachable_candidates: 86,
            }),
            326
        );
    }

    #[test]
    fn strict_validation_batch_never_exceeds_remaining_success_budget() {
        assert_eq!(strict_validation_batch_limit(240, 200, 350), 40);
        assert_eq!(strict_validation_batch_limit(240, 240, 350), 0);
        assert_eq!(strict_validation_batch_limit(304, 240, 350), 64);
    }

    #[test]
    fn adaptive_recheck_expands_when_yield_is_low() {
        assert_eq!(adaptive_recheck_limit(100, 500, 0, 0), 500);
        assert_eq!(adaptive_recheck_limit(100, 500, 100, 50), 250);
        assert_eq!(adaptive_recheck_limit(100, 500, 100, 20), 500);
    }

    #[test]
    fn recheck_exploration_is_capped_and_scales_with_budget() {
        assert_eq!(recheck_exploration_limit(350), 53);
        assert_eq!(recheck_exploration_limit(1000), 64);
        assert_eq!(recheck_exploration_limit(2), 1);
    }

    #[test]
    fn recheck_selection_allows_two_variants_per_endpoint() {
        let configs = vec![
            "vless://00000000-0000-0000-0000-000000000001@example.com:443".to_string(),
            "vless://00000000-0000-0000-0000-000000000002@example.com:443".to_string(),
        ];

        let (selected, explored) = select_recheck_candidates(&configs, &[], 2, 3, 0, 42);

        assert_eq!(explored, 0);
        assert_eq!(selected, configs);
    }

    #[test]
    fn recheck_selection_keeps_a_controlled_exploration_slice() {
        let configs = vec![
            "vless://a@example.com:443".to_string(),
            "vless://b@example.net:443".to_string(),
            "trojan://c@example.org:8443".to_string(),
            "vmess://d@example.dev:9443".to_string(),
            "hysteria2://e@example.io:443".to_string(),
            "socks5://f@example.xyz:1080".to_string(),
        ];

        let (selected, explored) = select_recheck_candidates(&configs, &configs, 4, 3, 2, 42);

        assert_eq!(selected.len(), 4);
        assert_eq!(explored, 2);
    }

    #[test]
    fn stability_batch_preserves_global_selection_capacity() {
        let stable_selected = vec![
            "vless://a@example.com:443".to_string(),
            "trojan://b@example.net:8443".to_string(),
        ];
        let ranked = vec![
            "vless://duplicate@example.com:443".to_string(),
            "vless://c@example.org:443".to_string(),
            "vless://d@example.dev:443".to_string(),
            "vless://e@example.io:443".to_string(),
        ];

        let batch = select_stability_test_batch(&ranked, &stable_selected, 3, 6, 1, 3);

        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0], ranked[1]);
        assert_eq!(batch[1], ranked[2]);
        assert_eq!(batch[2], ranked[3]);
    }

    #[test]
    fn stability_batch_can_move_to_a_new_endpoint_after_a_failed_one() {
        let stable_selected = vec!["vless://good@example.com:443".to_string()];
        let ranked = vec![
            "vless://failed1@example.net:443".to_string(),
            "vless://failed2@example.net:443".to_string(),
            "vless://new@example.org:443".to_string(),
        ];

        let batch = select_stability_test_batch(&ranked, &stable_selected, 2, 4, 1, 3);

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0], ranked[0]);
        assert_eq!(batch[1], ranked[2]);
    }

    #[test]
    fn rate_limits_back_transfer_workers_to_two() {
        assert_eq!(adjust_transfer_workers(4, 4, 4, 0), (2, 0));
        assert_eq!(adjust_transfer_workers(8, 2, 8, 0), (7, 0));
        assert_eq!(adjust_transfer_workers(2, 1, 8, 0), (2, 1));
    }

    #[test]
    fn clean_transfer_batches_ramp_workers_slowly() {
        assert_eq!(adjust_transfer_workers(2, 0, 8, 0), (2, 1));
        assert_eq!(adjust_transfer_workers(2, 0, 8, 1), (3, 0));
        assert_eq!(
            adjust_transfer_workers(8, 0, FINAL_TRANSFER_WORKERS, 1),
            (9, 0)
        );
    }

    #[test]
    fn discovery_ranking_spreads_exploration_across_large_pools() {
        let configs = (0..15_000)
            .map(|index| format!("vless://id{index}@node{index}.example.com:443"))
            .collect::<Vec<_>>();
        let light_gbm = LightGbmScores::default();
        let ranked = rank_discovery_candidates(&configs, &light_gbm, 123, &HashSet::new());
        let mut score_only = configs.clone();
        score_only.sort_unstable();

        assert_eq!(ranked.len(), configs.len());
        assert_eq!(
            ranked.iter().collect::<HashSet<_>>(),
            configs.iter().collect::<HashSet<_>>()
        );
        let score_only_prefix = score_only.into_iter().take(200).collect::<HashSet<_>>();
        let explored_in_prefix = ranked
            .iter()
            .take(200)
            .filter(|config| !score_only_prefix.contains(*config))
            .count();
        assert!(explored_in_prefix >= 20, "{explored_in_prefix}");
    }

    #[test]
    fn discovery_tests_fresh_families_before_recent_families() {
        let recent = (0..3)
            .map(|index| format!("vless://old{index}@old{index}.example.com:443"))
            .collect::<Vec<_>>();
        let fresh = (0..8)
            .map(|index| format!("vless://new{index}@new{index}.example.com:443"))
            .collect::<Vec<_>>();
        let mut configs = recent.clone();
        configs.extend(fresh.clone());
        let recent_families = recent
            .iter()
            .map(|config| super::family_key(config))
            .collect::<HashSet<_>>();

        let ranked =
            rank_discovery_candidates(&configs, &LightGbmScores::default(), 123, &recent_families);

        assert_eq!(ranked.len(), configs.len());
        assert!(ranked[..fresh.len()]
            .iter()
            .all(|config| !recent_families.contains(&super::family_key(config))));
    }

    #[test]
    fn strict_recheck_uses_positions_for_equal_model_scores() {
        let configs = (0..128)
            .map(|index| format!("vless://id{index}@node{index}.example.com:443"))
            .collect::<Vec<_>>();
        let positions =
            HashMap::from([(configs[77].clone(), 0usize), (configs[0].clone(), 1usize)]);

        let ranked =
            super::rank_strict_recheck_candidates(&configs, &LightGbmScores::default(), &positions);

        assert_eq!(ranked[0], configs[77]);
    }

    #[test]
    fn adaptive_discovery_uses_measured_downstream_yield() {
        let batch = adaptive_discovery_batch_size(200, 155, 240, 226, 182, 160, 160, 155, 500);
        assert!(batch >= 24);
        assert!(batch < 100);
    }

    #[test]
    fn adaptive_discovery_starts_with_small_bootstrap_batch() {
        assert_eq!(
            adaptive_discovery_batch_size(200, 0, 0, 0, 0, 0, 0, 0, 500),
            300
        );
    }

    #[test]
    fn adaptive_transfer_budget_uses_remaining_slots_and_pass_rate() {
        assert_eq!(adaptive_transfer_test_limit(200, 180, 0, 0, 100), 30);
        assert_eq!(adaptive_transfer_test_limit(200, 180, 100, 50, 100), 148);
    }

    #[test]
    fn adaptive_transfer_budget_respects_candidate_headroom() {
        assert_eq!(adaptive_transfer_test_limit(200, 0, 300, 240, 50), 350);
        assert_eq!(adaptive_transfer_test_limit(200, 190, 300, 240, 3), 303);
    }

    #[test]
    fn adaptive_transfer_budget_expands_for_observed_low_yield() {
        assert_eq!(adaptive_transfer_test_limit(210, 180, 313, 180, 100), 376);
        assert_eq!(adaptive_transfer_test_limit(200, 0, 400, 150, 100), 450);
    }

    #[test]
    fn discovery_stagnation_requires_downstream_output_growth() {
        assert!(!discovery_downstream_progressed(180, 180, 180, 180));
        assert!(discovery_downstream_progressed(180, 181, 180, 180));
        assert!(discovery_downstream_progressed(180, 180, 180, 181));
    }

    #[test]
    fn adaptive_transfer_budget_stops_when_target_is_already_met() {
        assert_eq!(adaptive_transfer_test_limit(200, 200, 320, 280, 20), 320);
    }

    #[test]
    fn adaptive_stability_reserve_expands_with_low_yield() {
        assert_eq!(adaptive_stability_pool_target(200, 0, 0), 356);
        assert_eq!(adaptive_stability_pool_target(200, 243, 169), 349);
        assert_eq!(adaptive_stability_pool_target(200, 200, 100), 450);
    }

    #[test]
    fn adaptive_stability_target_tracks_available_pool_and_yield() {
        assert_eq!(adaptive_stability_target(200, 0, 0, 261), 261);
        assert_eq!(adaptive_stability_target(200, 0, 0, 180), 180);
        let low_yield = adaptive_stability_target(200, 200, 100, 500);
        let high_yield = adaptive_stability_target(200, 200, 180, 500);
        assert!(low_yield > high_yield);
        assert_eq!(high_yield, 271);
    }

    #[test]
    fn transfer_target_quarantine_requires_real_target_evidence() {
        let zero_pass = TransferTargetState {
            tested: 12,
            passed: 0,
            batches: 1,
            ..TransferTargetState::default()
        };
        let healthy = TransferTargetState {
            tested: 12,
            passed: 9,
            batches: 1,
            ..TransferTargetState::default()
        };

        assert!(should_quarantine_transfer_target(&zero_pass));
        assert!(!should_quarantine_transfer_target(&healthy));
    }

    #[test]
    fn transfer_target_selector_balances_iranian_hosts_before_backups() {
        let mut states = vec![TransferTargetState::default(); 4];
        assert_eq!(select_transfer_target(&states, 0), Some(0));

        states[0].tested = 12;
        states[0].passed = 11;
        states[0].batches = 1;
        assert_eq!(select_transfer_target(&states, 1), Some(1));

        states[1].tested = 12;
        states[1].passed = 10;
        states[1].batches = 1;
        assert_eq!(select_transfer_target(&states, 0), Some(0));

        states[0].passed = 4;
        states[1].quarantined = true;
        states[2].tested = 12;
        states[2].passed = 12;
        states[2].batches = 1;
        states[3].tested = 12;
        states[3].passed = 3;
        states[3].batches = 1;
        assert_eq!(select_transfer_target(&states, 1), Some(0));
    }

    #[test]
    fn transfer_target_selector_falls_back_when_preferred_hosts_are_quarantined() {
        let mut states = vec![TransferTargetState::default(); 4];
        states[0].quarantined = true;
        states[1].quarantined = true;
        states[2].tested = 16;
        states[2].passed = 14;
        states[2].batches = 1;
        states[3].tested = 32;
        states[3].passed = 29;
        states[3].batches = 2;

        assert_eq!(select_transfer_target(&states, 0), Some(2));
        states[2].quarantined = true;
        assert_eq!(select_transfer_target(&states, 0), Some(3));
    }

    #[test]
    fn transfer_target_selector_stops_when_every_host_is_quarantined() {
        let states = vec![
            TransferTargetState {
                quarantined: true,
                ..TransferTargetState::default()
            };
            4
        ];
        assert_eq!(select_transfer_target(&states, 0), None);
    }

    #[test]
    fn transfer_target_probe_starts_small_then_uses_normal_batches() {
        let mut state = TransferTargetState::default();
        assert_eq!(transfer_target_batch_limit(&state, 32), 12);

        state.tested = 12;
        state.passed = 9;
        state.batches = 1;
        assert_eq!(transfer_target_batch_limit(&state, 32), 32);
        assert_eq!(transfer_target_batch_limit(&state, 5), 5);
    }

    #[test]
    fn transfer_target_state_accumulates_and_quarantines_after_bad_probe() {
        let mut state = TransferTargetState::default();
        update_transfer_target_state(&mut state, 12, 0, 0, 10);
        assert_eq!(state.tested, 12);
        assert_eq!(state.passed, 0);
        assert!(state.quarantined);
    }

    #[test]
    fn selection_potential_count_respects_existing_selection() {
        let selected = vec!["vless://a@example.com:443".to_string()];
        let untested = vec![
            "vless://b@example.com:443".to_string(),
            "vless://c@example.net:443".to_string(),
        ];
        assert_eq!(selection_potential_count(&selected, &untested, 1, 3), 2);
    }

    #[test]
    fn selection_additional_potential_accounts_for_selected_limits() {
        let selected = vec![
            "vless://a@example.com:443".to_string(),
            "vless://b@example.net:443".to_string(),
        ];
        let untested = vec![
            "vless://c@example.com:443".to_string(),
            "vless://d@example.net:443".to_string(),
            "vless://e@example.org:443".to_string(),
        ];

        assert_eq!(
            selection_additional_potential_count(&selected, &untested, 3, 1, 3),
            1
        );

        let selected = vec!["vless://shared@example.com:443".to_string()];
        let untested = vec![
            "vless://shared@example.net:443".to_string(),
            "vless://other@example.net:443".to_string(),
        ];

        assert_eq!(
            selection_additional_potential_count(&selected, &untested, 2, 1, 1),
            1
        );
    }

    #[test]
    fn selection_eligible_count_respects_endpoint_limit() {
        let configs = vec![
            "vless://a@example.com:443".to_string(),
            "vless://b@example.com:443".to_string(),
            "vless://c@example.net:443".to_string(),
        ];
        assert_eq!(selection_eligible_count(&configs, 1, 3), 2);
    }

    #[test]
    fn recheck_diversity_allows_multiple_configs_per_family() {
        let configs = vec![
            "vless://a@example.com:443?type=ws".to_string(),
            "vless://a@example.net:443?type=ws".to_string(),
            "vless://a@example.org:443?type=ws".to_string(),
            "vless://b@example.net:443?type=ws".to_string(),
        ];

        let selected = super::diversify_recheck_candidates(&configs, 4, 3);
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn history_fingerprint_ignores_vmess_ps_label() {
        let a = format!(
            "vmess://{}",
            STANDARD.encode(br#"{"ps":"A 01","add":"example.com","port":443}"#)
        );
        let b = format!(
            "vmess://{}",
            STANDARD.encode(br#"{"ps":"B 01","add":"example.com","port":443}"#)
        );
        assert_eq!(history_fingerprint(&a), history_fingerprint(&b));
    }

    #[test]
    fn observation_fingerprint_separates_vmess_variants_with_same_history_identity() {
        let a = format!(
            "vmess://{}",
            STANDARD.encode(br#"{"ps":"A 01","add":"example.com","port":443}"#)
        );
        let b = format!(
            "vmess://{}",
            STANDARD.encode(br#"{"ps":"B 01","add":"example.com","port":443}"#)
        );
        assert_ne!(observation_fingerprint(&a), observation_fingerprint(&b));
    }

    #[test]
    fn routes_basic_proxy_schemes_to_xray() {
        for config in [
            "http://proxy.example:8080",
            "socks://proxy.example:1080",
            "socks5://proxy.example:1080",
            "socks5h://proxy.example:1080",
        ] {
            assert_eq!(light_backend(config), LightBackend::Xray);
        }
    }

    #[test]
    fn routes_reality_to_backend_fallback() {
        let config =
            "vless://uuid@example.com:443?security=reality&type=tcp&pbk=public&sid=01&sni=example.com";
        assert_eq!(light_backend(config), LightBackend::Fallback);
    }

    #[test]
    fn routes_grpc_authority_to_xray() {
        let config =
            "vless://uuid@example.com:443?security=tls&type=grpc&serviceName=Tun&authority=grpc.example.com";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_grpc_host_to_xray() {
        let config =
            "vless://uuid@example.com:443?security=tls&type=grpc&serviceName=Tun&host=grpc.example.com";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_legacy_raw_http_to_singbox() {
        let config =
            "vless://uuid@example.com:443?security=tls&type=tcp&headerType=http&host=example.com&path=%2Fproxy";
        assert_eq!(light_backend(config), LightBackend::SingBox);
    }

    #[test]
    fn routes_vmess_xhttp_to_xray_only() {
        let payload = r#"{"v":"2","add":"example.com","port":"443","id":"00000000-0000-0000-0000-000000000001","net":"xhttp"}"#;
        let config = format!("vmess://{}", STANDARD.encode(payload));
        assert_eq!(light_backend(&config), LightBackend::Xray);
    }

    #[test]
    fn routes_vmess_splithttp_to_xray_only() {
        let payload = r#"{"v":"2","add":"example.com","port":"443","id":"00000000-0000-0000-0000-000000000001","net":"splithttp"}"#;
        let config = format!("vmess://{}", STANDARD.encode(payload));
        assert_eq!(light_backend(&config), LightBackend::Xray);
    }

    #[test]
    fn routes_vmess_kcp_to_xray_only() {
        let payload = r#"{"v":"2","add":"example.com","port":"443","id":"00000000-0000-0000-0000-000000000001","net":"kcp","type":"none"}"#;
        let config = format!("vmess://{}", STANDARD.encode(payload));
        assert_eq!(light_backend(&config), LightBackend::Xray);
    }

    #[test]
    fn routes_url_splithttp_to_xray_only() {
        let config = "vless://uuid@example.com:443?security=tls&type=splithttp";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_url_kcp_to_xray_only() {
        let config = "vless://uuid@example.com:443?security=tls&type=kcp";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_vmess_quic_to_singbox() {
        let payload = r#"{"v":"2","add":"example.com","port":"443","id":"00000000-0000-0000-0000-000000000001","net":"quic","type":"none"}"#;
        let config = format!("vmess://{}", STANDARD.encode(payload));
        assert_eq!(light_backend(&config), LightBackend::SingBox);
    }

    #[test]
    fn routes_url_quic_to_singbox() {
        let config = "vless://uuid@example.com:443?security=none&type=quic";
        assert_eq!(light_backend(config), LightBackend::SingBox);
    }

    #[test]
    fn routes_vmess_grpc_host_to_xray() {
        let payload = r#"{"v":"2","add":"example.com","port":"443","id":"00000000-0000-0000-0000-000000000001","net":"grpc","type":"gun","host":"grpc.example.com"}"#;
        let config = format!("vmess://{}", STANDARD.encode(payload));
        assert_eq!(light_backend(&config), LightBackend::Xray);
    }

    #[test]
    fn routes_grpc_multi_to_xray() {
        let config =
            "trojan://pass@example.com:443?security=tls&type=grpc&serviceName=Tun&mode=multi";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_xhttp_to_xray_only() {
        let config = "vless://uuid@example.com:443?security=none&type=xhttp";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_non_vless_xhttp_to_xray() {
        let config = "trojan://pass@example.com:443?security=tls&type=xhttp";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_reality_xhttp_to_xray_only() {
        let config =
            "vless://uuid@example.com:443?security=reality&type=xhttp&pbk=public&sni=example.com";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_reality_vision_udp443_to_xray() {
        let config =
            "vless://uuid@example.com:443?security=reality&type=tcp&flow=xtls-rprx-vision-udp443";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_vision_udp443_to_xray() {
        let config =
            "vless://uuid@example.com:443?security=tls&type=tcp&flow=xtls-rprx-vision-udp443";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_vless_ech_dns_resolver_to_xray() {
        let config =
            "vless://uuid@example.com:443?security=tls&type=ws&ech=example.com%2Bhttps%3A%2F%2Fdns.example%2Fdns-query";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_vless_certificate_pinning_to_xray() {
        let config =
            "vless://uuid@example.com:443?security=tls&type=ws&pcs=0000000000000000000000000000000000000000000000000000000000000000";
        assert_eq!(light_backend(config), LightBackend::Xray);

        let config = "vless://uuid@example.com:443?security=tls&type=ws&vcn=example.com";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn keeps_raw_vless_ech_on_singbox() {
        let config = "vless://uuid@example.com:443?security=tls&type=ws&ech=YWJj";
        assert_eq!(light_backend(config), LightBackend::SingBox);
    }

    #[test]
    fn routes_hysteria2_pin_sha256_to_xray() {
        let config = "hysteria2://password@example.com:443?pinSHA256=AA%3ABB%3ACC%3ADD";
        assert_eq!(light_backend(config), LightBackend::Xray);
    }

    #[test]
    fn routes_normal_vless_to_singbox() {
        let config = "vless://uuid@example.com:443?security=tls&type=ws&path=%2F&sni=example.com";
        assert_eq!(light_backend(config), LightBackend::SingBox);
    }

    #[test]
    fn routes_tuic_to_singbox() {
        assert_eq!(
            light_backend("tuic://00000000-0000-0000-0000-000000000001:password@example.com:443"),
            LightBackend::SingBox
        );
    }

    #[test]
    fn routes_socks4_to_singbox() {
        assert_eq!(
            light_backend("socks4://127.0.0.1:1080"),
            LightBackend::SingBox
        );
        assert_eq!(
            light_backend("socks4a://127.0.0.1:1080"),
            LightBackend::SingBox
        );
    }

    #[test]
    fn explicit_tcp_transport_is_supported_by_light_parser() {
        let config =
            "vless://00000000-0000-0000-0000-000000000001@example.com:443?security=tls&type=tcp";
        assert_eq!(light_backend(config), LightBackend::SingBox);
    }

    #[test]
    fn defaults_trojan_security_in_published_light_links() {
        assert_eq!(
            normalize_light_config("trojan://pass@example.com:443?sni=example.com#Trojan"),
            "trojan://pass@example.com:443?sni=example.com&security=tls#Trojan"
        );
        assert_eq!(
            normalize_light_config("trojan://pass@example.com:443?#Trojan"),
            "trojan://pass@example.com:443?security=tls#Trojan"
        );
        assert_eq!(
            normalize_light_config("trojan://pass@example.com:443?security=tls"),
            "trojan://pass@example.com:443?security=tls"
        );
        assert_eq!(
            normalize_light_config("trojan://pass@example.com:443?security="),
            "trojan://pass@example.com:443?security=tls"
        );
        assert_eq!(
            normalize_light_config("trojan://pass@example.com:443?path=%2Fa%3Fb&security="),
            "trojan://pass@example.com:443?path=%2Fa%3Fb&security=tls"
        );
    }

    #[test]
    fn light_training_features_are_precheck_only() {
        let metrics = ProxyMetrics {
            successes: 2,
            attempts: 3,
            median_ms: 120.0,
            min_ms: 80.0,
            jitter_ms: 10.0,
            throughput_kbps: 900.0,
            successful_targets: Vec::new(),
        };

        let features = super::light_training_features(
            "vless://uuid@example.com:443?security=tls&type=ws&sni=example.com&path=%2F",
            Some(&metrics),
            0.75,
            4,
        );

        assert_eq!(features.len(), proxyrift::light_training::FEATURE_COUNT);
        assert_eq!(
            features.get("protocol"),
            Some(&serde_json::Value::String("vless".to_string()))
        );
        assert_eq!(
            features
                .get("transport")
                .and_then(serde_json::Value::as_str),
            Some("ws")
        );
        assert_eq!(
            features.get("security").and_then(serde_json::Value::as_str),
            Some("tls")
        );
        assert_eq!(
            features
                .get("early_attempts")
                .and_then(serde_json::Value::as_u64),
            Some(3)
        );
        assert_eq!(
            features
                .get("history_checks")
                .and_then(serde_json::Value::as_u64),
            Some(4)
        );
        assert!(!features.values().any(|value| {
            value
                .as_str()
                .is_some_and(|text| text.contains("uuid@example.com"))
        }));
    }

    #[test]
    fn light_training_features_recognize_vmess_tls_string() {
        let payload = serde_json::json!({
            "v": "2",
            "add": "example.com",
            "port": "443",
            "id": "00000000-0000-0000-0000-000000000001",
            "net": "ws",
            "tls": "tls",
        });
        let encoded = STANDARD.encode(payload.to_string());
        let config = format!("vmess://{encoded}");

        let features = light_training_features(&config, None, 0.5, 0);

        assert_eq!(
            features.get("security").and_then(serde_json::Value::as_str),
            Some("tls")
        );
        assert_eq!(
            features
                .get("tls_enabled")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn only_sing_box_verified_configs_are_publishable() {
        let xray = HashMap::from([(
            "xray-only".to_string(),
            ProxyMetrics {
                successes: 5,
                attempts: 8,
                median_ms: 10.0,
                min_ms: 4.0,
                jitter_ms: 2.0,
                throughput_kbps: 80.0,
                successful_targets: Vec::new(),
            },
        )]);
        let singbox = HashMap::from([(
            "singbox-only".to_string(),
            ProxyMetrics {
                successes: 5,
                attempts: 6,
                median_ms: 12.0,
                min_ms: 6.0,
                jitter_ms: 1.5,
                throughput_kbps: 70.0,
                successful_targets: Vec::new(),
            },
        )]);

        let publishable = singbox_qualified_metadata(xray, singbox);

        assert!(!publishable.contains_key("xray-only"));
        assert!(publishable.contains_key("singbox-only"));
    }
    #[test]
    fn recheck_diversity_limits_are_not_relaxed_by_fallback() {
        let configs = vec![
            "vless://a@example.com:443".to_string(),
            "vless://b@example.com:443".to_string(),
            "vless://c@example.net:443".to_string(),
        ];

        let selected = super::diversify_recheck_candidates(&configs, 3, 1);

        assert_eq!(
            selected,
            vec![
                "vless://a@example.com:443".to_string(),
                "vless://c@example.net:443".to_string(),
            ]
        );
    }

    #[test]
    fn quality_first_preserves_rank_and_endpoint_limit() {
        let configs = vec![
            "vless://a@example.com:443".to_string(),
            "vless://b@example.com:443".to_string(),
            "trojan://c@example.net:8443".to_string(),
            "vmess://encoded@example.org:9443".to_string(),
        ];

        let selected = select_verified_configs(&configs, 4, 1, 3);

        assert_eq!(
            selected,
            vec![configs[0].clone(), configs[2].clone(), configs[3].clone()]
        );
    }
}
