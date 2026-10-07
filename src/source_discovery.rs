use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use url::Url;

const REGISTRY_VERSION: u64 = 1;
const SEARCH_PER_PAGE: usize = 100;
const MAX_DISCOVERY_REPOS: usize = 500;
const MAX_TREE_SCANS: usize = 150;
const MAX_TREE_FILES_PER_REPO: usize = 40;
const MAX_ACTIVE_SOURCES: usize = 600;
const MAX_NEW_SOURCES: usize = 800;
const MAX_NEW_SOURCES_PER_REPO: usize = 25;
const MAX_KNOWN_REFRESH_SOURCES: usize = 800;
const MAX_REGISTRY_SOURCES: usize = 10_000;
const MAX_NEW_ACTIVE_SOURCES: usize = 75;
const BOOTSTRAP_NEW_ACTIVE_SOURCES: usize = 300;
const MAX_UNCHECKED_ACTIVE_SOURCES: usize = 75;
const MAX_PROVEN_ACTIVE_SOURCES: usize = 250;
const MIN_PROVEN_SUCCESSFUL_RUNS: u64 = 2;
const MIN_PROVEN_CONFIGS_LAST_RUN: u64 = 250;
const MIN_PROVEN_TRANSPORT_RATE: f64 = 0.08;
const MIN_KNOWN_TRANSPORT_REACHABLE: u64 = 20;
const MAX_LOW_QUALITY_STREAK: u64 = 3;
const QUALITY_RETRY_COOLDOWN_SECS: u64 = 30 * 24 * 60 * 60;
const MAX_FAILURE_STREAK: u64 = 5;
const MAX_EMPTY_STREAK: u64 = 3;
const RETIRED_SOURCE_COOLDOWN_SECS: u64 = 7 * 24 * 60 * 60;
const MAX_TREE_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const GITHUB_REQUEST_RETRIES: usize = 2;
const GITHUB_RETRY_BASE_MS: u64 = 500;
const GITHUB_RETRY_AFTER_MAX_SECS: u64 = 10;
const GITHUB_ACCEPT: &str = "application/vnd.github+json";
const USER_AGENT: &str = "ProxyRift-source-discovery/1.0";
const README_MAX_BYTES: usize = 256 * 1024;
const MAX_README_API_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_SOURCE_URL_LENGTH: usize = 8192;
const MAX_SEARCH_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_README_CANDIDATES: usize = 150;
const MAX_README_URLS_SCANNED: usize = 750;
const MAX_DISCOVERED_CANDIDATES: usize = 12_000;
const MAX_NEW_ACTIVE_SOURCES_PER_REPO: usize = 8;
const MAX_GITHUB_SEARCH_REQUESTS_PER_RUN: usize = 16;
const GITHUB_SEARCH_MIN_INTERVAL_MS: u64 = 1_200;
const GITHUB_SEARCH_MAX_INTERVAL_MS: u64 = 8_000;
const GITHUB_SEARCH_MIN_REMAINING: u64 = 2;
const GITHUB_SEARCH_FRESH_DAYS: u64 = 120;

const SEARCH_SORTS: [&str; 2] = ["updated", "stars"];
const SEARCH_QUERY_SET_NAMES: [&str; 3] = ["protocol", "sources", "fresh-low-star"];

const SEARCH_QUERY_SETS: [[&str; 16]; 3] = [
    [
        "v2ray subscription",
        "vless subscription",
        "vmess subscription",
        "xray subscription",
        "sing-box subscription",
        "mihomo subscription",
        "clash subscription",
        "hysteria2 subscription",
        "tuic subscription",
        "reality configs",
        "v2ray nodes",
        "vless nodes",
        "vmess nodes",
        "proxy subscription",
        "subscription collector",
        "subscription aggregator",
    ],
    [
        "v2ray configs",
        "free v2ray configs",
        "xray configs",
        "singbox config",
        "clash config",
        "mihomo config",
        "hysteria2 config",
        "tuic config",
        "reality config",
        "proxy list",
        "node list",
        "v2ray collector",
        "proxy collector",
        "subconverter",
        "proxy aggregator",
        "free proxy configs",
    ],
    [
        "v2ray subscription",
        "vless nodes",
        "vmess nodes",
        "xray configs",
        "sing-box subscription",
        "mihomo subscription",
        "clash subscription",
        "hysteria2 subscription",
        "tuic subscription",
        "reality configs",
        "proxy list",
        "node list",
        "v2ray collector",
        "proxy collector",
        "public proxy",
        "free proxy configs",
    ],
];

fn search_sort_for_run(run_number: Option<u64>, now: u64) -> &'static str {
    let index = run_number
        .map(|number| number % SEARCH_SORTS.len() as u64)
        .unwrap_or((now / 3_600) % SEARCH_SORTS.len() as u64) as usize;
    SEARCH_SORTS[index]
}

fn search_query_set_for_run(run_number: Option<u64>, now: u64) -> usize {
    run_number
        .map(|number| number % SEARCH_QUERY_SETS.len() as u64)
        .unwrap_or((now / 3_600) % SEARCH_QUERY_SETS.len() as u64) as usize
}

fn search_strategy_for_run(run_number: Option<u64>, now: u64) -> (&'static str, usize) {
    let query_set = search_query_set_for_run(run_number, now);
    let sort = if query_set == 2 {
        "updated"
    } else {
        search_sort_for_run(run_number, now)
    };
    (sort, query_set)
}

fn unix_days_to_ymd(days_since_epoch: i64) -> (i64, u32, u32) {
    let mut z = days_since_epoch + 719_468;
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    z -= era * 146_097;
    let doe = z - z / 1_460 + z / 36_524 - z / 146_096;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    (y + i64::from(m <= 2), m as u32, d as u32)
}

fn search_fresh_cutoff_date(now: u64) -> String {
    let days = (now / 86_400) as i64 - GITHUB_SEARCH_FRESH_DAYS as i64;
    let (year, month, day) = unix_days_to_ymd(days);
    format!("{year:04}-{month:02}-{day:02}")
}

fn build_search_query(query: &str, query_set: usize, now: u64) -> String {
    let base =
        format!("{query} archived:false fork:false is:public in:name,description,readme");
    if query_set == 2 {
        format!(
            "{base} stars:0..100 pushed:>{}",
            search_fresh_cutoff_date(now)
        )
    } else {
        base
    }
}

async fn pace_search_request(
    last_started: &mut Option<Instant>,
    remaining: Option<u64>,
    reset_epoch: Option<u64>,
) {
    let mut interval_ms = GITHUB_SEARCH_MIN_INTERVAL_MS;

    if let (Some(remaining), Some(reset_epoch)) = (remaining, reset_epoch) {
        if remaining > GITHUB_SEARCH_MIN_REMAINING && remaining <= 8 {
            let window_ms = reset_epoch
                .saturating_sub(unix_now())
                .saturating_mul(1_000);
            if window_ms > 0 {
                interval_ms = interval_ms.max(
                    (window_ms / remaining).clamp(
                        GITHUB_SEARCH_MIN_INTERVAL_MS,
                        GITHUB_SEARCH_MAX_INTERVAL_MS,
                    ),
                );
            }
        }
    }

    if let Some(started) = *last_started {
        let elapsed_ms = started.elapsed().as_millis() as u64;
        if elapsed_ms < interval_ms {
            tokio::time::sleep(Duration::from_millis(interval_ms - elapsed_ms)).await;
        }
    }

    *last_started = Some(Instant::now());
}

const PATH_HINTS: [&str; 24] = [
    "sub",
    "subs",
    "subscription",
    "subscriptions",
    "config",
    "configs",
    "all",
    "list",
    "proxies",
    "v2ray",
    "vless",
    "vmess",
    "trojan",
    "shadowsocks",
    "hysteria",
    "hysteria2",
    "tuic",
    "reality",
    "clash",
    "sing",
    "singbox",
    "nodes",
    "servers",
    "proxy",
];

const NOISE_HINTS: [&str; 12] = [
    ".github/",
    "readme",
    "license",
    "changelog",
    "contributing",
    "issue",
    "pull/",
    "/actions/",
    ".git/",
    "package-lock",
    "cargo.lock",
    "go.sum",
];

const NOISE_PATH_TOKENS: [&str; 18] = [
    "archive",
    "archives",
    "backup",
    "backups",
    "broken",
    "dead",
    "deprecated",
    "obsolete",
    "old",
    "invalid",
    "example",
    "examples",
    "sample",
    "samples",
    "test",
    "tests",
    "fixture",
    "fixtures",
];

const SOURCE_EXTENSIONS: [&str; 8] = [
    ".txt", ".yaml", ".yml", ".json", ".conf", ".list", ".sub", ".ini",
];
const SOURCE_EXTENSIONS_WITHOUT_HINT: [&str; 2] = [".list", ".sub"];

const STRONG_PATH_HINTS: [&str; 17] = [
    "subscription",
    "subscriptions",
    "subs",
    "proxies",
    "proxy",
    "nodes",
    "servers",
    "v2ray",
    "vless",
    "vmess",
    "trojan",
    "shadowsocks",
    "hysteria",
    "hysteria2",
    "tuic",
    "reality",
    "singbox",
];

const OBVIOUS_NON_SOURCE_FILENAMES: [&str; 16] = [
    ".editorconfig",
    "cargo.lock",
    "cargo.toml",
    "composer.json",
    "docker-compose.override.yml",
    "docker-compose.override.yaml",
    "docker-compose.yml",
    "docker-compose.yaml",
    "go.mod",
    "go.sum",
    "manifest.json",
    "metadata.json",
    "package-lock.json",
    "package.json",
    "pnpm-lock.yaml",
    "yarn.lock",
];

#[derive(Clone, Debug)]
struct Repository {
    name: String,
    branch: String,
    pushed_at: String,
    search_hits: u16,
    best_search_rank: u16,
    stars: u64,
}

#[derive(Clone, Debug)]
struct Candidate {
    url: String,
    repo: String,
    repo_rank: usize,
    priority: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionOutcome {
    Success(usize),
    Failed,
    PermanentlyFailed(u16),
}

struct Registry {
    root: Map<String, Value>,
}

impl Registry {
    fn new(now: u64) -> Self {
        let mut root = Map::new();
        root.insert("schema_version".into(), Value::from(REGISTRY_VERSION));
        root.insert("updated_at".into(), Value::from(now));
        root.insert("sources".into(), Value::Object(Map::new()));
        Self { root }
    }

    fn from_value(value: Value, now: u64) -> Self {
        let Value::Object(mut root) = value else {
            return Self::new(now);
        };

        if root.get("schema_version").and_then(Value::as_u64) != Some(REGISTRY_VERSION) {
            return Self::new(now);
        }

        if !matches!(root.get("sources"), Some(Value::Object(_))) {
            root.insert("sources".into(), Value::Object(Map::new()));
        }

        root.insert("updated_at".into(), Value::from(now));
        Self { root }
    }

    fn sources(&self) -> &Map<String, Value> {
        self.root
            .get("sources")
            .and_then(Value::as_object)
            .expect("registry always contains sources")
    }

    fn sources_mut(&mut self) -> &mut Map<String, Value> {
        self.root
            .get_mut("sources")
            .and_then(Value::as_object_mut)
            .expect("registry always contains sources")
    }

    fn active_urls(
        &self,
        limit: usize,
        excluded: &HashSet<String>,
        recoverable: &HashSet<String>,
    ) -> Vec<String> {
        if limit == 0 {
            return Vec::new();
        }

        let proven = self.proven_urls(limit, excluded, recoverable);
        let proven_set = proven.iter().cloned().collect::<HashSet<_>>();

        let mut checked = Vec::new();
        let mut unchecked = Vec::new();

        for (url, record) in self.sources() {
            if proven_set.contains(url)
                || excluded.contains(url)
                || is_self_source(url)
                || is_legacy_noise_source(url)
            {
                continue;
            }

            let failures = record
                .get("failure_streak")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let empty_streak = record
                .get("empty_streak")
                .and_then(Value::as_u64)
                .unwrap_or_default();

            if (failures >= MAX_FAILURE_STREAK || empty_streak >= MAX_EMPTY_STREAK)
                && !recoverable.contains(url)
            {
                continue;
            }

            if !recoverable.contains(url)
                && record
                    .get("transport_reachable_last_run")
                    .and_then(Value::as_u64)
                    .is_some()
                && !source_has_acceptable_transport_history(record)
            {
                continue;
            }

            let first_seen = record
                .get("first_seen")
                .and_then(Value::as_u64)
                .unwrap_or_default();

            match record.get("last_checked").and_then(Value::as_u64) {
                Some(_) => checked.push((url.clone(), source_selection_score(record), first_seen)),
                None => unchecked.push((url.clone(), first_seen)),
            }
        }

        checked.sort_by(|a, b| {
            b.1.total_cmp(&a.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        unchecked.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let unchecked_limit = MAX_UNCHECKED_ACTIVE_SOURCES
            .min(limit.saturating_sub(proven.len()))
            .min(unchecked.len());

        let checked_limit = limit
            .saturating_sub(proven.len())
            .saturating_sub(unchecked_limit);

        proven
            .into_iter()
            .take(limit)
            .chain(
                checked
                    .into_iter()
                    .take(checked_limit)
                    .map(|(url, _, _)| url),
            )
            .chain(
                unchecked
                    .into_iter()
                    .take(unchecked_limit)
                    .map(|(url, _)| url),
            )
            .collect()
    }

    fn proven_urls(
        &self,
        limit: usize,
        excluded: &HashSet<String>,
        recoverable: &HashSet<String>,
    ) -> Vec<String> {
        let mut proven = self
            .sources()
            .iter()
            .filter_map(|(url, record)| {
                if excluded.contains(url)
                    || is_self_source(url)
                    || is_legacy_noise_source(url)
                    || !self.is_healthy_for_selection(record, url, recoverable)
                {
                    return None;
                }

                let successes = record
                    .get("successes")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let configs_last_run = record
                    .get("configs_last_run")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();

                if successes < MIN_PROVEN_SUCCESSFUL_RUNS
                    || configs_last_run < MIN_PROVEN_CONFIGS_LAST_RUN
                    || !source_has_meaningful_transport_history(record)
                {
                    return None;
                }

                let failures = record
                    .get("failures")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let empty_runs = record
                    .get("empty_runs")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let total_runs = successes
                    .saturating_add(failures)
                    .saturating_add(empty_runs);
                let reliability = if total_runs == 0 {
                    0
                } else {
                    successes
                        .saturating_mul(1_000)
                        .checked_div(total_runs)
                        .unwrap_or_default()
                };
                let last_success = record
                    .get("last_success")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let configs_total = record
                    .get("configs_total")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let transport_tested = source_transport_stats(record)
                    .map(|(tested, _)| tested)
                    .unwrap_or(configs_last_run);
                let effective_yield = source_selection_score(record)
                    .mul_add(transport_tested as f64, 0.0)
                    .round()
                    .clamp(0.0, u64::MAX as f64) as u64;

                Some((
                    url.clone(),
                    effective_yield,
                    configs_last_run,
                    configs_total,
                    reliability,
                    successes,
                    last_success,
                ))
            })
            .collect::<Vec<_>>();

        proven.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| b.3.cmp(&a.3))
                .then_with(|| b.4.cmp(&a.4))
                .then_with(|| b.5.cmp(&a.5))
                .then_with(|| b.6.cmp(&a.6))
                .then_with(|| a.0.cmp(&b.0))
        });

        proven
            .into_iter()
            .take(limit.min(MAX_PROVEN_ACTIVE_SOURCES))
            .map(|(url, _, _, _, _, _, _)| url)
            .collect()
    }

    fn is_healthy_for_selection(
        &self,
        record: &Value,
        url: &str,
        recoverable: &HashSet<String>,
    ) -> bool {
        if recoverable.contains(url) {
            return true;
        }

        let failures = record
            .get("failure_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let empty_streak = record
            .get("empty_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();

        failures < MAX_FAILURE_STREAK
            && empty_streak < MAX_EMPTY_STREAK
            && !source_quality_quarantined(record)
            && record.get("permanently_failed").and_then(Value::as_bool) != Some(true)
    }

    fn is_recoverable(&self, url: &str, now: u64) -> bool {
        let Some(record) = self.sources().get(url) else {
            return false;
        };

        let failure_streak = record
            .get("failure_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let empty_streak = record
            .get("empty_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let last_checked = record.get("last_checked").and_then(Value::as_u64);

        (failure_streak >= MAX_FAILURE_STREAK || empty_streak >= MAX_EMPTY_STREAK)
            && last_checked
                .is_some_and(|checked| now.saturating_sub(checked) >= RETIRED_SOURCE_COOLDOWN_SECS)
    }

    fn add_candidate(&mut self, candidate: &Candidate, now: u64) {
        let sources = self.sources_mut();
        let record = sources.entry(candidate.url.clone()).or_insert_with(|| {
            let mut object = Map::new();
            object.insert("url".into(), Value::String(candidate.url.clone()));
            object.insert("repo".into(), Value::String(candidate.repo.clone()));
            object.insert("first_seen".into(), Value::from(now));
            object.insert("last_discovered".into(), Value::from(now));
            object.insert("last_checked".into(), Value::Null);
            object.insert("successes".into(), Value::from(0u64));
            object.insert("failures".into(), Value::from(0u64));
            object.insert("failure_streak".into(), Value::from(0u64));
            object.insert("configs_total".into(), Value::from(0u64));
            object.insert("strict_tested_last_run".into(), Value::from(0u64));
            object.insert("strict_pass_last_run".into(), Value::from(0u64));
            object.insert("transfer_tested_last_run".into(), Value::from(0u64));
            object.insert("transfer_pass_last_run".into(), Value::from(0u64));
            object.insert("transport_low_quality_streak".into(), Value::from(0u64));
            object.insert("strict_low_quality_streak".into(), Value::from(0u64));
            object.insert("transfer_low_quality_streak".into(), Value::from(0u64));
            Value::Object(object)
        });

        if let Some(object) = record.as_object_mut() {
            let failure_streak = object
                .get("failure_streak")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let last_checked = object.get("last_checked").and_then(Value::as_u64);

            if failure_streak < MAX_FAILURE_STREAK
                || last_checked.is_some_and(|checked| {
                    now.saturating_sub(checked) >= RETIRED_SOURCE_COOLDOWN_SECS
                })
            {
                object.insert("last_discovered".into(), Value::from(now));
            }

            if object
                .get("repo")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                object.insert("repo".into(), Value::String(candidate.repo.clone()));
            }
        }
    }

    #[cfg(test)]
    fn record_result(&mut self, url: &str, produced_configs: usize, now: u64) {
        let outcome = if produced_configs > 0 {
            CollectionOutcome::Success(produced_configs)
        } else {
            CollectionOutcome::Failed
        };
        self.record_outcome(url, outcome, now);
    }

    fn record_outcome(&mut self, url: &str, outcome: CollectionOutcome, now: u64) {
        let Some(object) = self
            .sources_mut()
            .get_mut(url)
            .and_then(Value::as_object_mut)
        else {
            return;
        };

        let successes = object
            .get("successes")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let failures = object
            .get("failures")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let streak = object
            .get("failure_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let empty_runs = object
            .get("empty_runs")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let empty_streak = object
            .get("empty_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let configs_total = object
            .get("configs_total")
            .and_then(Value::as_u64)
            .unwrap_or_default();

        object.insert("last_checked".into(), Value::from(now));

        match outcome {
            CollectionOutcome::Success(produced_configs) => {
                object.insert(
                    "configs_last_run".into(),
                    Value::from(produced_configs as u64),
                );

                if produced_configs > 0 {
                    object.insert(
                        "configs_total".into(),
                        Value::from(configs_total.saturating_add(produced_configs as u64)),
                    );
                    object.insert("successes".into(), Value::from(successes.saturating_add(1)));
                    object.insert("failure_streak".into(), Value::from(0u64));
                    object.insert("empty_streak".into(), Value::from(0u64));
                    object.remove("permanently_failed");
                    object.remove("last_http_status");
                    object.remove("retired_at");
                    object.insert("last_success".into(), Value::from(now));
                } else {
                    object.insert("failure_streak".into(), Value::from(0u64));
                    object.insert(
                        "empty_runs".into(),
                        Value::from(empty_runs.saturating_add(1)),
                    );
                    object.insert(
                        "empty_streak".into(),
                        Value::from(empty_streak.saturating_add(1)),
                    );
                }
            }
            CollectionOutcome::Failed => {
                object.insert("configs_last_run".into(), Value::from(0u64));
                object.insert("failures".into(), Value::from(failures.saturating_add(1)));
                object.insert(
                    "failure_streak".into(),
                    Value::from(streak.saturating_add(1)),
                );
                object.insert("empty_streak".into(), Value::from(0u64));
            }
            CollectionOutcome::PermanentlyFailed(status) => {
                object.insert("configs_last_run".into(), Value::from(0u64));
                object.insert("failures".into(), Value::from(failures.saturating_add(1)));
                object.insert("failure_streak".into(), Value::from(MAX_FAILURE_STREAK));
                object.insert("empty_streak".into(), Value::from(0u64));
                object.insert("permanently_failed".into(), Value::Bool(true));
                object.insert("last_http_status".into(), Value::from(u64::from(status)));
                object.insert("retired_at".into(), Value::from(now));
            }
        }
    }

    fn quarantined_sources(&self) -> usize {
        self.sources()
            .values()
            .filter(|record| {
                let failure_streak = record
                    .get("failure_streak")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let empty_streak = record
                    .get("empty_streak")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();

                failure_streak >= MAX_FAILURE_STREAK
                    || empty_streak >= MAX_EMPTY_STREAK
                    || source_quality_quarantined(record)
            })
            .count()
    }

    fn prune(&mut self, limit: usize, protected: &HashSet<String>) -> usize {
        let source_count = self.sources().len();
        if source_count <= limit {
            return 0;
        }

        let removable = source_count.saturating_sub(protected.len().min(limit));
        let mut candidates = self
            .sources()
            .iter()
            .filter(|(url, _)| !protected.contains(*url))
            .map(|(url, record)| {
                let last_discovered = record
                    .get("last_discovered")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let last_success = record
                    .get("last_success")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let last_checked = record
                    .get("last_checked")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();

                (url.clone(), last_discovered, last_success, last_checked)
            })
            .collect::<Vec<_>>();

        candidates.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| b.3.cmp(&a.3))
                .then_with(|| a.0.cmp(&b.0))
        });

        let keep = candidates
            .into_iter()
            .take(removable.saturating_sub(source_count.saturating_sub(limit)))
            .map(|(url, _, _, _)| url)
            .collect::<HashSet<_>>();

        let before = self.sources().len();
        self.sources_mut()
            .retain(|url, _| protected.contains(url) || keep.contains(url));

        before.saturating_sub(self.sources().len())
    }

    fn json(&self) -> Value {
        Value::Object(self.root.clone())
    }
}

fn is_self_source(url: &str) -> bool {
    url.starts_with("https://raw.githubusercontent.com/rekt0ro/ProxyRift/")
}

fn source_transport_stats(record: &Value) -> Option<(u64, u64)> {
    let tested = record
        .get("transport_tested_last_run")
        .and_then(Value::as_u64)
        .or_else(|| record.get("configs_last_run").and_then(Value::as_u64))?;
    let reachable = record
        .get("transport_reachable_last_run")
        .and_then(Value::as_u64)?;

    Some((tested, reachable))
}

fn source_has_acceptable_transport_history(record: &Value) -> bool {
    source_transport_stats(record).is_some_and(|(tested, reachable)| {
        tested >= MIN_KNOWN_TRANSPORT_REACHABLE
            && reachable >= MIN_KNOWN_TRANSPORT_REACHABLE
            && (reachable as f64 / tested.max(1) as f64) >= MIN_PROVEN_TRANSPORT_RATE
    })
}

fn source_has_meaningful_transport_history(record: &Value) -> bool {
    source_transport_stats(record).is_some_and(|(tested, reachable)| {
        tested >= MIN_PROVEN_CONFIGS_LAST_RUN
            && reachable >= MIN_KNOWN_TRANSPORT_REACHABLE
            && (reachable as f64 / tested.max(1) as f64) >= MIN_PROVEN_TRANSPORT_RATE
    })
}

fn source_selection_score(record: &Value) -> f64 {
    let transport_tested = record
        .get("transport_tested_last_run")
        .and_then(Value::as_u64)
        .or_else(|| record.get("configs_last_run").and_then(Value::as_u64))
        .unwrap_or_default();
    let successes = record
        .get("successes")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let failures = record
        .get("failures")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let empty_runs = record
        .get("empty_runs")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let total_runs = successes
        .saturating_add(failures)
        .saturating_add(empty_runs);
    let reliability = if total_runs == 0 {
        0.5
    } else {
        successes as f64 / total_runs as f64
    };

    let Some(reachable) = record
        .get("transport_reachable_last_run")
        .and_then(Value::as_u64)
    else {
        return 0.05 * reliability;
    };

    if transport_tested == 0 {
        return 0.0;
    }

    let transport_rate = (reachable as f64 / transport_tested as f64).clamp(0.0, 1.0);

    let strict_rate = match (
        record.get("strict_tested_last_run").and_then(Value::as_u64),
        record.get("strict_pass_last_run").and_then(Value::as_u64),
    ) {
        (Some(tested), Some(passed)) if tested > 0 => (passed as f64 + 1.0) / (tested as f64 + 2.0),
        _ => 0.5,
    };

    let transfer_rate = match (
        record
            .get("transfer_tested_last_run")
            .and_then(Value::as_u64),
        record.get("transfer_pass_last_run").and_then(Value::as_u64),
    ) {
        (Some(tested), Some(passed)) if tested > 0 => (passed as f64 + 1.0) / (tested as f64 + 2.0),
        _ => 0.5,
    };

    let downstream_quality =
        (transport_rate * 0.65) + (strict_rate * 0.25) + (transfer_rate * 0.10);

    (downstream_quality * reliability).clamp(0.0, 1.0)
}

fn source_quality_quarantined(record: &Value) -> bool {
    let reached_threshold = [
        "transport_low_quality_streak",
        "strict_low_quality_streak",
        "transfer_low_quality_streak",
    ]
    .into_iter()
    .any(|key| {
        record.get(key).and_then(Value::as_u64).unwrap_or_default() >= MAX_LOW_QUALITY_STREAK
    });

    if !reached_threshold {
        return false;
    }

    record
        .get("last_checked")
        .and_then(Value::as_u64)
        .is_none_or(|checked| unix_now().saturating_sub(checked) < QUALITY_RETRY_COOLDOWN_SECS)
}

pub async fn record_transport_results(
    tested_by_source: &std::collections::HashMap<String, usize>,
    reachable_by_source: &std::collections::HashMap<String, usize>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if tested_by_source.is_empty() {
        return Ok(());
    }

    let root = project_root()?;
    let registry_path = root.join("subscriptions").join("source-registry.json");
    let now = unix_now();
    let mut registry = load_registry(&registry_path, now).await;

    for (url, tested) in tested_by_source {
        let Some(object) = registry
            .sources_mut()
            .get_mut(url)
            .and_then(Value::as_object_mut)
        else {
            continue;
        };

        let reachable = reachable_by_source.get(url).copied().unwrap_or_default();
        let previous = object
            .get("transport_low_quality_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();

        object.insert(
            "transport_tested_last_run".into(),
            Value::from(*tested as u64),
        );
        object.insert(
            "transport_reachable_last_run".into(),
            Value::from(reachable as u64),
        );

        let low_quality = *tested >= 100 && (reachable as f64 / *tested as f64) < 0.08;
        object.insert(
            "transport_low_quality_streak".into(),
            Value::from(if low_quality {
                previous.saturating_add(1)
            } else {
                0
            }),
        );
    }

    write_registry(&registry_path, &registry).await?;
    Ok(())
}

pub fn record_light_results(
    strict_tested: &std::collections::HashSet<String>,
    strict_passed: &std::collections::HashSet<String>,
    transfer_tested: &std::collections::HashSet<String>,
    transfer_passed: &std::collections::HashSet<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = project_root()?;
    let map_path = root.join("subscriptions").join(".source-config-map.json");
    let registry_path = root.join("subscriptions").join("source-registry.json");

    let map_text = match std::fs::read_to_string(&map_path) {
        Ok(text) => text,
        Err(_) => return Ok(()),
    };
    let source_map = serde_json::from_str::<Map<String, Value>>(&map_text).unwrap_or_default();

    let mut strict_tested_by_source = HashMap::<String, u64>::new();
    let mut strict_passed_by_source = HashMap::<String, u64>::new();
    let mut transfer_tested_by_source = HashMap::<String, u64>::new();
    let mut transfer_passed_by_source = HashMap::<String, u64>::new();

    for config in strict_tested {
        if let Some(Value::Array(sources)) = source_map.get(config) {
            for source in sources.iter().filter_map(Value::as_str) {
                *strict_tested_by_source
                    .entry(source.to_string())
                    .or_default() += 1;
                if strict_passed.contains(config) {
                    *strict_passed_by_source
                        .entry(source.to_string())
                        .or_default() += 1;
                }
            }
        }
    }

    for config in transfer_tested {
        if let Some(Value::Array(sources)) = source_map.get(config) {
            for source in sources.iter().filter_map(Value::as_str) {
                *transfer_tested_by_source
                    .entry(source.to_string())
                    .or_default() += 1;
                if transfer_passed.contains(config) {
                    *transfer_passed_by_source
                        .entry(source.to_string())
                        .or_default() += 1;
                }
            }
        }
    }

    if strict_tested_by_source.is_empty() && transfer_tested_by_source.is_empty() {
        return Ok(());
    }

    let now = unix_now();
    let registry_value = std::fs::read_to_string(&registry_path)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .unwrap_or_else(|| Registry::new(now).json());
    let mut registry = Registry::from_value(registry_value, now);

    let mut touched = HashSet::new();
    touched.extend(strict_tested_by_source.keys().cloned());
    touched.extend(transfer_tested_by_source.keys().cloned());

    for source in touched {
        let Some(object) = registry
            .sources_mut()
            .get_mut(&source)
            .and_then(Value::as_object_mut)
        else {
            continue;
        };

        let strict_tested = strict_tested_by_source
            .get(&source)
            .copied()
            .unwrap_or_default();
        let strict_passed = strict_passed_by_source
            .get(&source)
            .copied()
            .unwrap_or_default();
        let transfer_tested = transfer_tested_by_source
            .get(&source)
            .copied()
            .unwrap_or_default();
        let transfer_passed = transfer_passed_by_source
            .get(&source)
            .copied()
            .unwrap_or_default();

        object.insert("strict_tested_last_run".into(), Value::from(strict_tested));
        object.insert("strict_pass_last_run".into(), Value::from(strict_passed));
        object.insert(
            "transfer_tested_last_run".into(),
            Value::from(transfer_tested),
        );
        object.insert(
            "transfer_pass_last_run".into(),
            Value::from(transfer_passed),
        );

        let strict_previous = object
            .get("strict_low_quality_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let strict_low =
            strict_tested >= 20 && (strict_passed as f64 / strict_tested as f64) < 0.05;
        object.insert(
            "strict_low_quality_streak".into(),
            Value::from(if strict_low {
                strict_previous.saturating_add(1)
            } else {
                0
            }),
        );

        let transfer_previous = object
            .get("transfer_low_quality_streak")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let transfer_low =
            transfer_tested >= 10 && (transfer_passed as f64 / transfer_tested as f64) < 0.05;
        object.insert(
            "transfer_low_quality_streak".into(),
            Value::from(if transfer_low {
                transfer_previous.saturating_add(1)
            } else {
                0
            }),
        );
    }

    let data = serde_json::to_string_pretty(&registry.json())?;
    let temporary = registry_path.with_extension("json.tmp");
    std::fs::write(&temporary, format!("{data}\n"))?;
    if let Err(error) = std::fs::rename(&temporary, &registry_path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }

    Ok(())
}
pub async fn discover_and_write() -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>>
{
    let root = project_root()?;
    let registry_path = root.join("subscriptions").join("source-registry.json");
    let sources_path = root.join("sources.txt");
    let now = unix_now();

    let mut registry = load_registry(&registry_path, now).await;
    let token = env::var("GITHUB_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let client = Client::builder()
        .user_agent(USER_AGENT)
        .timeout(std::time::Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let mut repos = match search_repositories(&client, token.as_deref()).await {
        Ok(repos) => repos,
        Err(error) => {
            if registry
                .active_urls(MAX_ACTIVE_SOURCES, &HashSet::new(), &HashSet::new())
                .is_empty()
            {
                return Err(error);
            }

            println!(
                "[WARN] 🔭 [Discovery] GitHub search unavailable: {error}; using the persisted registry"
            );
            Vec::new()
        }
    };

    let mut seen_repositories = HashSet::new();
    repos.retain(|repo| seen_repositories.insert(repo.name.clone()));
    sort_discovered_repositories(&mut repos);
    repos.truncate(MAX_DISCOVERY_REPOS);

    let discovered = if repos.is_empty() {
        Vec::new()
    } else {
        discover_from_repos(&client, &repos, token.as_deref()).await?
    };
    let discovered = deduplicate_candidates(discovered);

    if discovered.is_empty() {
        if let Some(active) = persisted_active_sources(&registry) {
            println!(
                "[WARN] 🔭 [Discovery] No usable GitHub sources discovered; using {} persisted active sources",
                active.len()
            );
            write_sources(&sources_path, &active).await?;
            return Ok((0, active.len()));
        }

        return Err("GitHub discovery produced no usable subscription sources".into());
    }

    let existing_urls = registry.sources().keys().cloned().collect::<HashSet<_>>();

    let mut new_candidates = Vec::new();
    let mut new_counts_by_repo = HashMap::<String, usize>::new();

    for candidate in discovered
        .iter()
        .filter(|candidate| !existing_urls.contains(&candidate.url))
    {
        let count = new_counts_by_repo
            .entry(candidate.repo.clone())
            .or_default();
        if *count >= MAX_NEW_SOURCES_PER_REPO {
            continue;
        }

        new_candidates.push(candidate.clone());
        *count += 1;

        if new_candidates.len() >= MAX_NEW_SOURCES {
            break;
        }
    }

    let known_candidates = select_known_refresh_candidates(&discovered, &registry, now);

    for candidate in new_candidates.iter().chain(known_candidates.iter()) {
        registry.add_candidate(candidate, now);
    }

    let ordered = select_active_sources(
        &registry,
        &new_candidates,
        &known_candidates,
        now,
        MAX_ACTIVE_SOURCES,
    );

    let protected = ordered.iter().cloned().collect::<HashSet<_>>();
    let retired = registry.prune(MAX_REGISTRY_SOURCES, &protected);

    if ordered.is_empty() {
        return Err("GitHub discovery produced no usable subscription sources".into());
    }

    write_registry(&registry_path, &registry).await?;
    write_sources(&sources_path, &ordered).await?;

    let proven_set = registry
        .proven_urls(MAX_PROVEN_ACTIVE_SOURCES, &HashSet::new(), &HashSet::new())
        .into_iter()
        .collect::<HashSet<_>>();
    let proven_active = ordered
        .iter()
        .filter(|url| proven_set.contains(*url))
        .count();

    println!(
        "[INFO] 🔭 [Discovery] {} Repositories searched | {} New sources | {} Active sources | {} Proven core | {} Exploratory | {} Registry pruned",
        repos.len(),
        new_candidates.len(),
        ordered.len(),
        proven_active,
        ordered.len().saturating_sub(proven_active),
        retired
    );

    Ok((new_candidates.len(), ordered.len()))
}

pub async fn record_collection_results(
    results: &[(String, CollectionOutcome)],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    if results.is_empty() {
        return Ok(0);
    }

    let root = project_root()?;
    let registry_path = root.join("subscriptions").join("source-registry.json");
    let sources_path = root.join("sources.txt");
    let now = unix_now();
    let mut registry = load_registry(&registry_path, now).await;

    let permanently_failed = results
        .iter()
        .filter_map(|(url, outcome)| match outcome {
            CollectionOutcome::PermanentlyFailed(_) => Some(url.clone()),
            _ => None,
        })
        .collect::<HashSet<_>>();

    for (url, outcome) in results {
        registry.record_outcome(url, *outcome, now);
    }

    let quarantined = registry.quarantined_sources();
    write_registry(&registry_path, &registry).await?;

    if !permanently_failed.is_empty() {
        if let Ok(content) = fs::read_to_string(&sources_path).await {
            let mut active_sources = Vec::new();
            let mut removed = 0usize;

            for line in content.lines() {
                let url = line.trim();
                if url.is_empty() {
                    continue;
                }

                if permanently_failed.contains(url) {
                    removed += 1;
                } else {
                    active_sources.push(url.to_string());
                }
            }

            if removed > 0 {
                write_sources(&sources_path, &active_sources).await?;
                println!(
                    "[INFO] 🔭 [Discovery] Permanently dead sources retired | Removed {} | Active {}",
                    removed,
                    active_sources.len()
                );
            }
        }
    }

    println!(
        "[INFO] 🔭 [Discovery] Source health updated | Checked {} | Failed {} | Permanent {} | Quarantined {}",
        results.len(),
        results
            .iter()
            .filter(|(_, outcome)| *outcome == CollectionOutcome::Failed)
            .count(),
        permanently_failed.len(),
        quarantined
    );

    Ok(quarantined)
}

async fn discover_from_repos(
    client: &Client,
    repos: &[Repository],
    token: Option<&str>,
) -> Result<Vec<Candidate>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = stream::iter(repos.iter().cloned().enumerate().map(|(repo_rank, repo)| {
        let client = client.clone();
        let token = token.map(str::to_owned);
        async move { discover_repo(&client, &repo, repo_rank, token.as_deref()).await }
    }))
    .buffer_unordered(24);

    let mut all = Vec::new();
    while let Some(result) = stream.next().await {
        match result {
            Ok(candidates) => {
                all.extend(candidates);
            }
            Err(error) if is_expected_probe_skip(&error.to_string()) => {
                let _ = error;
            }
            Err(error) => {
                println!("[WARN] 🔭 [Discovery] Repository probe failed: {error}");
            }
        }
    }

    let readme_counts =
        all.iter()
            .fold(HashMap::<String, usize>::new(), |mut counts, candidate| {
                *counts.entry(candidate.repo.clone()).or_default() += 1;
                counts
            });

    let mut tree_targets = repos
        .iter()
        .enumerate()
        .filter(|(_, repo)| readme_counts.get(&repo.name).copied().unwrap_or_default() < 5)
        .take(MAX_TREE_SCANS)
        .map(|(repo_rank, repo)| (repo_rank, repo.clone()))
        .collect::<Vec<_>>();

    let mut selected = tree_targets
        .iter()
        .map(|(_, repo)| repo.name.clone())
        .collect::<HashSet<_>>();

    if tree_targets.len() < MAX_TREE_SCANS {
        for (repo_rank, repo) in repos.iter().enumerate() {
            if tree_targets.len() >= MAX_TREE_SCANS {
                break;
            }

            if selected.insert(repo.name.clone()) {
                tree_targets.push((repo_rank, repo.clone()));
            }
        }
    }

    if !tree_targets.is_empty() {
        let mut tree_stream = stream::iter(tree_targets.into_iter().map(|(repo_rank, repo)| {
            let client = client.clone();
            let token = token.map(str::to_owned);
            async move {
                (
                    repo.clone(),
                    scan_repo_tree(&client, &repo, repo_rank, token.as_deref()).await,
                )
            }
        }))
        .buffer_unordered(8);

        let mut tree_404_skipped = 0usize;
        let mut tree_conflict_skipped = 0usize;
        let mut tree_expected_skips = 0usize;

        while let Some((repo, result)) = tree_stream.next().await {
            match result {
                Ok(candidates) => all.extend(candidates),
                Err(error) if error.to_string().contains("HTTP 404 Not Found") => {
                    tree_404_skipped += 1;
                }
                Err(error) if is_expected_tree_probe_skip(&error.to_string()) => {
                    if error.to_string().contains("HTTP 409 Conflict") {
                        tree_conflict_skipped += 1;
                    } else {
                        tree_expected_skips += 1;
                    }
                }
                Err(error) => println!(
                    "[WARN] 🔭 [Discovery] Tree probe failed for {}: {error}",
                    repo.name
                ),
            }
        }

        if tree_404_skipped > 0 || tree_conflict_skipped > 0 || tree_expected_skips > 0 {
            println!(
                "[INFO] 🔭 [Discovery] Tree probes skipped | HTTP 404: {} | HTTP 409: {} | Size limit: {}",
                tree_404_skipped,
                tree_conflict_skipped,
                tree_expected_skips
            );
        }
    }

    let mut all = deduplicate_candidates(all);
    all.truncate(MAX_DISCOVERED_CANDIDATES);

    Ok(all)
}

fn is_expected_tree_probe_skip(error: &str) -> bool {
    error.contains("HTTP 409 Conflict") || error.contains("response exceeds discovery size limit")
}

fn is_expected_probe_skip(error: &str) -> bool {
    error.contains("HTTP 404 Not Found")
        || error.contains("response exceeds discovery size limit")
        || error.contains("GitHub README exceeds discovery size limit")
}

async fn discover_repo(
    client: &Client,
    repo: &Repository,
    repo_rank: usize,
    token: Option<&str>,
) -> Result<Vec<Candidate>, Box<dyn std::error::Error + Send + Sync>> {
    let name = &repo.name;
    let branch = &repo.branch;
    let url = format!(
        "https://api.github.com/repos/{}/readme?ref={}",
        percent_encode_path(name),
        percent_encode(branch)
    );

    let response = github_get(client, &url, token).await?;
    if !response.status().is_success() {
        return Err(format!("GitHub README API returned HTTP {}", response.status()).into());
    }

    if response
        .content_length()
        .is_some_and(|length| length > MAX_README_API_RESPONSE_BYTES as u64)
    {
        return Err("GitHub README API response exceeds discovery size limit".into());
    }

    let body = read_limited_body(response, MAX_README_API_RESPONSE_BYTES).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    if payload.get("encoding").and_then(Value::as_str) != Some("base64") {
        return Err("GitHub README API response did not use base64 encoding".into());
    }

    let content = payload
        .get("content")
        .and_then(Value::as_str)
        .ok_or("GitHub README API response did not contain content")?;

    let compact = content
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    let body = STANDARD.decode(compact)?;

    if body.len() > README_MAX_BYTES {
        return Err("GitHub README exceeds discovery size limit".into());
    }

    let text = String::from_utf8_lossy(&body);
    let mut candidates = extract_source_urls(&text, name, repo_rank);
    candidates.truncate(MAX_README_CANDIDATES);

    Ok(candidates)
}
async fn scan_repo_tree(
    client: &Client,
    repo: &Repository,
    repo_rank: usize,
    token: Option<&str>,
) -> Result<Vec<Candidate>, Box<dyn std::error::Error + Send + Sync>> {
    let name = &repo.name;
    let branch = &repo.branch;
    let url = format!(
        "https://api.github.com/repos/{}/git/trees/{}?recursive=1",
        name,
        percent_encode(branch)
    );

    let response = github_get(client, &url, token).await?;
    if !response.status().is_success() {
        return Err(format!("GitHub tree API returned HTTP {}", response.status()).into());
    }

    if response
        .content_length()
        .is_some_and(|length| length > MAX_TREE_RESPONSE_BYTES as u64)
    {
        return Err("GitHub tree response exceeds discovery size limit".into());
    }

    let body = read_limited_body(response, MAX_TREE_RESPONSE_BYTES).await?;
    let text = String::from_utf8(body)?;
    let payload: Value = serde_json::from_str(&text)?;

    if payload
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err("GitHub tree response was truncated".into());
    }

    let tree = payload
        .get("tree")
        .and_then(Value::as_array)
        .ok_or("GitHub tree response did not contain a tree")?;

    let mut paths = Vec::new();

    for entry in tree {
        if entry.get("type").and_then(Value::as_str) != Some("blob") {
            continue;
        }

        let path = entry.get("path").and_then(Value::as_str).unwrap_or("");
        if is_source_path(path) {
            paths.push(path.to_string());
        }
    }

    paths.sort_by(|a, b| {
        source_path_score(b)
            .cmp(&source_path_score(a))
            .then_with(|| a.matches('/').count().cmp(&b.matches('/').count()))
            .then_with(|| a.len().cmp(&b.len()))
            .then_with(|| a.cmp(b))
    });
    paths.truncate(MAX_TREE_FILES_PER_REPO);

    Ok(paths
        .into_iter()
        .filter_map(|path| {
            let url = raw_github_file_url(
                name,
                branch,
                &path
                    .split('/')
                    .map(percent_encode)
                    .collect::<Vec<_>>()
                    .join("/"),
            );

            (url.len() <= MAX_SOURCE_URL_LENGTH).then(|| Candidate {
                url,
                repo: name.clone(),
                repo_rank,
                priority: 50,
            })
        })
        .collect())
}

fn raw_github_file_url(repository: &str, branch: &str, path: &str) -> String {
    format!(
        "https://raw.githubusercontent.com/{}/refs/heads/{}/{}",
        repository,
        percent_encode_path(branch),
        path
    )
}

async fn search_repositories(
    client: &Client,
    token: Option<&str>,
) -> Result<Vec<Repository>, Box<dyn std::error::Error + Send + Sync>> {
    let mut repos_by_name = HashMap::<String, Repository>::new();
    let run_number = env::var("GITHUB_RUN_NUMBER")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    let (sort, query_set) = search_strategy_for_run(run_number, unix_now());
    let search_queries = &SEARCH_QUERY_SETS[query_set];
    let search_query_count = search_queries
        .len()
        .min(MAX_GITHUB_SEARCH_REQUESTS_PER_RUN);
    let mut search_requests_made = 0usize;
    let mut search_rate_remaining = None;
    let mut search_rate_reset = None;
    let mut stopped_for_rate_limit = false;
    let mut last_search_started = None;

    println!(
        "[INFO] 🔭 [Discovery] Repository search strategy | Set {} | Sort {} | Queries {} | Max search requests {} | Base interval {}ms",
        SEARCH_QUERY_SET_NAMES[query_set],
        sort,
        search_query_count,
        MAX_GITHUB_SEARCH_REQUESTS_PER_RUN,
        GITHUB_SEARCH_MIN_INTERVAL_MS
    );

    for query in search_queries
        .iter()
        .copied()
        .take(MAX_GITHUB_SEARCH_REQUESTS_PER_RUN)
    {
        if search_rate_remaining.is_some_and(|value| value <= GITHUB_SEARCH_MIN_REMAINING) {
            println!(
                "[INFO] 🔭 [Discovery] Search budget exhausted for this run | Remaining {}",
                search_rate_remaining.unwrap_or_default()
            );
            stopped_for_rate_limit = true;
            break;
        }

        pace_search_request(
            &mut last_search_started,
            search_rate_remaining,
            search_rate_reset,
        )
        .await;
        search_requests_made += 1;

        let search_query = build_search_query(query, query_set, unix_now());
        let url = format!(
            "https://api.github.com/search/repositories?q={}&sort={sort}&order=desc&per_page={}",
            percent_encode(&search_query),
            SEARCH_PER_PAGE
        );

        let response = match github_get(client, &url, token).await {
            Ok(response) => response,
            Err(error) => {
                println!("[WARN] 🔭 [Discovery] GitHub search query failed after retries: {error}");
                continue;
            }
        };

        let remaining = github_search_rate_limit_remaining(&response);
        let reset = github_search_rate_limit_reset(&response);
        search_rate_remaining = remaining.or(search_rate_remaining);
        search_rate_reset = reset.or(search_rate_reset);

        if !response.status().is_success() {
            let status = response.status();
            println!(
                "[WARN] 🔭 [Discovery] GitHub repository search returned HTTP {}{}",
                status,
                remaining
                    .map(|value| format!(" | search rate remaining {}", value))
                    .unwrap_or_default()
            );

            if status == reqwest::StatusCode::FORBIDDEN
                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            {
                let reset = github_search_rate_limit_reset(&response)
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                println!(
                    "[WARN] 🔭 [Discovery] Stopping remaining repository searches after rate-limit response | Reset {}",
                    reset
                );
                stopped_for_rate_limit = true;
                break;
            }
            continue;
        }

        if response
            .content_length()
            .is_some_and(|length| length > MAX_SEARCH_RESPONSE_BYTES as u64)
        {
            println!("[WARN] 🔭 [Discovery] GitHub search response exceeds size limit");
            continue;
        }

        let stop_after_response =
            remaining.is_some_and(|value| value <= GITHUB_SEARCH_MIN_REMAINING);
        if let Some(value) = remaining {
            search_rate_remaining = Some(value);
            if value <= GITHUB_SEARCH_MIN_REMAINING + 2 {
                println!(
                    "[INFO] 🔭 [Discovery] GitHub search budget low | Remaining {}",
                    value
                );
            }
        }

        let body = match read_limited_body(response, MAX_SEARCH_RESPONSE_BYTES).await {
            Ok(body) => body,
            Err(error) => {
                println!("[WARN] 🔭 [Discovery] GitHub search response read failed: {error}");
                if stop_after_response {
                    stopped_for_rate_limit = true;
                    break;
                }
                continue;
            }
        };

        let payload: Value = match serde_json::from_slice(&body) {
            Ok(payload) => payload,
            Err(error) => {
                println!("[WARN] 🔭 [Discovery] GitHub search response parse failed: {error}");
                if stop_after_response {
                    stopped_for_rate_limit = true;
                    break;
                }
                continue;
            }
        };

        if let Some(items) = payload.get("items").and_then(Value::as_array) {
            for (search_rank, item) in items.iter().enumerate() {
                let Some(name) = item.get("full_name").and_then(Value::as_str) else {
                    continue;
                };

                let branch = item
                    .get("default_branch")
                    .and_then(Value::as_str)
                    .unwrap_or("main");

                let pushed_at = item
                    .get("pushed_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default();

                let stars = item
                    .get("stargazers_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();

                let key = name.to_string();
                if let Some(repository) = repos_by_name.get_mut(&key) {
                    repository.search_hits = repository.search_hits.saturating_add(1);
                    repository.best_search_rank = repository
                        .best_search_rank
                        .min(search_rank.min(u16::MAX as usize) as u16);
                    repository.stars = repository.stars.max(stars);
                } else {
                    repos_by_name.insert(
                        key,
                        Repository {
                            name: name.to_string(),
                            branch: branch.to_string(),
                            pushed_at: pushed_at.to_string(),
                            search_hits: 1,
                            best_search_rank: search_rank.min(u16::MAX as usize) as u16,
                            stars,
                        },
                    );
                }
            }
        }

        if stop_after_response {
            stopped_for_rate_limit = true;
            println!(
                "[INFO] 🔭 [Discovery] Stopping repository searches early to preserve search-rate headroom | Remaining {}",
                remaining.unwrap_or_default()
            );
            break;
        }
    }

    let mut repos = repos_by_name.into_values().collect::<Vec<_>>();
    sort_discovered_repositories(&mut repos);

    let multi_query_repositories = repos
        .iter()
        .filter(|repository| repository.search_hits > 1)
        .count();

    println!(
        "[INFO] 🔭 [Discovery] Repository search complete | Set {} | Requests {} | Repositories discovered {} | Multi-query repos {} | Search rate remaining {} | Stopped for rate limit {}",
        SEARCH_QUERY_SET_NAMES[query_set],
        search_requests_made,
        repos.len(),
        multi_query_repositories,
        search_rate_remaining
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        stopped_for_rate_limit
    );

    if repos.is_empty() {
        return Err("GitHub repository search produced no usable repositories".into());
    }

    Ok(repos)
}

fn sort_discovered_repositories(repositories: &mut [Repository]) {
    repositories.sort_by(|a, b| {
        b.search_hits
            .cmp(&a.search_hits)
            .then_with(|| a.best_search_rank.cmp(&b.best_search_rank))
            .then_with(|| b.pushed_at.cmp(&a.pushed_at))
            .then_with(|| b.stars.cmp(&a.stars))
            .then_with(|| a.name.cmp(&b.name))
    });
}

async fn github_get(
    client: &Client,
    url: &str,
    token: Option<&str>,
) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
    let mut attempt = 0usize;

    loop {
        let mut request = client.get(url).header("Accept", GITHUB_ACCEPT);

        if let Some(token) = token {
            request = request.bearer_auth(token);
        }

        match request.send().await {
            Ok(response)
                if is_retryable_github_response(&response) && attempt < GITHUB_REQUEST_RETRIES =>
            {
                let delay = retry_after_delay(&response, attempt);
                drop(response);
                tokio::time::sleep(delay).await;
                attempt += 1;
            }

            Ok(response) => return Ok(response),

            Err(_error) if attempt < GITHUB_REQUEST_RETRIES => {
                let delay = Duration::from_millis(
                    GITHUB_RETRY_BASE_MS.saturating_mul(1u64 << attempt.min(4)),
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }

            Err(error) => return Err(error.into()),
        }
    }
}

fn github_rate_limit_status_is_retryable(
    status: reqwest::StatusCode,
    has_retry_after: bool,
) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
        || (status == reqwest::StatusCode::FORBIDDEN && has_retry_after)
}

fn is_retryable_github_response(response: &reqwest::Response) -> bool {
    github_rate_limit_status_is_retryable(
        response.status(),
        response.headers().contains_key("retry-after"),
    )
}

fn github_search_rate_limit_remaining(response: &reqwest::Response) -> Option<u64> {
    let resource = response
        .headers()
        .get("x-ratelimit-resource")
        .and_then(|value| value.to_str().ok())?;

    if resource != "search" {
        return None;
    }

    response
        .headers()
        .get("x-ratelimit-remaining")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn github_search_rate_limit_reset(response: &reqwest::Response) -> Option<u64> {
    let resource = response
        .headers()
        .get("x-ratelimit-resource")
        .and_then(|value| value.to_str().ok())?;

    if resource != "search" {
        return None;
    }

    response
        .headers()
        .get("x-ratelimit-reset")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn retry_after_delay(response: &reqwest::Response, attempt: usize) -> Duration {
    if let Some(seconds) = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
    {
        return Duration::from_secs(seconds.min(GITHUB_RETRY_AFTER_MAX_SECS));
    }

    Duration::from_millis(GITHUB_RETRY_BASE_MS.saturating_mul(1u64 << attempt.min(4)))
}

fn extract_source_urls(text: &str, repo: &str, repo_rank: usize) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let mut seen_urls = HashSet::new();
    let mut start = 0;
    let mut scanned_urls = 0usize;

    while scanned_urls < MAX_README_URLS_SCANNED {
        let Some(relative) = text[start..].find("http") else {
            break;
        };
        let absolute = start + relative;
        scanned_urls = scanned_urls.saturating_add(1);
        let end = text[absolute..]
            .find(|character: char| {
                character.is_whitespace()
                    || matches!(
                        character,
                        '<' | '>' | '"' | '\'' | ')' | ']' | '}' | '|' | '`'
                    )
            })
            .map(|offset| absolute + offset)
            .unwrap_or(text.len());

        let raw = text[absolute..end]
            .trim()
            .trim_end_matches(&['.', ',', ';', ':', '!', '?'][..]);

        if let Some(url) = normalize_github_source(raw) {
            if likely_source_url(&url) && seen_urls.insert(url.clone()) {
                let source_repo =
                    source_repository_from_raw_url(&url).unwrap_or_else(|| repo.to_string());

                candidates.push(Candidate {
                    url,
                    repo: source_repo,
                    repo_rank,
                    priority: 100,
                });
            }
        }

        start = end;
    }

    candidates.sort_by_key(|a| std::cmp::Reverse(source_url_score(&a.url)));
    candidates.truncate(MAX_README_CANDIDATES);

    candidates
}

fn normalize_github_source(raw: &str) -> Option<String> {
    if raw.len() > MAX_SOURCE_URL_LENGTH {
        return None;
    }

    let mut value = Url::parse(raw).ok()?;

    if !matches!(value.scheme(), "http" | "https") {
        return None;
    }

    if !value.username().is_empty() || value.password().is_some() {
        return None;
    }

    let host = value.host_str()?.to_ascii_lowercase();

    if host == "raw.githubusercontent.com" {
        if value.port().is_some() {
            return None;
        }

        value.set_scheme("https").ok()?;

        let segments = value.path_segments()?.collect::<Vec<_>>();
        if segments.len() < 4 || segments.iter().any(|segment| segment.is_empty()) {
            return None;
        }

        value.set_query(None);
        value.set_fragment(None);
        let normalized = value.to_string();
        if normalized.len() > MAX_SOURCE_URL_LENGTH {
            return None;
        }
        return Some(normalized);
    }

    if host != "github.com" {
        return None;
    }

    let segments = value.path_segments()?.collect::<Vec<_>>();

    if segments.len() < 5 {
        return None;
    }

    let owner = segments[0];
    let repo = segments[1];
    let marker = segments[2];

    if !matches!(marker, "blob" | "raw") {
        return None;
    }

    let tail = segments.get(3..)?.join("/");
    if tail.is_empty() {
        return None;
    }

    if segments.get(3) == Some(&"refs") {
        if segments.get(4) != Some(&"heads") || segments.len() < 7 {
            return None;
        }

        let normalized = format!("https://raw.githubusercontent.com/{owner}/{repo}/{tail}");
        return (normalized.len() <= MAX_SOURCE_URL_LENGTH).then_some(normalized);
    }

    if segments.len() < 5 {
        return None;
    }

    let branch = segments.get(3)?;
    let path = segments.get(4..)?.join("/");

    if branch.is_empty() || path.is_empty() {
        return None;
    }

    let normalized = format!("https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}");
    (normalized.len() <= MAX_SOURCE_URL_LENGTH).then_some(normalized)
}
fn select_active_sources(
    registry: &Registry,
    new_candidates: &[Candidate],
    known_candidates: &[Candidate],
    now: u64,
    limit: usize,
) -> Vec<String> {
    let bootstrap = registry
        .proven_urls(1, &HashSet::new(), &HashSet::new())
        .is_empty();
    let new_limit = if bootstrap {
        BOOTSTRAP_NEW_ACTIVE_SOURCES
    } else {
        MAX_NEW_ACTIVE_SOURCES
    };
    let new_urls = select_new_active_urls(new_candidates, new_limit);
    let discovered_new_set = new_urls.iter().cloned().collect::<HashSet<_>>();
    let recoverable_known_urls = known_candidates
        .iter()
        .filter(|candidate| registry.is_recoverable(&candidate.url, now))
        .map(|candidate| candidate.url.clone())
        .collect::<HashSet<_>>();

    let reserved_new_slots = new_urls.len().min(limit);

    let mut active = registry.active_urls(
        limit.saturating_sub(reserved_new_slots),
        &discovered_new_set,
        &recoverable_known_urls,
    );

    let remaining_slots = limit.saturating_sub(active.len());
    active.extend(new_urls.into_iter().take(remaining_slots));
    active.truncate(limit);
    active
}

fn select_new_active_urls(candidates: &[Candidate], limit: usize) -> Vec<String> {
    if limit == 0 || candidates.is_empty() {
        return Vec::new();
    }

    let mut pool = candidates.to_vec();
    let mut selected = Vec::with_capacity(limit.min(pool.len()));
    let mut selected_urls = HashSet::new();
    let mut repo_counts = HashMap::<String, usize>::new();
    let mut family_counts = HashMap::<&'static str, usize>::new();

    while selected.len() < limit && selected.len() < pool.len() {
        let mut best_index = None;
        let mut best_key = None::<(i32, i32, i32, i32, i32, String)>;

        for (index, candidate) in pool.iter().enumerate() {
            if selected_urls.contains(&candidate.url) {
                continue;
            }

            let repo_count = repo_counts
                .get(&candidate.repo)
                .copied()
                .unwrap_or_default();
            if repo_count >= MAX_NEW_ACTIVE_SOURCES_PER_REPO {
                continue;
            }

            let family = source_path_family(&candidate.url);
            let family_count = family_counts.get(family).copied().unwrap_or_default();

            let repo_bonus = if repo_count == 0 { 250 } else { 0 };
            let family_bonus = if family_count == 0 { 40 } else { 0 };
            let diversity_penalty = (repo_count as i32 * 35) + (family_count as i32 * 8);

            let key = (
                candidate.priority as i32 * 1_000
                    + source_url_score(&candidate.url) as i32 * 10
                    + repo_bonus
                    + family_bonus
                    - diversity_penalty,
                -(candidate.repo_rank as i32),
                -(family_count as i32),
                -(repo_count as i32),
                -(candidate.url.len() as i32),
                candidate.url.clone(),
            );

            if best_key.as_ref().is_none_or(|current| key > *current) {
                best_key = Some(key);
                best_index = Some(index);
            }
        }

        let Some(index) = best_index else {
            break;
        };

        let candidate = pool.swap_remove(index);
        selected_urls.insert(candidate.url.clone());
        *repo_counts.entry(candidate.repo.clone()).or_default() += 1;
        *family_counts
            .entry(source_path_family(&candidate.url))
            .or_default() += 1;
        selected.push(candidate.url);
    }

    selected
}

fn source_path_family(url: &str) -> &'static str {
    let Some(path) = github_source_path(url) else {
        return "generic";
    };

    for token in path
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        match token.to_ascii_lowercase().as_str() {
            "vless" => return "vless",
            "vmess" => return "vmess",
            "trojan" => return "trojan",
            "shadowsocks" => return "shadowsocks",
            "hysteria" | "hysteria2" | "hy2" => return "hysteria",
            "tuic" => return "tuic",
            "reality" => return "reality",
            "sing" | "singbox" => return "singbox",
            "clash" | "mihomo" => return "clash",
            _ => {}
        }
    }

    "generic"
}

fn persisted_active_sources(registry: &Registry) -> Option<Vec<String>> {
    let active = registry.active_urls(MAX_ACTIVE_SOURCES, &HashSet::new(), &HashSet::new());
    (!active.is_empty()).then_some(active)
}
fn select_known_refresh_candidates(
    discovered: &[Candidate],
    registry: &Registry,
    now: u64,
) -> Vec<Candidate> {
    let mut selected = Vec::with_capacity(MAX_KNOWN_REFRESH_SOURCES);
    let mut selected_urls = HashSet::new();

    for candidate in discovered {
        if !registry.sources().contains_key(&candidate.url) {
            continue;
        }

        let recoverable = registry.is_recoverable(&candidate.url, now);

        if recoverable {
            selected.push(candidate.clone());
            selected_urls.insert(candidate.url.clone());

            if selected.len() >= MAX_KNOWN_REFRESH_SOURCES {
                return selected;
            }
        }
    }

    for candidate in discovered {
        if selected.len() >= MAX_KNOWN_REFRESH_SOURCES
            || !registry.sources().contains_key(&candidate.url)
            || !selected_urls.insert(candidate.url.clone())
        {
            continue;
        }

        selected.push(candidate.clone());
    }

    selected
}

#[cfg(test)]
fn discovered_repository_names(candidates: &[Candidate], repos: &[Repository]) -> HashSet<String> {
    candidates
        .iter()
        .filter_map(|candidate| repos.get(candidate.repo_rank))
        .map(|repo| repo.name.clone())
        .collect()
}

fn deduplicate_candidates(candidates: Vec<Candidate>) -> Vec<Candidate> {
    let mut unique = HashMap::<String, Candidate>::new();

    for candidate in candidates {
        unique
            .entry(candidate.url.clone())
            .and_modify(|existing| {
                if candidate.priority > existing.priority
                    || (candidate.priority == existing.priority
                        && candidate.repo_rank < existing.repo_rank)
                {
                    *existing = candidate.clone();
                }
            })
            .or_insert(candidate);
    }

    let mut candidates = unique.into_values().collect::<Vec<_>>();
    candidates.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| a.repo_rank.cmp(&b.repo_rank))
            .then_with(|| a.repo.cmp(&b.repo))
            .then_with(|| a.url.cmp(&b.url))
    });
    candidates
}

fn source_repository_from_raw_url(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    if parsed.host_str()? != "raw.githubusercontent.com" {
        return None;
    }

    let mut segments = parsed.path_segments()?;
    let owner = segments.next()?;
    let repo = segments.next()?;

    if owner.is_empty() || repo.is_empty() {
        return None;
    }

    Some(format!("{owner}/{repo}"))
}

fn has_path_hint(path: &str) -> bool {
    path.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .any(|segment| PATH_HINTS.contains(&segment))
}

fn has_strong_path_hint(path: &str) -> bool {
    path.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .any(|segment| STRONG_PATH_HINTS.contains(&segment))
}

fn has_noise_path_token(path: &str) -> bool {
    path.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .any(|segment| NOISE_PATH_TOKENS.contains(&segment))
}

fn obvious_non_source_filename(path: &str) -> bool {
    let filename = path.rsplit('/').next().unwrap_or(path);

    OBVIOUS_NON_SOURCE_FILENAMES.contains(&filename)
        || filename.contains(".schema.")
        || filename.starts_with("schema.")
        || filename.ends_with(".schema.json")
}

fn source_path_score(path: &str) -> u8 {
    let lowered = path.trim_start_matches('/').to_ascii_lowercase();

    if lowered.is_empty()
        || NOISE_HINTS.iter().any(|hint| lowered.contains(hint))
        || has_noise_path_token(&lowered)
        || obvious_non_source_filename(&lowered)
    {
        return 0;
    }

    let filename = lowered.rsplit('/').next().unwrap_or("");

    let extension_ok = SOURCE_EXTENSIONS
        .iter()
        .any(|extension| lowered.ends_with(extension));
    let extension_without_hint = SOURCE_EXTENSIONS_WITHOUT_HINT
        .iter()
        .any(|extension| lowered.ends_with(extension));
    let has_extension = filename.contains('.');
    let hint_ok = has_path_hint(&lowered);

    let plausible =
        (extension_ok && (extension_without_hint || hint_ok)) || (!has_extension && hint_ok);
    if !plausible {
        return 0;
    }

    let mut score: u8 = if extension_without_hint { 70 } else { 50 };

    if has_strong_path_hint(&lowered) {
        score += 30;
    }

    match filename.split('.').next().unwrap_or(filename) {
        "all" | "full" | "sub" | "subs" | "subscription" | "subscriptions" | "nodes"
        | "proxies" | "servers" => score += 10,
        "config" | "settings" | "data" => score = score.saturating_sub(10),
        _ => {}
    }

    score
}

fn is_legacy_noise_source(url: &str) -> bool {
    if is_self_source(url) {
        return true;
    }

    let Ok(parsed) = Url::parse(url) else {
        return false;
    };

    let segments = parsed
        .path_segments()
        .map(|segments| segments.collect::<Vec<_>>());

    let Some(segments) = segments else {
        return false;
    };

    let source_start = if parsed.host_str() == Some("raw.githubusercontent.com")
        && segments.get(2) == Some(&"refs")
        && segments.get(3) == Some(&"heads")
    {
        5
    } else {
        3
    };

    let Some(source_segments) = segments.get(source_start..) else {
        return false;
    };

    has_noise_path_token(&format!("/{}", source_segments.join("/")).to_ascii_lowercase())
}

fn github_source_path(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    if !matches!(
        parsed.host_str(),
        Some("github.com") | Some("raw.githubusercontent.com")
    ) {
        return None;
    }

    let segments = parsed
        .path_segments()
        .map(|segments| segments.collect::<Vec<_>>())?;

    let source_start = if parsed.host_str() == Some("raw.githubusercontent.com")
        && segments.get(2) == Some(&"refs")
        && segments.get(3) == Some(&"heads")
    {
        5
    } else {
        3
    };

    let source_segments = segments.get(source_start..)?;
    (!source_segments.is_empty()).then(|| format!("/{}", source_segments.join("/")))
}

fn source_url_score(url: &str) -> u8 {
    github_source_path(url)
        .map(|path| source_path_score(&path))
        .unwrap_or_default()
}

fn likely_source_url(url: &str) -> bool {
    source_url_score(url) > 0
}

fn is_source_path(path: &str) -> bool {
    source_path_score(path) > 0
}

fn percent_encode_path(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(value.len());

    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            output.push(*byte as char);
        } else {
            output.push('%');
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0F) as usize] as char);
        }
    }

    output
}

fn percent_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(value.len());

    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(*byte as char);
        } else {
            output.push('%');
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0F) as usize] as char);
        }
    }

    output
}

async fn read_limited_body(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut response = response;
    let mut body = Vec::with_capacity(max_bytes.min(64 * 1024));

    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err("GitHub discovery response exceeds size limit".into());
        }
        body.extend_from_slice(&chunk);
    }

    Ok(body)
}

async fn load_registry(path: &Path, now: u64) -> Registry {
    match fs::read_to_string(path).await {
        Ok(text) => match serde_json::from_str::<Value>(&text) {
            Ok(value) => Registry::from_value(value, now),
            Err(_) => Registry::new(now),
        },
        Err(_) => Registry::new(now),
    }
}

async fn write_registry(
    path: &Path,
    registry: &Registry,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let data = serde_json::to_string_pretty(&registry.json())?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, format!("{data}\n")).await?;
    fs::rename(&temporary, path).await?;
    Ok(())
}

async fn write_sources(
    path: &Path,
    urls: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let temporary = path.with_file_name(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("sources")
    ));
    fs::write(&temporary, format!("{}\n", urls.join("\n"))).await?;
    if let Err(error) = fs::rename(&temporary, path).await {
        let _ = fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(())
}

fn project_root() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let cwd = env::current_dir()?;

    if cwd.join("Cargo.toml").is_file() {
        return Ok(cwd);
    }

    let exe = env::current_exe()?;

    for ancestor in exe.ancestors() {
        if ancestor.join("Cargo.toml").is_file() {
            return Ok(ancestor.to_path_buf());
        }
    }

    Err("could not locate ProxyRift project root".into())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #[test]
    fn discovery_search_alternates_sort_by_run_number() {
        assert_eq!(search_sort_for_run(Some(100), 0), "updated");
        assert_eq!(search_sort_for_run(Some(101), 0), "stars");
        assert_eq!(search_sort_for_run(Some(102), 0), "updated");
        assert_eq!(search_query_set_for_run(Some(100), 0), 1);
        assert_eq!(search_query_set_for_run(Some(101), 0), 2);
        assert_eq!(search_query_set_for_run(Some(102), 0), 0);
    }

    #[test]
    fn discovery_search_fallback_alternates_by_hour() {
        assert_eq!(search_sort_for_run(None, 0), "updated");
        assert_eq!(search_sort_for_run(None, 3_600), "stars");
        assert_eq!(search_sort_for_run(None, 7_200), "updated");
    }

    #[test]
    fn discovery_search_strategy_rotates_query_lenses() {
        assert_eq!(search_strategy_for_run(Some(100), 0), ("updated", 1));
        assert_eq!(search_strategy_for_run(Some(101), 0), ("updated", 2));
        assert_eq!(search_strategy_for_run(Some(102), 0), ("updated", 0));
        assert_eq!(search_strategy_for_run(Some(103), 0), ("stars", 1));
    }

    #[test]
    fn fresh_search_query_targets_recent_low_star_repositories() {
        assert_eq!(
            build_search_query("vless nodes", 2, 1_750_000_000),
            "vless nodes archived:false fork:false is:public in:name,description,readme stars:0..100 pushed:>2025-02-15"
        );
    }

    #[test]
    fn unix_epoch_date_conversion_is_stable() {
        assert_eq!(unix_days_to_ymd(0), (1970, 1, 1));
        assert_eq!(unix_days_to_ymd(-1), (1969, 12, 31));
    }

    use super::{
        build_search_query, extract_source_urls, is_source_path, likely_source_url,
        normalize_github_source, percent_encode_path, search_query_set_for_run,
        search_sort_for_run, search_strategy_for_run, select_new_active_urls, source_path_family,
        unix_days_to_ymd, Candidate, CollectionOutcome, Registry, Repository, Value,
        MAX_ACTIVE_SOURCES, MAX_DISCOVERED_CANDIDATES, MAX_EMPTY_STREAK, MAX_FAILURE_STREAK,
        MAX_KNOWN_REFRESH_SOURCES, MAX_SOURCE_URL_LENGTH, RETIRED_SOURCE_COOLDOWN_SECS, STANDARD,
    };
    use base64::Engine as _;
    use std::collections::HashSet;

    #[test]
    fn discovered_repository_sort_prefers_multi_query_evidence() {
        let mut repositories = vec![
            Repository {
                name: "fresh/repo".to_string(),
                branch: "main".to_string(),
                pushed_at: "2026-10-04T22:00:00Z".to_string(),
                search_hits: 1,
                best_search_rank: 0,
                stars: 1_000,
            },
            Repository {
                name: "corroborated/repo".to_string(),
                branch: "main".to_string(),
                pushed_at: "2026-10-01T22:00:00Z".to_string(),
                search_hits: 3,
                best_search_rank: 10,
                stars: 1,
            },
            Repository {
                name: "ranked/repo".to_string(),
                branch: "main".to_string(),
                pushed_at: "2026-10-03T22:00:00Z".to_string(),
                search_hits: 2,
                best_search_rank: 2,
                stars: 10,
            },
        ];

        super::sort_discovered_repositories(&mut repositories);

        assert_eq!(repositories[0].name, "corroborated/repo");
        assert_eq!(repositories[1].name, "ranked/repo");
        assert_eq!(repositories[2].name, "fresh/repo");
    }

    #[test]
    fn new_active_selection_spreads_across_repositories() {
        let candidates = (0..4)
            .flat_map(|repo_index| {
                (0..4).map(move |source_index| Candidate {
                    url: format!(
                        "https://raw.githubusercontent.com/example/repo-{repo_index}/main/subscriptions/vless-{source_index}.txt"
                    ),
                    repo: format!("example/repo-{repo_index}"),
                    repo_rank: repo_index,
                    priority: 100,
                })
            })
            .collect::<Vec<_>>();

        let selected = select_new_active_urls(&candidates, 8);
        let repos = selected
            .iter()
            .filter_map(|url| super::source_repository_from_raw_url(url))
            .collect::<HashSet<_>>();

        assert_eq!(selected.len(), 8);
        assert!(repos.len() >= 2);
    }

    #[test]
    fn source_path_family_detects_protocol_and_generic_paths() {
        assert_eq!(
            source_path_family(
                "https://raw.githubusercontent.com/example/repo/main/subscriptions/vless.txt"
            ),
            "vless"
        );
        assert_eq!(
            source_path_family(
                "https://raw.githubusercontent.com/example/repo/main/subscriptions/all.txt"
            ),
            "generic"
        );
    }

    #[test]
    fn permanent_source_failures_are_quarantined_immediately() {
        let mut registry = Registry::new(1);
        let candidate = Candidate {
            url: "source-gone".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&candidate, 1);
        registry.record_outcome(&candidate.url, CollectionOutcome::PermanentlyFailed(404), 2);

        let record = registry.sources().get(&candidate.url).expect("record");
        assert_eq!(record["failure_streak"], MAX_FAILURE_STREAK);
        assert_eq!(record["last_http_status"], 404);
        assert_eq!(record["permanently_failed"], true);
        assert_eq!(record["retired_at"], 2u64);
        assert!(registry
            .active_urls(1, &HashSet::new(), &HashSet::new())
            .is_empty());
    }

    #[test]
    fn empty_success_is_not_counted_as_healthy_success() {
        let mut registry = Registry::new(1);
        let candidate = Candidate {
            url: "source-empty".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&candidate, 1);
        registry.record_outcome(&candidate.url, CollectionOutcome::Failed, 2);
        registry.record_outcome(&candidate.url, CollectionOutcome::Success(0), 3);

        let record = registry.sources().get("source-empty").expect("record");
        assert_eq!(record["failure_streak"], 0);
        assert_eq!(record["empty_streak"], 1);
        assert_eq!(record["empty_runs"], 1);
        assert_eq!(record["successes"], 0);
        assert!(record.get("last_success").is_none());
    }

    #[test]
    fn repeated_empty_runs_are_quarantined() {
        let mut registry = Registry::new(1);
        let candidate = Candidate {
            url: "source-empty".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&candidate, 1);
        for now in 2..=(MAX_EMPTY_STREAK + 1) {
            registry.record_outcome(&candidate.url, CollectionOutcome::Success(0), now);
        }

        assert!(registry
            .active_urls(1, &HashSet::new(), &HashSet::new())
            .is_empty());
    }

    #[test]
    fn proven_sources_are_preferred_over_rotation() {
        let mut registry = Registry::new(1);
        let proven = Candidate {
            url: "source-proven".to_string(),
            repo: "example/proven".to_string(),
            repo_rank: 0,
            priority: 100,
        };
        registry.add_candidate(&proven, 1);
        registry.record_outcome(&proven.url, CollectionOutcome::Success(500), 2);
        registry.record_outcome(&proven.url, CollectionOutcome::Success(600), 3);
        registry
            .sources_mut()
            .get_mut(&proven.url)
            .and_then(Value::as_object_mut)
            .expect("proven source exists")
            .insert("transport_reachable_last_run".into(), Value::from(100u64));

        let active = super::select_active_sources(&registry, &[], &[], 4, 1);

        assert_eq!(active, vec![proven.url]);
    }

    #[test]
    fn newly_discovered_sources_get_exploration_slots_without_displacing_proven_core() {
        let mut registry = Registry::new(1);
        let proven = Candidate {
            url: "source-proven".to_string(),
            repo: "example/proven".to_string(),
            repo_rank: 0,
            priority: 100,
        };
        registry.add_candidate(&proven, 1);
        registry.record_outcome(&proven.url, CollectionOutcome::Success(500), 2);
        registry.record_outcome(&proven.url, CollectionOutcome::Success(600), 3);

        let new_sources = (0..3)
            .map(|index| Candidate {
                url: format!("source-new-{index}"),
                repo: format!("example/new-{index}"),
                repo_rank: index + 1,
                priority: 100,
            })
            .collect::<Vec<_>>();

        for candidate in &new_sources {
            registry.add_candidate(candidate, 3);
        }

        let active = super::select_active_sources(&registry, &new_sources, &[], 4, 4);

        assert!(active.contains(&proven.url));
        for candidate in &new_sources {
            assert!(active.contains(&candidate.url));
        }
    }

    #[test]
    fn collection_only_sources_are_not_proven() {
        let mut registry = Registry::new(1);
        let source = Candidate {
            url: "source-collection-only".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&source, 1);
        registry.record_outcome(&source.url, CollectionOutcome::Success(500), 2);
        registry.record_outcome(&source.url, CollectionOutcome::Success(600), 3);

        assert!(registry
            .proven_urls(1, &HashSet::new(), &HashSet::new())
            .is_empty());
    }

    #[test]
    fn low_transport_sources_are_not_proven() {
        let mut registry = Registry::new(1);
        let source = Candidate {
            url: "source-low-transport".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&source, 1);
        registry.record_outcome(&source.url, CollectionOutcome::Success(500), 2);
        registry.record_outcome(&source.url, CollectionOutcome::Success(600), 3);
        registry
            .sources_mut()
            .get_mut(&source.url)
            .and_then(Value::as_object_mut)
            .expect("source exists")
            .insert("transport_reachable_last_run".into(), Value::from(10u64));

        assert!(registry
            .proven_urls(1, &HashSet::new(), &HashSet::new())
            .is_empty());
    }

    #[test]
    fn retained_transport_sample_is_used_for_quality() {
        let mut registry = Registry::new(1);
        let source = Candidate {
            url: "source-retained-sample".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&source, 1);
        registry.record_outcome(&source.url, CollectionOutcome::Success(10_000), 2);
        registry.record_outcome(&source.url, CollectionOutcome::Success(10_000), 3);
        registry
            .sources_mut()
            .get_mut(&source.url)
            .and_then(Value::as_object_mut)
            .expect("source exists")
            .insert("transport_tested_last_run".into(), Value::from(250u64));
        registry
            .sources_mut()
            .get_mut(&source.url)
            .and_then(Value::as_object_mut)
            .expect("source exists")
            .insert("transport_reachable_last_run".into(), Value::from(20u64));

        assert!(super::source_has_meaningful_transport_history(
            registry.sources().get(&source.url).expect("source exists")
        ));
    }

    #[test]
    fn self_sources_are_never_active() {
        let mut registry = Registry::new(1);
        let source = Candidate {
            url: "https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all.txt"
                .to_string(),
            repo: "rekt0ro/ProxyRift".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&source, 1);
        registry.record_outcome(&source.url, CollectionOutcome::Success(500), 2);
        registry.record_outcome(&source.url, CollectionOutcome::Success(600), 3);
        registry
            .sources_mut()
            .get_mut(&source.url)
            .and_then(Value::as_object_mut)
            .expect("source exists")
            .insert("transport_reachable_last_run".into(), Value::from(500u64));

        assert!(registry
            .active_urls(1, &HashSet::new(), &HashSet::new())
            .is_empty());
    }

    #[test]
    fn checked_sources_are_ranked_by_transport_quality() {
        let mut registry = Registry::new(1);

        for (name, reachable) in [("source-good", 400u64), ("source-bad", 20u64)] {
            let candidate = Candidate {
                url: name.to_string(),
                repo: "example/repo".to_string(),
                repo_rank: 0,
                priority: 100,
            };
            registry.add_candidate(&candidate, 1);
            registry.record_outcome(&candidate.url, CollectionOutcome::Success(500), 2);
            registry.record_outcome(&candidate.url, CollectionOutcome::Success(500), 3);
            registry
                .sources_mut()
                .get_mut(name)
                .and_then(Value::as_object_mut)
                .expect("source exists")
                .insert(
                    "transport_reachable_last_run".into(),
                    Value::from(reachable),
                );
        }

        let active = registry.active_urls(2, &HashSet::new(), &HashSet::new());
        assert_eq!(active, vec!["source-good".to_string()]);
    }

    #[test]
    fn new_sources_get_a_fixed_exploration_slot() {
        let mut registry = Registry::new(1);
        let healthy = Candidate {
            url: "source-healthy".to_string(),
            repo: "example/healthy".to_string(),
            repo_rank: 0,
            priority: 100,
        };
        let new_source = Candidate {
            url: "source-new".to_string(),
            repo: "example/new".to_string(),
            repo_rank: 1,
            priority: 100,
        };

        registry.add_candidate(&healthy, 1);
        registry.record_outcome(&healthy.url, CollectionOutcome::Success(1), 2);
        let active =
            super::select_active_sources(&registry, std::slice::from_ref(&new_source), &[], 3, 2);

        assert!(active.contains(&healthy.url));
        assert!(active.contains(&new_source.url));
    }

    #[test]
    fn new_sources_replace_quarantined_slots_even_when_active_pool_is_full() {
        let mut registry = Registry::new(1);
        let healthy = (0..3)
            .map(|index| Candidate {
                url: format!("source-healthy-{index}"),
                repo: format!("example/healthy-{index}"),
                repo_rank: index,
                priority: 100,
            })
            .collect::<Vec<_>>();
        let quarantined = Candidate {
            url: "source-quarantined".to_string(),
            repo: "example/quarantined".to_string(),
            repo_rank: 3,
            priority: 100,
        };
        let new_source = Candidate {
            url: "source-new".to_string(),
            repo: "example/new".to_string(),
            repo_rank: 4,
            priority: 100,
        };

        for candidate in &healthy {
            registry.add_candidate(candidate, 1);
            registry.record_outcome(&candidate.url, CollectionOutcome::Success(1), 2);
        }
        registry.add_candidate(&quarantined, 1);
        for now in 2..=(MAX_EMPTY_STREAK + 1) {
            registry.record_outcome(&quarantined.url, CollectionOutcome::Success(0), now);
        }

        let active =
            super::select_active_sources(&registry, std::slice::from_ref(&new_source), &[], 10, 3);

        assert_eq!(active.len(), 3);
        assert!(active.contains(&new_source.url));
        assert!(!active.contains(&quarantined.url));
        assert_eq!(
            active
                .iter()
                .filter(|url| healthy.iter().any(|candidate| &candidate.url == *url))
                .count(),
            2
        );
    }

    #[test]
    fn new_sources_fill_quarantined_slots() {
        let mut registry = Registry::new(1);
        let healthy = Candidate {
            url: "source-healthy".to_string(),
            repo: "example/healthy".to_string(),
            repo_rank: 0,
            priority: 100,
        };
        let empty = Candidate {
            url: "source-empty".to_string(),
            repo: "example/empty".to_string(),
            repo_rank: 1,
            priority: 100,
        };
        let new_source = Candidate {
            url: "source-new".to_string(),
            repo: "example/new".to_string(),
            repo_rank: 2,
            priority: 100,
        };

        registry.add_candidate(&healthy, 1);
        registry.record_outcome(&healthy.url, CollectionOutcome::Success(1), 2);
        registry.add_candidate(&empty, 1);
        for now in 2..=(MAX_EMPTY_STREAK + 1) {
            registry.record_outcome(&empty.url, CollectionOutcome::Success(0), now);
        }

        let active =
            super::select_active_sources(&registry, std::slice::from_ref(&new_source), &[], 10, 2);

        assert!(active.contains(&healthy.url));
        assert!(active.contains(&new_source.url));
        assert!(!active.contains(&empty.url));
    }

    #[test]
    fn legacy_noise_sources_are_excluded_from_active_selection() {
        let mut registry = Registry::new(1);
        let noisy = Candidate {
            url: "https://raw.githubusercontent.com/example/repo/main/archive/all_broken.txt"
                .to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };
        let healthy = Candidate {
            url: "https://raw.githubusercontent.com/example/repo/main/subscriptions/all.txt"
                .to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 1,
            priority: 100,
        };

        registry.add_candidate(&noisy, 1);
        registry.record_outcome(&noisy.url, CollectionOutcome::Success(3), 2);
        registry.add_candidate(&healthy, 1);
        registry.record_outcome(&healthy.url, CollectionOutcome::Success(3), 2);

        let active = registry.active_urls(10, &HashSet::new(), &HashSet::new());

        assert_eq!(active, vec![healthy.url]);
        assert_eq!(registry.sources().len(), 2);
    }

    #[test]
    fn cross_repository_readme_links_preserve_tree_fallback_for_target_repo() {
        let repos = vec![
            Repository {
                name: "reader/repo".to_string(),
                branch: "main".to_string(),
                pushed_at: String::new(),
                search_hits: 1,
                best_search_rank: 0,
                stars: 0,
            },
            Repository {
                name: "source/repo".to_string(),
                branch: "main".to_string(),
                pushed_at: String::new(),
                search_hits: 1,
                best_search_rank: 1,
                stars: 0,
            },
        ];
        let candidates = vec![Candidate {
            url: "https://raw.githubusercontent.com/source/repo/main/subscriptions/all.txt"
                .to_string(),
            repo: "source/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        }];

        let discovered = super::discovered_repository_names(&candidates, &repos);

        assert!(discovered.contains("reader/repo"));
        assert!(!discovered.contains("source/repo"));
    }

    #[test]
    fn discovered_candidate_cap_is_applied_after_deduplication() {
        let mut candidates = Vec::new();
        for rank in 0..(MAX_DISCOVERED_CANDIDATES + 10) {
            candidates.push(Candidate {
                url: "https://raw.githubusercontent.com/example/project/main/subscriptions/all.txt"
                    .to_string(),
                repo: format!("example/repo-{rank}"),
                repo_rank: rank,
                priority: 100,
            });
        }
        candidates.push(Candidate {
            url: "https://raw.githubusercontent.com/example/unique/main/subscriptions/all.txt"
                .to_string(),
            repo: "example/unique".to_string(),
            repo_rank: MAX_DISCOVERED_CANDIDATES + 10,
            priority: 100,
        });

        let mut deduplicated = super::deduplicate_candidates(candidates);
        deduplicated.truncate(super::MAX_DISCOVERED_CANDIDATES);

        assert_eq!(deduplicated.len(), 2);
        assert_eq!(
            deduplicated[0].url,
            "https://raw.githubusercontent.com/example/project/main/subscriptions/all.txt"
        );
        assert_eq!(
            deduplicated[1].url,
            "https://raw.githubusercontent.com/example/unique/main/subscriptions/all.txt"
        );
    }

    #[test]
    fn recoverable_retired_sources_are_prioritized_for_refresh() {
        let mut registry = Registry::new(1);
        let retired = Candidate {
            url: "https://raw.githubusercontent.com/example/retired/sub.txt".to_string(),
            repo: "example/retired".to_string(),
            repo_rank: 900,
            priority: 100,
        };

        registry.add_candidate(&retired, 1);
        for _ in 0..MAX_FAILURE_STREAK {
            registry.record_result(&retired.url, 0, 1);
        }

        let mut discovered = Vec::new();
        for rank in 0..MAX_KNOWN_REFRESH_SOURCES {
            let candidate = Candidate {
                url: format!("https://raw.githubusercontent.com/example/known/{rank}.sub"),
                repo: format!("example/known-{rank}"),
                repo_rank: rank,
                priority: 100,
            };
            registry.add_candidate(&candidate, 1);
            discovered.push(candidate);
        }
        discovered.push(retired.clone());

        let selected = super::select_known_refresh_candidates(
            &discovered,
            &registry,
            RETIRED_SOURCE_COOLDOWN_SECS + 1,
        );

        assert_eq!(selected.len(), MAX_KNOWN_REFRESH_SOURCES);
        assert!(selected
            .iter()
            .any(|candidate| candidate.url == retired.url));
    }

    #[test]
    fn falls_back_to_persisted_active_sources_when_discovery_is_empty() {
        let mut registry = Registry::new(1);
        let candidate = Candidate {
            url: "source-a".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&candidate, 1);
        registry.record_result(&candidate.url, 1, 2);

        assert_eq!(
            super::persisted_active_sources(&registry),
            Some(vec!["source-a".to_string()])
        );
    }

    #[test]
    fn persisted_active_sources_excludes_retired_sources() {
        let mut registry = Registry::new(1);
        let candidate = Candidate {
            url: "source-retired".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&candidate, 1);
        for _ in 0..MAX_FAILURE_STREAK {
            registry.record_result(&candidate.url, 0, 1);
        }

        assert!(super::persisted_active_sources(&registry).is_none());
    }

    #[test]
    fn decodes_github_readme_base64_with_whitespace() {
        let encoded =
            STANDARD.encode("https://github.com/example/project/blob/main/subscriptions/all.txt");
        let wrapped = encoded.replace("Y", "Y\n");
        let compact = wrapped
            .bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect::<Vec<_>>();

        assert_eq!(
            STANDARD.decode(compact).unwrap(),
            b"https://github.com/example/project/blob/main/subscriptions/all.txt"
        );
    }

    #[test]
    fn discovery_client_policy_disables_automatic_redirects() {
        let policy = reqwest::redirect::Policy::none();
        let _ = policy;
    }

    #[test]
    fn github_forbidden_with_retry_after_is_retryable() {
        assert!(super::github_rate_limit_status_is_retryable(
            reqwest::StatusCode::FORBIDDEN,
            true
        ));
        assert!(!super::github_rate_limit_status_is_retryable(
            reqwest::StatusCode::FORBIDDEN,
            false
        ));
        assert!(super::github_rate_limit_status_is_retryable(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            false
        ));
    }

    #[test]
    fn expected_discovery_probe_skips_are_not_failures() {
        assert!(super::is_expected_probe_skip(
            "GitHub README API returned HTTP 404 Not Found"
        ));
        assert!(super::is_expected_probe_skip(
            "GitHub README API response exceeds discovery size limit"
        ));
        assert!(super::is_expected_probe_skip(
            "GitHub README exceeds discovery size limit"
        ));
        assert!(super::is_expected_tree_probe_skip(
            "GitHub tree response exceeds discovery size limit"
        ));
        assert!(super::is_expected_tree_probe_skip(
            "GitHub tree API returned HTTP 409 Conflict: another conflict"
        ));
        assert!(!super::is_expected_probe_skip(
            "GitHub README API response parse failed"
        ));
    }

    #[test]
    fn repository_search_requires_public_repositories() {
        let search_query = format!(
            "{} archived:false fork:false is:public",
            "v2ray subscription"
        );
        assert!(search_query.contains("is:public"));
    }

    #[test]
    fn retired_sources_require_rediscovery_after_cooldown() {
        let mut registry = Registry::new(1);
        let candidate = Candidate {
            url: "source-a".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&candidate, 1);
        for _ in 0..MAX_FAILURE_STREAK {
            registry.record_result("source-a", 0, 1);
        }

        registry.add_candidate(&candidate, RETIRED_SOURCE_COOLDOWN_SECS);
        assert!(registry
            .active_urls(1, &HashSet::new(), &HashSet::new())
            .is_empty());

        assert!(registry
            .active_urls(1, &HashSet::new(), &HashSet::new())
            .is_empty());

        assert_eq!(
            registry
                .sources()
                .get("source-a")
                .and_then(|record| record.get("failure_streak"))
                .and_then(Value::as_u64),
            Some(MAX_FAILURE_STREAK)
        );

        let recovery = HashSet::from(["source-a".to_string()]);
        assert_eq!(
            registry.active_urls(1, &HashSet::new(), &recovery),
            vec!["source-a".to_string()]
        );
    }

    #[test]
    fn rediscovery_does_not_clear_retired_health_before_collection() {
        let mut registry = Registry::new(1);
        let candidate = Candidate {
            url: "source-retired".to_string(),
            repo: "example/repo".to_string(),
            repo_rank: 0,
            priority: 100,
        };

        registry.add_candidate(&candidate, 1);
        for _ in 0..MAX_FAILURE_STREAK {
            registry.record_result(&candidate.url, 0, 1);
        }

        registry.add_candidate(&candidate, RETIRED_SOURCE_COOLDOWN_SECS + 1);

        assert_eq!(
            registry
                .sources()
                .get(&candidate.url)
                .and_then(|record| record.get("failure_streak"))
                .and_then(Value::as_u64),
            Some(MAX_FAILURE_STREAK)
        );
        assert_eq!(
            registry.active_urls(1, &HashSet::new(), &HashSet::from([candidate.url.clone()])),
            vec![candidate.url]
        );
    }

    #[test]
    fn preserves_oldest_source_rotation_order() {
        let mut registry = Registry::new(100);

        for (index, url) in ["source-a", "source-b", "source-c"].into_iter().enumerate() {
            registry.add_candidate(
                &Candidate {
                    url: url.to_string(),
                    repo: "example/repo".to_string(),
                    repo_rank: 0,
                    priority: 100,
                },
                100,
            );
            registry
                .sources_mut()
                .get_mut(url)
                .and_then(Value::as_object_mut)
                .expect("source record exists")
                .insert("last_checked".into(), Value::from(index as u64 + 1));
        }

        assert_eq!(
            registry.active_urls(MAX_ACTIVE_SOURCES, &HashSet::new(), &HashSet::new()),
            vec![
                "source-a".to_string(),
                "source-b".to_string(),
                "source-c".to_string()
            ]
        );
    }

    #[test]
    fn limits_unchecked_sources_during_rotation() {
        let mut registry = Registry::new(1);

        for (index, url) in ["source-a", "source-b", "source-c", "source-d"]
            .into_iter()
            .enumerate()
        {
            registry.add_candidate(
                &Candidate {
                    url: url.to_string(),
                    repo: "example/repo".to_string(),
                    repo_rank: 0,
                    priority: 100,
                },
                index as u64 + 1,
            );
        }

        registry
            .sources_mut()
            .get_mut("source-c")
            .and_then(Value::as_object_mut)
            .expect("source-c exists")
            .insert("last_checked".into(), Value::from(10u64));

        registry
            .sources_mut()
            .get_mut("source-d")
            .and_then(Value::as_object_mut)
            .expect("source-d exists")
            .insert("last_checked".into(), Value::from(20u64));

        let active = registry.active_urls(3, &HashSet::new(), &HashSet::new());

        assert_eq!(
            active,
            vec![
                "source-c".to_string(),
                "source-a".to_string(),
                "source-b".to_string(),
            ]
        );
    }

    #[test]
    fn prunes_registry_beyond_capacity() {
        let mut registry = Registry::new(1);
        let urls = ["source-a", "source-b", "source-c"];

        for (index, url) in urls.into_iter().enumerate() {
            let now = index as u64 + 1;
            registry.add_candidate(
                &Candidate {
                    url: url.to_string(),
                    repo: "example/repo".to_string(),
                    repo_rank: 0,
                    priority: 100,
                },
                now,
            );
            registry
                .sources_mut()
                .get_mut(url)
                .and_then(Value::as_object_mut)
                .expect("source record exists")
                .insert("last_discovered".into(), Value::from(now));
        }

        let protected = HashSet::from(["source-a".to_string()]);
        assert_eq!(registry.prune(2, &protected), 1);
        assert!(registry.sources().contains_key("source-a"));
        assert!(registry.sources().contains_key("source-c"));
        assert!(!registry.sources().contains_key("source-b"));
    }

    #[test]
    fn fills_remaining_rotation_slots_when_unchecked_pool_is_small() {
        let mut registry = Registry::new(1);

        for (index, url) in ["source-a", "source-b", "source-c", "source-d"]
            .into_iter()
            .enumerate()
        {
            let now = index as u64 + 1;
            registry.add_candidate(
                &Candidate {
                    url: url.to_string(),
                    repo: "example/repo".to_string(),
                    repo_rank: 0,
                    priority: 100,
                },
                now,
            );
        }

        for (url, last_checked) in [("source-a", 10u64), ("source-b", 20u64)] {
            registry
                .sources_mut()
                .get_mut(url)
                .and_then(Value::as_object_mut)
                .expect("source record exists")
                .insert("last_checked".into(), Value::from(last_checked));
        }

        let active = registry.active_urls(4, &HashSet::new(), &HashSet::new());

        assert_eq!(active.len(), 4);
        assert_eq!(
            active,
            vec![
                "source-a".to_string(),
                "source-b".to_string(),
                "source-c".to_string(),
                "source-d".to_string(),
            ]
        );
    }

    #[test]
    fn normalizes_http_raw_github_to_https() {
        assert_eq!(
            normalize_github_source(
                "http://raw.githubusercontent.com/example/project/main/subscriptions/all.txt"
            )
            .as_deref(),
            Some("https://raw.githubusercontent.com/example/project/main/subscriptions/all.txt")
        );
    }

    #[test]
    fn normalizes_github_blob_to_raw() {
        assert_eq!(
            normalize_github_source(
                "https://github.com/example/project/blob/main/subscriptions/all.txt"
            )
            .as_deref(),
            Some("https://raw.githubusercontent.com/example/project/main/subscriptions/all.txt")
        );
    }

    #[test]
    fn ignores_repository_names_when_filtering_source_paths() {
        assert!(!likely_source_url(
            "https://raw.githubusercontent.com/vless-owner/project/main/data.json"
        ));
        assert!(likely_source_url(
            "https://raw.githubusercontent.com/vless-owner/project/main/subscriptions/data.json"
        ));
    }

    #[test]
    fn rejects_generic_structured_files_without_source_hints() {
        assert!(!is_source_path("package.json"));
        assert!(!is_source_path("data.yaml"));
        assert!(!is_source_path("settings.ini"));
        assert!(is_source_path("subscriptions/config.json"));
        assert!(is_source_path("nodes/data.yaml"));
    }

    #[test]
    fn encodes_branch_path_safely() {
        assert_eq!(
            percent_encode_path("feature/source list"),
            "feature/source%20list"
        );
        assert_eq!(
            percent_encode_path("feature/source#1"),
            "feature/source%231"
        );
    }

    #[test]
    fn rejects_unrelated_text_files() {
        assert!(!is_source_path("docs/notes.txt"));
        assert!(!is_source_path("docs/todo.txt"));
        assert!(is_source_path("subscriptions/all.txt"));
        assert!(is_source_path("proxies.txt"));
    }

    #[test]
    fn rejects_oversized_source_urls() {
        let raw = format!(
            "https://raw.githubusercontent.com/example/project/main/{}.txt",
            "a".repeat(MAX_SOURCE_URL_LENGTH)
        );
        assert!(normalize_github_source(&raw).is_none());
    }

    #[test]
    fn attributes_readme_links_to_their_source_repository() {
        let text = "https://github.com/source-owner/source-repo/blob/main/subscriptions/all.txt";
        let candidates = extract_source_urls(text, "reader-owner/reader-repo", 7);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].repo, "source-owner/source-repo");
        assert_eq!(candidates[0].repo_rank, 7);
    }

    #[test]
    fn caps_readme_candidate_extraction() {
        let mut text = String::new();
        for index in 0..(super::MAX_README_CANDIDATES + 50) {
            text.push_str(&format!(
                "https://github.com/example/project-{index}/blob/main/subscriptions/all.txt\n"
            ));
        }

        let candidates = extract_source_urls(&text, "reader/example", 0);

        assert_eq!(candidates.len(), super::MAX_README_CANDIDATES);
    }

    #[test]
    fn extracts_urls_from_markdown_inline_code() {
        let text = "`https://github.com/example/project/blob/main/subscriptions/all.txt`";
        let candidates = extract_source_urls(text, "example/project", 0);

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].url,
            "https://raw.githubusercontent.com/example/project/main/subscriptions/all.txt"
        );
    }

    #[test]
    fn extracts_urls_from_markdown_table_cells() {
        let text = "| https://github.com/example/project/blob/main/subscriptions/all.txt |";
        let candidates = extract_source_urls(text, "example/project", 0);

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].url,
            "https://raw.githubusercontent.com/example/project/main/subscriptions/all.txt"
        );
    }

    #[test]
    fn rejects_malformed_raw_github_paths() {
        assert!(normalize_github_source("https://raw.githubusercontent.com/all.txt").is_none());
        assert!(
            normalize_github_source("https://raw.githubusercontent.com/example/project").is_none()
        );
        assert!(normalize_github_source(
            "https://raw.githubusercontent.com/example/project/main/subscriptions/all.txt"
        )
        .is_some());
    }

    #[test]
    fn rejects_non_github_sources() {
        assert!(normalize_github_source("https://example.com/sub.txt").is_none());
    }

    #[test]
    fn rejects_github_repository_pages() {
        assert!(normalize_github_source("https://github.com/example/project").is_none());
    }

    #[test]
    fn rejects_non_source_github_paths_with_refs() {
        assert!(normalize_github_source(
            "https://github.com/example/project/commits/refs/heads/main/subscriptions/all.txt"
        )
        .is_none());
        assert!(normalize_github_source(
            "https://github.com/example/project/tree/refs/heads/main/subscriptions/all.txt"
        )
        .is_none());
    }

    #[test]
    fn rejects_nonstandard_raw_github_ports() {
        assert!(normalize_github_source(
            "https://raw.githubusercontent.com:8443/example/project/main/subscriptions/all.txt"
        )
        .is_none());
    }

    #[test]
    fn rejects_unsupported_github_ref_urls() {
        assert!(normalize_github_source(
            "https://github.com/example/project/blob/refs/pull/123/head/subscriptions/all.txt"
        )
        .is_none());
        assert!(normalize_github_source(
            "https://github.com/example/project/raw/refs/tags/v1/subscriptions/all.txt"
        )
        .is_none());
    }

    #[test]
    fn preserves_modern_github_refs_for_branch_paths() {
        assert_eq!(
            normalize_github_source(
                "https://github.com/example/project/raw/refs/heads/feature/source-list/subscriptions/all.txt"
            )
            .as_deref(),
            Some(
                "https://raw.githubusercontent.com/example/project/refs/heads/feature/source-list/subscriptions/all.txt"
            )
        );
        assert_eq!(
            normalize_github_source(
                "https://github.com/example/project/blob/refs/heads/feature/source-list/subscriptions/all.txt"
            )
            .as_deref(),
            Some(
                "https://raw.githubusercontent.com/example/project/refs/heads/feature/source-list/subscriptions/all.txt"
            )
        );
    }

    #[test]
    fn builds_raw_urls_with_slash_branches_safely() {
        assert_eq!(
            super::raw_github_file_url(
                "example/project",
                "feature/source-list",
                "subscriptions/all.txt"
            ),
            "https://raw.githubusercontent.com/example/project/refs/heads/feature/source-list/subscriptions/all.txt"
        );
    }

    #[test]
    fn recognizes_source_paths() {
        assert!(is_source_path("configs/vless.txt"));
        assert!(is_source_path("subscriptions/all.yaml"));
        assert!(is_source_path("sub"));
        assert!(!is_source_path("src/config.rs"));
        assert!(!is_source_path("src/main.rs"));
    }

    #[test]
    fn rejects_obvious_non_source_files_even_with_source_hints() {
        for path in [
            "subscriptions/package.json",
            "configs/metadata.json",
            "nodes/schema.json",
            "proxy/package-lock.json",
            "v2ray/docker-compose.yml",
            "subscriptions/config.schema.json",
        ] {
            assert!(!is_source_path(path), "unexpected source path: {path}");
        }
    }

    #[test]
    fn accepts_unusual_but_plausible_source_names() {
        assert!(is_source_path("subscriptions/today.list"));
        assert!(is_source_path("nodes/feed.sub"));
        assert!(is_source_path("config/proxy-links.yaml"));
    }

    #[test]
    fn source_path_score_prefers_strong_source_hints() {
        assert!(
            super::source_path_score("subscriptions/config.json")
                > super::source_path_score("config.json")
        );
        assert!(
            super::source_path_score("subscriptions/all.txt")
                > super::source_path_score("subscriptions/config.json")
        );
        assert_eq!(super::source_path_score("subscriptions/package.json"), 0);
    }

    #[test]
    fn readme_candidates_prioritize_strong_source_paths() {
        let text = concat!(
            "https://github.com/example/project/blob/main/config.json\n",
            "https://github.com/example/project/blob/main/subscriptions/all.txt\n",
            "https://github.com/example/project/blob/main/nodes/feed.yaml\n",
        );

        let candidates = extract_source_urls(text, "example/reader", 0);

        assert_eq!(
            candidates.first().map(|candidate| candidate.url.as_str()),
            Some("https://raw.githubusercontent.com/example/project/main/subscriptions/all.txt")
        );
    }

    #[test]
    fn rejects_obvious_noise() {
        assert!(!likely_source_url(
            "https://raw.githubusercontent.com/example/project/main/.github/workflows/sub.txt"
        ));
        assert!(!likely_source_url(
            "https://raw.githubusercontent.com/example/project/main/README.md"
        ));
        assert!(!is_source_path("archive/all.txt"));
        assert!(!is_source_path("archive/all_broken.txt"));
        assert!(!is_source_path("backup/subscriptions.txt"));
        assert!(!is_source_path("deprecated/vless.txt"));
        assert!(!is_source_path("examples/configs.txt"));
        assert!(is_source_path("subscriptions/vless.txt"));
    }
}
