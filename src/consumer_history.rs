use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

const EVIDENCE_VERSION: u64 = 2;
const DECAY_HALF_LIFE_SECS: f64 = 30.0 * 24.0 * 60.0 * 60.0;
const PROTOCOL_PRIOR_STRENGTH: f64 = 8.0;
const ARCHETYPE_PRIOR_STRENGTH: f64 = 12.0;
const FAMILY_PRIOR_STRENGTH: f64 = 16.0;
const EXPLORATION_BONUS: f64 = 0.03;
// Keep permanent support bounded so repeated passes strengthen a pattern without growing forever.
const PERMANENT_SUPPORT_CAP: u32 = 8;
const RECENT_CONSUMER_WINDOW_SECS: u64 = 7 * 24 * 60 * 60;
const MAX_RECENT_CONSUMER_RESULTS: usize = 5000;

#[derive(Clone, Copy, Debug, Default)]
struct WeightedStats {
    observations: f64,
    passes: f64,
}

impl WeightedStats {
    fn add(&mut self, weight: f64, passed: bool) {
        let weight = weight.max(0.0);
        self.observations += weight;
        if passed {
            self.passes += weight;
        }
        self.passes = self.passes.min(self.observations);
    }
}

#[derive(Clone, Debug, Default)]
struct GroupStats {
    protocol: String,
    archetype_hash: String,
    stats: WeightedStats,
    last_seen: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct RecentConsumerResult {
    observed_at: u64,
    pass: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct LearnedGroupStats {
    confirmations: u32,
}

impl LearnedGroupStats {
    fn confirm(&mut self) {
        self.confirmations = self
            .confirmations
            .saturating_add(1)
            .min(PERMANENT_SUPPORT_CAP);
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConsumerEvidence {
    generated_at: u64,
    learned_through: u64,
    performance_generated_at: u64,
    global: WeightedStats,
    performance_global: WeightedStats,
    protocols: HashMap<String, WeightedStats>,
    archetypes: HashMap<String, GroupStats>,
    families: HashMap<String, GroupStats>,
    performance_protocols: HashMap<String, WeightedStats>,
    performance_archetypes: HashMap<String, GroupStats>,
    performance_families: HashMap<String, GroupStats>,
    learned_global: u32,
    learned_protocols: HashMap<String, u32>,
    learned_archetypes: HashMap<String, LearnedGroupStats>,
    learned_families: HashMap<String, LearnedGroupStats>,
    recent_consumer_results: HashMap<String, RecentConsumerResult>,
}

pub fn config_hash(config: &str) -> String {
    hash_bytes(config.as_bytes())
}

pub fn protocol(config: &str) -> String {
    config
        .split_once("://")
        .map(|(scheme, _)| scheme.to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn family_hash(config: &str) -> String {
    hash_bytes(family_key(config).as_bytes())
}

pub fn archetype_hash(config: &str) -> String {
    hash_bytes(archetype_key(config).as_bytes())
}

fn hash_bytes(bytes: &[u8]) -> String {
    let first = fnv64(bytes, 0xcbf29ce484222325);
    let second = fnv64(bytes, 0x9e3779b97f4a7c15);
    format!("{first:016x}{second:016x}")
}

fn fnv64(bytes: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn query_value(url: &Url, names: &[&str]) -> Option<String> {
    url.query_pairs()
        .find(|(key, value)| {
            names.iter().any(|name| key.eq_ignore_ascii_case(name)) && !value.trim().is_empty()
        })
        .map(|(_, value)| value.to_ascii_lowercase())
}

fn query_present(url: &Url, names: &[&str]) -> bool {
    url.query_pairs()
        .any(|(key, _)| names.iter().any(|name| key.eq_ignore_ascii_case(name)))
}

fn normalize_enum(value: Option<String>, fallback: &str) -> String {
    value
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
                .take(32)
                .collect::<String>()
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn default_transport(scheme: &str) -> &'static str {
    match scheme {
        "hysteria" | "hysteria2" | "hy2" | "tuic" => "quic",
        "wg" => "wireguard",
        _ => "tcp",
    }
}

fn default_security(scheme: &str) -> &'static str {
    match scheme {
        "trojan" | "https" => "tls",
        _ => "none",
    }
}

fn port_bucket(port: Option<u16>) -> &'static str {
    match port {
        Some(80) | Some(443) => "web",
        Some(8080) | Some(8443) | Some(2053) | Some(2083) | Some(2087) | Some(2096) => "alt-web",
        Some(_) => "other",
        None => "unknown",
    }
}

fn host_kind(host: Option<&str>) -> &'static str {
    host.and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .map(|_| "ip")
        .unwrap_or("domain")
}

fn value_string(value: Option<&Value>, key: &str) -> Option<String> {
    value
        .and_then(|object| object.get(key))
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
}

fn vmess_payload(config: &str) -> Option<Value> {
    let cleaned = config.split('#').next().unwrap_or(config);
    if !cleaned
        .split_once("://")
        .map(|(scheme, _)| scheme.eq_ignore_ascii_case("vmess"))
        .unwrap_or(false)
    {
        return None;
    }

    let payload = cleaned.split_once("://")?.1.trim();
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

fn shape(value: Option<&str>) -> &'static str {
    match value.filter(|value| !value.trim().is_empty()) {
        None => "absent",
        Some("/") => "root",
        Some(value) if value.starts_with('/') => "nested",
        Some(_) => "other",
    }
}

fn flow_class(value: Option<&str>) -> &'static str {
    match value.unwrap_or_default() {
        value if value.contains("vision") && value.contains("udp443") => "vision-udp443",
        value if value.contains("vision") => "vision",
        "" => "none",
        _ => "other",
    }
}

fn obfs_class(value: Option<&str>) -> &'static str {
    match value.unwrap_or_default() {
        "" => "none",
        "salamander" => "salamander",
        _ => "other",
    }
}

fn structural_parts(config: &str) -> (String, String) {
    let cleaned = config.split('#').next().unwrap_or(config);
    let scheme = protocol(cleaned);
    let vmess = vmess_payload(cleaned);
    let parsed = Url::parse(cleaned).ok();

    let transport = if let Some(value) = value_string(vmess.as_ref(), "net") {
        value
    } else {
        parsed
            .as_ref()
            .and_then(|url| query_value(url, &["type", "network", "transport", "net"]))
            .unwrap_or_else(|| default_transport(&scheme).to_string())
    };

    let security = if let Some(value) = value_string(vmess.as_ref(), "tls") {
        if matches!(value.as_str(), "1" | "true" | "tls" | "https") {
            "tls".to_string()
        } else {
            value
        }
    } else {
        let query_security = parsed
            .as_ref()
            .and_then(|url| query_value(url, &["security"]));
        let query_tls = parsed.as_ref().and_then(|url| query_value(url, &["tls"]));
        normalize_enum(
            query_security.or_else(|| {
                query_tls.map(|value| {
                    if matches!(value.as_str(), "1" | "true" | "tls" | "https") {
                        "tls".to_string()
                    } else {
                        value
                    }
                })
            }),
            default_security(&scheme),
        )
    };

    let port = parsed
        .as_ref()
        .map(|url| port_bucket(url.port_or_known_default()))
        .or_else(|| {
            vmess
                .as_ref()
                .and_then(|value| value.get("port"))
                .and_then(|value| {
                    value
                        .as_u64()
                        .or_else(|| value.as_str()?.parse::<u64>().ok())
                })
                .and_then(|port| u16::try_from(port).ok())
                .map(|port| port_bucket(Some(port)))
        })
        .unwrap_or("unknown");

    let host_value = parsed
        .as_ref()
        .and_then(Url::host_str)
        .map(str::to_string)
        .or_else(|| value_string(vmess.as_ref(), "add"));
    let host = host_kind(host_value.as_deref());

    let sni = parsed
        .as_ref()
        .and_then(|url| query_value(url, &["sni"]))
        .or_else(|| value_string(vmess.as_ref(), "sni"));
    let host_header = parsed
        .as_ref()
        .and_then(|url| query_value(url, &["host", "authority"]))
        .or_else(|| value_string(vmess.as_ref(), "host"));
    let path = parsed
        .as_ref()
        .and_then(|url| query_value(url, &["path"]))
        .or_else(|| {
            parsed
                .as_ref()
                .map(|url| url.path().to_string())
                .filter(|path| path != "/")
        })
        .or_else(|| value_string(vmess.as_ref(), "path"));

    let flow = parsed
        .as_ref()
        .and_then(|url| query_value(url, &["flow"]))
        .or_else(|| value_string(vmess.as_ref(), "flow"));
    let obfs = parsed
        .as_ref()
        .and_then(|url| query_value(url, &["obfs"]))
        .or_else(|| value_string(vmess.as_ref(), "obfs"));

    let has_grpc_service = parsed
        .as_ref()
        .is_some_and(|url| query_present(url, &["serviceName", "service_name"]))
        || value_string(vmess.as_ref(), "serviceName").is_some();

    let has_ech = parsed
        .as_ref()
        .is_some_and(|url| query_present(url, &["ech"]))
        || value_string(vmess.as_ref(), "ech").is_some();

    let has_certificate_pin = parsed
        .as_ref()
        .is_some_and(|url| query_present(url, &["pcs", "vcn", "pinSHA256"]))
        || value_string(vmess.as_ref(), "pcs").is_some()
        || value_string(vmess.as_ref(), "vcn").is_some();

    let has_fingerprint = parsed
        .as_ref()
        .is_some_and(|url| query_present(url, &["fp", "fingerprint"]))
        || value_string(vmess.as_ref(), "fp").is_some();

    let header_type = parsed
        .as_ref()
        .and_then(|url| query_value(url, &["headerType"]))
        .or_else(|| value_string(vmess.as_ref(), "type"));

    let has_udp = parsed
        .as_ref()
        .is_some_and(|url| query_present(url, &["udp", "udpRelay", "udp-over-tcp"]))
        || value_string(vmess.as_ref(), "udp").is_some();

    let mode = parsed
        .as_ref()
        .and_then(|url| query_value(url, &["mode"]))
        .unwrap_or_default();

    let family = format!(
        "{scheme}|transport:{transport}|security:{security}|port:{port}|host:{host}|sni:{}|hosthdr:{}|path:{}|flow:{}|obfs:{}|grpc:{}|ech:{}|pin:{}|fp:{}|header:{}|udp:{}|mode:{}",
        shape(sni.as_deref()),
        shape(host_header.as_deref()),
        shape(path.as_deref()),
        flow_class(flow.as_deref()),
        obfs_class(obfs.as_deref()),
        has_grpc_service,
        has_ech,
        has_certificate_pin,
        has_fingerprint,
        normalize_enum(header_type, "none"),
        has_udp,
        normalize_enum(Some(mode), "none"),
    );

    let archetype = format!(
        "{scheme}|transport:{transport}|security:{security}|port:{port}|host:{host}|flow:{}|obfs:{}",
        flow_class(flow.as_deref()),
        obfs_class(obfs.as_deref()),
    );

    (family, archetype)
}

fn family_key(config: &str) -> String {
    structural_parts(config).0
}

fn archetype_key(config: &str) -> String {
    structural_parts(config).1
}

impl ConsumerEvidence {
    pub fn from_rounds(rounds: &[Value], generated_at: u64) -> Self {
        Self::merge_rounds(&Self::default(), rounds, generated_at)
    }

    pub fn merge_rounds(existing: &Self, rounds: &[Value], generated_at: u64) -> Self {
        let mut evidence = existing.clone();
        evidence.generated_at = generated_at;
        evidence.global = WeightedStats::default();
        evidence.protocols.clear();
        evidence.archetypes.clear();
        evidence.families.clear();

        let mut newest_learned_round = existing.learned_through;

        for round in rounds {
            let observed_at = round
                .get("observed_at")
                .and_then(Value::as_u64)
                .unwrap_or(generated_at);
            let age = generated_at.saturating_sub(observed_at) as f64;
            let weight = 2.0_f64.powf(-age / DECAY_HALF_LIFE_SECS);
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

                let protocol = result
                    .get("protocol")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_ascii_lowercase();
                let family = result
                    .get("family_hash")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let archetype = result
                    .get("archetype_hash")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if family.is_empty() || archetype.is_empty() {
                    continue;
                }

                let passed = result.get("pass").and_then(Value::as_bool).unwrap_or(false);

                if passed && observed_at > existing.learned_through {
                    evidence.learned_global = evidence
                        .learned_global
                        .saturating_add(1)
                        .min(PERMANENT_SUPPORT_CAP);
                    let count = evidence
                        .learned_protocols
                        .entry(protocol.clone())
                        .or_default();
                    *count = count.saturating_add(1).min(PERMANENT_SUPPORT_CAP);
                    evidence
                        .learned_archetypes
                        .entry(archetype.clone())
                        .or_default()
                        .confirm();
                    evidence
                        .learned_families
                        .entry(family.clone())
                        .or_default()
                        .confirm();
                }

                evidence.global.add(weight, passed);
                evidence
                    .protocols
                    .entry(protocol.clone())
                    .or_default()
                    .add(weight, passed);

                let archetype_entry =
                    evidence
                        .archetypes
                        .entry(archetype.clone())
                        .or_insert_with(|| GroupStats {
                            protocol: protocol.clone(),
                            archetype_hash: String::new(),
                            ..GroupStats::default()
                        });
                archetype_entry.stats.add(weight, passed);
                archetype_entry.last_seen = archetype_entry.last_seen.max(observed_at);

                let family_entry =
                    evidence
                        .families
                        .entry(family.clone())
                        .or_insert_with(|| GroupStats {
                            protocol: protocol.clone(),
                            archetype_hash: archetype.clone(),
                            ..GroupStats::default()
                        });
                family_entry.stats.add(weight, passed);
                family_entry.last_seen = family_entry.last_seen.max(observed_at);
            }

            newest_learned_round = newest_learned_round.max(observed_at);
        }

        evidence.learned_through = newest_learned_round;
        evidence
    }

    fn record_recent_consumer_results(&mut self, rounds: &[Value], now: u64) {
        for round in rounds {
            let observed_at = round
                .get("observed_at")
                .and_then(Value::as_u64)
                .unwrap_or(now);
            let Some(results) = round.get("results").and_then(Value::as_array) else {
                continue;
            };

            for result in results {
                let Some(config_hash) = result.get("config_hash").and_then(Value::as_str) else {
                    continue;
                };
                let pass = result.get("pass").and_then(Value::as_bool).unwrap_or(false);
                let replace = self
                    .recent_consumer_results
                    .get(config_hash)
                    .map(|entry| observed_at >= entry.observed_at)
                    .unwrap_or(true);
                if replace {
                    self.recent_consumer_results.insert(
                        config_hash.to_string(),
                        RecentConsumerResult { observed_at, pass },
                    );
                }
            }
        }

        self.recent_consumer_results.retain(|_, entry| {
            entry
                .observed_at
                .saturating_add(RECENT_CONSUMER_WINDOW_SECS)
                >= now
        });

        if self.recent_consumer_results.len() > MAX_RECENT_CONSUMER_RESULTS {
            let mut entries = self.recent_consumer_results.drain().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(_, entry)| std::cmp::Reverse(entry.observed_at));
            entries.truncate(MAX_RECENT_CONSUMER_RESULTS);
            self.recent_consumer_results = entries.into_iter().collect();
        }
    }

    pub fn load_with_consumer_history(evidence_path: &str, history_path: &str) -> Self {
        let mut evidence = Self::load(evidence_path);
        let Ok(content) = fs::read_to_string(history_path) else {
            return evidence;
        };
        let Ok(value) = serde_json::from_str::<Value>(&content) else {
            return evidence;
        };
        let Some(rounds) = value.get("rounds").and_then(Value::as_array) else {
            return evidence;
        };
        let now = now_unix().unwrap_or(evidence.generated_at);
        evidence = Self::merge_rounds(&evidence, rounds, now);
        evidence.record_recent_consumer_results(rounds, now);
        evidence
    }

    pub fn recent_consumer_priority(&self, config: &str) -> u8 {
        match self.recent_consumer_results.get(&config_hash(config)) {
            Some(entry) if entry.pass => 2,
            Some(_) => 0,
            None => 1,
        }
    }

    pub fn recent_consumer_observed_at(&self, config: &str) -> u64 {
        self.recent_consumer_results
            .get(&config_hash(config))
            .map(|entry| entry.observed_at)
            .unwrap_or(0)
    }

    pub fn load(path: &str) -> Self {
        let Ok(content) = fs::read_to_string(path) else {
            return Self::default();
        };
        let Ok(value) = serde_json::from_str::<Value>(&content) else {
            return Self::default();
        };

        let version = value.get("version").and_then(Value::as_u64).unwrap_or(0);
        if version != 1 && version != EVIDENCE_VERSION {
            return Self::default();
        }

        let mut evidence = Self {
            generated_at: value
                .get("generated_at")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            ..Self::default()
        };

        evidence.global = read_stats(value.get("global"));
        if let Some(performance) = value.get("performance").and_then(Value::as_object) {
            evidence.performance_generated_at = performance
                .get("generated_at")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            evidence.performance_global = read_stats(performance.get("global"));
            if let Some(protocols) = performance.get("protocols").and_then(Value::as_object) {
                for (protocol, entry) in protocols {
                    evidence
                        .performance_protocols
                        .insert(protocol.clone(), read_stats(Some(entry)));
                }
            }
            if let Some(archetypes) = performance.get("archetypes").and_then(Value::as_object) {
                for (hash, entry) in archetypes {
                    evidence
                        .performance_archetypes
                        .insert(hash.clone(), read_group_stats(entry, String::new()));
                }
            }
            if let Some(families) = performance.get("families").and_then(Value::as_object) {
                for (hash, entry) in families {
                    let archetype = entry
                        .get("archetype_hash")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    evidence
                        .performance_families
                        .insert(hash.clone(), read_group_stats(entry, archetype));
                }
            }
        }

        if let Some(protocols) = value.get("protocols").and_then(Value::as_object) {
            for (protocol, entry) in protocols {
                evidence
                    .protocols
                    .insert(protocol.clone(), read_stats(Some(entry)));
            }
        }
        if let Some(archetypes) = value.get("archetypes").and_then(Value::as_object) {
            for (hash, entry) in archetypes {
                evidence
                    .archetypes
                    .insert(hash.clone(), read_group_stats(entry, String::new()));
            }
        }
        if let Some(families) = value.get("families").and_then(Value::as_object) {
            for (hash, entry) in families {
                let archetype = entry
                    .get("archetype_hash")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                evidence
                    .families
                    .insert(hash.clone(), read_group_stats(entry, archetype));
            }
        }

        if version == 1 {
            evidence.learned_through = evidence.generated_at;
            evidence.learned_global = permanent_from_weighted(evidence.global.passes);
            for (protocol, stats) in &evidence.protocols {
                let confirmations = permanent_from_weighted(stats.passes);
                if confirmations > 0 {
                    evidence
                        .learned_protocols
                        .insert(protocol.clone(), confirmations);
                }
            }
            for (hash, stats) in &evidence.archetypes {
                let confirmations = permanent_from_weighted(stats.stats.passes);
                if confirmations > 0 {
                    evidence
                        .learned_archetypes
                        .insert(hash.clone(), LearnedGroupStats { confirmations });
                }
            }
            for (hash, stats) in &evidence.families {
                let confirmations = permanent_from_weighted(stats.stats.passes);
                if confirmations > 0 {
                    evidence
                        .learned_families
                        .insert(hash.clone(), LearnedGroupStats { confirmations });
                }
            }
        } else if let Some(learning) = value.get("permanent_learning").and_then(Value::as_object) {
            evidence.learned_through = learning
                .get("learned_through")
                .and_then(Value::as_u64)
                .unwrap_or(evidence.generated_at);
            evidence.learned_global = learning
                .get("global_confirmations")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(0)
                .min(PERMANENT_SUPPORT_CAP);

            if let Some(protocols) = learning.get("protocols").and_then(Value::as_object) {
                for (protocol, value) in protocols {
                    let confirmations = value
                        .as_u64()
                        .and_then(|value| u32::try_from(value).ok())
                        .unwrap_or(0)
                        .min(PERMANENT_SUPPORT_CAP);
                    if confirmations > 0 {
                        evidence
                            .learned_protocols
                            .insert(protocol.clone(), confirmations);
                    }
                }
            }

            if let Some(archetypes) = learning.get("archetypes").and_then(Value::as_object) {
                for (hash, entry) in archetypes {
                    let confirmations = entry
                        .get("confirmations")
                        .and_then(Value::as_u64)
                        .and_then(|value| u32::try_from(value).ok())
                        .unwrap_or(0)
                        .min(PERMANENT_SUPPORT_CAP);
                    if confirmations > 0 {
                        evidence
                            .learned_archetypes
                            .insert(hash.clone(), LearnedGroupStats { confirmations });
                    }
                }
            }

            if let Some(families) = learning.get("families").and_then(Value::as_object) {
                for (hash, entry) in families {
                    let confirmations = entry
                        .get("confirmations")
                        .and_then(Value::as_u64)
                        .and_then(|value| u32::try_from(value).ok())
                        .unwrap_or(0)
                        .min(PERMANENT_SUPPORT_CAP);
                    if confirmations > 0 {
                        evidence
                            .learned_families
                            .insert(hash.clone(), LearnedGroupStats { confirmations });
                    }
                }
            }
        }

        evidence
    }

    pub fn record_performance_observations(
        &mut self,
        observations: &[(&str, bool)],
        observed_at: u64,
    ) {
        if observed_at < self.performance_generated_at {
            return;
        }

        if self.performance_generated_at > 0 && observed_at > self.performance_generated_at {
            let age = observed_at.saturating_sub(self.performance_generated_at) as f64;
            let decay = 2.0_f64.powf(-age / DECAY_HALF_LIFE_SECS);

            self.performance_global.observations *= decay;
            self.performance_global.passes *= decay;
            for stats in self.performance_protocols.values_mut() {
                stats.observations *= decay;
                stats.passes *= decay;
            }
            for stats in self.performance_archetypes.values_mut() {
                stats.stats.observations *= decay;
                stats.stats.passes *= decay;
            }
            for stats in self.performance_families.values_mut() {
                stats.stats.observations *= decay;
                stats.stats.passes *= decay;
            }
        }

        self.performance_generated_at = observed_at;

        for (config, passed) in observations {
            let protocol_name = protocol(config);
            let archetype = archetype_hash(config);
            let family = family_hash(config);

            self.performance_global.add(1.0, *passed);
            self.performance_protocols
                .entry(protocol_name.clone())
                .or_default()
                .add(1.0, *passed);

            let archetype_entry = self
                .performance_archetypes
                .entry(archetype.clone())
                .or_insert_with(|| GroupStats {
                    protocol: protocol_name.clone(),
                    archetype_hash: String::new(),
                    ..GroupStats::default()
                });
            archetype_entry.stats.add(1.0, *passed);
            archetype_entry.last_seen = observed_at;

            let family_entry =
                self.performance_families
                    .entry(family)
                    .or_insert_with(|| GroupStats {
                        protocol: protocol_name,
                        archetype_hash: archetype,
                        ..GroupStats::default()
                    });
            family_entry.stats.add(1.0, *passed);
            family_entry.last_seen = observed_at;
        }
    }

    pub fn performance_observation_count(&self) -> f64 {
        self.performance_global.observations
    }

    pub fn performance_family_score(&self, config: &str) -> Option<f64> {
        let family = family_hash(config);
        let family_stats = self
            .performance_families
            .get(&family)
            .map(|entry| entry.stats)?;

        if family_stats.observations <= 0.0 {
            return None;
        }

        let protocol_name = protocol(config);
        let protocol_stats = self
            .performance_protocols
            .get(&protocol_name)
            .copied()
            .unwrap_or_default();

        let global_rate = smoothed_rate(self.performance_global, 0.5, 4.0);
        let protocol_rate = smoothed_rate(protocol_stats, global_rate, PROTOCOL_PRIOR_STRENGTH);

        Some(smoothed_rate(
            family_stats,
            protocol_rate,
            FAMILY_PRIOR_STRENGTH,
        ))
    }

    pub fn save(&self, path: &str) -> Result<(), String> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
        }

        let mut protocols = BTreeMap::new();
        for (protocol, stats) in &self.protocols {
            protocols.insert(protocol, stats_json(*stats));
        }

        let mut archetypes = BTreeMap::new();
        for (hash, stats) in &self.archetypes {
            archetypes.insert(hash, group_stats_json(stats, None));
        }

        let mut families = BTreeMap::new();
        for (hash, stats) in &self.families {
            families.insert(hash, group_stats_json(stats, Some(&stats.archetype_hash)));
        }

        let mut learned_protocols = BTreeMap::new();
        for (protocol, confirmations) in &self.learned_protocols {
            learned_protocols.insert(protocol, *confirmations);
        }

        let mut learned_archetypes = BTreeMap::new();
        for (hash, stats) in &self.learned_archetypes {
            learned_archetypes.insert(
                hash,
                serde_json::json!({
                    "confirmations": stats.confirmations
                }),
            );
        }

        let mut learned_families = BTreeMap::new();
        for (hash, stats) in &self.learned_families {
            learned_families.insert(
                hash,
                serde_json::json!({
                    "confirmations": stats.confirmations
                }),
            );
        }

        let mut performance_protocols = BTreeMap::new();
        for (protocol, stats) in &self.performance_protocols {
            performance_protocols.insert(protocol, stats_json(*stats));
        }

        let mut performance_archetypes = BTreeMap::new();
        for (hash, stats) in &self.performance_archetypes {
            performance_archetypes.insert(hash, group_stats_json(stats, None));
        }

        let mut performance_families = BTreeMap::new();
        for (hash, stats) in &self.performance_families {
            performance_families.insert(hash, group_stats_json(stats, Some(&stats.archetype_hash)));
        }

        let value = serde_json::json!({
            "version": EVIDENCE_VERSION,
            "generated_at": self.generated_at,
            "decay_half_life_days": 30,
            "permanent_learning": {
                "learned_through": self.learned_through,
                "global_confirmations": self.learned_global,
                "protocols": learned_protocols,
                "archetypes": learned_archetypes,
                "families": learned_families
            },
            "privacy": {
                "stores_raw_configs": false,
                "stores_exact_config_hashes": false,
                "stores_country_or_isp": false
            },
            "global": stats_json(self.global),
            "protocols": protocols,
            "performance": {
                "generated_at": self.performance_generated_at,
                "global": stats_json(self.performance_global),
                "protocols": performance_protocols,
                "archetypes": performance_archetypes,
                "families": performance_families
            },
            "archetypes": archetypes,
            "families": families
        });

        let body = serde_json::to_vec_pretty(&value).map_err(|error| error.to_string())?;
        let temporary = format!("{path}.tmp");
        fs::write(&temporary, body).map_err(|error| error.to_string())?;
        if let Err(error) = fs::rename(&temporary, path) {
            let _ = fs::remove_file(&temporary);
            return Err(error.to_string());
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.global.observations <= 0.0
    }

    pub fn family_count(&self) -> usize {
        self.families.len()
    }

    pub fn observation_count(&self) -> f64 {
        self.global.observations
    }

    pub fn learning_priority(&self, config: &str) -> u8 {
        match self.recent_consumer_priority(config) {
            2 => return 4,
            0 => return 0,
            _ => {}
        }
        let family = family_hash(config);
        if self
            .learned_families
            .get(&family)
            .is_some_and(|entry| entry.confirmations > 0)
        {
            return 3;
        }

        let archetype = archetype_hash(config);
        if self
            .learned_archetypes
            .get(&archetype)
            .is_some_and(|entry| entry.confirmations > 0)
        {
            return 2;
        }

        if self
            .learned_protocols
            .get(&protocol(config))
            .copied()
            .unwrap_or(0)
            > 0
        {
            return 1;
        }

        0
    }

    pub fn score(&self, config: &str) -> f64 {
        let learned_global_rate = smoothed_rate(learned_stats(self.learned_global), 0.5, 4.0);
        let global_rate = smoothed_rate(self.global, learned_global_rate, 4.0);

        let protocol_name = protocol(config);
        let protocol_stats = self
            .protocols
            .get(&protocol_name)
            .copied()
            .unwrap_or_default();
        let learned_protocol_rate = self
            .learned_protocols
            .get(&protocol_name)
            .copied()
            .map(learned_stats)
            .map(|stats| smoothed_rate(stats, learned_global_rate, PROTOCOL_PRIOR_STRENGTH))
            .unwrap_or(learned_global_rate);
        let protocol_rate = smoothed_rate(
            protocol_stats,
            learned_protocol_rate.max(global_rate),
            PROTOCOL_PRIOR_STRENGTH,
        );

        let archetype = archetype_hash(config);
        let archetype_stats = self
            .archetypes
            .get(&archetype)
            .map(|entry| entry.stats)
            .unwrap_or_default();
        let learned_archetype_rate = self
            .learned_archetypes
            .get(&archetype)
            .map(|entry| entry.confirmations)
            .map(learned_stats)
            .map(|stats| smoothed_rate(stats, protocol_rate, ARCHETYPE_PRIOR_STRENGTH))
            .unwrap_or(protocol_rate);
        let archetype_rate = smoothed_rate(
            archetype_stats,
            learned_archetype_rate,
            ARCHETYPE_PRIOR_STRENGTH,
        );

        let family = family_hash(config);
        let family_stats = self
            .families
            .get(&family)
            .map(|entry| entry.stats)
            .unwrap_or_default();
        let learned_family_rate = self
            .learned_families
            .get(&family)
            .map(|entry| entry.confirmations)
            .map(learned_stats)
            .map(|stats| smoothed_rate(stats, archetype_rate, FAMILY_PRIOR_STRENGTH))
            .unwrap_or(archetype_rate);
        let family_rate = smoothed_rate(family_stats, learned_family_rate, FAMILY_PRIOR_STRENGTH);
        let consumer_rate = if let Some(performance_rate) = self.performance_family_score(config) {
            0.60 * family_rate + 0.40 * performance_rate
        } else {
            family_rate
        };

        let exploration = EXPLORATION_BONUS
            / (family_stats.observations
                + self
                    .learned_families
                    .get(&family)
                    .map(|entry| entry.confirmations as f64)
                    .unwrap_or(0.0)
                + 1.0)
                .sqrt();
        (consumer_rate + exploration).clamp(0.0, 1.0)
    }

    pub fn scores(&self, configs: &[String]) -> HashMap<String, f64> {
        configs
            .iter()
            .map(|config| (config.clone(), self.score(config)))
            .collect()
    }
}

fn learned_stats(confirmations: u32) -> WeightedStats {
    let confirmations = confirmations.min(PERMANENT_SUPPORT_CAP) as f64;
    WeightedStats {
        observations: confirmations,
        passes: confirmations,
    }
}

fn permanent_from_weighted(passes: f64) -> u32 {
    passes.ceil().clamp(0.0, PERMANENT_SUPPORT_CAP as f64) as u32
}

fn smoothed_rate(stats: WeightedStats, prior: f64, strength: f64) -> f64 {
    if stats.observations <= 0.0 {
        return prior.clamp(0.0, 1.0);
    }

    ((stats.passes + prior * strength) / (stats.observations + strength)).clamp(0.0, 1.0)
}

fn read_stats(value: Option<&Value>) -> WeightedStats {
    let Some(value) = value else {
        return WeightedStats::default();
    };
    let observations = value
        .get("observations")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
        .max(0.0);
    let passes = value
        .get("passes")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
        .clamp(0.0, observations);
    WeightedStats {
        observations,
        passes,
    }
}

fn read_group_stats(value: &Value, fallback_archetype: String) -> GroupStats {
    GroupStats {
        protocol: value
            .get("protocol")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        archetype_hash: value
            .get("archetype_hash")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or(&fallback_archetype)
            .to_string(),
        stats: read_stats(Some(value)),
        last_seen: value.get("last_seen").and_then(Value::as_u64).unwrap_or(0),
    }
}

fn stats_json(stats: WeightedStats) -> Value {
    serde_json::json!({
        "observations": round_float(stats.observations),
        "passes": round_float(stats.passes.min(stats.observations))
    })
}

fn group_stats_json(stats: &GroupStats, archetype: Option<&String>) -> Value {
    let mut value = match stats_json(stats.stats) {
        Value::Object(value) => value,
        _ => serde_json::Map::new(),
    };
    value.insert(
        "protocol".to_string(),
        Value::String(stats.protocol.clone()),
    );
    if let Some(archetype) = archetype {
        value.insert(
            "archetype_hash".to_string(),
            Value::String(archetype.clone()),
        );
    }
    value.insert(
        "last_seen".to_string(),
        Value::Number(stats.last_seen.into()),
    );
    Value::Object(value)
}

fn round_float(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

pub fn now_unix() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .map_err(|error| format!("system clock error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{archetype_hash, config_hash, family_hash, ConsumerEvidence};
    use serde_json::json;

    #[test]
    fn recent_consumer_results_are_prioritized() {
        let config = "vless://one@example.com:443?security=reality&type=tcp";
        let mut evidence = ConsumerEvidence::default();
        let rounds = vec![serde_json::json!({
            "observed_at": 2_000_000_u64,
            "results": [{
                "config_hash": config_hash(config),
                "family_hash": family_hash(config),
                "archetype_hash": archetype_hash(config),
                "protocol": "vless",
                "compatible": true,
                "pass": true
            }]
        })];
        evidence.record_recent_consumer_results(&rounds, 2_000_001);
        assert_eq!(evidence.recent_consumer_priority(config), 2);
        assert_eq!(evidence.learning_priority(config), 4);
        assert_eq!(evidence.recent_consumer_observed_at(config), 2_000_000);
    }

    #[test]
    fn family_hash_ignores_per_config_identity() {
        let a =
            "vless://one@example.com:443?security=reality&type=tcp&sni=site.example&path=%2Ffoo";
        let b =
            "vless://two@example.net:443?security=reality&type=tcp&sni=other.example&path=%2Fbar";
        assert_eq!(family_hash(a), family_hash(b));
        assert_eq!(archetype_hash(a), archetype_hash(b));
    }

    #[test]
    fn family_hash_separates_structural_variants() {
        let tcp = "vless://one@example.com:443?security=reality&type=tcp&sni=site.example";
        let ws = "vless://two@example.com:443?security=reality&type=ws&sni=site.example";
        assert_ne!(family_hash(tcp), family_hash(ws));
        assert_ne!(archetype_hash(tcp), archetype_hash(ws));
    }

    #[test]
    fn new_config_inherits_family_history() {
        let rounds = vec![
            json!({
                "observed_at": 1_000_000_u64,
                "results": [{
                    "protocol": "vless",
                    "family_hash": family_hash("vless://one@example.com:443?security=reality&type=tcp&sni=site.example"),
                    "archetype_hash": archetype_hash("vless://one@example.com:443?security=reality&type=tcp&sni=site.example"),
                    "pass": true
                }]
            }),
            json!({
                "observed_at": 1_000_001_u64,
                "results": [{
                    "protocol": "vless",
                    "family_hash": family_hash("vless://one@example.com:443?security=reality&type=tcp&sni=site.example"),
                    "archetype_hash": archetype_hash("vless://one@example.com:443?security=reality&type=tcp&sni=site.example"),
                    "pass": true
                }]
            }),
        ];

        let evidence = ConsumerEvidence::from_rounds(&rounds, 1_000_002);
        let unseen = "vless://new@example.org:443?security=reality&type=tcp&sni=another.example";
        assert!(evidence.score(unseen) > 0.55);
    }

    #[test]
    fn performance_observations_contribute_to_consumer_score() {
        let config = "vless://one@example.com:443?security=reality&type=tcp&sni=site.example";
        let mut evidence = ConsumerEvidence::default();
        let baseline = evidence.score(config);

        evidence.record_performance_observations(&[(config, false)], 1_000);
        let failed = evidence.score(config);
        assert!(failed < baseline);

        evidence.record_performance_observations(&[(config, true)], 1_001);
        let recovered = evidence.score(config);
        assert!(recovered > failed);

        let path = std::env::temp_dir().join(format!(
            "proxyrift-performance-evidence-{}.json",
            std::process::id()
        ));
        evidence
            .save(path.to_str().expect("path"))
            .expect("save evidence");
        let loaded = ConsumerEvidence::load(path.to_str().expect("path"));
        assert!(loaded.performance_observation_count() > 0.0);
        assert!(loaded.performance_family_score(config).is_some());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn permanent_consumer_learning_survives_missing_future_observations() {
        let known = "vless://one@example.com:443?security=reality&type=tcp&sni=site.example";
        let unseen = "vless://new@example.org:443?security=reality&type=tcp&sni=another.example";
        let family = family_hash(known);
        let archetype = archetype_hash(known);
        let rounds = vec![json!({
            "observed_at": 1_000_000_u64,
            "results": [{
                "protocol": "vless",
                "family_hash": family,
                "archetype_hash": archetype,
                "pass": true
            }]
        })];

        let evidence = ConsumerEvidence::from_rounds(&rounds, 1_000_001);
        let stale = ConsumerEvidence::merge_rounds(&evidence, &[], 1_000_001 + 365 * 24 * 60 * 60);

        assert!(stale.score(unseen) > 0.65);
        assert_eq!(
            stale
                .learned_families
                .get(&family)
                .map(|entry| entry.confirmations),
            Some(1)
        );
    }

    #[test]
    fn permanent_learning_does_not_double_count_old_rounds() {
        let known = "vless://one@example.com:443?security=reality&type=tcp&sni=site.example";
        let family = family_hash(known);
        let archetype = archetype_hash(known);
        let rounds = vec![json!({
            "observed_at": 1_000_000_u64,
            "results": [{
                "protocol": "vless",
                "family_hash": family,
                "archetype_hash": archetype,
                "pass": true
            }]
        })];

        let evidence = ConsumerEvidence::from_rounds(&rounds, 1_000_001);
        let merged = ConsumerEvidence::merge_rounds(&evidence, &rounds, 2_000_000);

        assert_eq!(
            merged
                .learned_families
                .get(&family)
                .map(|entry| entry.confirmations),
            Some(1)
        );
    }

    #[test]
    fn permanent_learning_priority_prefers_family_then_archetype_then_protocol() {
        let known = "vless://one@example.com:443?security=reality&type=tcp&sni=site.example";
        let same_family = "vless://two@example.com:443?security=reality&type=tcp&sni=site.example";
        let same_archetype =
            "vless://three@example.org:443?security=reality&type=tcp&sni=other.example&headerType=http";
        let protocol_only = "vless://four@example.net:443?security=tls&type=ws&sni=other.example";

        let family = family_hash(known);
        let archetype = archetype_hash(known);
        let rounds = vec![json!({
            "observed_at": 1_000_000_u64,
            "results": [{
                "protocol": "vless",
                "family_hash": family,
                "archetype_hash": archetype,
                "pass": true
            }]
        })];

        let evidence = ConsumerEvidence::from_rounds(&rounds, 1_000_001);

        assert_eq!(evidence.learning_priority(same_family), 3);
        assert_eq!(evidence.learning_priority(same_archetype), 2);
        assert_eq!(evidence.learning_priority(protocol_only), 1);
    }

    #[test]
    fn failed_observation_is_not_permanent_learning() {
        let known = "vless://one@example.com:443?security=reality&type=tcp&sni=site.example";
        let family = family_hash(known);
        let archetype = archetype_hash(known);
        let rounds = vec![json!({
            "observed_at": 1_000_000_u64,
            "results": [{
                "protocol": "vless",
                "family_hash": family,
                "archetype_hash": archetype,
                "pass": false
            }]
        })];

        let evidence = ConsumerEvidence::from_rounds(&rounds, 1_000_001);
        assert!(!evidence.learned_families.contains_key(&family));
    }
}
