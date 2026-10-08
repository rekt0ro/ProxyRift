# ProxyRift

Public proxy collector and validator.

**Supported:** VLESS · VMess · Trojan · Shadowsocks · Hysteria · Hysteria2 · SOCKS · HTTP

## 📡 Subscriptions

Subscriptions are **updated hourly** with freshly collected and validated configurations.

| List      | Description                              |
| --------- | ---------------------------------------- |
| **Light** | Up to 200 configs, deeply validated      |
| **All**   | Up to 2,000 configs, transport-reachable |

### Light

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light.txt
```

Base64:

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-base64.txt
```

Clash / Mihomo:

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-clash.yaml
```

sing-box:

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-singbox.json
```

### All

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all.txt
```

Base64:

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-base64.txt
```

Clash / Mihomo:

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-clash.yaml
```

sing-box:

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-singbox.json
```

## ⚙️ How It Works

```text
Public Sources
      │
      ▼
Normalize & Deduplicate
      │
      ▼
Transport Reachability
      │
      ├──────────────► All
      │                 │
      │                 ▼
      │              Rank & Limit
      │
      └──────────────► Light candidates
                        │
                        ▼
              LightGBM + Consumer Evidence
                 60%          40%
                        │
                        ▼
                 Candidate Ranking
                        │
                        ▼
                Consumer-like First
                        │
                        ▼
                  Deep Validation
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
        Xray Validation     sing-box Validation
              └─────────┬─────────┘
                        ▼
              Quality & Diversity
                        │
                        ▼
                     Publish
```

ProxyRift collects public proxy configurations, normalizes and deduplicates them, then checks whether they are reachable.

From there, the pipeline builds two lists:

**All** is a larger pool of configurations that passed transport-level screening.

**Light** is a smaller quality-focused pool. Candidates are ranked using LightGBM and historical consumer evidence before going through deeper validation.

Deep validation can include real HTTP requests, repeated checks, endpoint diversity, latency, jitter, transfer performance, and protocol-specific checks.

## 🎯 What the Lists Mean

### Light

Light is intended to contain the better-performing configurations found during each update.

A configuration must pass the current validation pipeline before it can be published to Light.

Passing validation does **not** mean that a proxy will remain available or continue performing well later. Public endpoints can disappear, become overloaded, change configuration, or stop responding.

### All

All is a larger transport-reachable pool.

A configuration appearing in All means it passed the initial transport screening at collection time. It is not a guarantee of speed, stability, or long-term usability.

## 🧠 LightGBM

The Light pool uses LightGBM as a ranking aid.

The model is trained from historical validation data and uses structural characteristics of proxy configurations together with previous validation outcomes to estimate which candidates are more likely to be useful.

It is not intended to predict proxy quality perfectly. Its main purpose is to reduce unnecessary deep validation work and prioritize promising candidates.

When there is not enough training data, ProxyRift falls back to a neutral score instead of pretending the model is confident.

## 🔒 Security

ProxyRift handles third-party proxy configurations and launches Xray and/or sing-box during validation.

The validator parses supported proxy URI formats and builds constrained configurations rather than directly executing arbitrary proxy configuration blobs.

Endpoint resolution also rejects private, loopback, link-local, multicast, and other disallowed address ranges.

These checks reduce the attack surface, but they do not make third-party proxy endpoints trustworthy. Run ProxyRift in an isolated or sandboxed environment when operating it yourself.

## 🧪 Validation Notes

Proxy validation is a snapshot.

A proxy can pass every check during one run and fail later. Network conditions, server configuration, congestion, filtering, and endpoint availability can all change independently of ProxyRift.

For that reason, ProxyRift reports observed behavior rather than claiming that a node is permanently "good".

Independent testing of the published subscriptions is encouraged. A useful test is to sample published nodes from an environment separate from the collection runner and compare usability, stability, latency, and transfer performance against the published pool.

## 🔄 Updates

ProxyRift regenerates the subscriptions **once every hour**.

Each update:

1. Collects fresh public configurations
2. Normalizes and deduplicates them
3. Performs transport screening
4. Builds the All and Light candidate pools
5. Validates the selected Light candidates
6. Generates the subscription formats
7. Publishes the updated results

The GitHub Actions workflow is manually dispatched by an external scheduler on the hourly cadence.

[![Update Configs](https://github.com/rekt0ro/ProxyRift/actions/workflows/update.yml/badge.svg?branch=main)](https://github.com/rekt0ro/ProxyRift/actions/workflows/update.yml)
