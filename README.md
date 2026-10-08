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

ProxyRift collects third-party proxy configurations and validates selected candidates using Xray and/or sing-box.

During Light validation, supported proxy URI formats are parsed and constrained configurations are constructed instead of directly executing arbitrary proxy configuration blobs. Endpoint resolution also rejects private, loopback, link-local, multicast, and other disallowed address ranges.

The All list does not go through the same deep Xray/sing-box validation. It is based on transport-level screening and should be treated accordingly.

These checks reduce the attack surface, but they do not make third-party proxy endpoints trustworthy. Run ProxyRift in an isolated or sandboxed environment when operating it yourself.

## 🧪 Validation Notes

Proxy validation is a snapshot.

A proxy can pass every check during one run and fail later. Network conditions, server configuration, congestion, filtering, and endpoint availability can all change independently of ProxyRift.

ProxyRift publishes the results of those validation runs as subscription lists. Passing validation means that a configuration met the relevant checks at the time it was tested, not that it will remain available or performant indefinitely.

Independent testing of the published subscriptions is encouraged. The most useful way to evaluate the lists is to sample nodes and test them from your own environment against real-world usability, stability, latency, and transfer performance.

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
