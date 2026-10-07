# ProxyRift

**Supported:** VLESS · VMess · Trojan · Shadowsocks · Hysteria · Hysteria2 · SOCKS · HTTP

## 📡 Subscriptions

| List      | Description                              |
| --------- | ---------------------------------------- |
| **Light** | Up to 200 configs, deeply validated      |
| **All**   | Up to 2,000 configs, transport-reachable |


**Light**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light.txt
```

**Light · Base64**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-base64.txt
```

**Light · Clash / Mihomo YAML**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-clash.yaml
```

**Light · sing-box JSON**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-singbox.json
```

**All**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all.txt
```

**All · Base64**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-base64.txt
```

**All · Clash / Mihomo YAML**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-clash.yaml
```

**All · sing-box JSON**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-singbox.json
```

## ⚙️ Pipeline

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

The **All** list is built from transport-reachable configurations and limited to 2,000 entries.

The **Light** list is ranked using native LightGBM scoring combined with learned consumer evidence, with consumer-proven configurations prioritized before deeper validation. Candidates are then validated against multiple targets using endpoint diversity, reliability, latency, jitter, transfer performance, and protocol-specific checks.

## 🔄 Automatic Updates

ProxyRift regenerates its subscriptions hourly through GitHub Actions.

Each update collects fresh public configurations, screens them, builds the All and Light pools, generates the subscription formats, and publishes the results.

[![Update Configs](https://github.com/rekt0ro/ProxyRift/actions/workflows/update.yml/badge.svg?branch=main)](https://github.com/rekt0ro/ProxyRift/actions/workflows/update.yml)
