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

**All**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all.txt
```

**All · Base64**

```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-base64.txt
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
      └──────────────► Light
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

The **Light** list goes through deeper validation using multiple targets, endpoint diversity, reliability, latency, jitter, transfer performance, and protocol-specific checks.

### 🧠 Consumer Learning

Light consumer validation keeps two kinds of evidence. Recent observations use a 30-day decay so the ranking can adapt to changing conditions. Successful consumer connections are also accumulated as permanent structural support.

The permanent learning stores structural hashes and aggregates rather than raw proxy URLs. New configurations are scored through a hierarchy of global, protocol, archetype, and structural-family evidence, so a new configuration can inherit consumer compatibility from previously successful structures even when that exact configuration has never been observed.

## 🔄 Automatic Updates

ProxyRift regenerates its subscriptions through GitHub Actions.

Each update collects fresh public configurations, screens them, builds the All and Light pools, generates the subscription formats, and publishes the results.

[![Update Configs](https://github.com/rekt0ro/ProxyRift/actions/workflows/update.yml/badge.svg?branch=main)](https://github.com/rekt0ro/ProxyRift/actions/workflows/update.yml) [![Dependabot Updates](https://github.com/rekt0ro/ProxyRift/actions/workflows/dependabot/dependabot-updates/badge.svg?branch=main)](https://github.com/rekt0ro/ProxyRift/actions/workflows/dependabot/dependabot-updates) [![Auto-merge Dependabot Actions](https://github.com/rekt0ro/ProxyRift/actions/workflows/dependabot-auto-merge.yml/badge.svg?branch=main)](https://github.com/rekt0ro/ProxyRift/actions/workflows/dependabot-auto-merge.yml)
