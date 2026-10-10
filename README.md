# ProxyRift

Public proxy collector and validator.

**Supported:** VLESS · VMess · Trojan · Shadowsocks · Hysteria · Hysteria2 · TUIC · SOCKS · HTTP · WireGuard (All only)

## 📡 Subscriptions

Subscriptions are **regenerated hourly** from newly collected configurations. The All and Light lists use different screening and validation paths.

| List | Description |
| --- | --- |
| **Light** | Up to 200 configs, deeply tested |
| **All** | Up to 2,000 configs, primarily transport-screened |

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

## 🎯 Subscription Lists

### Light

Light is intended to contain the higher-quality configurations found during each update.

Candidates are ranked using LightGBM together with historical validation evidence before undergoing deeper validation.

A configuration must pass the current validation pipeline before it can be published to Light.

Passing validation does **not** mean that a proxy will remain available or continue performing well later. Public endpoints can disappear, become overloaded, change configuration, or stop responding.

### All

All is a larger pool of configurations. Most entries pass the initial transport-level screening. Some candidates whose transports are deferred from the initial probe can be added to All only after passing Light validation.

A configuration appearing in All means it passed transport screening at collection time or, if its transport was deferred, passed the Light validation pipeline. Neither path guarantees speed, stability, or long-term usability.

## 🧠 LightGBM

The Light pool uses LightGBM as a ranking aid.

The model is trained from historical validation data and uses structural characteristics of proxy configurations together with previous validation outcomes to estimate which candidates are more likely to perform well.

It is not intended to predict proxy quality perfectly. Its main purpose is to reduce the amount of deep validation required and prioritize promising candidates.

When there is not enough training data, ProxyRift falls back to a neutral score instead of assigning confidence where insufficient training data exists.

## 🔒 Security

ProxyRift collects third-party proxy configurations and validates selected candidates using Xray and/or sing-box.

During Light validation, supported proxy URI formats are parsed and constrained configurations are constructed instead of directly executing arbitrary proxy configuration blobs. Endpoint resolution also rejects private, loopback, link-local, multicast, and other disallowed address ranges.

The All list does not go through the same deep Xray/sing-box validation for every entry. It is primarily based on transport-level screening, with deferred candidates added only after they pass Light validation, and should be treated accordingly.

These checks reduce the attack surface, but they do not make third-party proxy endpoints trustworthy. Run ProxyRift in an isolated or sandboxed environment when operating it locally.

## 🧪 Validation Notes

Proxy validation is a snapshot.

A proxy can pass every check during one run and fail later. Network conditions, server configuration, congestion, filtering, and endpoint availability can all change independently of ProxyRift.

ProxyRift publishes the results of those validation runs as subscription lists. Passing validation means that a configuration met the relevant checks at the time it was tested, not that it will remain available or performant indefinitely.

Independent testing of the published subscriptions is encouraged. The best way to evaluate the lists is to sample nodes and test them from your own environment against real-world usability, stability, latency, and transfer performance.

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

[![Update Configs](https://github.com/rekt0ro/ProxyRift/actions/workflows/update.yml/badge.svg?branch=main)](https://github.com/rekt0ro/ProxyRift/actions/workflows/update.yml)
