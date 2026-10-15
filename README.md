# ProxyRift

Public proxy configs, collected and tested automatically.

## Subscriptions

### Light

Up to 200 configs that pass stricter validation, including transfer and sustained-stream checks.

**Plain text**
```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light.txt
```

**Base64**
```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-base64.txt
```

**Clash / Mihomo**
```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-clash.yaml
```

**sing-box**
```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/light-singbox.json
```

### All

Up to 2,000 configs that pass an HTTP handshake through the proxy. Transport reachability is tested where supported, but not every config goes through the full Light validation pipeline.

**Plain text**
```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all.txt
```

**Base64**
```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-base64.txt
```

**Clash / Mihomo**
```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-clash.yaml
```

**sing-box**
```text
https://raw.githubusercontent.com/rekt0ro/ProxyRift/main/subscriptions/all-singbox.json
```

## Supported Protocols

VLESS, VMess, Trojan, Shadowsocks, Hysteria, Hysteria2, TUIC, SOCKS, HTTP and WireGuard.

Hysteria v1 and WireGuard are not included in Light. WireGuard entries may appear in All's plain-text and Base64 lists, but are not supported by the current Clash/Mihomo or sing-box exporters.

Clash/Mihomo exports may omit configs they cannot represent. All sing-box may also contain fewer entries than its plain-text list. Light sing-box output is published only when every Light entry can be converted.

## Security and Trust

ProxyRift validates proxy compatibility, connectivity, and performance. It does not operate or independently audit the proxy servers it collects.

Passing validation does not guarantee that a proxy is trustworthy, private, anonymous, or secure. Proxy operators may observe connection metadata and inspect traffic that is not protected by end-to-end encryption. Use public proxies at your own risk.

For security vulnerabilities affecting ProxyRift itself, see our [Security Policy](SECURITY.md).

## Updates

Subscriptions are refreshed automatically through GitHub Actions.

Validation reflects the conditions at the time of testing.
