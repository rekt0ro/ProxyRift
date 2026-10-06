#!/usr/bin/env python3
import argparse
import base64
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


PROTOCOLS = (
    "vless", "vmess", "trojan", "ss", "socks", "socks4", "socks4a",
    "socks5", "socks5h", "http", "hysteria", "hysteria2", "hy2", "tuic",
    "wg", "other",
)
BACKENDS = ("sing-box", "xray", "fallback", "other")
TRANSPORTS = (
    "tcp", "ws", "grpc", "xhttp", "splithttp", "h2", "httpupgrade",
    "quic", "wireguard", "raw", "default", "other",
)
SECURITIES = ("none", "tls", "reality", "xtls", "default", "other")
PORT_BUCKETS = ("web", "alt-web", "other", "unknown")

DEFAULT_TRANSPORT = {
    "hysteria": "quic",
    "hysteria2": "quic",
    "hy2": "quic",
    "tuic": "quic",
    "wg": "wireguard",
}

DEFAULT_SECURITY = {
    "trojan": "tls",
    "https": "tls",
}

KNOWN_PORTS = {
    "vless": 443,
    "vmess": 443,
    "trojan": 443,
    "ss": 443,
    "hysteria": 443,
    "hysteria2": 443,
    "hy2": 443,
    "tuic": 443,
    "http": 80,
    "https": 443,
}


def _one_hot(value, vocabulary):
    return [1.0 if value == item else 0.0 for item in vocabulary] + [
        1.0 if value not in vocabulary else 0.0
    ]


def _port_bucket(port):
    if port in (80, 443):
        return "web"
    if port in (8080, 8443, 2053, 2083, 2087, 2096):
        return "alt-web"
    if port is None or port <= 0:
        return "unknown"
    return "other"


def _value_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "tls"}
    return False


def _decode_vmess(config):
    try:
        payload = config.split("://", 1)[1].split("#", 1)[0].strip()
    except IndexError:
        return None

    padding = "=" * ((4 - len(payload) % 4) % 4)
    candidates = [payload + padding]
    candidates.append(payload.replace("-", "+").replace("_", "/") + padding)

    for candidate in candidates:
        try:
            decoded = base64.b64decode(candidate, validate=False)
            value = json.loads(decoded.decode("utf-8"))
            if isinstance(value, dict):
                return value
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            continue
    return None


def _parse_config(config):
    cleaned = config.split("#", 1)[0]
    parsed = urlsplit(cleaned)
    scheme = parsed.scheme.lower()

    vmess = _decode_vmess(config) if scheme == "vmess" else None
    if vmess is not None:
        protocol = "vmess"
        raw_transport = str(vmess.get("net") or "default").strip().lower()
        transport = raw_transport or "default"
        security = "tls" if _value_bool(vmess.get("tls")) else "none"
        try:
            port = int(vmess.get("port", 0))
        except (TypeError, ValueError):
            port = 0
        has_sni = any(
            isinstance(vmess.get(key), str) and vmess.get(key).strip()
            for key in ("sni", "serverName", "servername")
        )
        has_host = any(
            isinstance(vmess.get(key), str) and vmess.get(key).strip()
            for key in ("host", "Host")
        )
        has_path = isinstance(vmess.get("path"), str) and bool(vmess.get("path").strip())
        query_parameter_count = 0
    else:
        protocol = scheme or "other"
        query = parse_qs(parsed.query, keep_blank_values=True)
        first = lambda names: next(
            (query[name][0] for name in names if name in query and query[name]),
            "",
        )
        transport = first(("type", "network", "transport", "net")).strip().lower()
        if not transport:
            transport = DEFAULT_TRANSPORT.get(protocol, "default")
        security = first(("security", "tls")).strip().lower()
        if not security:
            security = DEFAULT_SECURITY.get(protocol, "default")
        port = parsed.port
        if port is None:
            port = KNOWN_PORTS.get(protocol, 0)
        has_sni = bool(first(("sni", "serverName", "servername")).strip())
        has_host = bool(first(("host", "authority")).strip())
        has_path = bool(parsed.path and parsed.path != "/") or bool(
            first(("path",)).strip()
        )
        query_parameter_count = len(query)

    if protocol not in PROTOCOLS:
        protocol = "other"
    if transport not in TRANSPORTS:
        transport = "other"
    if security not in SECURITIES:
        security = "other"

    if protocol in {"http", "socks", "socks4", "socks4a", "socks5", "socks5h"}:
        backend = "xray"
    elif security == "reality":
        backend = "fallback"
    elif transport in {"xhttp", "splithttp"}:
        backend = "xray"
    else:
        backend = "sing-box"

    tls_enabled = security in {"tls", "reality", "xtls"}
    reality_enabled = security == "reality"

    return {
        "protocol": protocol,
        "backend": backend,
        "transport": transport,
        "security": security,
        "port": int(port or 0),
        "query_parameter_count": int(query_parameter_count),
        "has_sni": bool(has_sni),
        "has_host": bool(has_host),
        "has_path": bool(has_path),
        "tls_enabled": tls_enabled,
        "reality_enabled": reality_enabled,
    }


def vector_from_fields(fields):
    return (
        _one_hot(str(fields.get("protocol", "other")).lower(), PROTOCOLS)
        + _one_hot(str(fields.get("backend", "other")).lower(), BACKENDS)
        + _one_hot(str(fields.get("transport", "other")).lower(), TRANSPORTS)
        + _one_hot(str(fields.get("security", "other")).lower(), SECURITIES)
        + _one_hot(_port_bucket(int(fields.get("port", 0) or 0)), PORT_BUCKETS)
        + [
            min(max(float(fields.get("query_parameter_count", 0) or 0), 0.0), 32.0) / 32.0,
            float(bool(fields.get("has_sni"))),
            float(bool(fields.get("has_host"))),
            float(bool(fields.get("has_path"))),
            float(bool(fields.get("tls_enabled"))),
            float(bool(fields.get("reality_enabled"))),
        ]
    )


def load_training(path):
    features = []
    labels = []
    positive = 0
    negative = 0
    file_path = Path(path)
    if not file_path.is_file():
        return features, labels, positive, negative

    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            label = row.get("label")
            fields = row.get("features")
            if not isinstance(label, dict) or not isinstance(fields, dict):
                continue
            if not isinstance(label.get("strict_pass"), bool):
                continue
            if int(label.get("strict_checks", 0) or 0) <= 0:
                continue

            features.append(vector_from_fields(fields))
            passed = bool(label["strict_pass"])
            labels.append(1 if passed else 0)
            if passed:
                positive += 1
            else:
                negative += 1

    return features, labels, positive, negative


def train_predict(training_path, candidates):
    from lightgbm import LGBMClassifier

    x_train, y_train, positive, negative = load_training(training_path)
    if len(x_train) < 500 or positive < 50 or negative < 50:
        return {config: 0.5 for config in candidates}, len(x_train), positive, negative, False

    model = LGBMClassifier(
        objective="binary",
        n_estimators=140,
        learning_rate=0.05,
        num_leaves=15,
        min_child_samples=40,
        subsample=0.90,
        colsample_bytree=0.90,
        reg_alpha=0.10,
        reg_lambda=0.10,
        class_weight="balanced",
        random_state=42,
        n_jobs=4,
        verbosity=-1,
    )
    model.fit(x_train, y_train)

    x_candidates = [vector_from_fields(_parse_config(config)) for config in candidates]
    probabilities = model.predict_proba(x_candidates)[:, 1]
    scores = {
        config: max(0.0, min(1.0, float(score)))
        for config, score in zip(candidates, probabilities)
    }
    return scores, len(x_train), positive, negative, True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--training", required=True)
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    candidates = []
    with Path(args.candidates).open("r", encoding="utf-8") as handle:
        seen = set()
        for line in handle:
            config = line.rstrip("\n")
            if config and config not in seen:
                seen.add(config)
                candidates.append(config)

    scores, rows, positive, negative, trained = train_predict(args.training, candidates)
    payload = {
        "version": 1,
        "trained": trained,
        "training_rows": rows,
        "training_passes": positive,
        "training_failures": negative,
        "feature_count": len(next(iter(scores.values()), [])) if False else 0,
        "scores": scores,
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(
        f"[INFO] [LightGBM] trained={trained} rows={rows} passes={positive} "
        f"failures={negative} candidates={len(candidates)}"
    )


if __name__ == "__main__":
    main()
