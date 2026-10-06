# Light consumer validation

The GitHub runner cannot tell whether a Light proxy works from a user's network path. This repository now includes a local consumer validator that runs the same public Xray/sing-box validation logic from the machine that actually uses the subscription.

Build and run it locally:

```bash
cargo run --release --bin light_consumer_test
```

By default it reads `subscriptions/light.txt`, uses `xray` and `sing-box` from `PATH`, and writes anonymous results to `subscriptions/light-consumer-results.json`.

The history file never stores raw proxy URLs, country names, ISP names, or other user-identifying fields. Each observation stores a stable hash, protocol, pass/fail, attempt counts, and validation metrics.

Useful options:

```bash
cargo run --release --bin light_consumer_test -- \
  --xray /path/to/xray \
  --singbox /path/to/sing-box \
  --rounds 3
```

Run the validator again whenever the Light subscription changes. Multiple rounds are stored, so the tool can distinguish one-off success from repeated consumer success.

This PR intentionally stops at local consumer observation. It does not change the published Light list automatically yet. The next integration step is to feed these anonymous observations into the ranking/training pipeline after the local validator has been proven on real consumer traffic.
