# Local Light consumer validation

`light_consumer_test` measures Light subscription candidates from a real consumer network. The result is intentionally split into two layers:

- `subscriptions/light-consumer-results.json` is local/private history. It stores an exact config hash plus structural hashes and pass/fail observations. Raw proxy URLs, country, and ISP are never written.
- `subscriptions/light-consumer-evidence.json` is the reusable knowledge layer. It stores only aggregated protocol, archetype, and structural-family statistics. It does not store raw configs or exact config hashes, so new configs can inherit evidence from similar historical structures.

The structural family deliberately ignores per-instance identity such as IP/host values, UUIDs, passwords, remarks, exact SNI values, and exact paths. It keeps reusable shape information such as protocol, transport, security, port bucket, host kind, SNI/header/path presence and shape, flow/obfs class, and selected feature presence.

The evidence model is hierarchical:

1. Exact config history remains useful locally when the same config returns.
2. A new config first inherits its structural-family history.
3. An unseen family falls back to its broader archetype.
4. An unseen archetype falls back to protocol history.
5. Completely new cases fall back to the global consumer prior.

Evidence uses a 30-day exponential half-life, so old observations do not disappear immediately but naturally lose influence as the consumer network or proxy ecosystem changes. A small exploration bonus prevents unseen families from becoming permanently invisible.

## Local usage

From the repository root:

```bash
./target/release/light_consumer_test
```

Typical defaults:

- input: `subscriptions/light.txt`
- private history: `subscriptions/light-consumer-results.json`
- reusable evidence: `subscriptions/light-consumer-evidence.json`
- Xray: `xray`
- sing-box: `sing-box`
- workers: 8
- batch size: 24
- timeout: 15 seconds
- max latency: 800 ms
- rounds: 1

For repeated observation:

```bash
./target/release/light_consumer_test --rounds 10
```

Do not commit `subscriptions/light-consumer-results.json`. The generated evidence file is designed to be reviewed and, after enough local rounds, committed to the repository so `polish_light` can use it during future update runs.

`polish_light` reads `subscriptions/light-consumer-evidence.json` automatically when it exists. The consumer signal is blended with the existing Light intelligence for strict candidate ranking, while current strict/transfer/stream validation remains authoritative for publication.

The current tester records one observation per compatible candidate per round. Candidates rejected before consumer validation are marked incompatible and are excluded from the reusable consumer evidence.

## Privacy

The local history contains stable exact and structural hashes. Stable hashes are not raw proxy URLs, but anyone who already has the original config can recompute them. The public evidence file is safer: it contains only aggregated structural-family, archetype, and protocol statistics.

The next step after the initial 10 local rounds is to review `subscriptions/light-consumer-evidence.json`, commit that aggregate file, and let normal `Update Configs` runs consume it.
