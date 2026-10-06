# Light ML Training Dataset

ProxyRift records supervised-learning observations from Light strict rechecks in
`subscriptions/light-training.jsonl`, with the machine-readable readiness summary in
`subscriptions/light-training-stats.json`.

## Purpose

This dataset is groundwork for the future local LightGBM model. The current
validator does not use these rows for ranking or publication decisions.

Each row represents one candidate observed during one update run. Repeated
observations of the same candidate across different runs are intentional so a
future model can learn time-varying behavior.

## Row schema

Each JSONL row contains:

- `schema_version`: dataset format version.
- `observation_id`: unique run/candidate observation identifier.
- `observed_at`: Unix timestamp for the observation.
- `run_id`: GitHub Actions run ID when available.
- `candidate_fingerprint`: stable candidate identity used for linking
  observations without storing the raw proxy URL.
- `features`: values known before strict validation starts.
- `label.strict_pass`: whether the candidate survived strict validation.
- `label.strict_checks`: number of strict recheck rounds applied.
- `label.transfer_tested`: whether the candidate reached the 10 MiB gate.
- `label.transfer_pass`: transfer result when the 10 MiB gate was tested,
  otherwise `null`.
- `label.stream_tested`: whether the sustained stream-continuity gate was tested.
- `label.stream_pass`: stream-continuity result when tested, otherwise `null`.

The current feature contract has 19 fields:

`protocol`, `backend`, `transport`, `security`, `port`,
`query_parameter_count`, `has_sni`, `has_host`, `has_path`,
`tls_enabled`, `reality_enabled`, `early_attempts`,
`early_success_rate`, `early_median_ms`, `early_min_ms`,
`early_jitter_ms`, `early_throughput_kbps`, `history_checks`,
`history_pass_rate`.

## Leakage rules

Only information available before the strict validation decision may be stored
under `features`.

Strict-validation, 10 MiB transfer, and sustained stream-continuity outcomes are
labels, not model features. Raw proxy URLs are not stored in the training dataset.

A future training pipeline should preserve this separation and must not derive
features from strict or transfer results.

## Dataset hygiene

Rows older than 45 days are removed during persistence. The dataset is capped
at 50,000 rows. Malformed or incompatible rows are ignored rather than causing
the Light validation pipeline to fail.

History pass rates and AI anomaly measurements are candidate-level metrics.
Repeated strict rechecks of the same candidate do not count as additional
candidate observations, so retries cannot artificially dilute those rates.

Persistence is atomic and observation IDs are idempotent, so retrying the same
update run does not duplicate its observations.

Light strict rechecks reserve 15% of each recheck budget, capped at 64
candidates, for run-seeded exploration. Exploration candidates are selected
without the learned ranking signal while still respecting endpoint and family
diversity limits. This creates a controlled source of off-policy examples for
future model training without removing the deterministic safety gates.

## LightGBM readiness gate

The dataset now records all three technical quality layers used by the Light
pipeline: strict validation, 10 MiB transfer, and sustained stream continuity.
The persisted readiness report exposes two conservative gates:

- `strict_model_ready`: at least 5,000 rows, 20 update runs, 1,000 unique candidates,
  500 strict passes, and 500 strict failures.
- `end_to_end_model_ready`: the strict gate plus at least 1,000 transfer tests,
  100 transfer passes, 100 transfer failures, 500 stream tests, 50 stream passes,
  and 50 stream failures.

These thresholds are readiness heuristics, not quality guarantees. Before enabling
LightGBM, training must still use time-ordered train/validation/holdout splits,
keep repeated candidate identities grouped appropriately, and compare Top-K
outcomes against the deterministic baseline.

The recommended rollout is:

1. train offline and evaluate on a future time window;
2. compare strict, transfer, and stream yield at the actual Light selection sizes;
3. run the model in shadow mode without changing publication decisions;
4. enable model ranking only after it improves or matches the deterministic baseline
   without degrading the mandatory safety gates.

The existing strict validator, 10 MiB transfer gate, and sustained stream gate
remain authoritative regardless of model output.

Rows from dataset schema version 1 are upgraded in place by adding an unlabeled
stream result (`stream_tested=false`, `stream_pass=null`); no historical stream
outcome is fabricated.
