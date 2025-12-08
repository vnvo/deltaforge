# Checkpoints

Checkpoints record the progress of each pipeline so ingestion can resume from the last processed point.

- 🛰️ **Restart/crash-friendly**: checkpoints anchor exactly where each pipeline left off.

- By default, checkpoints are persisted to `./data/df_checkpoints.json`.
- Checkpoint commits occur after sinks acknowledge a batch according to the pipeline's commit policy.
- Required sinks must succeed before a checkpoint is written; optional sinks are best-effort unless `mode: all` is configured.

NB: the plan is to add more production grade storage backends for the checkpoints.

Use the REST API to pause or stop pipelines before maintenance to ensure checkpoints are flushed cleanly.