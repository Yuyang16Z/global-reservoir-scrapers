# Source admission

Use this reference before adding a new source or materially changing how a source is retained.

## 1. Prove the routing decision

Search the repository and parent workspace for the agency, endpoint, source ID, country aliases, workflows, existing data, and an active writer. Create or update an evidence-backed source Profile before implementation. When the parent workspace is available, use its `agent-skills/reservoir-data-pipeline/assets/source-profile.template.json` and validator. In a standalone repository checkout, record the same evidence and unresolved gates in the source README/handoff; do not copy the complete workspace Skill into this repository.

Classify retention:

- `rolling_window`, `current_snapshot`, or `overwrite_prone`: continue the archival admission review;
- `permanent_archive`: route to the parent workspace's local `resovoir data/<country>/` by default;
- `unknown_review`: investigate only; do not create a deployment.

Do not treat static metadata, river cross-reference, or other legacy repository exceptions as precedents. A permanent archive requires a documented operational reason and human decision to enter this repository.

## 2. Prove archival need and safe operation

Record the direct endpoint, source objects and fields, official date semantics, timezone, publication cadence, visible retention, historical-query capability, terms evidence, and exact uncertainty. Then apply `WINDOWED_SOURCE_POLICY.md` rather than duplicating its deployment checklist here.

Use an isolated output directory for a representative fetch. Verify raw evidence, source dates, units, stable identifiers, missing-value behavior, merge keys, output paths, and run summary. Run twice to prove idempotency and simulate at least one missed-run recovery using saved evidence where practical.

## 3. Separate local work from deployment

A scraper, workflow draft, or registry entry is not a deployment. Before calling the source operational, verify registry/workflow/path consistency, applicable tests, one authorized real workflow run, archived output, rerun behavior, and recovery margin.

Read `DATA_LICENCE_NOTICE.md`. The recorded decision for existing collection does not automatically authorize a new source, new data path, or expanded public exposure. Stop for human review when redistribution or public archival rights are not established.

End with separate states for retention routing, local code, scheduled deployment, archive freshness/coverage, licence/public exposure, and formatted delivery.
