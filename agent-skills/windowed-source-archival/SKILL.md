---
name: windowed-source-archival
description: "Admit, operate, audit, and repair official reservoir sources in the Global Reservoir Scrapers windowed/ephemeral archival subsystem. Use for rolling-window, current-snapshot, overwrite-prone source capture, registry/workflow consistency, freshness, archival gaps, and repository handoff. Do not use for generic scraping, permanent-history country pipelines, formatted-delivery validation, or whole-workspace reporting."
---

# Windowed Source Archival

This repository is a narrow upstream archive for source observations that can disappear. It is not the full reservoir-data project and it is not the delivery package.

## Recover repository truth

1. Read repository `AGENTS.md`, `README.md`, `WINDOWED_SOURCE_POLICY.md`, `config/windowed_sources.json`, and `DATA_LICENCE_NOTICE.md`.
2. Check the repository root, branch, worktree status, `origin/main` tracking state, and the target source's README, workflow, data, and recent run summaries.
3. If the parent workspace is available, read its `AGENTS.md`, `PROJECT_STATUS.md`, and `docs/PROJECT_MAP.md` for cross-layer facts. Use the workspace `reservoir-data-pipeline` Skill when the task crosses into country research, formatted delivery, or whole-project reporting.
4. Distinguish local files, tracked remote state, deployed workflow revision, archived data freshness, and official source freshness. None proves the others.

## Choose one mode

- **Proposed new source or materially changed retention:** read [source admission](references/source-admission.md).
- **Failed/stale run, archival gap, workflow repair, or repository handoff:** read [archive maintenance](references/archive-maintenance.md).

Load only the reference needed for the current task.

## Invariants

- Classify retention before implementation or deployment. Admit `rolling_window`, `current_snapshot`, and `overwrite_prone` only after evidence and gates; keep `permanent_archive` local by default; do not deploy `unknown_review`.
- Preserve official observation/bulletin dates separately from fetch time. Never fabricate history, units, missing values, or source freshness.
- Preserve raw or checksum-traceable evidence, fetch the available overlap, and merge idempotently with stable source keys.
- A green workflow does not prove fresh data. A passing registry audit does not prove every eligible repository source is registered.
- Read `DATA_LICENCE_NOTICE.md`. Existing collection under the recorded owner decision is not blanket permission for new public exposure or redistribution.
- Do not call archived observations formatted or delivered. Those claims belong to the parent workspace's independent delivery gate.
- Preserve unrelated worktree changes. External reruns, workflow edits on GitHub, pushes, PRs, data publication, secrets, or new public exposure require explicit authorization or human review as applicable.

## Completion contract

Report these states separately using `verified`, `partial`, `blocked`, `regressed`, or `unverified`:

1. retention classification and admission evidence;
2. local implementation and deterministic checks;
3. actual scheduled deployment;
4. source freshness and archived historical coverage;
5. licence/public-exposure decision; and
6. formatted delivery, which remains independent and must not be inferred here.
