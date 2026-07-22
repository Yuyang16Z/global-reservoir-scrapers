#!/usr/bin/env python3
"""Audit mandatory deployment metadata for rolling and ephemeral sources."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "config" / "windowed_sources.json"
REQUIRED_FIELDS = {
    "source_id", "country", "retention_class", "retention_window_hours",
    "publication_cadence_hours", "timezone", "cron",
    "max_schedule_gap_hours", "minimum_capture_opportunities",
    "overlap_strategy", "schedule_rationale", "workflow", "scraper",
    "data_path", "deployment_status", "licence_gate",
}
WINDOWED_CLASSES = {"rolling_window", "current_snapshot", "overwrite_prone"}
ACTIVE = "active"


def load_registry() -> dict:
    return json.loads(REGISTRY.read_text(encoding="utf-8"))


def audit_source(source: dict) -> list[str]:
    source_id = source.get("source_id", "<missing source_id>")
    issues = []
    missing = sorted(REQUIRED_FIELDS - set(source))
    if missing:
        issues.append(f"{source_id}: missing registry fields: {', '.join(missing)}")
        return issues
    if source["retention_class"] not in WINDOWED_CLASSES:
        issues.append(f"{source_id}: invalid retention_class {source['retention_class']!r}")
    if not source["cron"] or not all(isinstance(item, str) and item.strip() for item in source["cron"]):
        issues.append(f"{source_id}: cron must contain at least one schedule")
    if not isinstance(source["minimum_capture_opportunities"], int) or source["minimum_capture_opportunities"] < 2:
        issues.append(f"{source_id}: minimum_capture_opportunities must be at least 2")
    for field in ("timezone", "overlap_strategy", "schedule_rationale", "licence_gate"):
        if not isinstance(source[field], str) or not source[field].strip():
            issues.append(f"{source_id}: {field} must be documented")

    if source["deployment_status"] != ACTIVE:
        return issues

    for field in ("workflow", "scraper", "data_path"):
        value = source[field]
        if not isinstance(value, str) or not value:
            issues.append(f"{source_id}: active deployment requires {field}")
            continue
        if not (ROOT / value).exists():
            issues.append(f"{source_id}: {field} does not exist: {value}")

    workflow_value = source.get("workflow")
    if isinstance(workflow_value, str) and (ROOT / workflow_value).is_file():
        workflow_text = (ROOT / workflow_value).read_text(encoding="utf-8")
        required_tokens = {
            "schedule": "schedule:",
            "manual recovery": "workflow_dispatch",
            "write permission": "contents: write",
            "concurrency protection": "concurrency:",
            "persistent commit": "git push",
        }
        for label, token in required_tokens.items():
            if token not in workflow_text:
                issues.append(f"{source_id}: workflow missing {label} ({token})")
        for cron in source["cron"]:
            if cron not in workflow_text:
                issues.append(f"{source_id}: registered cron not found in workflow: {cron}")
        if source["data_path"] not in workflow_text:
            issues.append(f"{source_id}: workflow does not reference registered data_path {source['data_path']}")
    return issues


def build_report() -> dict:
    registry = load_registry()
    sources = registry.get("sources", [])
    issues = []
    seen = set()
    for source in sources:
        source_id = source.get("source_id")
        if source_id in seen:
            issues.append(f"duplicate source_id: {source_id}")
        seen.add(source_id)
        issues.extend(audit_source(source))
    active = [source for source in sources if source.get("deployment_status") == ACTIVE]
    pending = [source for source in sources if source.get("deployment_status") != ACTIVE]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "repository": registry.get("repository"),
        "policy_version": registry.get("policy_version"),
        "sources_registered": len(sources),
        "active_deployments": len(active),
        "pending_or_blocked": [
            {"source_id": source["source_id"], "status": source["deployment_status"]}
            for source in pending
        ],
        "issues": issues,
        "status": "PASS" if not issues else "FAIL",
    }


def write_report(report: dict) -> None:
    (ROOT / "WINDOWED_SOURCE_AUDIT.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    lines = [
        "# Windowed source deployment audit",
        "",
        f"Generated: {report['generated_at']}",
        "",
        f"Status: **{report['status']}**",
        "",
        f"- Registered windowed or ephemeral sources: **{report['sources_registered']}**",
        f"- Active deployments checked: **{report['active_deployments']}**",
        f"- Pending or source-blocked deployments: **{len(report['pending_or_blocked'])}**",
        "",
        "## Pending or blocked",
        "",
    ]
    if report["pending_or_blocked"]:
        lines.extend(
            f"- `{item['source_id']}`: `{item['status']}`"
            for item in report["pending_or_blocked"]
        )
    else:
        lines.append("None.")
    if report["issues"]:
        lines.extend(["", "## Compliance issues", ""])
        lines.extend(f"- {issue}" for issue in report["issues"])
    (ROOT / "WINDOWED_SOURCE_AUDIT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = build_report()
    if args.write_report:
        write_report(report)
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
