"""
Load etl/sokuho_import_overrides.yaml and resolve which PDF to import per report date.

Does not rename files on disk; resolution is recorded in etl_sokuho_import_resolution.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from etl.pdf_parser import parse_date_from_filename

logger = logging.getLogger(__name__)

DEFAULT_YAML = Path(__file__).resolve().parent / "sokuho_import_overrides.yaml"


@dataclass
class Resolution:
    """Metadata for one import (standard vs chosen file)."""

    report_date: date
    chosen_path: Path
    default_source_file: str | None
    note: str | None
    ignored_basenames: list[str]


def load_overrides_yaml(path: Path | None = None) -> dict[str, Any]:
    p = path or DEFAULT_YAML
    if not p.exists():
        return {"version": 1, "overrides": {}}
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data:
        return {"version": 1, "overrides": {}}
    data.setdefault("overrides", {})
    return data


def _entry_for_date(data: dict[str, Any], d: date) -> dict[str, Any] | None:
    key = d.isoformat()
    ov = data.get("overrides") or {}
    return ov.get(key)


def collect_pdfs_by_date(root: Path) -> dict[date, list[Path]]:
    """Recursive *.pdf under root, grouped by date parsed from filename."""
    by: dict[date, list[Path]] = {}
    for path in root.rglob("*.pdf"):
        rd = parse_date_from_filename(path.name)
        if rd is None:
            continue
        by.setdefault(rd, []).append(path)
    for paths in by.values():
        paths.sort(key=lambda p: str(p))
    return by


def resolve_file_for_date(
    candidates: list[Path],
    report_date: date,
    data: dict[str, Any],
) -> tuple[Path | None, Resolution | None]:
    """
    Pick exactly one Path for this report date, applying YAML rules.

    Returns (None, None) if no usable file.
    """
    entry = _entry_for_date(data, report_date)
    ignored = set()
    if entry:
        for b in entry.get("ignore_basenames") or []:
            ignored.add(b.lower())

    filtered = [p for p in candidates if p.name.lower() not in ignored]
    if not filtered:
        logger.warning("No PDF left for %s after ignore_basenames", report_date)
        return None, None

    default_name: str | None = None
    note: str | None = entry.get("note") if entry else None
    ignored_list = [p.name for p in candidates if p.name.lower() in ignored]

    if entry and entry.get("use_basename"):
        want = entry["use_basename"]
        path_needle = entry.get("path_contains")
        matches = [p for p in filtered if p.name == want]
        if path_needle:
            matches = [p for p in matches if path_needle in str(p).replace("\\", "/")]
        if len(matches) == 1:
            chosen = matches[0]
            default_name = entry.get("instead_of_basename")
            return chosen, Resolution(
                report_date=report_date,
                chosen_path=chosen,
                default_source_file=default_name,
                note=note,
                ignored_basenames=ignored_list,
            )
        if len(matches) > 1:
            logger.error("Multiple matches for %s use_basename=%r path_contains=%r", report_date, want, path_needle)
            return None, None
        logger.error("No match for %s use_basename=%r (path_contains=%r)", report_date, want, path_needle)
        return None, None

    # No explicit use_basename: single file wins
    if len(filtered) == 1:
        chosen = filtered[0]
        return chosen, Resolution(
            report_date=report_date,
            chosen_path=chosen,
            default_source_file=None,
            note=note if entry else None,
            ignored_basenames=ignored_list,
        )

    # Multiple files, no use_basename — try same basename (duplicate folders)
    names = {p.name for p in filtered}
    if len(names) == 1:
        # identical basename in multiple folders — take first sorted (deterministic but ambiguous)
        logger.warning(
            "Duplicate paths for %s (%d copies of %s); add path_contains to sokuho_import_overrides.yaml",
            report_date,
            len(filtered),
            next(iter(names)),
        )
        chosen = filtered[0]
        return chosen, Resolution(
            report_date=report_date,
            chosen_path=chosen,
            default_source_file=None,
            note=(note or "") + " [警告：同日多路徑未指定 path_contains，取排序第一筆]",
            ignored_basenames=ignored_list,
        )

    logger.error(
        "Ambiguous PDFs for %s (%s); configure overrides YAML",
        report_date,
        [p.name for p in filtered],
    )
    return None, None


def build_job_list(
    root: Path,
    yaml_path: Path | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[tuple[Path, Resolution | None]]:
    """
    Returns ordered list of (pdf_path, resolution) for all dates under root.
    Skips dates with no PDF or unresolved ambiguity.
    """
    data = load_overrides_yaml(yaml_path)
    by_date = collect_pdfs_by_date(root)
    jobs: list[tuple[Path, Resolution | None]] = []

    for rd in sorted(by_date.keys()):
        if date_from and rd < date_from:
            continue
        if date_to and rd > date_to:
            continue
        path, res = resolve_file_for_date(by_date[rd], rd, data)
        if path is None:
            continue
        jobs.append((path, res))

    return jobs
