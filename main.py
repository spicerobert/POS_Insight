#!/usr/bin/env python
"""
POS Insight ETL entry point.

SOKUHO PDF 預設根目錄為 OneDrive 下的 SOKUHO 資料夾（依年度分子資料夾，如 SOKUHO\\2026）。
可覆寫：環境變數 PDF_DIR（見 .env.example）或 --pdf-dir。

Usage:
  python main.py
  python main.py --pdf-dir "D:/other/SOKUHO"
  python main.py --file "SOKUHO 2020.10.01.pdf"
  python main.py --force
  python main.py --dry-run
  --overrides-yaml etl/sokuho_import_overrides.yaml  # non-standard filenames (default)
  --from-date 2020-01-01 --to-date 2025-12-31
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

from etl.pdf_parser import parse_pdf, parse_date_from_filename
from etl.sokuho_overrides import build_job_list, load_overrides_yaml, resolve_file_for_date
from etl.transformer import records_to_dataframe
from etl import loader as db

# ── Logging setup ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# 實務上 SOKUHO 僅集中於此樹狀目錄；請以 PDF_DIR 或 --pdf-dir 覆寫其他環境。
DEFAULT_PDF_DIR = r"E:\OneDrive - Aunt Stella Company\SOKUHO"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SOKUHO PDF → SQL Server ETL")
    p.add_argument(
        "--pdf-dir",
        default=os.environ.get("PDF_DIR", DEFAULT_PDF_DIR),
        help=(
            "SOKUHO 根目錄（遞迴搜尋 *.pdf）。預設：環境變數 PDF_DIR，否則為 OneDrive 下 SOKUHO 路徑。"
        ),
    )
    p.add_argument("--file", help="Process a single PDF (basename only; must exist under pdf-dir)")
    p.add_argument("--force", action="store_true", help="Re-process already-loaded files")
    p.add_argument("--dry-run", action="store_true", help="Parse PDFs but do not write to DB")
    p.add_argument("--verbose", action="store_true", help="Enable DEBUG logging")
    p.add_argument(
        "--overrides-yaml",
        default=None,
        help="YAML mapping for non-standard SOKUHO filenames (default: etl/sokuho_import_overrides.yaml)",
    )
    p.add_argument("--from-date", type=str, default=None, help="Only import report_date >= this (YYYY-MM-DD)")
    p.add_argument("--to-date", type=str, default=None, help="Only import report_date <= this (YYYY-MM-DD)")
    p.add_argument(
        "--unknown-store-policy",
        choices=["skip_day", "skip_row"],
        default="skip_row",
        help=(
            "How to handle store_name not found in dim_store: "
            "skip_day = skip whole file/day, skip_row = ignore only unknown rows."
        ),
    )
    return p.parse_args()


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    y, m, d = s.strip().split("-")
    return date(int(y), int(m), int(d))


def main() -> None:
    load_dotenv()
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    pdf_dir = Path(args.pdf_dir)
    if not pdf_dir.is_dir():
        logger.error("Not a directory: %s", pdf_dir)
        sys.exit(1)

    yaml_path = Path(args.overrides_yaml) if args.overrides_yaml else None
    date_from = _parse_date(args.from_date)
    date_to = _parse_date(args.to_date)

    if args.file:
        pdf_path = pdf_dir / args.file
        if not pdf_path.exists():
            logger.error("File not found: %s", pdf_path)
            sys.exit(1)
        report_date = parse_date_from_filename(pdf_path.name)
        if report_date is None:
            logger.error("Cannot parse date from filename: %s", pdf_path.name)
            sys.exit(1)
        data = load_overrides_yaml(yaml_path)
        chosen, resolution = resolve_file_for_date([pdf_path], report_date, data)
        if chosen is None:
            logger.error(
                "Cannot resolve %s for %s — check etl/sokuho_import_overrides.yaml.",
                args.file,
                report_date,
            )
            sys.exit(1)
        if chosen.resolve() != pdf_path.resolve():
            logger.error(
                "For %s the YAML selects a different file: %s (you asked for %s).",
                report_date,
                chosen,
                pdf_path,
            )
            sys.exit(1)
        jobs = [(pdf_path, resolution)]
    else:
        jobs = build_job_list(pdf_dir, yaml_path=yaml_path, date_from=date_from, date_to=date_to)
        if not jobs:
            logger.error("No PDFs to process under %s (check date range and overrides).", pdf_dir)
            sys.exit(1)

    logger.info("Found %d PDF file(s) to process.", len(jobs))
    logger.info("Unknown store policy: %s", args.unknown_store_policy)

    conn = None
    store_map: dict[str, int] = {}

    if not args.dry_run:
        try:
            conn = db.get_connection(
                server=os.environ["DB_SERVER"],
                database=os.environ["DB_NAME"],
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
            )
            store_map = db.load_store_map(conn)
            logger.info("Connected to SQL Server. %d stores in dim_store.", len(store_map))
        except KeyError as e:
            logger.error("Missing environment variable: %s. Check your .env file.", e)
            sys.exit(1)
        except Exception as e:
            logger.error("Cannot connect to SQL Server: %s", e)
            sys.exit(1)

    total_loaded = 0
    total_skipped = 0
    total_failed = 0

    for pdf_path, resolution in tqdm(jobs, desc="Processing PDFs", unit="file"):
        try:
            records = parse_pdf(pdf_path)
            if not records:
                logger.warning("No records extracted from %s", pdf_path.name)
                total_failed += 1
                continue

            df = records_to_dataframe(records)
            if df.empty:
                logger.warning("Empty DataFrame after transform for %s", pdf_path.name)
                total_failed += 1
                continue

            report_date = parse_date_from_filename(pdf_path.name)
            if report_date is None:
                logger.error("Cannot parse date from %s", pdf_path.name)
                total_failed += 1
                continue

            if args.dry_run:
                logger.info(
                    "DRY-RUN %s → %d records (DAILY: %d, MTD: %d)",
                    pdf_path.name,
                    len(df),
                    (df["record_type"] == "DAILY").sum(),
                    (df["record_type"] == "MTD").sum(),
                )
                continue

            n = db.load_file(
                conn=conn,
                df=df,
                store_map=store_map,
                source_file=pdf_path.name,
                report_date=report_date,
                force=args.force,
                resolution=resolution,
                unknown_store_policy=args.unknown_store_policy,
            )

            if n == 0:
                total_skipped += 1
            else:
                total_loaded += n

        except Exception as exc:
            logger.error("ERROR processing %s: %s", pdf_path.name, exc, exc_info=True)
            total_failed += 1

    if conn:
        conn.close()

    logger.info(
        "Done. Loaded: %d rows | Skipped files: %d | Failed files: %d",
        total_loaded,
        total_skipped,
        total_failed,
    )
    if total_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
