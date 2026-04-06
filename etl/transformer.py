"""
Data transformer: validates and normalises records from pdf_parser
before database loading.
"""

import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

# All store names that must appear in dim_store
EXPECTED_STORE_NAMES = {
    "A4 Mitsukoshi", "Ban-Ciao Far Eastern", "Ban-Ciao", "BR4 Fuxing SOGO",
    "CS Far Eastern", "Nan-Shi Mitsukoshi", "Takashimaya",
    "Tian-Mu Sogo", "Tian-Mu Mitsukoshi", "Zhong-Xiao Sogo", "Breeze Nanjing",
    "Taipei Area", "Dome Hanshin",
    "Hsin-Chu Big City", "Taichung Far Eastern",
    "Kaohsiung Sogo", "Kaohsiung Zuoying", "Tainan Mitsukoshi",
    "Non-Taipei Area", "Existing Store Sales",
    "Far Eastern A13", "CL SOGO B1", "Hanshin Kaohsiung",
    "Taichung Mitsukoshi", "New Store Sales", "ALL 18 STORES",
    "Webshop", "Showroom", "Taiwan High Speed Rail",
    "TNHR", "Costco", "Anhe store", "Taipei 101", "MOMO", "Rainbow Market", "Temporary Stall",
    "A8 Mitsukoshi", "Gloria Outlets",
    "Business Development",
    "OFFICE & WEB TOTAL", "GRAND TOTAL",
}

# Numeric columns and their expected ranges for basic sanity checks
# (value, min_ok, max_ok) – None means no check
RANGE_CHECKS: dict[str, tuple[float | None, float | None]] = {
    "sales_result":             (0, None),
    "sales_budget_pct":         (0, 100_000),
    "sales_yoy_pct":            (0, 100_000),
    "customer_count":           (0, None),
    "unit_price":               (0, None),
    "ft_expense":               (0, None),
    "pt_expense":               (0, None),
    "total_personnel_expense":  (0, None),
    "transfer_amount":          (None, None),
}


def records_to_dataframe(records: list[dict]) -> pd.DataFrame:
    """Convert a list of record dicts to a validated DataFrame."""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # ── Validate store names ────────────────────────────────────────────────
    unknown = set(df["store_name"].unique()) - EXPECTED_STORE_NAMES
    if unknown:
        logger.warning("Unknown store names found (will still load): %s", unknown)

    # ── Validate record_type ────────────────────────────────────────────────
    bad_types = df[~df["record_type"].isin({"DAILY", "MTD"})]
    if not bad_types.empty:
        logger.error("Invalid record_type values:\n%s", bad_types[["store_name", "record_type"]])
        df = df[df["record_type"].isin({"DAILY", "MTD"})]

    # ── Sanity-check numeric ranges ─────────────────────────────────────────
    for col, (lo, hi) in RANGE_CHECKS.items():
        if col not in df.columns:
            continue
        if lo is not None:
            violations = df[df[col].notna() & (df[col] < lo)]
            if not violations.empty:
                logger.warning(
                    "Column '%s' has %d values below %s (will keep).",
                    col, len(violations), lo,
                )
        if hi is not None:
            violations = df[df[col].notna() & (df[col] > hi)]
            if not violations.empty:
                logger.warning(
                    "Column '%s' has %d values above %s (will keep).",
                    col, len(violations), hi,
                )

    # ── MTD cross-check on day 1: DAILY should equal MTD ───────────────────
    for report_date, grp in df.groupby("report_date"):
        if isinstance(report_date, date) and report_date.day == 1:
            daily = grp[grp["record_type"] == "DAILY"].set_index("store_name")["sales_result"]
            mtd   = grp[grp["record_type"] == "MTD"].set_index("store_name")["sales_result"]
            mismatches = daily.compare(mtd, result_names=("DAILY", "MTD")).dropna()
            if not mismatches.empty:
                logger.warning(
                    "Day-1 DAILY ≠ MTD mismatch for %s:\n%s", report_date, mismatches
                )

    return df
