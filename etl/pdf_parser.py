"""
PDF Parser for SOKUHO daily sales reports.

Each PDF (2 pages) encodes two rows per store:
  Row 1 → DAILY  : single-day figures
  Row 2 → MTD    : month-to-date cumulative

Four PDF formats are handled (detected by row-count):

  Format A  (Oct)        : page1=17 stores, page2=10 stores
  Format B  (Nov 1-5)    : page1=17 stores, page2=8  stores  (no OFFICE/GRAND)
  Format D  (Nov 6-10)   : page1=17 stores, page2=11 stores  (+Taiwan HSR)
  Format C  (Nov 11+)    : page1=19 stores, page2=9  stores  (CL SOGO B1 &
                                                               Taichung Mitsukoshi
                                                               moved to page 1)

Column layout (0-indexed; NAME at col 0):
  0  NAME
  1  SALES RESULT             2  SALES BUDGET %        3  SALES YoY %
  4  CUSTOMER COUNT           5  CUSTOMER YoY %
  6  UNIT PRICE               7  UNIT PRICE YoY %
  8  FT EXPENSE               9  FT BUDGET %
  10 PT EXPENSE               11 PT BUDGET %
  12 TOTAL PERSONNEL EXPENSE  13 TOTAL PERSONNEL BUDGET %
  14 TRANSFER
  15 SALES(INCL) RESULT       16 SALES(INCL) BUDGET %  17 SALES(INCL) YoY %
"""

import re
import logging
from pathlib import Path
from datetime import date

import pdfplumber

logger = logging.getLogger(__name__)

# ── Store sequences per format / page ──────────────────────────────────────

# Page 1 – shared by formats A, B, D
_P1_v1 = [
    "A4 Mitsukoshi",
    "Ban-Ciao Far Eastern",
    "BR4 Fuxing SOGO",
    "CS Far Eastern",
    "Nan-Shi Mitsukoshi",
    "Takashimaya",
    "Tian-Mu Sogo",
    "Zhong-Xiao Sogo",
    "Breeze Nanjing",
    "Taipei Area",
    "Dome Hanshin",
    "Hsin-Chu Big City",
    "Taichung Far Eastern",
    "Kaohsiung Sogo",
    "Tainan Mitsukoshi",
    "Non-Taipei Area",
    "Existing Store Sales",
]  # 17 stores → 34 rows

# Page 1 – format C (Nov 11+): CL SOGO B1 and Taichung Mitsukoshi promoted
_P1_v2 = [
    "A4 Mitsukoshi",
    "Ban-Ciao Far Eastern",
    "BR4 Fuxing SOGO",
    "CS Far Eastern",
    "Nan-Shi Mitsukoshi",
    "Takashimaya",
    "Tian-Mu Sogo",
    "Zhong-Xiao Sogo",
    "Breeze Nanjing",
    "Taipei Area",
    "CL SOGO B1",            # promoted from New Store
    "Dome Hanshin",
    "Hsin-Chu Big City",
    "Taichung Mitsukoshi",   # promoted from New Store
    "Taichung Far Eastern",
    "Kaohsiung Sogo",
    "Tainan Mitsukoshi",
    "Non-Taipei Area",
    "Existing Store Sales",
]  # 19 stores → 38 rows

# Page 2 – format A (Oct)
_P2_A = [
    "Far Eastern A13",
    "CL SOGO B1",
    "Hanshin Kaohsiung",
    "Taichung Mitsukoshi",
    "New Store Sales",
    "ALL 18 STORES",
    "Webshop",
    "Showroom",
    "OFFICE & WEB TOTAL",
    "GRAND TOTAL",
]  # 10 stores → 20 rows

# Page 2 – format B (Nov 1-5): OFFICE & WEB TOTAL and GRAND TOTAL absent
_P2_B = [
    "Far Eastern A13",
    "CL SOGO B1",
    "Hanshin Kaohsiung",
    "Taichung Mitsukoshi",
    "New Store Sales",
    "ALL 18 STORES",
    "Webshop",
    "Showroom",
]  # 8 stores → 16 rows

# Page 2 – format D (Nov 6-10): Taiwan HSR added, all previous stores present
_P2_D = [
    "Far Eastern A13",
    "CL SOGO B1",
    "Hanshin Kaohsiung",
    "Taichung Mitsukoshi",
    "New Store Sales",
    "ALL 18 STORES",
    "Webshop",
    "Showroom",
    "Taiwan High Speed Rail",
    "OFFICE & WEB TOTAL",
    "GRAND TOTAL",
]  # 11 stores → 22 rows

# Page 2 – format C (Nov 11+): CL SOGO B1 & Taichung Mitsukoshi moved to page 1
_P2_C = [
    "Far Eastern A13",
    "Hanshin Kaohsiung",
    "New Store Sales",
    "ALL 18 STORES",
    "Webshop",
    "Showroom",
    "Taiwan High Speed Rail",
    "OFFICE & WEB TOTAL",
    "GRAND TOTAL",
]  # 9 stores → 18 rows

# Map: (page1_store_count, page2_store_count) → (p1_list, p2_list, format_label)
_FORMAT_TABLE = [
    (17, 10, _P1_v1, _P2_A, "A"),
    (17,  8, _P1_v1, _P2_B, "B"),
    (17, 11, _P1_v1, _P2_D, "D"),
    (19,  9, _P1_v2, _P2_C, "C"),
]

# ── Table extraction settings ───────────────────────────────────────────────
_TABLE_SETTINGS = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": 3,
    "join_tolerance": 3,
    "edge_min_length": 3,
}


# ── Helpers ─────────────────────────────────────────────────────────────────

def parse_date_from_filename(filename: str) -> date | None:
    """Extract report date from filenames like 'SOKUHO 2020.10.01.pdf' or 'SOKUHO 2025.6.04.pdf'."""
    last: date | None = None
    for m in re.finditer(r"(\d{4})\.(\d{1,2})\.(\d{1,2})", str(filename)):
        try:
            last = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            continue
    return last


def to_float(value) -> float | None:
    if value is None:
        return None
    s = str(value).replace(",", "").strip()
    if s in ("", "-", "N/A", "#N/A", "#VALUE!"):
        return None
    # Accounting-style negatives from PDF/Excel exports, e.g. "(1234)".
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1].strip()
    try:
        return float(s)
    except ValueError:
        return None


def to_int(value) -> int | None:
    f = to_float(value)
    return int(f) if f is not None else None


def _is_data_row(row: list) -> bool:
    """
    True when the row carries numeric data (not a header row).
    We check columns 1-5 (skip NAME at 0).
    An all-zero row still counts as data.
    """
    if not row:
        return False
    check = row[1:6] if len(row) > 5 else row[1:]
    for c in check:
        v = to_float(c)
        if v is not None:
            return True
    return False


def _extract_data_rows(page) -> list[list]:
    """Extract numeric data rows from a page."""
    tables = page.extract_tables(_TABLE_SETTINGS)
    if not tables:
        return []
    table = max(tables, key=len)
    return [row for row in table if _is_data_row(row)]


def _detect_format(p1_rows: int, p2_rows: int) -> tuple[list, list, str]:
    """
    Pick the best matching format given the number of data rows on each page.
    Uses nearest-match on store counts (rows / 2).
    """
    p1_stores = p1_rows / 2
    p2_stores = p2_rows / 2

    best = None
    best_dist = float("inf")
    for exp_p1, exp_p2, p1_list, p2_list, label in _FORMAT_TABLE:
        dist = abs(p1_stores - exp_p1) + abs(p2_stores - exp_p2)
        if dist < best_dist:
            best_dist = dist
            best = (p1_list, p2_list, label)

    return best


def _build_record(
    row: list,
    store_name: str,
    record_type: str,
    report_date: date,
    source_file: str,
) -> dict:
    """
    Map a raw table row → typed record dict.

    pdfplumber may or may not include the NAME cell at index 0.
    Detect offset: if col-0 is not numeric → NAME column is present.
    """
    offset = 0 if to_float(row[0]) is not None else 1

    def g(col: int):
        real = col + offset
        return row[real] if real < len(row) else None

    return {
        "store_name":                   store_name,
        "record_type":                  record_type,
        "report_date":                  report_date,
        # SALES
        "sales_result":                 to_int(g(0)),
        "sales_budget_pct":             to_float(g(1)),
        "sales_yoy_pct":                to_float(g(2)),
        # CUSTOMER COUNT
        "customer_count":               to_int(g(3)),
        "customer_yoy_pct":             to_float(g(4)),
        # UNIT PRICE
        "unit_price":                   to_float(g(5)),
        "unit_price_yoy_pct":           to_float(g(6)),
        # PERSONNEL EXPENDITURES
        "ft_expense":                   to_int(g(7)),
        "ft_budget_pct":                to_float(g(8)),
        "pt_expense":                   to_int(g(9)),
        "pt_budget_pct":                to_float(g(10)),
        "total_personnel_expense":      to_int(g(11)),
        "total_personnel_budget_pct":   to_float(g(12)),
        # TRANSFER
        "transfer_amount":              to_int(g(13)),
        # SALES INCLUDING TRANSFER
        "sales_incl_result":            to_int(g(14)),
        "sales_incl_budget_pct":        to_float(g(15)),
        "sales_incl_yoy_pct":           to_float(g(16)),
        # METADATA
        "source_file":                  source_file,
    }


# ── Store name normalisation ────────────────────────────────────────────────

_UNICODE_HYPHENS = ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212", "\uff0d")


def _normalize_store_name(name: str) -> str:
    """Align PDF label variations with seeded dim_store store_name."""
    n = name.strip()
    # Merged cells sometimes yield "New Store Sales\nALL 19 STORES" in one NAME cell;
    # use the first non-empty line as the row's store label.
    if "\n" in n or "\r" in n:
        for line in n.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            s = line.strip()
            if s:
                n = s
                break
    # PDF 常用 Unicode 連字號；dim_store / seed 使用 ASCII "-"
    for u in _UNICODE_HYPHENS:
        n = n.replace(u, "-")
    if n in ("ALL 19 STORES", "ALL 17 STORES"):
        return "ALL 18 STORES"
    # PDF 有時用 BD，dim_store 主檔為 Business Development（同代號 4011）
    if n == "BD":
        return "Business Development"
    return n


# ── Public API ──────────────────────────────────────────────────────────────

def parse_pdf(pdf_path: str | Path) -> list[dict]:
    """
    Parse a SOKUHO PDF and return a flat list of record dicts.
    Each store yields 2 records: DAILY and MTD.
    """
    pdf_path = Path(pdf_path)
    report_date = parse_date_from_filename(pdf_path.name)
    if report_date is None:
        raise ValueError(f"Cannot parse date from filename: {pdf_path.name}")

    records: list[dict] = []

    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) < 2:
            logger.error("%s: expected 2 pages, got %d.", pdf_path.name, len(pdf.pages))
            return records

        rows_p1 = _extract_data_rows(pdf.pages[0])
        rows_p2 = _extract_data_rows(pdf.pages[1])

        p1_list, p2_list, fmt = _detect_format(len(rows_p1), len(rows_p2))

        logger.debug(
            "%s → format %s (page1=%d rows / %d stores, page2=%d rows / %d stores)",
            pdf_path.name, fmt,
            len(rows_p1), len(p1_list),
            len(rows_p2), len(p2_list),
        )

        # Warn only if the mismatch is more than 2 rows (i.e. a real format issue)
        for page_num, (rows, store_list) in enumerate(
            [(rows_p1, p1_list), (rows_p2, p2_list)], start=1
        ):
            expected = len(store_list) * 2
            if abs(len(rows) - expected) > 2:
                logger.warning(
                    "%s page %d: expected %d data rows (%d stores), got %d. "
                    "Format detected: %s",
                    pdf_path.name, page_num, expected, len(store_list),
                    len(rows), fmt,
                )

        # Build records from both pages
        for rows, store_list in [(rows_p1, p1_list), (rows_p2, p2_list)]:
            store_pairs = len(rows) // 2
            for store_idx in range(store_pairs):
                daily_idx = store_idx * 2
                mtd_idx = daily_idx + 1

                daily_row = rows[daily_idx] if daily_idx < len(rows) else None
                mtd_row = rows[mtd_idx] if mtd_idx < len(rows) else None

                if not daily_row:
                    logger.warning(
                        "%s: empty daily row at index %d (format %s).",
                        pdf_path.name, store_idx, fmt,
                    )
                    continue

                # Fallback: use the hardcoded store_list label for this index (when available).
                fallback_name = store_list[store_idx] if store_idx < len(store_list) else None

                # Prefer the NAME cell in PDF table (first column) when it is textual.
                store_name_daily = fallback_name
                if daily_row:
                    first = daily_row[0]
                    if to_float(first) is None and first not in (None, ""):
                        store_name_daily = _normalize_store_name(str(first))

                if store_name_daily is None:
                    logger.warning(
                        "%s: cannot resolve store_name for page row %d (format %s).",
                        pdf_path.name, store_idx, fmt,
                    )
                    continue

                records.append(
                    _build_record(daily_row, store_name_daily, "DAILY", report_date, pdf_path.name)
                )

                store_name_mtd = store_name_daily
                if mtd_row:
                    first = mtd_row[0]
                    if to_float(first) is None and first not in (None, ""):
                        store_name_mtd = _normalize_store_name(str(first))

                if mtd_row is not None:
                    records.append(
                        _build_record(mtd_row, store_name_mtd, "MTD", report_date, pdf_path.name)
                    )

    logger.info(
        "Parsed %s [fmt=%s] → %d records.",
        pdf_path.name, fmt, len(records),
    )
    return records
