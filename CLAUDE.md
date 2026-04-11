# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 專案概述

SOKUHO 是將每日 PDF 銷售速報（SOKUHO）匯入 SQL Server 的 ETL 工具，最終供 PowerBI 報表使用。

---

## 常用指令

**環境設定（首次）：**
```bash
cp .env.example .env        # 填入 DB 連線資訊
docker-compose up -d        # 啟動 SQL Server（可選，也可用現有 SQL Server）
uv sync                     # 安裝依賴
```

**初始化資料庫（首次）：**
```
sqlcmd -S localhost,1433 -U sa -P <password> -i db/schema.sql
sqlcmd -S localhost,1433 -U sa -P <password> -i db/seed_stores.sql
```

**執行 ETL：**
```bash
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31        # 增量匯入
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31 --force  # 強制重跑
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31 --dry-run  # 驗證不寫入
uv run python main.py --file "SOKUHO 2026.03.01.pdf"                     # 單一檔案
```

---

## 架構說明

### ETL 流程

```
PDF 檔案 → pdf_parser.py → transformer.py → loader.py → SQL Server
              parse_pdf()   records_to_dataframe()  load_file()
```

1. **`etl/sokuho_overrides.py`** — 掃描 PDF 根目錄，依 `sokuho_import_overrides.yaml` 決定每個日期要匯入哪個 PDF（處理改版、同日多檔、非標準檔名）。
2. **`etl/pdf_parser.py`** — 用 `pdfplumber` 提取表格資料。每份 PDF 共 2 頁，每家門市各有 DAILY 和 MTD 兩列。
3. **`etl/transformer.py`** — 轉為 DataFrame 並做基本驗證（未知門市名稱、數值範圍、Day-1 DAILY≈MTD 校驗）。
4. **`etl/loader.py`** — 逐列執行 MERGE 寫入 `fact_sales`，並在 `etl_log` 記錄結果（冪等性：成功載入的檔案預設跳過）。

### PDF 格式偵測

PDF 有四種格式（以 page1/page2 的門市數量組合判斷）：

| 格式 | Page 1 門市數 | Page 2 門市數 | 適用時期 |
|------|-------------|-------------|---------|
| A    | 17          | 10          | 10 月   |
| B    | 17          | 8           | 11/1–5  |
| D    | 17          | 11          | 11/6–10 |
| C    | 19          | 9           | 11/11+  |

`_detect_format()` 使用最近距離匹配（store_count = rows / 2）。

### 資料庫結構

- **`dim_store`** — 門市主檔（store_code, store_name, store_type）。`store_type` 值：`store`、`subtotal`、`grand_total`、`HQ`。
- **`fact_sales`** — 主事實表，唯一鍵為 `(report_date, store_id, record_type)`。
- **`etl_log`** — 每次匯入結果（`SUCCESS` / `FAILED` / `SKIPPED`）。
- **`etl_sokuho_import_resolution`** — 記錄每個 report_date 實際使用哪個 PDF 檔（override 情況追蹤）。

PowerBI 透過三個 View 存取資料：`vw_daily_store_sales`、`vw_daily_grand_total`、`vw_month_end_reconcile`。

### 重要設計決策

- **人事費欄位（ft_expense、pt_expense 等）在 `load_dataframe` 強制設為 NULL**，避免 PowerBI DAX 誤用。即使 PDF 中有此數值，也不寫入 DB。
- **`unknown-store-policy`**：預設 `skip_row`（未知門市只略過該列）；`skip_day` 則整天不匯入。
- **門市名稱正規化**（`_normalize_store_name`）：PDF 的 Unicode 連字號統一換成 ASCII `-`；`ALL 17/19 STORES` 統一為 `ALL 18 STORES`；`BD` → `Business Development`。
- **非標準 PDF 覆寫** 設定在 `etl/sokuho_import_overrides.yaml`，異動時需同步更新此 YAML（不修改 OneDrive 原始檔名）。
