# POS Insight - PowerBI 匯入與 SQL 語法備忘

此文件整理自 `terminals/3.txt` 對話紀錄，以及專案既有 SQL 檔，目的是讓你後續可直接複製語法到 SQL Server / PowerBI 使用。

## A. PowerBI 欄位選擇重點（含轉單）

與目前 `db/schema.sql` 一致：**SQL 視圖不再帶出預算％、YOY％、人員費用**（底層 `fact_sales` 仍可有欄位，但 ETL 寫入為 NULL；報表請用 DAX 計算）。

- **含轉單業績（主指標）**：`sales_incl_result`
- **未含轉單前實績**：`sales_result`
- **轉單金額**：`transfer_amount`
- **客數／客單**：`customer_count`、`unit_price`
- **門市分類**：`store_type`（`store`＝實體門市、`HQ`＝電商／通路等；**不含** `subtotal`／`grand_total`，區域小計與合計在 Power BI 依群組加總）

**月累計**：不使用 `vw_mtd_store_sales`；**月累、YOY、預算達成請在 PowerBI 以 DAILY 用 DAX 聚合**（與你規劃的跨月事件區間一致）。

## B. PowerBI 匯入查詢（來自對話紀錄）

連線方式（PowerBI Desktop）：

- 取得資料 -> SQL Server
- Server: `localhost,1433`
- Database: `PosInsight`
- 展開進階選項後，貼入下列查詢

### Query 1: DIM_Store

```sql
SELECT
    store_id,
    store_code,
    store_name,
    store_short_name,
    store_type,
    display_order,
    is_active
FROM dim_store
ORDER BY display_order;
```

### Query 2: FACT_DailySales（對應 `vw_daily_store_sales`）

```sql
SELECT
    report_date,
    report_year,
    report_month,
    report_day,
    weekday_name,
    store_id,
    store_code,
    store_name,
    store_short_name,
    store_type,
    customer_count,
    unit_price,
    sales_result,
    transfer_amount,
    sales_incl_result
FROM vw_daily_store_sales
-- 視圖已含 store_type IN ('store','HQ')；不含 PDF 小計／總計列。
-- 若報表只要直營店，取消下行註解：
-- WHERE store_type = 'store'
ORDER BY store_name, report_date;
```

### Query 3: FACT_GrandTotal（對應 `vw_daily_grand_total`）

> `vw_daily_grand_total` 目前僅回傳每月最後一天資料列（month-end only）。

```sql
SELECT
    report_date,
    report_year,
    report_month,
    customer_count,
    unit_price,
    sales_result,
    transfer_amount,
    sales_incl_result,
    record_type
FROM vw_daily_grand_total
ORDER BY report_date, record_type;
```

### Query 4: Date（改用 PowerBI DAX 計算表）

此段不再用 SQL 查詢，改在 PowerBI 建立「新增資料表」：

```DAX
Date =
    ADDCOLUMNS (
        CALENDAR (
            DATE ( 2020, 1, 1 ),
            DATE ( 2026, 12, 31 )
        ),
        "年", YEAR ( [Date] ),
        "季度", ROUNDUP ( MONTH ( [Date] ) / 3, 0 ),
        "月", MONTH ( [Date] ),
        "周", WEEKNUM ( [Date] ),
        "年季度", YEAR ( [Date] ) & "Q" & ROUNDUP ( MONTH ( [Date] ) / 3, 0 ),
        "年月", YEAR ( [Date] ) * 100 + MONTH ( [Date] ),
        "年周", YEAR ( [Date] ) * 100 + WEEKNUM ( [Date] ),
        "星期幾",
            SWITCH (
                WEEKDAY ( [Date] ),
                1, "Sun",
                2, "Mon",
                3, "Tue",
                4, "Wed",
                5, "Thu",
                6, "Fri",
                7, "Sat"
            )
    )
```

關聯時請使用 `Date[Date]` 對應事實表的 `report_date`。

## C. 資料模型關聯（對話紀錄）

- `Date.Date` -> `FACT_DailySales.report_date`
- `Date.Date` -> `FACT_GrandTotal.report_date`
- `DIM_Store.store_name` -> `FACT_DailySales.store_name`（或以 `store_code` 與預算表關聯）
- 年度預算 Excel：`store_code + month`（或 `store_name + month`）-> `FACT_DailySales`

## D. View 名稱（與 PowerBI 查詢一致）

請直接使用 `db/schema.sql` 內建視圖，**不要**再找 `vw_sales_for_powerbi`（專案未建立此物件）：

- **門市日報**：`vw_daily_store_sales`（Query 2）
- **全司總計列**：`vw_daily_grand_total`（Query 3）
- **MTD 視圖**：`vw_mtd_store_sales` 已移除；如需 MTD 原值請直接查 `fact_sales`（`record_type='MTD'`）

## E. SQL Server 建表語法（原文）- `db/schema.sql`

```sql
-- ============================================================
-- POS Insight Database Schema
-- SQL Server 2022 | Designed for PowerBI integration
-- ============================================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'PosInsight')
BEGIN
    CREATE DATABASE PosInsight
    COLLATE Chinese_Taiwan_Stroke_CS_AS;
END
GO

USE PosInsight;
GO

-- ============================================================
-- DIMENSION: Store / Channel master
-- ============================================================
IF OBJECT_ID('dim_store', 'U') IS NULL
CREATE TABLE dim_store (
    store_id         SMALLINT        IDENTITY(1,1)   PRIMARY KEY,
    store_code       NVARCHAR(20)    NOT NULL,
    store_name       NVARCHAR(100)   NOT NULL,
    store_short_name NVARCHAR(30)    NULL,
    store_type       NVARCHAR(20)    NOT NULL,
    display_order    SMALLINT        NOT NULL,
    is_active        BIT             NOT NULL DEFAULT 1,
    CONSTRAINT uq_dim_store_store_name UNIQUE (store_name),
    CONSTRAINT uq_dim_store_store_code UNIQUE (store_code)
);
GO

-- ============================================================
-- FACT: Daily sales + Month-to-Date (MTD) accumulation
--
-- Each PDF row appears twice:
--   record_type = 'DAILY' -> single-day figures
--   record_type = 'MTD'   -> month-to-date cumulative
--
-- Column semantics (from PDF header):
--   *_result       : actual amount / count
--   *_budget_pct   : achievement rate vs. budget plan (%)
--   *_yoy_pct      : year-over-year growth rate (%)
--   ft_expense     : full-time labour expenditure
--   pt_expense     : part-time labour expenditure
-- ============================================================
IF OBJECT_ID('fact_sales', 'U') IS NULL
CREATE TABLE fact_sales (
    id                          BIGINT          IDENTITY(1,1)   PRIMARY KEY,
    report_date                 DATE            NOT NULL,
    store_id                    SMALLINT        NOT NULL,
    record_type                 CHAR(5)         NOT NULL,       -- 'DAILY' | 'MTD'

    -- SALES
    sales_result                DECIMAL(18,0),                  -- 売上実績 (JPY or TWD)
    sales_budget_pct            DECIMAL(10,2),                  -- 予算比 %
    sales_yoy_pct               DECIMAL(12,2),                  -- 前年比 %

    -- CUSTOMER COUNT
    customer_count              INT,
    customer_yoy_pct            DECIMAL(10,2),

    -- CUSTOMER UNIT PRICE
    unit_price                  DECIMAL(12,2),                  -- 客単価
    unit_price_yoy_pct          DECIMAL(10,2),

    -- PERSONNEL EXPENDITURES
    ft_expense                  DECIMAL(12,0),                  -- Full-time labour cost
    ft_budget_pct               DECIMAL(10,2),                  -- FT vs budget %
    pt_expense                  DECIMAL(12,0),                  -- Part-time labour cost
    pt_budget_pct               DECIMAL(10,2),                  -- PT vs budget %
    total_personnel_expense     DECIMAL(12,0),                  -- FT + PT total cost
    total_personnel_budget_pct  DECIMAL(10,2),                  -- Total vs budget %

    -- TRANSFER
    transfer_amount             DECIMAL(18,0)   DEFAULT 0,

    -- SALES (including transfer)
    sales_incl_result           DECIMAL(18,0),
    sales_incl_budget_pct       DECIMAL(10,2),
    sales_incl_yoy_pct          DECIMAL(12,2),

    -- ETL metadata
    source_file                 NVARCHAR(255),
    loaded_at                   DATETIME2       NOT NULL DEFAULT GETDATE(),

    CONSTRAINT fk_fact_store    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    CONSTRAINT uq_fact_sales    UNIQUE (report_date, store_id, record_type),
    CONSTRAINT chk_record_type  CHECK (record_type IN ('DAILY', 'MTD'))
);
GO

-- Indexes optimised for PowerBI time-intelligence and store slicing
CREATE INDEX ix_fact_date
    ON fact_sales (report_date, record_type)
    INCLUDE (store_id, sales_result, sales_budget_pct, customer_count, unit_price);
GO

CREATE INDEX ix_fact_store_date
    ON fact_sales (store_id, report_date, record_type)
    INCLUDE (sales_result, sales_incl_result);
GO

-- ============================================================
-- ETL AUDIT LOG
-- ============================================================
IF OBJECT_ID('etl_log', 'U') IS NULL
CREATE TABLE etl_log (
    log_id          INT             IDENTITY(1,1)   PRIMARY KEY,
    source_file     NVARCHAR(255)   NOT NULL,
    report_date     DATE            NOT NULL,
    records_loaded  INT             NOT NULL DEFAULT 0,
    status          NVARCHAR(20)    NOT NULL,   -- 'SUCCESS' | 'FAILED' | 'SKIPPED'
    error_message   NVARCHAR(MAX),
    run_at          DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

-- ============================================================
-- VIEWS for PowerBI
-- ============================================================

-- Daily：store / HQ（不含 subtotal、grand_total）
CREATE OR ALTER VIEW vw_daily_store_sales AS
SELECT
    fs.report_date,
    YEAR(fs.report_date)                            AS report_year,
    MONTH(fs.report_date)                           AS report_month,
    DAY(fs.report_date)                             AS report_day,
    DATENAME(WEEKDAY, fs.report_date)               AS weekday_name,
    ds.store_id,
    ds.store_code,
    ds.store_name,
    ds.store_short_name,
    ds.store_type,
    fs.customer_count,
    fs.unit_price,
    fs.sales_result,
    fs.transfer_amount,
    fs.sales_incl_result
FROM fact_sales        fs
JOIN dim_store         ds ON ds.store_id = fs.store_id
WHERE fs.record_type = 'DAILY'
  AND ds.store_type IN ('store', 'HQ');
GO

IF OBJECT_ID('vw_mtd_store_sales', 'V') IS NOT NULL
    DROP VIEW vw_mtd_store_sales;
GO

-- Grand total summary by day (for KPI cards)
CREATE OR ALTER VIEW vw_daily_grand_total AS
SELECT
    fs.report_date,
    YEAR(fs.report_date)    AS report_year,
    MONTH(fs.report_date)   AS report_month,
    fs.customer_count,
    fs.unit_price,
    fs.sales_result,
    fs.transfer_amount,
    fs.sales_incl_result,
    fs.record_type
FROM fact_sales fs
JOIN dim_store  ds ON ds.store_id = fs.store_id
WHERE ds.store_name = 'GRAND TOTAL';
GO
```

## F. 門市種子資料 - `db/seed_stores.sql`

門市代號、簡稱與 PDF `store_name` 對照已改為 **MERGE**（可重複執行）。  
若資料庫仍是舊版 `dim_store`（含 `area` 欄位），請先執行 [`db/migrate_dim_store_codes.sql`](db/migrate_dim_store_codes.sql)，再執行 seed。

完整腳本請直接開啟專案內 [`db/seed_stores.sql`](db/seed_stores.sql)。
