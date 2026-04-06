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
    -- 'store' | 'subtotal' | 'grand_total' | 'HQ'
    store_type       NVARCHAR(20)    NOT NULL,
    display_order    SMALLINT        NOT NULL,
    is_active        BIT             NOT NULL DEFAULT 1,
    CONSTRAINT uq_dim_store_store_name UNIQUE (store_name),
    CONSTRAINT uq_dim_store_store_code UNIQUE (store_code)
);
GO

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

-- SOKUHO: which physical file was imported per report date (non-standard names / overrides)
IF OBJECT_ID('etl_sokuho_import_resolution', 'U') IS NULL
CREATE TABLE etl_sokuho_import_resolution (
    report_date            DATE            NOT NULL PRIMARY KEY,
    actual_source_file     NVARCHAR(500)   NOT NULL,
    default_source_file    NVARCHAR(500)   NULL,
    resolution_note        NVARCHAR(1000)  NULL,
    ignored_files_note     NVARCHAR(1000)  NULL,
    first_loaded_at        DATETIME2       NOT NULL DEFAULT GETDATE(),
    last_loaded_at         DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

-- ============================================================
-- VIEWS for PowerBI
-- ============================================================

-- Daily sales：store、HQ（排除 subtotal / grand_total；區域小計與總計在 Power BI 聚合）
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
WHERE ds.store_name = 'GRAND TOTAL'
  AND RTRIM(fs.record_type) = 'MTD'
  AND fs.report_date = EOMONTH(fs.report_date);
GO

-- Month-end reconcile: compare MTD( sales_result + transfer ) vs month-summed DAILY(sales_incl)
CREATE OR ALTER VIEW vw_month_end_reconcile AS
WITH mtd_month_end AS (
    SELECT
        YEAR(fs.report_date)  AS report_year,
        MONTH(fs.report_date) AS report_month,
        fs.report_date        AS month_end_date,
        COALESCE(fs.sales_result, 0) + COALESCE(fs.transfer_amount, 0) AS mtd_calc_incl
    FROM fact_sales fs
    JOIN dim_store ds ON ds.store_id = fs.store_id
    WHERE ds.store_name = 'GRAND TOTAL'
      AND RTRIM(fs.record_type) = 'MTD'
      AND fs.report_date = EOMONTH(fs.report_date)
),
daily_month_sum AS (
    SELECT
        YEAR(fs.report_date)  AS report_year,
        MONTH(fs.report_date) AS report_month,
        SUM(COALESCE(fs.sales_incl_result, 0)) AS daily_sum_sales_incl
    FROM fact_sales fs
    JOIN dim_store ds ON ds.store_id = fs.store_id
    WHERE RTRIM(fs.record_type) = 'DAILY'
      AND ds.store_type IN ('store', 'HQ')
    GROUP BY YEAR(fs.report_date), MONTH(fs.report_date)
)
SELECT
    m.report_year,
    m.report_month,
    m.month_end_date,
    m.mtd_calc_incl,
    d.daily_sum_sales_incl,
    m.mtd_calc_incl - d.daily_sum_sales_incl AS diff
FROM mtd_month_end m
LEFT JOIN daily_month_sum d
  ON d.report_year = m.report_year
 AND d.report_month = m.report_month;
GO
