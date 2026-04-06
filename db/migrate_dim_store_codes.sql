-- ============================================================
-- Migrate dim_store: add store_code / store_short_name, drop area
-- Run once on databases created from an older schema.sql
-- Order: add columns -> refresh views -> drop area -> MERGE -> tighten NOT NULL
-- ============================================================
USE PosInsight;
GO

IF COL_LENGTH('dim_store', 'store_code') IS NULL
    ALTER TABLE dim_store ADD store_code NVARCHAR(20) NULL;
GO

IF COL_LENGTH('dim_store', 'store_short_name') IS NULL
    ALTER TABLE dim_store ADD store_short_name NVARCHAR(30) NULL;
GO

-- Daily sales：store、HQ（排除 subtotal / grand_total）
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

IF COL_LENGTH('dim_store', 'area') IS NOT NULL
    ALTER TABLE dim_store DROP COLUMN area;
GO

;MERGE dim_store AS t
USING (
    VALUES
    (N'1031', N'A4 Mitsukoshi',           N'A4',    N'store',       1),
    (N'1049', N'Ban-Ciao Far Eastern',    N'BCFE',  N'store',       2),
    (N'1067', N'Ban-Ciao',                N'BanCiao', N'store',    2),
    (N'1035', N'BR4 Fuxing SOGO',         N'BR4',   N'store',       3),
    (N'1030', N'CS Far Eastern',          N'CS',    N'store',       4),
    (N'1026', N'Nan-Shi Mitsukoshi',      N'NH',    N'store',       5),
    (N'1002', N'Takashimaya',             N'TAK',   N'store',       6),
    (N'1043', N'Tian-Mu Sogo',            N'TM',    N'store',       7),
    (N'1075', N'Tian-Mu Mitsukoshi',      N'TMMK',  N'store',       8),
    (N'1076', N'A8 Mitsukoshi',          N'A8',    N'store',       8),
    (N'1001', N'Zhong-Xiao Sogo',         N'ZX',    N'store',       9),
    (N'1053', N'Breeze Nanjing',          N'BN',    N'store',       10),
    (N'9001', N'Taipei Area',             N'TPE_SUB',   N'subtotal',    11),
    (N'1073', N'Far Eastern A13',         N'A13',   N'store',       12),
    (N'1008', N'CL SOGO B1',              N'CL',    N'store',       13),
    (N'1077', N'Gloria Outlets',         N'Gloria', N'store',     13),
    (N'9101', N'Taoyuan Area',            N'TYN_SUB',   N'subtotal',    14),
    (N'1050', N'Hsin-Chu Big City',       N'HCBC',  N'store',       15),
    (N'9102', N'Hsinchu Area',           N'HSC_SUB',   N'subtotal',    16),
    (N'1025', N'Taichung Mitsukoshi',     N'TC',    N'store',       17),
    (N'1051', N'Taichung Far Eastern',    N'TCFE',  N'store',       18),
    (N'9103', N'Taichung Area',          N'TXG_SUB',   N'subtotal',    19),
    (N'1024', N'Tainan Mitsukoshi',       N'TN',    N'store',       20),
    (N'9104', N'Tainan Area',             N'TNN_SUB',   N'subtotal',    21),
    (N'1078', N'TNHR',                   N'TNHR',  N'store',      21),
    (N'1042', N'Kaohsiung Sogo',          N'KS',    N'store',       22),
    (N'1003', N'Hanshin Kaohsiung',       N'HS',    N'store',       23),
    (N'1041', N'Dome Hanshin',            N'DOME',  N'store',       24),
    (N'1074', N'Rainbow Market',         N'Rainbow', N'store',     24),
    (N'1072', N'Kaohsiung Zuoying',      N'Zuoying', N'store',     24),
    (N'9105', N'Kaohsiung Area',          N'KHH_SUB',   N'subtotal',    25),
    (N'9002', N'Non-Taipei Area',         N'NTP_SUB',   N'subtotal',    26),
    (N'9003', N'Existing Store Sales',    N'EXIST_SUB', N'subtotal',    27),
    (N'9004', N'New Store Sales',         N'NEW_SUB',   N'subtotal',    28),
    (N'9005', N'ALL 18 STORES',           N'ALL18_SUB', N'subtotal',    29),
    (N'4100', N'Webshop',                 N'WEB',   N'HQ',          30),
    (N'4001', N'Showroom',                N'SR',    N'HQ',          31),
    (N'4006', N'Taiwan High Speed Rail',  N'THSR',  N'HQ',          32),
    (N'4005', N'Temporary Stall',         N'Temp',  N'HQ',          32),
    (N'4007', N'Taipei 101',             N'101',   N'HQ',          33),
    (N'4009', N'MOMO',                   N'MOMO',  N'HQ',          34),
    (N'4010', N'Costco',                 N'Costco', N'HQ',          35),
    (N'4008', N'Anhe store',            N'Anhe',  N'HQ',          36),
    (N'4011', N'Business Development',  N'BD',    N'HQ',          37),
    (N'9013', N'OFFICE & WEB TOTAL',      N'OFFWEB_SUB', N'subtotal', 33),
    (N'9999', N'GRAND TOTAL',             N'GRAND', N'grand_total', 34)
) AS s (store_code, store_name, store_short_name, store_type, display_order)
ON t.store_name = s.store_name
WHEN MATCHED THEN UPDATE SET
    store_code       = s.store_code,
    store_short_name = s.store_short_name,
    store_type       = s.store_type,
    display_order    = s.display_order
WHEN NOT MATCHED BY TARGET THEN
    INSERT (store_code, store_name, store_short_name, store_type, display_order)
    VALUES (s.store_code, s.store_name, s.store_short_name, s.store_type, s.display_order);
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('dim_store')
      AND name = 'store_code'
      AND is_nullable = 1
)
    ALTER TABLE dim_store ALTER COLUMN store_code NVARCHAR(20) NOT NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('dim_store') AND name = 'uq_dim_store_store_code'
)
    CREATE UNIQUE INDEX uq_dim_store_store_code ON dim_store (store_code);
GO

PRINT 'migrate_dim_store_codes: done.';
GO
