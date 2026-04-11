# SOKUHO — POS Insight ETL

將每日 SOKUHO PDF 銷售速報匯入 SQL Server，供 PowerBI 報表使用。

---

## 一、ETL 執行

### 增量匯入（只補尚未成功載入的檔案）

```powershell
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31
```

### 強制重跑指定區間

```powershell
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31 --force
```

### 指定 SOKUHO 根目錄（非預設路徑時）

```powershell
uv run python main.py --pdf-dir "E:/OneDrive - Aunt Stella Company/SOKUHO/2026" --from-date 2026-03-01 --to-date 2026-03-31
```

### 僅檢查不寫入資料庫（驗證用）

```powershell
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31 --dry-run
```

**補充**

- `--unknown-store-policy` 預設為 `skip_row`（未知門市只略過該列，其餘照常匯入）。
- 若只新增不覆蓋舊資料，請不要加 `--force`。

---

## 二、PowerBI 連線與查詢

連線方式（PowerBI Desktop）：取得資料 → SQL Server → Server: `localhost,1433`，Database: `PosInsight`，展開進階選項後貼入查詢。

### 欄位選擇重點

- **含轉單業績（主指標）**：`sales_incl_result`
- **未含轉單前實績**：`sales_result`
- **轉單金額**：`transfer_amount`
- **客數／客單**：`customer_count`、`unit_price`
- **門市分類**：`store_type`（`store`＝實體門市、`HQ`＝電商／通路等；不含 `subtotal`／`grand_total`，區域小計與合計在 PowerBI 依群組加總）

月累計、YOY、預算達成請在 PowerBI 以 DAILY 用 DAX 聚合（不使用 MTD 原值）。

### Query 1 — DIM_Store

```sql
SELECT store_id, store_code, store_name, store_short_name, store_type, display_order, is_active
FROM dim_store
ORDER BY display_order;
```

### Query 2 — FACT_DailySales（對應 `vw_daily_store_sales`）

```sql
SELECT
    report_date, report_year, report_month, report_day, weekday_name,
    store_id, store_code, store_name, store_short_name, store_type,
    customer_count, unit_price, sales_result, transfer_amount, sales_incl_result
FROM vw_daily_store_sales
-- 視圖已含 store_type IN ('store','HQ')；不含 PDF 小計／總計列。
-- 若只要直營店，取消下行註解：
-- WHERE store_type = 'store'
ORDER BY store_name, report_date;
```

### Query 3 — FACT_GrandTotal（對應 `vw_daily_grand_total`）

```sql
SELECT report_date, report_year, report_month,
       customer_count, unit_price, sales_result, transfer_amount, sales_incl_result, record_type
FROM vw_daily_grand_total
ORDER BY report_date, record_type;
```

### Query 4 — Date（DAX 計算表）

在 PowerBI 建立「新增資料表」：

```dax
Date =
    ADDCOLUMNS (
        CALENDAR ( DATE ( 2020, 1, 1 ), DATE ( 2026, 12, 31 ) ),
        "年",     YEAR ( [Date] ),
        "季度",   ROUNDUP ( MONTH ( [Date] ) / 3, 0 ),
        "月",     MONTH ( [Date] ),
        "周",     WEEKNUM ( [Date] ),
        "年季度", YEAR ( [Date] ) & "Q" & ROUNDUP ( MONTH ( [Date] ) / 3, 0 ),
        "年月",   YEAR ( [Date] ) * 100 + MONTH ( [Date] ),
        "年周",   YEAR ( [Date] ) * 100 + WEEKNUM ( [Date] ),
        "星期幾",
            SWITCH ( WEEKDAY ( [Date] ),
                1, "Sun", 2, "Mon", 3, "Tue", 4, "Wed",
                5, "Thu", 6, "Fri", 7, "Sat" )
    )
```

關聯時使用 `Date[Date]` 對應事實表的 `report_date`。

---

## 三、資料模型關聯

- `Date.Date` → `FACT_DailySales.report_date`
- `Date.Date` → `FACT_GrandTotal.report_date`
- `DIM_Store.store_id` → `FACT_DailySales.store_id`
- 年度預算 Excel：`store_code + month`（或 `store_name + month`）→ `FACT_DailySales`

---

## 四、View 對照

| 用途 | View |
|------|------|
| 門市日報 | `vw_daily_store_sales` |
| 全司總計（每月最後一天）| `vw_daily_grand_total` |
| 月末對帳 | `vw_month_end_reconcile` |

> `vw_mtd_store_sales` 已移除。如需 MTD 原值請直接查 `fact_sales`（`record_type='MTD'`）。

---

## 五、資料庫初始化

```powershell
# 啟動 SQL Server（Docker）
docker-compose up -d

# 建立資料庫與 Views
sqlcmd -S localhost,1433 -U sa -P <password> -i db/schema.sql

# 匯入門市主檔
sqlcmd -S localhost,1433 -U sa -P <password> -i db/seed_stores.sql
```

若資料庫仍是含 `area` 欄位的舊版 `dim_store`，請先執行 `db/migrate_dim_store_codes.sql` 再執行 seed。
