# POS_Insight

## 指定日期區間新增 SOKUHO 資料

`main.py` 已支援用 `--from-date` 與 `--to-date` 只處理特定日期區間（依檔名日期判斷）。

### 1) 一般增量匯入（只補尚未成功載入的檔案）

```powershell
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31
```

### 2) 強制重跑指定區間（即使之前已載入成功）

```powershell
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31 --force
```

### 3) 指定 SOKUHO 根目錄（非預設路徑時）

```powershell
uv run python main.py --pdf-dir "E:/OneDrive - Aunt Stella Company/SOKUHO/2026" --from-date 2026-03-01 --to-date 2026-03-31
```

### 4) 僅檢查不寫入資料庫（驗證用）

```powershell
uv run python main.py --from-date 2026-03-01 --to-date 2026-03-31 --dry-run
```

### 補充

- `--unknown-store-policy` 預設為 `skip_row`（未知門市只略過該列，其餘照常匯入）。
- 若你要「某個日期區間只新增，不覆蓋舊資料」，請不要加 `--force`。