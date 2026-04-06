-- 記錄 SOKUHO 匯入時實際採用的檔名（含非標準檔名／手動指定），不修改原始檔名
USE PosInsight;
GO

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
