-- ============================================================
-- Seed / upsert dim_store (idempotent MERGE)
--
-- 地區：有標示地區的門市集中，並在該區最後放一筆「地區小計」列
-- （名稱不在 PDF 內，ETL 不會寫入 fact；僅供 PowerBI 維度／階層用）。
-- 未標地區的 PDF 列（Non-Taipei、Existing、New、ALL 18、Online、GRAND 等）
-- 排在最後，方便你之後再分類。
--
-- 天母：PDF 為 Tian-Mu Sogo → 1043、TM（ETL 對應）；另列 ERP 全名 Tian-Mu Mitsukoshi → 1075、TMMK（PDF 無此字串，無 fact）。
-- Dome Hanshin：1041 DOME；高雄區：1042 Kaohsiung Sogo KS、1003 Hanshin Kaohsiung HS、9105 Kaohsiung Area 小計。
--
-- 地區小計代號 9101–9105 為暫用，可日後換成 ERP 正式碼。
-- store_type：N'store'＝實體門市（含原臨時櫃），其餘為 HQ、subtotal、grand_total 等。
-- ============================================================
USE PosInsight;
GO

MERGE dim_store AS t
USING (
    VALUES
    -- ── Taipei：… → Tian-Mu Sogo (PDF) / Tian-Mu Mitsukoshi (ERP) → … → Taipei Area → A13 ──
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
    -- ── Taoyuan ───────────────────────────────────────────────
    (N'1008', N'CL SOGO B1',              N'CL',    N'store',       13),
    (N'1077', N'Gloria Outlets',         N'Gloria', N'store',     13),
    (N'9101', N'Taoyuan Area',            N'TYN_SUB',   N'subtotal',    14),
    -- ── Hsinchu ──────────────────────────────────────────────
    (N'1050', N'Hsin-Chu Big City',       N'HCBC',  N'store',       15),
    (N'9102', N'Hsinchu Area',           N'HSC_SUB',   N'subtotal',    16),
    -- ── Taichung ─────────────────────────────────────────────
    (N'1025', N'Taichung Mitsukoshi',     N'TC',    N'store',       17),
    (N'1051', N'Taichung Far Eastern',    N'TCFE',  N'store',       18),
    (N'9103', N'Taichung Area',          N'TXG_SUB',   N'subtotal',    19),
    -- ── Tainan ───────────────────────────────────────────────
    (N'1024', N'Tainan Mitsukoshi',       N'TN',    N'store',       20),
    (N'9104', N'Tainan Area',             N'TNN_SUB',   N'subtotal',    21),
    (N'1078', N'TNHR',                   N'TNHR',  N'store',      21),
    -- ── Kaohsiung（Dome 與 KS、HS 同區，最後為地區小計）────────
    (N'1042', N'Kaohsiung Sogo',          N'KS',    N'store',       22),
    (N'1003', N'Hanshin Kaohsiung',       N'HS',    N'store',       23),
    (N'1041', N'Dome Hanshin',            N'DOME',  N'store',       24),
    (N'1074', N'Rainbow Market',         N'Rainbow', N'store',     24),
    (N'1072', N'Kaohsiung Zuoying',      N'Zuoying', N'store',     24),
    (N'9105', N'Kaohsiung Area',          N'KHH_SUB',   N'subtotal',    25),
    -- ── 其餘 PDF 列（未標地區者置底，你再分類）────────────────
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
    (N'4010', N'Costco',                 N'Costco', N'HQ',         35),
    (N'4008', N'Anhe store',            N'Anhe',  N'HQ',          36),
    (N'4011', N'Business Development',  N'BD',    N'HQ',          37),
    (N'4003', N'China Airlines',         N'China Air', N'HQ',      38),
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

PRINT 'dim_store MERGE completed; rows touched: ' + CAST(@@ROWCOUNT AS NVARCHAR);
GO
