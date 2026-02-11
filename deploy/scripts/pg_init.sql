-- ============================================================
-- 以图搜商品系统 · PostgreSQL DDL
-- 对齐: 系统设计 v1.2 §3.2
-- ============================================================

-- 扩展
CREATE EXTENSION IF NOT EXISTS roaringbitmap;

-- ============================================================
-- 3.2.1 URI 全局去重表
-- ============================================================
CREATE TABLE IF NOT EXISTS uri_dedup (
    image_pk    CHAR(32)     PRIMARY KEY,
    uri_hash    CHAR(64)     NOT NULL,
    ts_month    INT          NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_uri_dedup_ts ON uri_dedup (ts_month);

COMMENT ON TABLE uri_dedup IS 'URI 全局去重表 — image_pk 全局唯一';
COMMENT ON COLUMN uri_dedup.image_pk IS 'hex(sha256(uri)[0:16])';
COMMENT ON COLUMN uri_dedup.ts_month IS '写入月份 YYYYMM, 常青=999999';

-- ============================================================
-- 3.2.2 商家 Bitmap 表
-- ============================================================
CREATE TABLE IF NOT EXISTS image_merchant_bitmaps (
    image_pk        CHAR(32)        PRIMARY KEY,
    bitmap          roaringbitmap   NOT NULL DEFAULT '\x3a30000000000000'::roaringbitmap,
    is_evergreen    BOOLEAN         NOT NULL DEFAULT false,
    merchant_count  INT GENERATED ALWAYS AS (rb_cardinality(bitmap)) STORED,
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

ALTER TABLE image_merchant_bitmaps REPLICA IDENTITY FULL;

COMMENT ON TABLE image_merchant_bitmaps IS '商家 Bitmap — CDC 同步到 RocksDB';

-- ============================================================
-- 3.2.3 商家字典编码表
-- ============================================================
CREATE TABLE IF NOT EXISTS merchant_id_mapping (
    merchant_str   VARCHAR(64)   PRIMARY KEY,
    bitmap_index   SERIAL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT bitmap_index_unique UNIQUE (bitmap_index)
);

COMMENT ON TABLE merchant_id_mapping IS '商家 ID → uint32 字典编码';

-- ============================================================
-- 3.2.4 常青类目配置表
-- ============================================================
CREATE TABLE IF NOT EXISTS evergreen_categories (
    category_l1_code  INT          PRIMARY KEY,
    category_l1_name  VARCHAR(64)  NOT NULL,
    enabled           BOOLEAN      NOT NULL DEFAULT true,
    enabled_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

INSERT INTO evergreen_categories (category_l1_code, category_l1_name) VALUES
    (101, '家具'),
    (102, '珠宝'),
    (103, '经典箱包')
ON CONFLICT DO NOTHING;

-- ============================================================
-- 3.2.5 词表映射表
-- ============================================================
CREATE TABLE IF NOT EXISTS vocabulary_mapping (
    vocab_type   VARCHAR(32)   NOT NULL,
    string_val   VARCHAR(128)  NOT NULL,
    int_code     INT           NOT NULL,
    PRIMARY KEY (vocab_type, string_val),
    UNIQUE (vocab_type, int_code)
);

COMMENT ON TABLE vocabulary_mapping IS '词表: category/tag/color/material/style/season';

-- ============================================================
-- 3.2.6 商家范围预注册表
-- ============================================================
CREATE TABLE IF NOT EXISTS merchant_scope_registry (
    scope_id       VARCHAR(64)   PRIMARY KEY,
    caller_id      VARCHAR(64)   NOT NULL,
    merchant_ids   TEXT          NOT NULL,
    bitmap_cache   BYTEA,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scope_caller ON merchant_scope_registry (caller_id);

-- ============================================================
-- API Key 表 (§8.1)
-- ============================================================
-- FIX-U: DDL 统一, api_key_hash 作为 PK (对齐 init_db.sql 和代码 SHA256 查找逻辑)
CREATE TABLE IF NOT EXISTS api_keys (
    api_key_hash   CHAR(64)      PRIMARY KEY,
    caller_id      VARCHAR(64)   NOT NULL,
    description    VARCHAR(256),
    rate_limit_qps INT           NOT NULL DEFAULT 50,
    enabled        BOOLEAN       NOT NULL DEFAULT true,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_caller ON api_keys (caller_id);

-- FIX-Y: 分区轮转补偿记录 (cron-scheduler 失败时写入, 供后续重试)
CREATE TABLE IF NOT EXISTS partition_rotation_compensation (
    partition_name VARCHAR(16)   PRIMARY KEY,
    failed_step    VARCHAR(32)   NOT NULL,      -- milvus_release|pg_delete|milvus_drop
    error_msg      TEXT,
    retry_count    INT           NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ
);

-- ============================================================
-- ClickHouse DDL (§3.6) — 单独执行
-- ============================================================
-- 见 deploy/scripts/clickhouse_ddl.sql
