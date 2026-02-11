-- ============================================================
-- 以图搜商品系统 · 数据库初始化脚本
-- 对齐系统设计 v1.2 §3.2 PostgreSQL DDL
-- ============================================================

-- 启用 RoaringBitmap 扩展
CREATE EXTENSION IF NOT EXISTS roaringbitmap;

-- ── §3.2.1 URI 全局去重表 ──
CREATE TABLE IF NOT EXISTS uri_dedup (
    image_pk    CHAR(32)     PRIMARY KEY,
    uri_hash    CHAR(64)     NOT NULL,
    ts_month    INT          NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_uri_dedup_ts ON uri_dedup (ts_month);

-- ── §3.2.2 商家 Bitmap 表 ──
CREATE TABLE IF NOT EXISTS image_merchant_bitmaps (
    image_pk        CHAR(32)        PRIMARY KEY,
    bitmap          roaringbitmap   NOT NULL DEFAULT '\x3a30000000000000'::roaringbitmap,
    is_evergreen    BOOLEAN         NOT NULL DEFAULT false,
    merchant_count  INT GENERATED ALWAYS AS (rb_cardinality(bitmap)) STORED,
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);
ALTER TABLE image_merchant_bitmaps REPLICA IDENTITY FULL;

-- ── §3.2.3 商家字典编码表 ──
CREATE TABLE IF NOT EXISTS merchant_id_mapping (
    merchant_str   VARCHAR(64)   PRIMARY KEY,
    bitmap_index   SERIAL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT bitmap_index_unique UNIQUE (bitmap_index)
);

-- ── §3.2.4 常青类目配置表 ──
CREATE TABLE IF NOT EXISTS evergreen_categories (
    category_l1_code  INT          PRIMARY KEY,
    category_l1_name  VARCHAR(64)  NOT NULL,
    enabled           BOOLEAN      NOT NULL DEFAULT true,
    enabled_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
INSERT INTO evergreen_categories (category_l1_code, category_l1_name) VALUES
    (101, '家具'), (102, '珠宝'), (103, '经典箱包')
ON CONFLICT DO NOTHING;

-- ── §3.2.5 词表映射表 ──
CREATE TABLE IF NOT EXISTS vocabulary_mapping (
    vocab_type   VARCHAR(32)   NOT NULL,
    string_val   VARCHAR(128)  NOT NULL,
    int_code     INT           NOT NULL,
    PRIMARY KEY (vocab_type, string_val),
    UNIQUE (vocab_type, int_code)
);

-- ── §3.2.6 商家范围预注册表 ──
CREATE TABLE IF NOT EXISTS merchant_scope_registry (
    scope_id       VARCHAR(64)   PRIMARY KEY,
    caller_id      VARCHAR(64)   NOT NULL,
    merchant_ids   TEXT          NOT NULL,
    bitmap_cache   BYTEA,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scope_caller ON merchant_scope_registry (caller_id);

-- ── §8.1 API Key 表 ──
CREATE TABLE IF NOT EXISTS api_keys (
    api_key_hash   CHAR(64)      PRIMARY KEY,
    caller_id      VARCHAR(64)   NOT NULL,
    description    VARCHAR(256),
    enabled        BOOLEAN       NOT NULL DEFAULT true,
    rate_limit_qps INT           NOT NULL DEFAULT 50,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ── 初始化种子数据 ──

-- 默认词表 (示例)
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l1', '家具', 101),
    ('category_l1', '珠宝', 102),
    ('category_l1', '经典箱包', 103),
    ('category_l1', '服装', 201),
    ('category_l1', '鞋靴', 202),
    ('category_l1', '数码', 301),
    ('color', '红色', 1), ('color', '蓝色', 2), ('color', '黑色', 3),
    ('color', '白色', 4), ('color', '绿色', 5),
    ('season', '春', 1), ('season', '夏', 2), ('season', '秋', 3), ('season', '冬', 4)
ON CONFLICT DO NOTHING;

-- 测试用 API Key (SHA256 of "imgsrch_test_key_12345")
INSERT INTO api_keys (api_key_hash, caller_id, description) VALUES
    ('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
     'test_caller', 'Development test key')
ON CONFLICT DO NOTHING;

-- ============================================================
-- ClickHouse DDL (需在 ClickHouse 中单独执行)
-- ============================================================
-- 见系统设计 v1.2 §3.6
-- CREATE TABLE search_quality_log ...
-- CREATE TABLE behavior_log ...
-- CREATE MATERIALIZED VIEW search_quality_mv ...
