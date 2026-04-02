-- ============================================================
-- 以图搜商品系统 · 数据库初始化脚本
-- 对齐系统设计 v1.2 §3.2 PostgreSQL DDL
-- ============================================================

-- 启用 RoaringBitmap 扩展 (可选 — 标准 PG 镜像中不可用)
DO $$ BEGIN
    CREATE EXTENSION IF NOT EXISTS roaringbitmap;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'roaringbitmap extension not available, skipping';
END $$;

-- ── §3.2.1 URI 全局去重表 ──
CREATE TABLE IF NOT EXISTS uri_dedup (
    image_pk    CHAR(32)     PRIMARY KEY,
    uri_hash    CHAR(64)     NOT NULL,
    uri         TEXT,
    ts_month    INT          NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_uri_dedup_ts ON uri_dedup (ts_month);

-- ── §3.2.2 商家 Bitmap 表 ──
-- 注: 依赖 roaringbitmap 扩展; 若不可用则用 BYTEA 降级
DO $$ BEGIN
    CREATE TABLE IF NOT EXISTS image_merchant_bitmaps (
        image_pk        CHAR(32)        PRIMARY KEY,
        bitmap          roaringbitmap   NOT NULL DEFAULT '\x3a30000000000000'::roaringbitmap,
        is_evergreen    BOOLEAN         NOT NULL DEFAULT false,
        merchant_count  INT GENERATED ALWAYS AS (rb_cardinality(bitmap)) STORED,
        updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
    );
    ALTER TABLE image_merchant_bitmaps REPLICA IDENTITY FULL;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Creating image_merchant_bitmaps with BYTEA fallback';
    CREATE TABLE IF NOT EXISTS image_merchant_bitmaps (
        image_pk        CHAR(32)        PRIMARY KEY,
        bitmap          BYTEA           NOT NULL DEFAULT '\x',
        is_evergreen    BOOLEAN         NOT NULL DEFAULT false,
        merchant_count  INT             NOT NULL DEFAULT 0,
        updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
    );
    ALTER TABLE image_merchant_bitmaps REPLICA IDENTITY FULL;
END $$;

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
    (101, '家具'), (102, '珠宝'), (103, '经典箱包'),
    (1007, '家具类'), (1004, '珠宝首饰类'), (1017, '钟表类')
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

-- ── v2.0 两级分类品类体系 (L1 18大类 + L2 子品类) ──

-- L1 大类
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l1', '服装类',     1001),
    ('category_l1', '箱包类',     1002),
    ('category_l1', '鞋帽类',     1003),
    ('category_l1', '珠宝首饰类', 1004),
    ('category_l1', '房地产类',   1005),
    ('category_l1', '五金建材类', 1006),
    ('category_l1', '家具类',     1007),
    ('category_l1', '化妆品类',   1008),
    ('category_l1', '小家电类',   1009),
    ('category_l1', '手机类',     1010),
    ('category_l1', '电脑类',     1011),
    ('category_l1', '食品类',     1012),
    ('category_l1', '玩具类',     1013),
    ('category_l1', '运动户外类', 1014),
    ('category_l1', '汽车配件类', 1015),
    ('category_l1', '办公用品类', 1016),
    ('category_l1', '钟表类',     1017),
    ('category_l1', '眼镜类',     1018)
ON CONFLICT DO NOTHING;

-- L2 时尚类 (服装)
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '上装',   100101), ('category_l2', '下装',   100102),
    ('category_l2', '连衣裙', 100103), ('category_l2', '外套',   100104),
    ('category_l2', '内衣',   100105), ('category_l2', '运动服', 100106),
    ('category_l2', '套装',   100107)
ON CONFLICT DO NOTHING;
-- L2 时尚类 (箱包)
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '手提包', 100201), ('category_l2', '双肩包', 100202),
    ('category_l2', '单肩包', 100203), ('category_l2', '钱包',   100204),
    ('category_l2', '旅行箱', 100205), ('category_l2', '腰包',   100206)
ON CONFLICT DO NOTHING;
-- L2 时尚类 (鞋帽)
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '运动鞋',   100301), ('category_l2', '皮鞋',     100302),
    ('category_l2', '高跟鞋',   100303), ('category_l2', '靴子',     100304),
    ('category_l2', '凉鞋拖鞋', 100305), ('category_l2', '帽子',     100306)
ON CONFLICT DO NOTHING;
-- L2 珠宝首饰
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '戒指',     100401), ('category_l2', '项链',     100402),
    ('category_l2', '手镯手链', 100403), ('category_l2', '耳环耳饰', 100404),
    ('category_l2', '翡翠玉器', 100405), ('category_l2', '黄金饰品', 100406),
    ('category_l2', '钻石宝石', 100407), ('category_l2', '胸针别针', 100408)
ON CONFLICT DO NOTHING;
-- L2 房地产
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '住宅',     100501), ('category_l2', '商铺',     100502),
    ('category_l2', '室内装修', 100503), ('category_l2', '户型图',   100504)
ON CONFLICT DO NOTHING;
-- L2 五金建材
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '水暖管件', 100601), ('category_l2', '电气配件', 100602),
    ('category_l2', '工具',     100603), ('category_l2', '装饰建材', 100604),
    ('category_l2', '门窗五金', 100605)
ON CONFLICT DO NOTHING;
-- L2 家具
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '沙发', 100701), ('category_l2', '桌子', 100702),
    ('category_l2', '椅子', 100703), ('category_l2', '柜子', 100704),
    ('category_l2', '床',   100705), ('category_l2', '灯具', 100706)
ON CONFLICT DO NOTHING;
-- L2 化妆品
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '彩妆',     100801), ('category_l2', '护肤',     100802),
    ('category_l2', '香水',     100803), ('category_l2', '美容工具', 100804),
    ('category_l2', '身体护理', 100805)
ON CONFLICT DO NOTHING;
-- L2 小家电
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '厨房小家电', 100901), ('category_l2', '个人护理', 100902),
    ('category_l2', '清洁电器',   100903), ('category_l2', '生活电器', 100904)
ON CONFLICT DO NOTHING;
-- L2 手机
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '智能手机', 101001), ('category_l2', '手机配件', 101002),
    ('category_l2', '平板电脑', 101003)
ON CONFLICT DO NOTHING;
-- L2 电脑
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '笔记本电脑', 101101), ('category_l2', '台式电脑', 101102),
    ('category_l2', '电脑配件',   101103), ('category_l2', '存储设备', 101104)
ON CONFLICT DO NOTHING;
-- L2 食品
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '零食',   101201), ('category_l2', '饮品',   101202),
    ('category_l2', '保健品', 101203), ('category_l2', '调味品', 101204)
ON CONFLICT DO NOTHING;
-- L2 玩具
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '积木拼图', 101301), ('category_l2', '玩偶公仔', 101302),
    ('category_l2', '遥控玩具', 101303), ('category_l2', '益智玩具', 101304)
ON CONFLICT DO NOTHING;
-- L2 运动户外
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '健身器材', 101401), ('category_l2', '户外装备', 101402),
    ('category_l2', '球类运动', 101403), ('category_l2', '骑行装备', 101404)
ON CONFLICT DO NOTHING;
-- L2 汽车配件
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '车内饰品', 101501), ('category_l2', '车外配件', 101502),
    ('category_l2', '汽车电子', 101503), ('category_l2', '汽车养护', 101504)
ON CONFLICT DO NOTHING;
-- L2 办公用品
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '文具',     101601), ('category_l2', '打印耗材', 101602),
    ('category_l2', '办公设备', 101603)
ON CONFLICT DO NOTHING;
-- L2 钟表
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '机械表',   101701), ('category_l2', '石英表',   101702),
    ('category_l2', '智能手表', 101703), ('category_l2', '座钟挂钟', 101704)
ON CONFLICT DO NOTHING;
-- L2 眼镜
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l2', '太阳镜',   101801), ('category_l2', '近视眼镜', 101802),
    ('category_l2', '镜框镜片', 101803)
ON CONFLICT DO NOTHING;

-- L3 时尚细分 (上装)
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l3', 'T恤',     10010101), ('category_l3', '衬衫',     10010102),
    ('category_l3', '卫衣',     10010103), ('category_l3', '毛衣',     10010104),
    ('category_l3', 'Polo衫',  10010105), ('category_l3', '背心吊带', 10010106),
    ('category_l3', '马甲',     10010107)
ON CONFLICT DO NOTHING;
-- L3 下装
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l3', '牛仔裤', 10010201), ('category_l3', '休闲裤', 10010202),
    ('category_l3', '短裤',   10010203), ('category_l3', '运动裤', 10010204),
    ('category_l3', '半身裙', 10010205), ('category_l3', '打底裤', 10010206)
ON CONFLICT DO NOTHING;
-- L3 连衣裙
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l3', '休闲连衣裙', 10010301), ('category_l3', '正式连衣裙', 10010302),
    ('category_l3', '衬衫裙',     10010303), ('category_l3', '针织裙',     10010304),
    ('category_l3', '吊带裙',     10010305)
ON CONFLICT DO NOTHING;
-- L3 外套
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l3', '羽绒服',   10010401), ('category_l3', '风衣',     10010402),
    ('category_l3', '西装外套', 10010403), ('category_l3', '皮夹克',   10010404),
    ('category_l3', '牛仔外套', 10010405), ('category_l3', '大衣',     10010406)
ON CONFLICT DO NOTHING;
-- L3 手提包
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l3', '托特包',     10020101), ('category_l3', '迷你手提包', 10020102),
    ('category_l3', '公文包',     10020103)
ON CONFLICT DO NOTHING;
-- L3 运动鞋
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('category_l3', '跑步鞋', 10030101), ('category_l3', '板鞋',   10030102),
    ('category_l3', '篮球鞋', 10030103), ('category_l3', '老爹鞋', 10030104)
ON CONFLICT DO NOTHING;

-- 标签词表
INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code) VALUES
    ('tag', '真皮', 1), ('tag', 'PU皮', 2), ('tag', '棉质', 3),
    ('tag', '丝绸', 4), ('tag', '羊毛', 5), ('tag', '金属', 6),
    ('tag', '塑料', 7), ('tag', '陶瓷', 8), ('tag', '木质', 9), ('tag', '玻璃', 10),
    ('tag', '复古风', 11), ('tag', '简约风', 12), ('tag', '商务风', 13),
    ('tag', '运动风', 14), ('tag', '潮流风', 15), ('tag', '奢华风', 16),
    ('tag', '红色', 17), ('tag', '蓝色', 18), ('tag', '黑色', 19),
    ('tag', '白色', 20), ('tag', '棕色', 21), ('tag', '绿色', 22),
    ('tag', '粉色', 23), ('tag', '灰色', 24), ('tag', '金色', 25),
    ('tag', '春季', 26), ('tag', '夏季', 27), ('tag', '秋季', 28), ('tag', '冬季', 29),
    ('tag', '日常', 30), ('tag', '派对', 31), ('tag', '户外', 32), ('tag', '办公', 33)
ON CONFLICT DO NOTHING;

-- 旧版兼容 (保留原有编码, 不影响已有数据)
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
