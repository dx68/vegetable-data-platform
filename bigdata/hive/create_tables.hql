-- =============================================================
-- 蔬菜产业数据 - Hive 建表脚本（数仓四层架构）
-- ODS → DWD → DWS → ADS
-- 基于清洗后数据（data/processed/）建表
-- =============================================================

CREATE DATABASE IF NOT EXISTS vegetable_db;
USE vegetable_db;


-- =============================================================
-- ODS 层：原始清洗数据（External，指向 HDFS 原始 CSV）
-- =============================================================

-- 1. 蔬菜价格表
-- CSV列: 品种,批发市场,最低价,最高价,平均价,日期,省份,蔬菜类型,省份简称
DROP TABLE IF EXISTS ods_vegetable_prices;
CREATE EXTERNAL TABLE ods_vegetable_prices (
    veg_name        STRING    COMMENT '品种',
    market_name     STRING    COMMENT '批发市场',
    min_price       DOUBLE    COMMENT '最低价',
    max_price       DOUBLE    COMMENT '最高价',
    avg_price       DOUBLE    COMMENT '平均价',
    price_date      STRING    COMMENT '日期(yyyy-MM-dd)',
    province        STRING    COMMENT '省份(全称)',
    veg_type        STRING    COMMENT '蔬菜类型(拼音)',
    province_short  STRING    COMMENT '省份简称'
)
COMMENT '蔬菜批发价格表（ODS层）'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/vegetable_data/raw/prices'
TBLPROPERTIES ('skip.header.line.count'='1');


-- 2. 蔬菜种子表
-- CSV列: 产品名称,供应地区,企业名称,点击量,日期,产品链接,分类,品种,省份简称
DROP TABLE IF EXISTS ods_vegetable_seeds;
CREATE EXTERNAL TABLE ods_vegetable_seeds (
    product_name    STRING    COMMENT '产品名称',
    supply_region   STRING    COMMENT '供应地区(省>市>县)',
    company_name    STRING    COMMENT '企业名称',
    click_count     INT       COMMENT '点击量',
    publish_date    STRING    COMMENT '日期',
    product_url     STRING    COMMENT '产品链接',
    veg_type        STRING    COMMENT '分类(拼音)',
    variety_name    STRING    COMMENT '品种(中文)',
    province_short  STRING    COMMENT '省份简称'
)
COMMENT '蔬菜种子供应信息表（ODS层）'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/vegetable_data/raw/seeds'
TBLPROPERTIES ('skip.header.line.count'='1');


-- 3. 蔬菜供应商表
-- CSV列: 产品名称,产地,联系人,企业类型,点击量,日期,产品链接,分类,子分类,品种,省份简称
DROP TABLE IF EXISTS ods_vegetable_suppliers;
CREATE EXTERNAL TABLE ods_vegetable_suppliers (
    product_name    STRING    COMMENT '产品名称',
    origin          STRING    COMMENT '产地(省>市>县)',
    contact_person  STRING    COMMENT '联系人',
    company_type    STRING    COMMENT '企业类型',
    click_count     INT       COMMENT '点击量',
    publish_date    STRING    COMMENT '日期',
    product_url     STRING    COMMENT '产品链接',
    veg_type        STRING    COMMENT '分类(拼音)',
    sub_type        STRING    COMMENT '子分类',
    variety_name    STRING    COMMENT '品种(中文)',
    province_short  STRING    COMMENT '省份简称'
)
COMMENT '蔬菜供应商信息表（ODS层）'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/vegetable_data/raw/suppliers'
TBLPROPERTIES ('skip.header.line.count'='1');


-- =============================================================
-- DWD 层：标准化明细数据（规范化命名、过滤脏数据）
-- =============================================================

DROP TABLE IF EXISTS dwd_vegetable_prices;
CREATE TABLE dwd_vegetable_prices AS
SELECT
    veg_name        AS variety,
    province_short  AS province,
    market_name,
    min_price,
    max_price,
    avg_price,
    price_date,
    veg_type        AS veg_type_pinyin
FROM ods_vegetable_prices
WHERE avg_price IS NOT NULL
  AND province_short IS NOT NULL
  AND province_short NOT IN ('台湾', '香港', '澳门');


DROP TABLE IF EXISTS dwd_vegetable_seeds;
CREATE TABLE dwd_vegetable_seeds AS
SELECT
    variety_name    AS variety,
    province_short  AS province,
    company_name,
    click_count,
    publish_date,
    veg_type        AS veg_type_pinyin
FROM ods_vegetable_seeds
WHERE province_short IS NOT NULL
  AND province_short NOT IN ('台湾', '香港', '澳门');


DROP TABLE IF EXISTS dwd_vegetable_suppliers;
CREATE TABLE dwd_vegetable_suppliers AS
SELECT
    variety_name    AS variety,
    province_short  AS province,
    contact_person,
    company_type,
    click_count,
    publish_date,
    veg_type        AS veg_type_pinyin
FROM ods_vegetable_suppliers
WHERE province_short IS NOT NULL
  AND province_short NOT IN ('台湾', '香港', '澳门');


-- =============================================================
-- DWS 层：综合宽表（按品种+省份聚合三表数据）
-- =============================================================

DROP TABLE IF EXISTS dws_vegetable_panorama;
CREATE TABLE dws_vegetable_panorama AS
SELECT
    p.variety,
    p.province,
    p.price_record_count,
    p.avg_price_num,
    p.min_price_num,
    p.max_price_num,
    COALESCE(s.seed_count, 0)       AS seed_count,
    COALESCE(sup.supplier_count, 0) AS supplier_count
FROM (
    SELECT
        variety,
        province,
        COUNT(*)                    AS price_record_count,
        ROUND(AVG(avg_price), 2)    AS avg_price_num,
        ROUND(MIN(min_price), 2)    AS min_price_num,
        ROUND(MAX(max_price), 2)    AS max_price_num
    FROM dwd_vegetable_prices
    GROUP BY variety, province
) p
LEFT JOIN (
    SELECT variety, province, COUNT(*) AS seed_count
    FROM dwd_vegetable_seeds
    GROUP BY variety, province
) s ON p.variety = s.variety AND p.province = s.province
LEFT JOIN (
    SELECT variety, province, COUNT(*) AS supplier_count
    FROM dwd_vegetable_suppliers
    GROUP BY variety, province
) sup ON p.variety = sup.variety AND p.province = sup.province;


-- =============================================================
-- ADS 层：应用数据层（直接对接前端 API）
-- =============================================================

-- ADS1: 品种-省份综合画像（三表深度关联）
DROP TABLE IF EXISTS ads_variety_province_profile;
CREATE TABLE ads_variety_province_profile AS
SELECT
    p.variety,
    p.province,
    ROUND(p.avg_price, 2)           AS avg_price,
    p.record_count                  AS price_records,
    COALESCE(s.seed_count, 0)       AS seed_count,
    COALESCE(sup.supplier_count, 0) AS supplier_count,
    -- 供应链成熟度指数（种子+供应商，越高越成熟）
    COALESCE(s.seed_count, 0) + COALESCE(sup.supplier_count, 0) AS supply_maturity,
    -- 价格竞争力（该省该品种均价 / 全国该品种均价，<1 表示低于全国均价）
    ROUND(p.avg_price / NULLIF(nat.national_avg, 0), 3) AS price_competitiveness
FROM (
    SELECT variety, province,
           AVG(avg_price) AS avg_price,
           COUNT(*)       AS record_count
    FROM dwd_vegetable_prices
    GROUP BY variety, province
) p
LEFT JOIN (
    SELECT variety, province, COUNT(*) AS seed_count
    FROM dwd_vegetable_seeds
    GROUP BY variety, province
) s ON p.variety = s.variety AND p.province = s.province
LEFT JOIN (
    SELECT variety, province, COUNT(*) AS supplier_count
    FROM dwd_vegetable_suppliers
    GROUP BY variety, province
) sup ON p.variety = sup.variety AND p.province = sup.province
LEFT JOIN (
    SELECT variety, AVG(avg_price) AS national_avg
    FROM dwd_vegetable_prices
    GROUP BY variety
) nat ON p.variety = nat.variety
WHERE p.record_count >= 10;


-- ADS2: 品种全国汇总（品种竞争力排名）
DROP TABLE IF EXISTS ads_variety_ranking;
CREATE TABLE ads_variety_ranking AS
SELECT
    p.variety,
    p.national_avg_price,
    p.price_record_count,
    COALESCE(s.total_seed_count, 0)       AS total_seed_count,
    COALESCE(sup.total_supplier_count, 0) AS total_supplier_count,
    -- 综合竞争力评分
    ROUND(
        COALESCE(s.total_seed_count, 0)    * 0.3 +
        COALESCE(sup.total_supplier_count, 0) * 0.4 +
        (100 - p.national_avg_price) * 0.3, 2
    ) AS competitiveness_score
FROM (
    SELECT variety,
           ROUND(AVG(avg_price), 2) AS national_avg_price,
           COUNT(*)                 AS price_record_count
    FROM dwd_vegetable_prices
    GROUP BY variety
) p
LEFT JOIN (
    SELECT variety, COUNT(*) AS total_seed_count
    FROM dwd_vegetable_seeds
    GROUP BY variety
) s ON p.variety = s.variety
LEFT JOIN (
    SELECT variety, COUNT(*) AS total_supplier_count
    FROM dwd_vegetable_suppliers
    GROUP BY variety
) sup ON p.variety = sup.variety
ORDER BY competitiveness_score DESC;


-- =============================================================
-- 验证各层记录数
-- =============================================================
SELECT 'ODS-价格表'  AS layer_table, COUNT(*) AS cnt FROM ods_vegetable_prices
UNION ALL
SELECT 'ODS-种子表',               COUNT(*) FROM ods_vegetable_seeds
UNION ALL
SELECT 'ODS-供应商表',             COUNT(*) FROM ods_vegetable_suppliers
UNION ALL
SELECT 'DWD-价格明细',             COUNT(*) FROM dwd_vegetable_prices
UNION ALL
SELECT 'DWD-种子明细',             COUNT(*) FROM dwd_vegetable_seeds
UNION ALL
SELECT 'DWD-供应商明细',           COUNT(*) FROM dwd_vegetable_suppliers
UNION ALL
SELECT 'DWS-综合宽表',             COUNT(*) FROM dws_vegetable_panorama
UNION ALL
SELECT 'ADS-品种省份画像',         COUNT(*) FROM ads_variety_province_profile
UNION ALL
SELECT 'ADS-品种竞争力排名',       COUNT(*) FROM ads_variety_ranking;

