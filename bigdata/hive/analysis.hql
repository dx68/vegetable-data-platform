-- =============================================================
-- 蔬菜产业数据 - HiveQL 分析查询
-- 基于清洗后数据，6 个核心分析
-- =============================================================

USE vegetable_db;

-- =============================================================
-- 分析1: 各省份蔬菜平均价格
-- =============================================================
DROP TABLE IF EXISTS result_price_by_province;
CREATE TABLE result_price_by_province AS
SELECT
    province,
    ROUND(AVG(avg_price), 2) AS avg_price,
    COUNT(*) AS record_count
FROM ods_vegetable_prices
WHERE avg_price IS NOT NULL
GROUP BY province
ORDER BY avg_price DESC;

-- =============================================================
-- 分析2: 各品种平均价格 Top 20
-- =============================================================
DROP TABLE IF EXISTS result_price_by_vegetable;
CREATE TABLE result_price_by_vegetable AS
SELECT
    veg_type,
    ROUND(AVG(avg_price), 2) AS avg_price,
    COUNT(*) AS record_count
FROM ods_vegetable_prices
WHERE avg_price IS NOT NULL
GROUP BY veg_type
ORDER BY avg_price DESC
LIMIT 20;

-- =============================================================
-- 分析3: 种子供应省份分布
-- =============================================================
DROP TABLE IF EXISTS result_seed_by_province;
CREATE TABLE result_seed_by_province AS
SELECT
    split(supply_region, ' > ')[0] AS province,
    COUNT(*) AS seed_count
FROM ods_vegetable_seeds
WHERE supply_region IS NOT NULL AND supply_region != ''
GROUP BY split(supply_region, ' > ')[0]
ORDER BY seed_count DESC;

-- =============================================================
-- 分析4: 供应商省份分布
-- =============================================================
DROP TABLE IF EXISTS result_supplier_by_province;
CREATE TABLE result_supplier_by_province AS
SELECT
    split(origin, ' > ')[0] AS province,
    COUNT(*) AS supplier_count
FROM ods_vegetable_suppliers
WHERE origin IS NOT NULL AND origin != ''
GROUP BY split(origin, ' > ')[0]
ORDER BY supplier_count DESC;

-- =============================================================
-- 分析5: 品种综合评分（种子丰富度 + 供应商数量 + 价格稳定性）
-- =============================================================

-- 先创建中间表，避免嵌套窗口函数
DROP TABLE IF EXISTS tmp_seed_score;
CREATE TABLE tmp_seed_score AS
SELECT
    veg_type,
    COUNT(*) AS seed_count,
    CAST(COUNT(*) AS DOUBLE) / MAX(COUNT(*)) OVER() AS seed_norm
FROM ods_vegetable_seeds
GROUP BY veg_type;

DROP TABLE IF EXISTS tmp_supplier_score;
CREATE TABLE tmp_supplier_score AS
SELECT
    veg_type,
    COUNT(*) AS supplier_count,
    CAST(COUNT(*) AS DOUBLE) / MAX(COUNT(*)) OVER() AS supplier_norm
FROM ods_vegetable_suppliers
GROUP BY veg_type;

-- 价格稳定性：先用子查询计算变异系数，再归一化
DROP TABLE IF EXISTS tmp_price_cv;
CREATE TABLE tmp_price_cv AS
SELECT
    veg_type,
    AVG(avg_price) AS mean_price,
    STDDEV(avg_price) AS std_price
FROM ods_vegetable_prices
WHERE avg_price IS NOT NULL
GROUP BY veg_type;

DROP TABLE IF EXISTS tmp_price_score;
CREATE TABLE tmp_price_score AS
SELECT
    veg_type,
    ROUND(1.0 - (std_price / mean_price) / max_cv, 2) AS price_stability,
    1.0 - (std_price / mean_price) / max_cv AS stability_norm
FROM (
    SELECT veg_type, std_price, mean_price
    FROM tmp_price_cv
    WHERE mean_price > 0
) t
CROSS JOIN (
    SELECT MAX(std_price / mean_price) AS max_cv FROM tmp_price_cv WHERE mean_price > 0
) m;

-- 三表 JOIN 计算综合评分
DROP TABLE IF EXISTS result_vegetable_score;
CREATE TABLE result_vegetable_score AS
SELECT
    t1.veg_type,
    t1.seed_count,
    t2.supplier_count,
    t3.price_stability,
    ROUND(t1.seed_norm * 0.3 + t2.supplier_norm * 0.4 + t3.stability_norm * 0.3, 2) AS total_score
FROM tmp_seed_score t1
JOIN tmp_supplier_score t2 ON t1.veg_type = t2.veg_type
JOIN tmp_price_score t3 ON t1.veg_type = t3.veg_type
ORDER BY total_score DESC
LIMIT 15;

-- 清理临时表
DROP TABLE IF EXISTS tmp_seed_score;
DROP TABLE IF EXISTS tmp_supplier_score;
DROP TABLE IF EXISTS tmp_price_cv;
DROP TABLE IF EXISTS tmp_price_score;

-- =============================================================
-- 分析6: 价格趋势（按日期聚合）
-- =============================================================
DROP TABLE IF EXISTS result_price_trend;
CREATE TABLE result_price_trend AS
SELECT
    price_date,
    ROUND(AVG(avg_price), 2) AS avg_price,
    COUNT(*) AS record_count
FROM ods_vegetable_prices
WHERE avg_price IS NOT NULL
  AND price_date IS NOT NULL AND price_date != ''
GROUP BY price_date
ORDER BY price_date;

-- =============================================================
-- 导出结果
-- =============================================================
-- 方法1: INSERT OVERWRITE LOCAL DIRECTORY
-- INSERT OVERWRITE LOCAL DIRECTORY '/tmp/veg_export/result_price_by_province'
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
-- SELECT * FROM result_price_by_province;
--
-- 方法2: hive -e 命令行直接导出
-- hive -e "USE vegetable_db; SELECT * FROM result_price_by_province;" > result_price_by_province.csv
--
-- 方法3: 使用 PySpark 脚本（见 bigdata/spark/veg_analysis.py）
