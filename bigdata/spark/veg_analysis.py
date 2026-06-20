#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
蔬菜产业数据 - PySpark 分析脚本（完整版）
包含：基础分析(1-6) + 跨表关联分析(8) + MLlib价格预测(9)

运行方式:
    spark-submit --packages org.apache.spark:spark-mllib_2.12:3.5.5 bigdata/spark/veg_analysis.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import os
from datetime import datetime, timedelta

# =============================================================
# 配置
# =============================================================

HDFS_PRICES = '/vegetable_data/raw/prices/prices_cleaned.csv'
HDFS_SEEDS  = '/vegetable_data/raw/seeds/seeds_cleaned.csv'
HDFS_SUPPLIERS = '/vegetable_data/raw/suppliers/suppliers_cleaned.csv'

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
EXPORT_DIR  = os.path.join(PROJECT_DIR, 'export')

# =============================================================
# 初始化 Spark
# =============================================================

spark = SparkSession.builder \
    .appName('VegetableIndustryAnalysis') \
    .config('spark.sql.shuffle.partitions', '8') \
    .config('spark.sql.legacy.timeParserPolicy', 'LEGACY') \
    .config('spark.local.dir', '/tmp/spark-tmp') \
    .config('spark.driver.memory', '4g') \
    .config('spark.driver.maxResultSize', '2g') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
spark.sparkContext.setCheckpointDir('/tmp/spark-checkpoint')
os.makedirs('/tmp/spark-tmp', exist_ok=True)
os.makedirs('/tmp/spark-checkpoint', exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

TOTAL_STEPS = 9
EXCLUDE_REGIONS = ['台湾', '香港', '澳门']

print('=' * 55)
print('  PySpark 蔬菜产业数据分析')
print('=' * 55)


# =============================================================
# 数据加载（清洗后数据）
# =============================================================

print(f'[1/{TOTAL_STEPS}] 加载价格数据（清洗后）...')
df_prices = spark.read.csv(HDFS_PRICES, header=True, inferSchema=True) \
    .filter(F.col('平均价').isNotNull()) \
    .filter(F.col('省份简称').isNotNull()) \
    .filter(~F.col('省份简称').isin(*EXCLUDE_REGIONS))
price_count = df_prices.count()
print(f'      价格数据: {price_count} 条')

print(f'[2/{TOTAL_STEPS}] 加载种子数据（清洗后）...')
df_seeds = spark.read.csv(HDFS_SEEDS, header=True, inferSchema=False) \
    .withColumn('click_count_num', F.coalesce(F.col('点击量').cast('int'), F.lit(0))) \
    .filter(F.col('省份简称').isNotNull()) \
    .filter(~F.col('省份简称').isin(*EXCLUDE_REGIONS))
seed_count = df_seeds.count()
print(f'      种子数据: {seed_count} 条')

print(f'[3/{TOTAL_STEPS}] 加载供应商数据（清洗后）...')
df_suppliers = spark.read.csv(HDFS_SUPPLIERS, header=True, inferSchema=False) \
    .withColumn('click_count_num', F.coalesce(F.col('点击量').cast('int'), F.lit(0))) \
    .filter(F.col('省份简称').isNotNull()) \
    .filter(~F.col('省份简称').isin(*EXCLUDE_REGIONS))
supplier_count = df_suppliers.count()
print(f'      供应商数据: {supplier_count} 条')


# =============================================================
# 分析计算
# =============================================================

# ---------- 分析1: 各省份平均价格 ----------
print(f'[4/{TOTAL_STEPS}] 分析: 各省份平均价格...')
result_province_price = df_prices \
    .groupBy('省份简称') \
    .agg(
        F.round(F.avg('平均价'), 2).alias('avg_price'),
        F.count('*').alias('record_count')
    ) \
    .orderBy(F.desc('avg_price'))

result_province_price.coalesce(1).write \
    .mode('overwrite').option('header', True).csv(os.path.join(EXPORT_DIR, 'price_by_province'))


# ---------- 分析2: 各品种平均价格 Top 20 ----------
print(f'[4/{TOTAL_STEPS}] 分析: 各品种平均价格...')
result_veg_price = df_prices \
    .groupBy('品种') \
    .agg(
        F.round(F.avg('平均价'), 2).alias('avg_price'),
        F.count('*').alias('record_count')
    ) \
    .orderBy(F.desc('avg_price')) \
    .limit(20)

result_veg_price.coalesce(1).write \
    .mode('overwrite').option('header', True).csv(os.path.join(EXPORT_DIR, 'price_by_vegetable'))


# ---------- 分析3: 种子供应省份分布 ----------
print(f'[5/{TOTAL_STEPS}] 分析: 种子/供应商分布 + 品种评分...')
result_seed_prov = df_seeds \
    .filter(F.col('省份简称').isNotNull() & (F.col('省份简称') != '')) \
    .groupBy('省份简称') \
    .agg(F.count('*').alias('seed_count')) \
    .orderBy(F.desc('seed_count'))

result_seed_prov.coalesce(1).write \
    .mode('overwrite').option('header', True).csv(os.path.join(EXPORT_DIR, 'seed_by_province'))


# ---------- 分析4: 供应商省份分布 ----------
result_supplier_prov = df_suppliers \
    .filter(F.col('省份简称').isNotNull() & (F.col('省份简称') != '')) \
    .groupBy('省份简称') \
    .agg(F.count('*').alias('supplier_count')) \
    .orderBy(F.desc('supplier_count'))

result_supplier_prov.coalesce(1).write \
    .mode('overwrite').option('header', True).csv(os.path.join(EXPORT_DIR, 'supplier_by_province'))


# ---------- 分析5: 品种综合评分 ----------
seed_scores = df_seeds.groupBy('品种').count().withColumnRenamed('count', 'seed_count')
seed_max = seed_scores.agg(F.max('seed_count')).first()[0] or 1
seed_scores = seed_scores.withColumn('seed_norm', F.col('seed_count') / seed_max)

supplier_scores = df_suppliers.groupBy('品种').count().withColumnRenamed('count', 'supplier_count')
supplier_max = supplier_scores.agg(F.max('supplier_count')).first()[0] or 1
supplier_scores = supplier_scores.withColumn('supplier_norm', F.col('supplier_count') / supplier_max)

price_stats = df_prices.groupBy('品种').agg(
    F.avg('平均价').alias('mean_price'),
    F.stddev('平均价').alias('std_price')
).filter(F.col('mean_price') > 0).withColumn('cv', F.col('std_price') / F.col('mean_price'))

cv_max = price_stats.agg(F.max('cv')).first()[0] or 1
price_scores = price_stats.withColumn('price_stability', F.round(1.0 - F.col('cv') / cv_max, 2))

result_veg_score = seed_scores \
    .join(supplier_scores, on='品种', how='inner') \
    .join(price_scores.select('品种', 'price_stability', 'cv'),
          on='品种', how='inner') \
    .withColumn('total_score', F.round(
        F.col('seed_norm') * 0.3 + F.col('supplier_norm') * 0.4 +
        (1.0 - F.col('cv') / cv_max) * 0.3, 2
    )) \
    .select(
        F.col('品种'),
        F.col('seed_count'),
        F.col('supplier_count'),
        F.col('price_stability'),
        F.col('total_score')
    ) \
    .orderBy(F.desc('total_score')) \
    .limit(15)

result_veg_score.coalesce(1).write \
    .mode('overwrite').option('header', True).csv(os.path.join(EXPORT_DIR, 'vegetable_score'))


# ---------- 分析6: 价格趋势 ----------
print(f'[6/{TOTAL_STEPS}] 分析: 价格趋势...')
result_price_trend = df_prices \
    .filter(F.col('日期').isNotNull()) \
    .groupBy('日期') \
    .agg(
        F.round(F.avg('平均价'), 2).alias('avg_price'),
        F.count('*').alias('record_count')
    ) \
    .orderBy('日期')

result_price_trend.coalesce(1).write \
    .mode('overwrite').option('header', True).csv(os.path.join(EXPORT_DIR, 'price_trend'))


# ---------- 分析7: 省份-品种热力图 ----------
print(f'[7/{TOTAL_STEPS}] 分析: 省份-品种价格热力图...')
result_heatmap = df_prices \
    .filter(F.col('日期').isNotNull()) \
    .groupBy('省份简称', '品种') \
    .agg(F.round(F.avg('平均价'), 2).alias('avg_price')) \
    .filter(F.col('avg_price').isNotNull()) \
    .orderBy(F.desc('avg_price'))

result_heatmap.coalesce(1).write \
    .mode('overwrite').option('header', True).csv(os.path.join(EXPORT_DIR, 'heatmap_data'))


# ---------- 分析8: 跨表关联深度分析（品种+省份综合画像）----------
print(f'[8/{TOTAL_STEPS}] 分析: 跨表关联分析（品种-省份综合画像）...')

# 价格维度：品种+省份均价
price_cross = df_prices \
    .groupBy('品种', '省份简称') \
    .agg(
        F.round(F.avg('平均价'), 2).alias('avg_price'),
        F.round(F.stddev('平均价'), 2).alias('price_std'),
        F.count('*').alias('price_records')
    )

# 全国品种均价（用于计算价格竞争力）
national_avg = df_prices.groupBy('品种') \
    .agg(F.round(F.avg('平均价'), 4).alias('national_avg_price'))

# 种子维度：品种+省份数量
seed_cross = df_seeds \
    .filter(F.col('省份简称').isNotNull()) \
    .groupBy('品种', '省份简称') \
    .agg(F.count('*').alias('seed_count'))

# 供应商维度：品种+省份数量
supplier_cross = df_suppliers \
    .filter(F.col('省份简称').isNotNull()) \
    .groupBy('品种', '省份简称') \
    .agg(F.count('*').alias('supplier_count'))

# 三表关联（INNER JOIN 保证三表均有数据）
result_cross_table = price_cross \
    .join(seed_cross, on=['品种', '省份简称'], how='left') \
    .join(supplier_cross, on=['品种', '省份简称'], how='left') \
    .join(national_avg, on='品种', how='left') \
    .withColumn('seed_count', F.coalesce(F.col('seed_count'), F.lit(0))) \
    .withColumn('supplier_count', F.coalesce(F.col('supplier_count'), F.lit(0))) \
    .withColumn('supply_maturity',
                F.col('seed_count') + F.col('supplier_count')) \
    .withColumn('price_competitiveness',
                F.round(F.col('avg_price') / F.col('national_avg_price'), 3)) \
    .withColumn('price_volatility',
                F.round(F.col('price_std') / F.col('avg_price'), 3)) \
    .filter(F.col('price_records') >= 10) \
    .select(
        '品种', '省份简称',
        'avg_price', 'price_std', 'price_records',
        'seed_count', 'supplier_count', 'supply_maturity',
        'national_avg_price', 'price_competitiveness', 'price_volatility'
    ) \
    .orderBy(F.desc('supply_maturity'))

result_cross_table.coalesce(1).write \
    .mode('overwrite').option('header', True) \
    .csv(os.path.join(EXPORT_DIR, 'cross_table_profile'))

cross_count = result_cross_table.count()
print(f'      跨表关联记录数: {cross_count}')


# ---------- 分析9: Spark MLlib 价格预测（优化版）----------
print(f'[9/{TOTAL_STEPS}] 分析: Spark MLlib 价格预测...')

from pyspark.sql.window import Window as W

# 9.1 计算品种级统计特征（全国均价、供应链指标）
variety_stats = df_prices.groupBy('品种').agg(
    F.avg('平均价').alias('variety_national_avg'),
    F.stddev('平均价').alias('variety_price_std'),
    F.count('*').alias('variety_price_records')
)
seed_variety_stats = df_seeds.groupBy('品种').count().withColumnRenamed('count', 'variety_seed_count')
sup_variety_stats = df_suppliers.groupBy('品种').count().withColumnRenamed('count', 'variety_supplier_count')

variety_stats = variety_stats \
    .join(seed_variety_stats, on='品种', how='left') \
    .join(sup_variety_stats, on='品种', how='left') \
    .withColumn('variety_seed_count', F.coalesce(F.col('variety_seed_count'), F.lit(0))) \
    .withColumn('variety_supplier_count', F.coalesce(F.col('variety_supplier_count'), F.lit(0)))

# 9.1b 计算各品种最近30天均价（用作预测锚点，防止预测值漂移）
max_date_row = df_prices.filter(F.col('日期').isNotNull()) \
    .agg(F.max('日期').alias('max_date')).first()
max_date = max_date_row['max_date']

if hasattr(max_date, 'date'):
    max_date_py = max_date.date()
elif hasattr(max_date, 'toordinal'):
    max_date_py = max_date
else:
    max_date_py = datetime.strptime(str(max_date), '%Y-%m-%d').date()

recent_cutoff = max_date_py - timedelta(days=30)
recent_avg = df_prices \
    .filter(F.col('日期') >= F.lit(str(recent_cutoff))) \
    .groupBy('品种') \
    .agg(F.round(F.avg('平均价'), 4).alias('recent_30d_avg'))

variety_stats = variety_stats.join(recent_avg, on='品种', how='left') \
    .withColumn('recent_30d_avg',
                F.coalesce(F.col('recent_30d_avg'), F.col('variety_national_avg')))

# 9.1c 计算季节性月度修正因子（品种月度均价 / 品种年均价）
seasonal_factor = df_prices \
    .filter(F.col('日期').isNotNull()) \
    .withColumn('month', F.month(F.col('日期')).cast('int')) \
    .groupBy('品种', 'month') \
    .agg(F.avg('平均价').alias('monthly_avg')) \
    .join(variety_stats.select('品种', 'variety_national_avg'), on='品种', how='left') \
    .withColumn('seasonal_factor', F.round(
        F.col('monthly_avg') / F.col('variety_national_avg'), 4)) \
    .select('品种', 'month', 'seasonal_factor')

# 9.1d IQR 离群值过滤：每个品种剔除超出 [Q1-1.5*IQR, Q3+1.5*IQR] 的极端价格
iqr_bounds = df_prices.groupBy('品种').agg(
    F.expr('percentile(`平均价`, 0.25)').alias('q1'),
    F.expr('percentile(`平均价`, 0.75)').alias('q3')
).withColumn('iqr', F.col('q3') - F.col('q1')) \
 .withColumn('lower_fence', F.col('q1') - F.lit(1.5) * F.col('iqr')) \
 .withColumn('upper_fence', F.col('q3') + F.lit(1.5) * F.col('iqr'))

df_prices_clean = df_prices.join(
    F.broadcast(iqr_bounds), on='品种', how='left'
).filter(
    (F.col('平均价') >= F.col('lower_fence')) &
    (F.col('平均价') <= F.col('upper_fence'))
)
outlier_count = df_prices.count() - df_prices_clean.count()
print(f'      离群值过滤: 移除 {outlier_count} 条极端价格记录')

# 9.2 构建训练数据集（加入品种级特征 + 近期均价锚点）
ml_data = df_prices_clean \
    .filter(F.col('日期').isNotNull()) \
    .withColumn('month', F.month(F.col('日期')).cast('int')) \
    .withColumn('day_of_year', F.dayofyear(F.col('日期')).cast('int')) \
    .withColumn('year', F.year(F.col('日期')).cast('int')) \
    .join(F.broadcast(variety_stats), on='品种', how='left') \
    .withColumn('price_ratio', F.round(F.col('平均价') / F.col('variety_national_avg'), 3)) \
    .withColumn('price_vs_recent', F.round(
        F.col('平均价') / F.col('recent_30d_avg'), 3)) \
    .filter(F.col('month').isNotNull())

print(f'      ML训练样本数: {ml_data.count()}')

# 9.3 特征编码
province_indexer = StringIndexer(
    inputCol='省份简称', outputCol='province_idx', handleInvalid='keep')
veg_indexer = StringIndexer(
    inputCol='品种', outputCol='veg_idx', handleInvalid='keep')

prov_idx_model = province_indexer.fit(ml_data)
veg_idx_model = veg_indexer.fit(ml_data)
ml_data = prov_idx_model.transform(ml_data)
ml_data = veg_idx_model.transform(ml_data)

# 9.4 组装特征向量（11个特征：新增近期均价比）
feature_cols = [
    'province_idx', 'veg_idx',
    'month', 'day_of_year', 'year',
    'variety_national_avg', 'variety_price_std',
    'variety_seed_count', 'variety_supplier_count',
    'variety_price_records',
    'price_vs_recent'
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol='features',
    handleInvalid='skip')

ml_prepared = assembler.transform(ml_data) \
    .select('features', '平均价', '品种', '省份简称',
            'month', 'day_of_year', 'year') \
    .filter(F.col('features').isNotNull()) \
    .cache()

# Checkpoint 截断 RDD 血统，避免 shuffle 文件被清理
ml_prepared.checkpoint()
ml_prepared.count()  # 触发 checkpoint

train_df, test_df = ml_prepared.randomSplit([0.8, 0.2], seed=42)
train_df.cache()
test_df.cache()
train_count = train_df.count()
test_count = test_df.count()
print(f'      训练集: {train_count} | 测试集: {test_count}')

# 9.5 训练随机森林（50棵树，更深以捕捉季节性）
rf = RandomForestRegressor(
    featuresCol='features',
    labelCol='平均价',
    numTrees=50,
    maxDepth=10,
    maxBins=64,
    minInstancesPerNode=20,
    seed=42)

model = rf.fit(train_df)

# 9.6 模型评估
predictions = model.transform(test_df)
evaluator_rmse = RegressionEvaluator(
    labelCol='平均价', predictionCol='prediction', metricName='rmse')
evaluator_r2 = RegressionEvaluator(
    labelCol='平均价', predictionCol='prediction', metricName='r2')

rmse = evaluator_rmse.evaluate(predictions)
r2   = evaluator_r2.evaluate(predictions)
mae_evaluator = RegressionEvaluator(
    labelCol='平均价', predictionCol='prediction', metricName='mae')
mae = mae_evaluator.evaluate(predictions)
print(f'      模型评估 → RMSE: {rmse:.4f} | R²: {r2:.4f} | MAE: {mae:.4f}')

# 9.7 特征重要性
feature_names = ['省份', '品种', '月份', '年内天数', '年份',
                 '品种全国均价', '品种价格标准差',
                 '品种种子数', '品种供应商数', '品种价格记录数',
                 '近期均价比']
importances = model.featureImportances.toArray()
for name, imp in zip(feature_names, importances):
    print(f'        {name}: {imp:.4f}')

# 9.8 生成多窗口预测（7/14/30/60 天）
# max_date_py 已在步骤 9.1b 计算

# 获取每个品种+省份组合的最近价格记录数作为权重
variety_prov_weights = df_prices_clean.groupBy('品种', '省份简称').count() \
    .withColumnRenamed('count', 'weight')

current_month = max_date_py.month
recent_seasonal = seasonal_factor.filter(F.col('month') == current_month) \
    .withColumnRenamed('seasonal_factor', 'recent_sf')

# 组装基础预测输入（品种×省份 全组合）
forecast_base = variety_prov_weights \
    .join(variety_stats, on='品种', how='left')
forecast_base = prov_idx_model.transform(forecast_base)
forecast_base = veg_idx_model.transform(forecast_base)

# 预计算模型原始预测
from pyspark.ml import PipelineModel
model_raw = model.transform(
    forecast_base.withColumn('price_vs_recent', F.lit(1.0))
)

# 多窗口循环预测
PREDICT_WINDOWS = [7, 14, 30, 60]
for w in PREDICT_WINDOWS:
    future_date = max_date_py + timedelta(days=w)
    future_year  = future_date.year
    future_month = future_date.month
    future_doy   = future_date.timetuple().tm_yday

    future_seasonal = seasonal_factor.filter(F.col('month') == future_month) \
        .withColumnRenamed('seasonal_factor', 'future_sf')

    forecast_input = model_raw \
        .withColumn('month', F.lit(future_month)) \
        .withColumn('day_of_year', F.lit(future_doy)) \
        .withColumn('year', F.lit(future_year))
    forecast_input = assembler.transform(forecast_input)

    forecast_raw = model.transform(forecast_input) \
        .select(
            '品种', '省份简称', 'weight',
            F.col('prediction').alias('raw_price')
        ) \
        .filter(F.col('raw_price') > 0)

    forecast_agg = forecast_raw \
        .withColumn('weighted_price', F.col('raw_price') * F.col('weight')) \
        .groupBy('品种').agg(
            F.round(F.sum('weighted_price') / F.sum('weight'), 2).alias('predicted_price')
        )

    forecast_result = forecast_agg \
        .join(variety_stats.select('品种', 'variety_national_avg', 'recent_30d_avg',
                                   'variety_price_std'), on='品种', how='left') \
        .join(future_seasonal, on='品种', how='left') \
        .join(recent_seasonal, on='品种', how='left') \
        .withColumn('future_sf', F.coalesce(F.col('future_sf'), F.lit(1.0))) \
        .withColumn('recent_sf', F.coalesce(F.col('recent_sf'), F.lit(1.0))) \
        .withColumn('seasonal_ratio', F.round(
            F.col('future_sf') / F.col('recent_sf'), 4)) \
        .withColumn('predicted_price', F.round(
            F.col('predicted_price') * F.col('seasonal_ratio'), 2)) \
        .withColumn('upper_bound', F.round(F.col('recent_30d_avg') * 1.3, 2)) \
        .withColumn('lower_bound', F.round(
            F.greatest(F.col('recent_30d_avg') * 0.7, F.lit(0.1)), 2)) \
        .withColumn('predicted_price',
            F.when(F.col('predicted_price') > F.col('upper_bound'),
                   F.col('upper_bound'))
            .when(F.col('predicted_price') < F.col('lower_bound'),
                  F.col('lower_bound'))
            .otherwise(F.col('predicted_price'))) \
        .select(
            '品种',
            'predicted_price',
            F.round('recent_30d_avg', 2).alias('current_avg_price'),
            F.lit(str(future_date)).alias('forecast_date')
        ) \
        .orderBy(F.desc('predicted_price')) \
        .limit(30)

    export_name = f'price_prediction_{w}d' if w != 30 else 'price_prediction'
    forecast_result.coalesce(1).write \
        .mode('overwrite').option('header', True) \
        .csv(os.path.join(EXPORT_DIR, export_name))

    print(f'      [{w}天] 预测日期: {future_date} | 品种数: {forecast_result.count()}')

# 打印最终窗口指标
print(f'      模型指标: RMSE={rmse:.3f}, R²={r2:.3f}, MAE={mae:.3f}')

# 导出模型评估指标
model_metrics = spark.createDataFrame([{
    'metric': 'RMSE', 'value': float(round(rmse, 4)),
}, {
    'metric': 'R2', 'value': float(round(r2, 4)),
}, {
    'metric': 'MAE', 'value': float(round(mae, 4)),
}, {
    'metric': 'numTrees', 'value': float(50),
}, {
    'metric': 'maxDepth', 'value': float(10),
}, {
    'metric': 'featureCount', 'value': float(len(feature_cols)),
}])
model_metrics.coalesce(1).write \
    .mode('overwrite').option('header', True) \
    .csv(os.path.join(EXPORT_DIR, 'model_metrics'))


# =============================================================
# 汇总输出
# =============================================================
print()
print(f'[导出完成] 共 {TOTAL_STEPS} 项分析结果：')
print(f'  导出目录: {EXPORT_DIR}')
print('  ─────────────────────────────────────────')
print('  基础分析:')
print('    price_by_province/      各省份平均价格')
print('    price_by_vegetable/     各品种平均价格 Top 20')
print('    seed_by_province/       种子供应省份分布')
print('    supplier_by_province/   供应商省份分布')
print('    vegetable_score/        品种综合评分')
print('    price_trend/            价格趋势（历史）')
print('    heatmap_data/           省份-品种价格热力图')
print('  跨表关联分析:')
print('    cross_table_profile/    品种-省份综合画像（三表JOIN）')
print('  MLlib 价格预测:')
for w in PREDICT_WINDOWS:
    fd = max_date_py + timedelta(days=w)
    exp = 'price_prediction' if w == 30 else f'price_prediction_{w}d'
    print(f'    {exp + "/":28s} 未来{w}天价格预测（{fd}）')
print('    model_metrics/          模型评估指标（RMSE/R²/MAE）')
print()
print('=' * 55)
print('  PySpark 分析完成！')
print('=' * 55)

spark.stop()
