#!/bin/bash
# =============================================================
# 蔬菜产业数据 - HDFS 上传脚本
# 将本地 CSV 原始数据上传到 HDFS 分布式文件系统
# =============================================================

set -e

# 项目根目录（脚本所在目录的上一级）
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

# 本地数据路径（使用清洗后的数据）
PRICES_CSV="${PROJECT_DIR}/data/processed/prices_cleaned.csv"
SEEDS_CSV="${PROJECT_DIR}/data/processed/seeds_cleaned.csv"
SUPPLIERS_CSV="${PROJECT_DIR}/data/processed/suppliers_cleaned.csv"

# HDFS 目标路径
HDFS_BASE="/vegetable_data"
HDFS_PRICES="${HDFS_BASE}/raw/prices"
HDFS_SEEDS="${HDFS_BASE}/raw/seeds"
HDFS_SUPPLIERS="${HDFS_BASE}/raw/suppliers"

echo "============================================"
echo "  蔬菜产业数据 - HDFS 上传"
echo "============================================"

# 检查文件是否存在
for f in "$PRICES_CSV" "$SEEDS_CSV" "$SUPPLIERS_CSV"; do
    if [ ! -f "$f" ]; then
        echo "[错误] 文件不存在: $f"
        exit 1
    fi
done

echo "[1/6] 创建 HDFS 目录（如已存在则先删除）..."
hdfs dfs -rm -r -f "$HDFS_BASE" || true
hdfs dfs -mkdir -p "$HDFS_PRICES"
hdfs dfs -mkdir -p "$HDFS_SEEDS"
hdfs dfs -mkdir -p "$HDFS_SUPPLIERS"

echo "[2/6] 上传价格数据..."
echo "  本地: $PRICES_CSV"
echo "  目标: $HDFS_PRICES/"
hdfs dfs -put -f "$PRICES_CSV" "$HDFS_PRICES/"

echo "[3/6] 上传种子数据..."
echo "  本地: $SEEDS_CSV"
echo "  目标: $HDFS_SEEDS/"
hdfs dfs -put -f "$SEEDS_CSV" "$HDFS_SEEDS/"

echo "[4/6] 上传供应商数据..."
echo "  本地: $SUPPLIERS_CSV"
echo "  目标: $HDFS_SUPPLIERS/"
hdfs dfs -put -f "$SUPPLIERS_CSV" "$HDFS_SUPPLIERS/"
echo "[5/6] 验证上传结果..."
echo ""
echo "--- HDFS 文件列表 ---"
hdfs dfs -ls -h "$HDFS_PRICES/"
hdfs dfs -ls -h "$HDFS_SEEDS/"
hdfs dfs -ls -h "$HDFS_SUPPLIERS/"

echo ""
echo "[6/6] 统计行数..."
PRICES_LINES=$(hdfs dfs -cat "$HDFS_PRICES/prices_cleaned.csv" | wc -l)
SEEDS_LINES=$(hdfs dfs -cat "$HDFS_SEEDS/seeds_cleaned.csv" | wc -l)
SUPPLIERS_LINES=$(hdfs dfs -cat "$HDFS_SUPPLIERS/suppliers_cleaned.csv" | wc -l)

echo "  价格数据:   ${PRICES_LINES} 行"
echo "  种子数据:   ${SEEDS_LINES} 行"
echo "  供应商数据: ${SUPPLIERS_LINES} 行"

echo ""
echo "============================================"
echo "  HDFS 上传完成！"
echo "============================================"
