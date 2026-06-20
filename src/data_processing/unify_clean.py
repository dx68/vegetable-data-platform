"""
统一清洗三张表：prices / seeds / suppliers
目标：
  1. 日期 → YYYY-MM-DD（统一格式）
  2. 新增「品种」列（中文，三表一致，可直接 join）
  3. 新增「省份简称」列（如"山东"，三表一致，可直接 join）
输出覆盖写入 *_cleaned.csv（原地更新）
"""
import pandas as pd
import numpy as np
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.categories import SEED_CATEGORY_SPECS

# ============================================================
# 1. 构建映射表
# ============================================================

# 拼音 ↔ 中文
PINYIN_TO_CN = {}
CN_TO_PINYIN = {}
for specs in SEED_CATEGORY_SPECS.values():
    for cn_name, pinyin_code in specs:
        PINYIN_TO_CN[pinyin_code] = cn_name
        CN_TO_PINYIN[cn_name] = pinyin_code
# 补充
_extra = {
    '花椰菜': 'huayecai', '菜花': 'caihua', '西兰花': 'xilanhua',
    '黄瓜': 'huanggua', '西瓜': 'xigua', '山药': 'shanyao',
    '大蒜': 'dasuan', '大葱': 'dacong',
}
for cn, py in _extra.items():
    PINYIN_TO_CN[py] = cn
    CN_TO_PINYIN[cn] = py

# 省份全称 → 简称
GEO_TO_SHORT = {
    '北京市': '北京', '天津市': '天津', '上海市': '上海', '重庆市': '重庆',
    '河北省': '河北', '山西省': '山西', '辽宁省': '辽宁', '吉林省': '吉林',
    '黑龙江省': '黑龙江', '江苏省': '江苏', '浙江省': '浙江', '安徽省': '安徽',
    '福建省': '福建', '江西省': '江西', '山东省': '山东', '河南省': '河南',
    '湖北省': '湖北', '湖南省': '湖南', '广东省': '广东', '海南省': '海南',
    '四川省': '四川', '贵州省': '贵州', '云南省': '云南', '陕西省': '陕西',
    '甘肃省': '甘肃', '青海省': '青海', '台湾省': '台湾',
    '内蒙古自治区': '内蒙古', '广西壮族自治区': '广西',
    '西藏自治区': '西藏', '宁夏回族自治区': '宁夏', '新疆维吾尔自治区': '新疆',
    '香港特别行政区': '香港', '澳门特别行政区': '澳门',
}
PROVINCE_PINYIN_MAP = {'xicang': '西藏'}

def to_short_province(name):
    """任意省份名 → 简称"""
    if not name or pd.isna(name):
        return np.nan
    name = str(name).strip()
    # 拼音 → 中文
    name = PROVINCE_PINYIN_MAP.get(name, name)
    # 全称 → 简称
    short = GEO_TO_SHORT.get(name, name)
    # 排除无意义值
    if short in ('未知', '', 'zhongguo', '中国'):
        return np.nan
    return short

def parse_date(val):
    """将任意日期格式 → YYYY-MM-DD，失败返回空"""
    if not val or pd.isna(val):
        return np.nan
    s = str(val).strip()
    if s in ('未知', '', 'nan', 'None'):
        return np.nan
    # 已经是 YYYY-MM-DD (10字符)
    for fmt in ('%Y-%m-%d', '%Y/%m/%d %H:%M:%S', '%Y/%m/%d', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return np.nan

def extract_province_from_region(region):
    """从 '省 > 市 > 区' 格式提取省份简称"""
    if not region or pd.isna(region):
        return np.nan
    parts = str(region).split('>')
    if parts:
        return to_short_province(parts[0].strip())
    return np.nan

# ============================================================
# 2. 清洗 prices_cleaned.csv
# ============================================================
print("=" * 60)
print("清洗 prices_cleaned.csv ...")
prices_path = '../../data/processed/prices_cleaned.csv'
df = pd.read_csv(prices_path, encoding='utf-8-sig')
print(f"  原始行数: {len(df):,}")

# 日期统一
df['日期'] = df['日期'].apply(parse_date)

# 品种：蔬菜类型(拼音) → 中文名
df['品种'] = df['蔬菜类型'].map(PINYIN_TO_CN)

# 省份简称
df['省份简称'] = df['省份'].apply(to_short_province)

# 保存
df.to_csv(prices_path, index=False, encoding='utf-8-sig')
print(f"  品种覆盖: {df['品种'].notna().sum():,} / {len(df):,}")
print(f"  省份简称覆盖: {df['省份简称'].notna().sum():,} / {len(df):,}")
print(f"  日期覆盖: {df['日期'].notna().sum():,} / {len(df):,}")
print(f"  列: {df.columns.tolist()}")

# ============================================================
# 3. 清洗 seeds_cleaned.csv
# ============================================================
print("\n" + "=" * 60)
print("清洗 seeds_cleaned.csv ...")
seeds_path = '../../data/processed/seeds_cleaned.csv'
df = pd.read_csv(seeds_path, encoding='utf-8-sig')
print(f"  原始行数: {len(df):,}")

# 日期统一
df['日期'] = df['日期'].apply(parse_date)

# 品种：分类已经是中文
df['品种'] = df['分类']

# 省份简称：从供应地区提取
df['省份简称'] = df['供应地区'].apply(extract_province_from_region)

# 保存
df.to_csv(seeds_path, index=False, encoding='utf-8-sig')
print(f"  品种覆盖: {df['品种'].notna().sum():,} / {len(df):,}")
print(f"  省份简称覆盖: {df['省份简称'].notna().sum():,} / {len(df):,}")
print(f"  日期覆盖: {df['日期'].notna().sum():,} / {len(df):,}")
print(f"  列: {df.columns.tolist()}")

# ============================================================
# 4. 清洗 suppliers_cleaned.csv
# ============================================================
print("\n" + "=" * 60)
print("清洗 suppliers_cleaned.csv ...")
suppliers_path = '../../data/processed/suppliers_cleaned.csv'
df = pd.read_csv(suppliers_path, encoding='utf-8-sig')
print(f"  原始行数: {len(df):,}")

# 日期统一
df['日期'] = df['日期'].apply(parse_date)

# 品种：分类已经是中文
df['品种'] = df['分类']

# 省份简称：从产地提取
df['省份简称'] = df['产地'].apply(extract_province_from_region)

# 保存
df.to_csv(suppliers_path, index=False, encoding='utf-8-sig')
print(f"  品种覆盖: {df['品种'].notna().sum():,} / {len(df):,}")
print(f"  省份简称覆盖: {df['省份简称'].notna().sum():,} / {len(df):,}")
print(f"  日期覆盖: {df['日期'].notna().sum():,} / {len(df):,}")
print(f"  列: {df.columns.tolist()}")

# ============================================================
# 5. 验证三表可连接性
# ============================================================
print("\n" + "=" * 60)
print("=== 三表连接验证 ===")
p = pd.read_csv(prices_path, encoding='utf-8-sig')
s = pd.read_csv(seeds_path, encoding='utf-8-sig')
sp = pd.read_csv(suppliers_path, encoding='utf-8-sig')

p_veg = set(p['品种'].dropna().unique())
s_veg = set(s['品种'].dropna().unique())
sp_veg = set(sp['品种'].dropna().unique())
print(f"\n品种交集（三表）: {sorted(p_veg & s_veg & sp_veg)}")
print(f"品种交集（prices∩seeds）: {len(p_veg & s_veg)} 个")
print(f"品种交集（prices∩suppliers）: {len(p_veg & sp_veg)} 个")
print(f"品种交集（seeds∩suppliers）: {len(s_veg & sp_veg)} 个")

p_prov = set(p['省份简称'].dropna().unique())
s_prov = set(s['省份简称'].dropna().unique())
sp_prov = set(sp['省份简称'].dropna().unique())
print(f"\n省份交集（三表）: {sorted(p_prov & s_prov & sp_prov)}")
print(f"省份交集（prices∩seeds）: {len(p_prov & s_prov)} 个")
print(f"省份交集（prices∩suppliers）: {len(p_prov & sp_prov)} 个")
print(f"省份交集（seeds∩suppliers）: {len(s_prov & sp_prov)} 个")

# 示例 join
print("\n--- 示例: seeds JOIN suppliers ON 品种+省份简称 ---")
s_agg = s.groupby(['品种', '省份简称']).size().reset_index(name='种子数')
sp_agg = sp.groupby(['品种', '省份简称']).size().reset_index(name='供应商数')
merged = s_agg.merge(sp_agg, on=['品种', '省份简称'], how='inner')
print(f"  匹配行数: {len(merged)}")
print(merged.head(10).to_string(index=False))

print("\n--- 示例: prices均价 JOIN 种子数 ON 品种+省份简称 ---")
p_agg = p.groupby(['品种', '省份简称'])['平均价'].mean().round(2).reset_index(name='均价')
merged2 = p_agg.merge(s_agg, on=['品种', '省份简称'], how='inner')
print(f"  匹配行数: {len(merged2)}")
print(merged2.head(10).to_string(index=False))

print("\n✅ 清洗完成！三表现在可以通过「品种」+「省份简称」直接 join。")
