"""
数据清洗脚本：data/raw/ → data/processed/
将爬虫原始数据清洗为前端可直接使用的格式
"""
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import pandas as pd
import logging
from config.categories import RAW_DATA_DIR, PROCESSED_DATA_DIR

logger = logging.getLogger(__name__)

# 价格数据：URL拼音省份 → 中文简称
PROVINCE_PINYIN_TO_CN = {
    'gansu': '甘肃', 'shandong': '山东', 'shanxi': '山西', 'xinjiang': '新疆',
    'hubei': '湖北', 'anhui': '安徽', 'jiangsu': '江苏', 'hebei': '河北',
    'zhejiang': '浙江', 'beijing': '北京', 'sichuan': '四川', 'liaoning': '辽宁',
    'henan': '河南', 'tianjin': '天津', 'neimenggu': '内蒙古', 'jiangxi': '江西',
    'hunan': '湖南', 'yunnan': '云南', 'guangdong': '广东', 'heilongjian': '黑龙江',
    'ningxia': '宁夏', 'shan_xi': '陕西', 'fujian': '福建', 'chongqing': '重庆',
    'guizhou': '贵州', 'jilin': '吉林', 'guangxi': '广西', 'xizang': '西藏',
    'hainan': '海南', 'qinghai': '青海', 'shanghai': '上海',
    'xicang': '西藏',
}


def clean_prices():
    """清洗价格数据"""
    raw_path = os.path.join(RAW_DATA_DIR, 'prices', 'prices_all.csv')
    clean_path = os.path.join(PROCESSED_DATA_DIR, 'prices_cleaned.csv')

    if not os.path.exists(raw_path):
        logger.warning(f'价格原始文件不存在: {raw_path}')
        return

    df = pd.read_csv(raw_path, dtype=str, on_bad_lines='skip', encoding='utf-8-sig')
    original_count = len(df)

    # 映射省份拼音 → 中文简称
    if '省份' in df.columns:
        df['省份简称'] = df['省份'].map(PROVINCE_PINYIN_TO_CN).fillna(df['省份'])

    # 删除爬取时间列
    if '爬取时间' in df.columns:
        df = df.drop(columns=['爬取时间'])

    # 转换数值（去掉￥/元等前缀）
    for col in ['最低价', '最高价', '平均价']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'[￥¥元,，\s]', '', regex=True)
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 转换日期
    if '日期' in df.columns:
        df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        df = df.dropna(subset=['日期'])

    # 去除均价为空的行
    df = df.dropna(subset=['平均价'])

    # 去重（保留最后一条）
    df = df.drop_duplicates(keep='last')

    df.to_csv(clean_path, index=False, encoding='utf-8-sig')
    logger.info(f'价格清洗完成: {original_count} → {len(df)} 条 → {clean_path}')


def clean_seeds():
    """清洗种子数据"""
    raw_path = os.path.join(RAW_DATA_DIR, 'seeds', 'seeds_all.csv')
    clean_path = os.path.join(PROCESSED_DATA_DIR, 'seeds_cleaned.csv')

    if not os.path.exists(raw_path):
        logger.warning(f'种子原始文件不存在: {raw_path}')
        return

    df = pd.read_csv(raw_path, dtype=str, on_bad_lines='skip', encoding='utf-8-sig')
    original_count = len(df)

    # 从供应地区提取省份简称
    if '供应地区' in df.columns:
        df['省份简称'] = df['供应地区'].apply(
            lambda x: str(x).split('>')[0].strip() if pd.notna(x) else None
        )

    # 统一品种名：分类 → 品种
    if '分类' in df.columns:
        df['品种'] = df['分类']

    # 删除企业链接（节省空间）
    if '企业链接' in df.columns:
        df = df.drop(columns=['企业链接'])

    # 转换点击量
    if '点击量' in df.columns:
        df['点击量'] = pd.to_numeric(df['点击量'], errors='coerce').fillna(0).astype(int)

    # 去重
    df = df.drop_duplicates(keep='last')

    df.to_csv(clean_path, index=False, encoding='utf-8-sig')
    logger.info(f'种子清洗完成: {original_count} → {len(df)} 条 → {clean_path}')


def clean_suppliers():
    """清洗供应商数据"""
    raw_path = os.path.join(RAW_DATA_DIR, 'suppliers', 'suppliers_all.csv')
    clean_path = os.path.join(PROCESSED_DATA_DIR, 'suppliers_cleaned.csv')

    if not os.path.exists(raw_path):
        logger.warning(f'供应商原始文件不存在: {raw_path}')
        return

    df = pd.read_csv(raw_path, dtype=str, on_bad_lines='skip', encoding='utf-8-sig')
    original_count = len(df)

    # 从产地提取省份简称
    if '产地' in df.columns:
        df['省份简称'] = df['产地'].apply(
            lambda x: str(x).split('>')[0].strip() if pd.notna(x) else None
        )

    # 统一品种名：分类 → 品种
    if '分类' in df.columns:
        df['品种'] = df['分类']

    # 转换点击量
    if '点击量' in df.columns:
        df['点击量'] = pd.to_numeric(df['点击量'], errors='coerce').fillna(0).astype(int)

    # 去重
    df = df.drop_duplicates(keep='last')

    df.to_csv(clean_path, index=False, encoding='utf-8-sig')
    logger.info(f'供应商清洗完成: {original_count} → {len(df)} 条 → {clean_path}')


def clean_all():
    """执行全部清洗"""
    clean_prices()
    clean_seeds()
    clean_suppliers()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    clean_all()
