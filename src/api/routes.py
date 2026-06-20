from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Optional, List
import io
import logging
from src.data_processing.processor import DataProcessor

logger = logging.getLogger(__name__)

router = APIRouter(prefix='/api')
processor = DataProcessor.get_instance()


@router.get('/overview', tags=['数据总览'], summary='数据总览', description='返回价格/种子/供应商数据总量、品种数、省份数、市场数等概览信息')
def overview():
    """数据总览"""
    return processor.get_overview()


@router.get('/price/province', tags=['价格分析'], summary='各省份平均价格', description='返回各省份蔬菜平均批发价格，用于地图展示')
def price_by_province():
    """各省份平均价格"""
    return processor.get_price_by_province()


@router.get('/price/vegetable', tags=['价格分析'], summary='各品种平均价格', description='返回平均价格最高的 N 个品种')
def price_by_vegetable(top_n: int = 20):
    """各品种平均价格"""
    return processor.get_price_by_vegetable(top_n)


@router.get('/price/trend', tags=['价格分析'], summary='价格趋势', description='返回每日平均价格时间序列，支持按品种和日期范围筛选')
def price_trend(
    vegetable_type: Optional[str] = Query(None, description='品种'),
    date_from: Optional[str] = Query(None, description='起始日期 YYYY-MM-DD'),
    date_to: Optional[str] = Query(None, description='结束日期 YYYY-MM-DD'),
):
    """价格趋势"""
    return processor.get_price_trend(vegetable_type, date_from, date_to)


@router.get('/price/scatter/{vegetable_type}')
def price_scatter(vegetable_type: str):
    """某品种价格散点"""
    return processor.get_price_scatter(vegetable_type)


@router.get('/seed/distribution', tags=['种子与供应商'], summary='种子供应省份分布', description='返回各省份种子供应数量')
def seed_distribution():
    """种子供应省份分布"""
    return processor.get_seed_distribution()


@router.get('/seed/category')
def seed_category(top_n: int = 20):
    """种子品种分布"""
    return processor.get_seed_by_category(top_n)


@router.get('/supplier/distribution', tags=['种子与供应商'], summary='供应商省份分布', description='返回各省份供应商数量')
def supplier_distribution():
    """供应商省份分布"""
    return processor.get_supplier_distribution()


@router.get('/supplier/category')
def supplier_category(top_n: int = 20):
    """供应商品种分布"""
    return processor.get_supplier_by_category(top_n)


@router.get('/vegetable/score')
def vegetable_score(top_n: int = 15):
    """品种综合评分"""
    return processor.get_vegetable_score(top_n)


@router.get('/price/volatility', tags=['价格分析'], summary='价格波动排行', description='返回变异系数最高的 N 个品种，反映价格稳定性')
def price_volatility(
    top_n: int = 15,
    date_from: Optional[str] = Query(None, description='起始日期 YYYY-MM-DD'),
    date_to: Optional[str] = Query(None, description='结束日期 YYYY-MM-DD'),
):
    """价格波动排行"""
    return processor.get_price_volatility(top_n, date_from, date_to)


@router.get('/market/ranking')
def market_ranking(
    top_n: int = 15,
    date_from: Optional[str] = Query(None, description='起始日期 YYYY-MM-DD'),
    date_to: Optional[str] = Query(None, description='结束日期 YYYY-MM-DD'),
):
    """批发市场交易量排行"""
    return processor.get_market_ranking(top_n, date_from, date_to)


@router.get('/province/radar')
def province_radar(top_n: int = 8):
    """省份产业综合雷达图"""
    return processor.get_province_radar(top_n)


@router.get('/supplier/type')
def supplier_type():
    """供应商企业类型分布"""
    return processor.get_supplier_type_distribution()


@router.get('/price/heatmap', tags=['价格分析'], summary='品种-省份价格热力矩阵', description='返回品种×省份的平均价格矩阵，用于热力图展示')
def price_heatmap(top_vegs: int = 12, top_provs: Optional[int] = Query(None, description='Top N省份，不传则全部')):
    """品种-省份价格热力矩阵"""
    return processor.get_vegetable_province_heatmap(top_vegs, top_provs)


@router.get('/search/prices')
def search_prices(
    province: Optional[str] = Query(None, description='省份'),
    category: Optional[str] = Query(None, description='品种'),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """搜索价格数据"""
    return processor.search_prices(province, category, page, page_size)


@router.get('/search/seeds')
def search_seeds(
    keyword: Optional[str] = Query(None, description='关键词'),
    province: Optional[str] = Query(None, description='省份'),
    category: Optional[str] = Query(None, description='品种'),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """搜索种子数据"""
    return processor.search_seeds(keyword, province, category, page, page_size)


@router.get('/search/suppliers')
def search_suppliers(
    keyword: Optional[str] = Query(None, description='关键词'),
    province: Optional[str] = Query(None, description='省份'),
    category: Optional[str] = Query(None, description='品种'),
    ent_type: Optional[str] = Query(None, description='企业类型'),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """搜索供应商数据"""
    return processor.search_suppliers(keyword, province, category, ent_type, page, page_size)


@router.get('/seed/categories_by_province')
def seed_categories_by_province(province: str):
    """某省份种子品种分布"""
    return processor.get_seed_categories_by_province(province)


@router.get('/supplier/categories_by_province')
def supplier_categories_by_province(province: str):
    """某省份供应商品种分布"""
    return processor.get_supplier_categories_by_province(province)


@router.get('/vegetable/types')
def vegetable_types():
    """所有蔬菜品种列表"""
    return processor.get_vegetable_types()


@router.get('/province/list')
def province_list():
    """所有省份列表"""
    return processor.get_province_list()


@router.get('/province/compare')
def province_compare(provinces: str = Query(..., description='逗号分隔的省份名')):
    """多省份对比"""
    prov_list = [p.strip() for p in provinces.split(',') if p.strip()]
    return processor.get_province_comparison(prov_list[:5])


@router.get('/variety/compare', tags=['对比分析'], summary='多品种对比', description='最多 6 个品种横向对比均价、波动率、种子数、供应商数等指标')
def variety_compare(varieties: str = Query(..., description='逗号分隔的品种名')):
    """多品种对比"""
    veg_list = [v.strip() for v in varieties.split(',') if v.strip()]
    return processor.get_variety_comparison(veg_list[:6])


@router.post('/analysis/time-compare', tags=['对比分析'], summary='时间段对比', description='同一品种在不同时间段的均价、极值、记录数对比')
async def time_period_compare(request: Request):
    """时间段对比"""
    body = await request.json()
    veg = body.get('vegetable_type')
    periods = body.get('periods', [])
    return processor.get_time_period_comparison(veg, periods)


@router.post('/data/refresh', tags=['系统管理'], summary='刷新数据', description='手动触发数据重新加载，清空缓存')
def data_refresh():
    """手动触发数据重新加载"""
    processor.reload_data()
    return {'status': 'ok'}


@router.get('/export/csv')
def export_csv(
    type: str = Query('seeds', description='seeds 或 suppliers'),
    keyword: Optional[str] = None,
    province: Optional[str] = None,
    category: Optional[str] = None,
    ent_type: Optional[str] = None,
):
    """导出搜索结果CSV"""
    csv_text = processor.export_search_csv(type, keyword, province, category, ent_type)
    buf = io.BytesIO()
    buf.write(b'\xef\xbb\xbf')  # UTF-8 BOM for Excel
    buf.write(csv_text.encode('utf-8'))
    buf.seek(0)
    filename = f'{type}_export.csv'
    return StreamingResponse(buf, media_type='text/csv; charset=utf-8',
                             headers={'Content-Disposition': f'attachment; filename="{filename}"'})


# =============================================================
# 跨表关联分析 & MLlib 价格预测
# =============================================================


@router.get('/analysis/cross-table', tags=['大数据深度分析'], summary='品种-省份综合画像', description='三表 JOIN 跨表关联分析，返回品种×省份的均价、种子数、供应商数等综合指标')
def cross_table_profile(
    vegetable: Optional[str] = Query(None, description='品种名称'),
    province: Optional[str] = Query(None, description='省份名称'),
    top_n: int = Query(50, ge=1, le=500),
):
    """品种-省份综合画像（跨表三表关联分析）"""
    return processor.get_cross_table_profile(vegetable, province, top_n)


@router.get('/analysis/cross-table/summary')
def cross_table_summary():
    """跨表关联分析汇总（各品种供应链成熟度分布）"""
    return processor.get_cross_table_summary()


@router.get('/analysis/data-quality')
def data_quality():
    """数据质量报告（各品种记录数与完整性）"""
    return processor.get_data_quality()


@router.get('/price/prediction', tags=['大数据深度分析'], summary='价格预测', description='Spark MLlib 随机森林价格预测结果，支持 7/14/30/60 天窗口')
def price_prediction(
    window: int = Query(30, description='预测时间窗口（天）', enum=[7, 14, 30, 60]),
):
    """价格预测结果（未来 N 天各品种预测价格）"""
    return processor.get_price_prediction(window)


@router.get('/analysis/seasonal')
def seasonal_analysis(top_n: int = Query(8, ge=1, le=20)):
    """季节性分析：各品种 12 个月价格趋势 + 季节性指数"""
    return processor.get_seasonal_analysis(top_n)


@router.get('/price/alerts')
def price_alerts():
    """价格预警：检测近期价格异常波动品种（红色>30%、黄色>20%）"""
    return processor.get_price_alerts()


@router.get('/analysis/supply-demand')
def supply_demand_analysis():
    """供需关联分析：种子数/供应商数与价格波动率的相关性"""
    return processor.get_supply_demand_correlation()


@router.get('/analysis/supply-chain-health')
def supply_chain_health():
    """供应链健康指数：品种综合评分 + 等级 + 模拟预测"""
    return processor.get_supply_chain_health()


@router.get('/province/{province}')
def province_panorama(province: str):
    """省份全景"""
    return processor.get_province_panorama(province)
