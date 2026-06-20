import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
ANALYSIS_DIR = os.path.join(DATA_DIR, 'analysis')

for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, ANALYSIS_DIR]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

SEED_CATEGORY_SPECS = {
    '独立域名': [
        ('白菜', 'baicai'),
        ('芹菜', 'qincai'),
        ('菠菜', 'bocai'),
        ('莴苣', 'woju'),
        ('番茄', 'fanqie'),
        ('辣椒', 'lajiao'),
        ('茄子', 'qiezi'),
        ('甘蓝', 'ganlan'),
        ('花椰菜', 'huacai'),
        ('西瓜', 'gua'),
        ('黄瓜', 'hg'),
        ('南瓜', 'nangua'),
        ('西葫芦', 'xihulu'),
        ('冬瓜', 'donggua'),
        ('豆角', 'doujiao'),
        ('萝卜', 'luobo'),
        ('姜', 'jiang'),
        ('大葱', 'cong'),
        ('大蒜', 'suan'),
        ('土豆', 'tudou'),
    ],
    'M域名': [
        ('香菜', 'xiangcai'),
        ('芥菜', 'jiecai'),
        ('油菜', 'youcai'),
        ('茼蒿', 'tonghao'),
        ('甜椒', 'tianjiao'),
        ('西兰花', 'xilanhua'),
        ('甜瓜', 'tiangua'),
        ('苦瓜', 'kugua'),
        ('丝瓜', 'sigua'),
        ('菜豆', 'caidou'),
        ('豌豆', 'wandou'),
        ('胡萝卜', 'huluobo'),
        ('莲藕', 'lianou'),
        ('洋葱', 'yangcong'),
        ('韭菜', 'jiucai'),
        ('食用菌', 'shiyongjun'),
        ('蒜薹', 'suantai'),
        ('蒜苗', 'suanmiao'),
        ('红薯', 'hongshu'),
        ('芋头', 'yutou'),
        ('芦笋', 'lusun'),
    ],
}

PRICE_MOBILE_TYPES = {
    'xiangcai', 'jiecai', 'youcai', 'tonghao', 'tianjiao', 'tiangua',
    'yangcong', 'jiucai', 'suantai', 'suanmiao', 'caidou', 'wandou',
    'huluobo', 'lianou', 'hongshu', 'yutou', 'shiyongjun', 'lusun', 'xilanhua',
}

# 使用 /price/all/ 路径的特殊类型（桌面端）
PRICE_ALL_PATH_TYPES = {
    'lajiao',
}

# 需要在URL中包含类别路径的类型，如 /price/xigua/{city}/
PRICE_CATEGORY_PATH_MAP = {
    'xigua': 'xigua',
    'huayecai': 'huayecai',
}

# 只需爬取全国数据（zhongguo）的类型，不遍历各省份
PRICE_NATIONAL_ONLY_TYPES = {
    'xigua',
    'huayecai',
}

PRICE_SUBDOMAIN_MAP = {
    'huayecai': 'huacai',
    'xigua': 'gua',
    'huanggua': 'hg',
    'dacong': 'cong',
    'dasuan': 'suan',
}

PRICE_CITIES = [
    'gansu', 'shandong', 'shanxi', 'xinjiang', 'hubei', 'anhui', 'jiangsu',
    'hebei', 'zhejiang', 'beijing', 'sichuan', 'liaoning', 'henan', 'tianjin',
    'neimenggu', 'jiangxi', 'hunan', 'yunnan', 'guangdong', 'heilongjian',
    'ningxia', 'shan_xi','fujian', 'chongqing', 'guizhou', 'jilin', 'guangxi', 'xizang',
    'hainan', 'qinghai', 'shanghai'
]

PRICE_TYPES = [
    'baicai', 'qincai', 'bocai', 'woju', 'xiangcai', 'jiecai', 'youcai', 'tonghao',
    'fanqie', 'lajiao', 'qiezi', 'tianjiao',
    'ganlan', 'huayecai', 'xilanhua',
    'xigua', 'huanggua', 'tiangua', 'nangua', 'xihulu', 'donggua', 'kugua', 'sigua',
    'doujiao', 'caidou', 'wandou',
    'luobo', 'huluobo', 'jiang', 'lianou',
    'dacong', 'yangcong', 'jiucai', 'dasuan', 'suantai', 'suanmiao',
    'tudou', 'shanyao', 'hongshu', 'yutou',
    'shiyongjun', 'lusun'
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
}

SEED_BASE_DOMAIN = 'cnveg.com'
PRICE_BASE_DOMAIN = 'cnveg.com'
SUPPLIER_BASE_DOMAIN = 'cnveg.com'

SUPPLIER_SPECS = {
    '独立域名': [
        ('白菜', 'baicai'), ('芹菜', 'qincai'), ('菠菜', 'bocai'), ('莴苣', 'woju'),
        ('番茄', 'fanqie'), ('辣椒', 'lajiao'), ('茄子', 'qiezi'), ('甘蓝', 'ganlan'),
        ('花椰菜', 'huacai'), ('西瓜', 'gua'), ('黄瓜', 'hg'), ('南瓜', 'nangua'),
        ('西葫芦', 'xihulu'), ('冬瓜', 'donggua'), ('豆角', 'doujiao'), ('萝卜', 'luobo'),
        ('姜', 'jiang'), ('大葱', 'cong'), ('大蒜', 'suan'), ('土豆', 'tudou'),
        ('山药', 'shanyao'),
    ],
    'M域名': [
        ('香菜', 'xiangcai'), ('芥菜', 'jiecai'), ('油菜', 'youcai'), ('茼蒿', 'tonghao'),
        ('甜椒', 'tianjiao'), ('西兰花', 'xilanhua'), ('甜瓜', 'tiangua'), ('苦瓜', 'kugua'),
        ('丝瓜', 'sigua'), ('菜豆', 'caidou'), ('豌豆', 'wandou'), ('胡萝卜', 'huluobo'),
        ('莲藕', 'lianou'), ('洋葱', 'yangcong'), ('韭菜', 'jiucai'), ('食用菌', 'shiyongjun'),
        ('蒜薹', 'suantai'), ('蒜苗', 'suanmiao'), ('红薯', 'hongshu'), ('芋头', 'yutou'),
        ('芦笋', 'lusun'),
    ],
}


def get_seed_categories():
    categories = []
    
    for name, domain in SEED_CATEGORY_SPECS['独立域名']:
        categories.append({
            'name': name,
            'domain': domain,
            'url': f'http://{domain}.{SEED_BASE_DOMAIN}/zhongzi/',
        })
    
    for name, path in SEED_CATEGORY_SPECS['M域名']:
        categories.append({
            'name': name,
            'domain': 'm',
            'url': f'http://m.{SEED_BASE_DOMAIN}/{path}/zhongzi/',
        })
    
    return categories


def get_price_subdomain(v_type):
    return PRICE_SUBDOMAIN_MAP.get(v_type, v_type)


def build_price_url(v_type, city, page):
    if v_type in PRICE_MOBILE_TYPES:
        return f'http://m.{PRICE_BASE_DOMAIN}/{v_type}/price/{city}/p{page}.html'
    elif v_type in PRICE_ALL_PATH_TYPES:
        subdomain = get_price_subdomain(v_type)
        return f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/all/{city}/p{page}.html'
    elif v_type in PRICE_CATEGORY_PATH_MAP:
        subdomain = get_price_subdomain(v_type)
        cat_path = PRICE_CATEGORY_PATH_MAP[v_type]
        return f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/{cat_path}/{city}/p{page}.html'
    else:
        subdomain = get_price_subdomain(v_type)
        return f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/{city}/p{page}.html'


def get_supplier_categories():
    categories = []
    for name, domain in SUPPLIER_SPECS['独立域名']:
        categories.append({
            'name': name,
            'domain': domain,
            'url': f'http://{domain}.{SUPPLIER_BASE_DOMAIN}/shucai/',
        })
    for name, path in SUPPLIER_SPECS['M域名']:
        categories.append({
            'name': name,
            'domain': 'm',
            'path': path,
            'url': f'http://m.{SUPPLIER_BASE_DOMAIN}/{path}/shucai/',
        })
    return categories
