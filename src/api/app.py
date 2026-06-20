import sys
import os
import logging

# 确保项目根目录在 sys.path 中
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from src.api.routes import router

# 模板目录
TEMPLATES_DIR = os.path.join(BASE_DIR, 'src', 'templates')
os.makedirs(TEMPLATES_DIR, exist_ok=True)

app = FastAPI(
    title='基于HDFS+Hive+Spark的全国蔬菜产业智能分析平台',
    version='1.0.0',
    description='''
## 平台简介
基于中国蔬菜网真实数据构建的蔬菜产业大数据分析平台，提供价格趋势、供应链分析、ML预测等功能。

## 数据源
- 价格数据：86万+条批发市场报价
- 种子数据：5.4万+条供应商信息
- 供应商数据：8.6万+条企业档案

## 分析能力
- Spark MLlib 随机森林价格预测
- 季节性分析与价格预警
- 供需关联分析与供应链健康评分
    ''',
    contact={'name': '蔬菜产业平台团队'},
    license_info={'name': 'MIT'},
)
app.include_router(router)

templates = Jinja2Templates(directory=TEMPLATES_DIR)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.getLogger(__name__).error(f"API error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": str(exc), "detail": "服务器内部错误，请稍后重试"}
    )


@app.get('/', response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse('index.html', {'request': request})
