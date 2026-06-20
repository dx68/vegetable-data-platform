import sys
import os
import csv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

import random
import time
import re
import json
import requests
from io import StringIO
from datetime import datetime
from bs4 import BeautifulSoup

from config.categories import (
    PRICE_MOBILE_TYPES,
    PRICE_ALL_PATH_TYPES,
    PRICE_CATEGORY_PATH_MAP,
    PRICE_NATIONAL_ONLY_TYPES,
    PRICE_SUBDOMAIN_MAP,
    PRICE_CITIES,
    PRICE_TYPES,
    PRICE_BASE_DOMAIN,
    HEADERS,
    build_price_url,
    get_price_subdomain,
    RAW_DATA_DIR
)

DATA_DIR = os.path.join(RAW_DATA_DIR, 'prices')
PROGRESS_FILE = os.path.join(DATA_DIR, 'prices_progress.json')
OUTPUT_FILE = os.path.join(DATA_DIR, 'prices_all.csv')

CSV_HEADER = ['品种', '批发市场', '最低价', '最高价', '平均价', '日期', '省份', '爬取时间', '蔬菜类型']

COLUMN_NAMES = ['vegetable_name', 'market_name', 'min_price', 'max_price', 'avg_price', 'date',
                'province', 'timestamp', 'vegetable_type']

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('completed', {}), data.get('current_type'), data.get('current_city'), data.get(
                    'last_page')
        except Exception as e:
            print(f"加载进度文件失败：{e}")
    return {}, None, None, 0


def save_progress(completed, current_type=None, current_city=None, last_page=0):
    try:
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'completed': completed,
                'current_type': current_type,
                'current_city': current_city,
                'last_page': last_page,
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"保存进度失败：{e}")


def get_total_pages(v_type, sf):
    url = build_price_url(v_type, sf, 1)
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"获取总页数失败，状态码：{resp.status_code}")
        return 5

    try:
        content = resp.content.decode('utf-8')
        match = re.search(r'共\s*(\d+)\s*页', content)
        if match:
            return int(match.group(1))
        match = re.search(r'</b>(\d+)</b>\s*页', content)
        if match:
            return int(match.group(1))
        match = re.search(r'&nbsp;/<b>(\d+)</b>\s*页', content)
        if match:
            return int(match.group(1))
        match = re.search(r'pageCount\s*=\s*(\d+)', content)
        if match:
            return int(match.group(1))
        pager_match = re.search(r'<div align="center" id="pager".*?</div>', content, re.DOTALL)
        if pager_match:
            pager_html = pager_match.group(0)
            page_links = re.findall(r'/p(\d+)\.html', pager_html)
            if page_links:
                max_page = max(int(p) for p in page_links)
                return max_page
    except Exception as e:
        print(f"解析总页数失败：{e}")

    return 5


def clean_text(text):
    text = str(text)
    text = text.replace('"', '').replace("'", '')
    text = text.replace('\n', ' ').replace('\r', ' ')
    text = text.replace('\t', ' ')
    text = text.replace('\r\n', ' ')
    text = re.sub(r'\\n', ' ', text)
    text = re.sub(r'\\r', ' ', text)
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
    text = re.sub(r'[\s]+', ' ', text)
    text = text.strip()
    text = re.sub(r',', '，', text)
    return text


def write_csv_row(row_data, output_file, header=None):
    file_exists = os.path.exists(output_file)
    mode = 'a' if file_exists else 'w'

    with open(output_file, mode, encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)

        if not file_exists and header:
            writer.writerow(header)

        writer.writerow(row_data)


def get_data(v_type, sf, page, url_override=None):
    url = url_override if url_override else build_price_url(v_type, sf, page)
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"访问失败，状态码：{resp.status_code}")
        return 0

    try:
        content = resp.content.decode('utf-8', errors='replace')

        if '共找到0条' in content:
            print(f"无数据：{v_type} - {sf}")
            return 0

        soup = BeautifulSoup(content, 'html.parser')
        all_tables = soup.find_all('table')

        price_table = None

        for idx, table in enumerate(all_tables):
            rows = table.find_all('tr')
            if len(rows) >= 5:
                valid_row_count = 0
                for row in rows[:5]:
                    cells = row.find_all('td')
                    if len(cells) == 6:
                        valid_row_count += 1

                if valid_row_count >= 3:
                    first_row = rows[0]
                    cells = first_row.find_all('td')
                    if len(cells) == 6:
                        header_text = ''.join([c.get_text(strip=True)[:10] for c in cells])
                        print(f"找到价格表，位于表格{idx}，共{len(rows)}行")
                        price_table = table
                        break

        if price_table is None:
            for idx, table in enumerate(all_tables):
                rows = table.find_all('tr')
                if len(rows) >= 2:
                    first_row = rows[0]
                    cells = first_row.find_all('td')
                    if len(cells) >= 3:
                        header_text = ''.join([c.get_text(strip=True) for c in cells])
                        if '品种' in header_text and '批发市场' in header_text and '价格' in header_text:
                            price_table = table
                            print(f"找到价格表（备选），位于表格{idx}，共{len(rows)}行")
                            break

        if price_table is None:
            print(f"未找到价格表：{v_type} - {sf}")
            return 0

        rows = price_table.find_all('tr')
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_count = 0

        for row_idx, row in enumerate(rows):
            cells = row.find_all('td')
            print(f"第{row_idx}行，列数：{len(cells)}")
            for idx, cell in enumerate(cells):
                print(f"  cells[{idx}]: {repr(cell.get_text(strip=True)[:30])}")

            if len(cells) >= 6:
                first_cell = cells[0].get_text(strip=True)

                if first_cell == '品种' or '价格' in first_cell or not first_cell:
                    continue

                if first_cell in ['发布时间', '日期', '时间']:
                    continue

                raw_veg_name = cells[0].get_text()

                if len(raw_veg_name) > 200 or raw_veg_name.count('菠菜') > 1 or raw_veg_name.count('￥') > 1:
                    continue

                veg_name = clean_text(raw_veg_name)

                market_cell = cells[1]
                market_name = ""

                # 递归查找所有a标签，而不是只找直接子节点
                a_tags = market_cell.find_all('a', recursive=True)
                for a_tag in a_tags:
                    if a_tag.has_attr('title'):
                        raw_title = a_tag['title'].strip()
                        if raw_title:
                            market_name = clean_text(raw_title)
                            print(f"成功从title获取完整市场名：{market_name}")
                            break

                # 如果没有获取到有效的title，再取单元格文本
                if not market_name:
                    raw_text = market_cell.get_text(strip=True)
                    market_name = clean_text(raw_text)
                    print(f"警告：未获取到title，使用截断文本：{market_name}")

                min_price = clean_text(cells[2].get_text())
                max_price = clean_text(cells[3].get_text())
                avg_price = clean_text(cells[4].get_text())
                publish_date = clean_text(cells[5].get_text())

                if not veg_name or veg_name.isspace():
                    continue

                if len(veg_name) > 100:
                    continue

                if not publish_date or publish_date.isspace():
                    publish_date = datetime.now().strftime('%Y-%m-%d')

                row_data = [
                    veg_name,
                    market_name,
                    min_price,
                    max_price,
                    avg_price,
                    publish_date,
                    sf,
                    current_time,
                    v_type
                ]

                write_csv_row(row_data, OUTPUT_FILE, CSV_HEADER)
                row_count += 1

        if row_count == 0:
            print(f"价格表为空：{v_type} - {sf}")
            return 0

        return row_count

    except Exception as e:
        print(f"解析失败：{e}")
        return 0


def _build_alt_urls(v_type, city, page):
    """生成备用 URL 格式列表"""
    subdomain = get_price_subdomain(v_type)
    alts = []

    if v_type in PRICE_MOBILE_TYPES:
        # 当前是移动端，尝试: 标准端, /all/路径, 类别路径
        alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/{city}/p{page}.html')
        alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/all/{city}/p{page}.html')
        if v_type in PRICE_CATEGORY_PATH_MAP:
            cat = PRICE_CATEGORY_PATH_MAP[v_type]
            alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/{cat}/{city}/p{page}.html')
    elif v_type in PRICE_ALL_PATH_TYPES:
        # 当前是 /all/ 路径，尝试: 移动端, 标准端无/all/, 类别路径
        alts.append(f'http://m.{PRICE_BASE_DOMAIN}/{v_type}/price/{city}/p{page}.html')
        alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/{city}/p{page}.html')
        if v_type in PRICE_CATEGORY_PATH_MAP:
            cat = PRICE_CATEGORY_PATH_MAP[v_type]
            alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/{cat}/{city}/p{page}.html')
    elif v_type in PRICE_CATEGORY_PATH_MAP:
        # 当前是类别路径，尝试: 标准端无类别, /all/路径, 移动端
        alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/{city}/p{page}.html')
        alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/all/{city}/p{page}.html')
        alts.append(f'http://m.{PRICE_BASE_DOMAIN}/{v_type}/price/{city}/p{page}.html')
    else:
        # 当前是标准端，尝试: /all/路径, 类别路径, 移动端
        alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/all/{city}/p{page}.html')
        if v_type in PRICE_CATEGORY_PATH_MAP:
            cat = PRICE_CATEGORY_PATH_MAP[v_type]
            alts.append(f'http://{subdomain}.{PRICE_BASE_DOMAIN}/price/{cat}/{city}/p{page}.html')
        alts.append(f'http://m.{PRICE_BASE_DOMAIN}/{v_type}/price/{city}/p{page}.html')

    return alts


def _get_alt_total_pages(v_type, city):
    """尝试多种备用 URL 格式，返回 (总页数, 可用的备用URL生成函数)"""
    alt_urls = _build_alt_urls(v_type, city, 1)

    for idx, url in enumerate(alt_urls):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            content = resp.content.decode('utf-8')

            total = 0
            match = re.search(r'共\s*(\d+)\s*页', content)
            if match:
                total = int(match.group(1))
            if not total:
                match = re.search(r'&nbsp;/<b>(\d+)</b>\s*页', content)
                if match:
                    total = int(match.group(1))
            if not total:
                pager_match = re.search(r'<div[^>]*id="pager"[^>]*>.*?</div>', content, re.DOTALL)
                if pager_match:
                    page_links = re.findall(r'/p(\d+)\.html', pager_match.group(0))
                    if page_links:
                        total = max(int(p) for p in page_links)

            if total > 0:
                print(f"备用URL[{idx}] 可用: {url} -> {total}页")
                return total, idx
        except Exception:
            continue

    return 0, -1


def _try_alternate_format(v_type, city, original_pages):
    """当主 URL 格式获取不到数据时，尝试备用 URL 格式"""
    print(f"主 URL 无数据，尝试备用格式: {v_type} - {city}")

    alt_pages, alt_idx = _get_alt_total_pages(v_type, city)
    if alt_pages == 0:
        print(f"所有备用格式均无数据")
        return 0

    # 获取对应的备用URL列表
    all_alt_urls = _build_alt_urls(v_type, city, 1)  # 只是为确认索引
    print(f"使用备用URL[{alt_idx}]: {all_alt_urls[alt_idx]}，共 {alt_pages} 页")

    total_rows = 0
    for page in range(1, alt_pages + 1):
        try:
            time.sleep(random.randint(5, 10))
            alt_urls = _build_alt_urls(v_type, city, page)
            alt_url = alt_urls[alt_idx]
            rows = get_data(v_type, city, page, url_override=alt_url)
            total_rows += rows
            print(f"备用格式 {v_type} - {city} 第 {page}/{alt_pages} 页，获取 {rows} 条")
        except Exception as e:
            print(f"备用格式第{page}页报错：{e}")
            continue

    if total_rows > 0:
        print(f"备用格式成功: {v_type} - {city}，共 {total_rows} 条")
    return total_rows


def scrape_all():
    completed, saved_type, saved_city, saved_page = load_progress()
    resume_mode = saved_type is not None

    if resume_mode:
        print(f"检测到之前的进度，从 {saved_type} - {saved_city} 的第 {saved_page + 1} 页继续")

    for v_type in PRICE_TYPES:
        if resume_mode and v_type < saved_type:
            continue

        # 全国模式类型只爬取 zhongguo，不遍历各省份
        cities_to_scrape = ['zhongguo'] if v_type in PRICE_NATIONAL_ONLY_TYPES else PRICE_CITIES

        for city in cities_to_scrape:
            if resume_mode and v_type == saved_type and city < saved_city:
                continue

            task_key = f"{v_type}_{city}"
            if task_key in completed:
                print(f"跳过已完成: {v_type} - {city}")
                continue

            try:
                total_pages = get_total_pages(v_type, city)
                print(f"开始爬取 {v_type} - {city}，共 {total_pages} 页")

                start_page = 1
                if resume_mode and v_type == saved_type and city == saved_city:
                    start_page = saved_page + 1
                    print(f"从中断点继续，从第 {start_page} 页开始")
                    resume_mode = False

                page_success_count = 0
                total_rows = 0

                for page in range(start_page, total_pages + 1):
                    try:
                        time.sleep(random.randint(5, 10))
                        rows = get_data(v_type, city, page)
                        total_rows += rows
                        if rows > 0:
                            page_success_count += 1
                        print(f"{v_type} - {city} 第 {page}/{total_pages} 页爬取完毕，获取 {rows} 条")
                        save_progress(completed, v_type, city, page)
                    except Exception as e:
                        print(f"{v_type} - {city} 第{page}页报错：{e}")
                        continue

                # 如果获取0条数据，尝试备用URL格式
                if total_rows == 0:
                    alt_rows = _try_alternate_format(v_type, city, total_pages)
                    total_rows += alt_rows

                if total_rows > 0:
                    completed[task_key] = {'total_pages': total_pages, 'rows': total_rows,
                                           'completed_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    save_progress(completed)
                    print(f"完成: {v_type} - {city}，共 {total_rows} 条数据")
                else:
                    print(f"跳过: {v_type} - {city} (未获取到任何数据，将在下次运行时重试)")

            except Exception as e:
                print(f"获取 {v_type} - {city} 总页数失败：{e}")
                continue

    print("所有爬取任务完成！")
    save_progress(completed)
    print("进度文件已保存")


def show_progress():
    completed, saved_type, saved_city, saved_page = load_progress()

    print("\n" + "=" * 60)
    print("蔬菜报价爬取进度")
    print("=" * 60)

    total = len(PRICE_TYPES) * len(PRICE_CITIES)
    completed_count = len(completed)

    print(f"\n总任务数: {total}")
    print(f"已完成: {completed_count}")
    print(f"剩余: {total - completed_count}")

    if saved_type:
        print(f"\n最后断点位置: {saved_type} - {saved_city}, 第 {saved_page} 页")

    print(f"\n进度: {completed_count}/{total}")


def cleanup_progress():
    """清理进度文件中无数据的完成条目"""
    completed, saved_type, saved_city, saved_page = load_progress()

    if not os.path.exists(OUTPUT_FILE):
        print("CSV文件不存在，无需清理")
        return

    import pandas as pd
    df = pd.read_csv(OUTPUT_FILE, encoding='utf-8-sig')
    type_city_counts = df.groupby(['蔬菜类型', '省份']).size().to_dict()

    to_remove = []
    for key in list(completed.keys()):
        parts = key.split('_', 1)
        if len(parts) == 2:
            vtype, city = parts
            count = type_city_counts.get((vtype, city), 0)
            if count == 0:
                to_remove.append(key)

    print(f"原始完成条目: {len(completed)}")
    print(f"无数据条目: {len(to_remove)}")

    for key in to_remove:
        del completed[key]

    save_progress(completed, saved_type, saved_city, saved_page)
    print(f"清理后完成条目: {len(completed)}")
    print("进度文件已更新")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == '--status':
            show_progress()
        elif sys.argv[1] == '--cleanup':
            cleanup_progress()
        else:
            print("用法: python scrape_vegetable_prices.py [--status|--cleanup]")
    else:
        scrape_all()