import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
import json
from datetime import datetime

from config.categories import (
    get_seed_categories,
    HEADERS,
    RAW_DATA_DIR,
    SEED_BASE_DOMAIN
)

DATA_DIR = os.path.join(RAW_DATA_DIR, 'seeds')
PROGRESS_FILE = os.path.join(DATA_DIR, 'seeds_progress.json')
OUTPUT_FILE = os.path.join(DATA_DIR, 'seeds_all.csv')

CATEGORIES = get_seed_categories()

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_progress(progress):
    progress['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def print_flush(msg):
    print(msg, flush=True)


def get_pagination_info_from_home(base_url):
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return None, 1

        content = resp.content.decode('utf-8')
        soup = BeautifulSoup(content, 'html.parser')

        all_links = soup.find_all('a')
        page_links = [a for a in all_links if a.get('href') and re.search(r'/p\d+\.html', a.get('href', ''))]

        if page_links:
            first_link = page_links[0].get('href', '')
            match = re.search(r'(.+/)p\d+\.html', first_link)
            page_template = None
            if match:
                page_template = match.group(1)

            max_page = 1
            for link in page_links:
                href = link.get('href', '')
                match = re.search(r'/p(\d+)\.html', href)
                if match:
                    page_num = int(match.group(1))
                    if page_num > max_page:
                        max_page = page_num

            return page_template, max_page

        return None, 1

    except Exception as e:
        return None, 1


def build_page_url(base_url, page_template, page_num):
    if page_template:
        if page_template.startswith('http'):
            return f"{page_template}p{page_num}.html"
        elif page_template.startswith('/'):
            match = re.match(r'https?://[^/]+', base_url)
            if match:
                domain = match.group(0)
                return f"{domain}{page_template}p{page_num}.html"
        else:
            return f"{base_url.rstrip('/')}/{page_template}p{page_num}.html"
    return f"{base_url.rstrip('/')}/p{page_num}.html"


def scrape_page(url, category):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []

        content = resp.content.decode('utf-8')
        soup = BeautifulSoup(content, 'html.parser')

        tables = soup.find_all('table')
        data_list = []

        for table in tables:
            rows = table.find_all('tr')

            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    cell1 = cells[1]
                    cell2 = cells[2]

                    p_tags = cell1.find_all('p')
                    if len(p_tags) >= 3:
                        product_link = p_tags[0].find('a')
                        region_text = p_tags[1].get_text(strip=True)

                        product_name = product_link.get_text(strip=True) if product_link else ''
                        product_url = ''
                        if product_link and 'href' in product_link.attrs:
                            href = product_link['href']
                            if href.startswith('http'):
                                product_url = href
                            else:
                                domain = category['domain']
                                if href.startswith('/'):
                                    if domain == 'm':
                                        product_url = f'http://m.{SEED_BASE_DOMAIN}{href}'
                                    else:
                                        product_url = f'http://{domain}.{SEED_BASE_DOMAIN}{href}'
                                else:
                                    if domain == 'm':
                                        product_url = f'http://m.{SEED_BASE_DOMAIN}/{href}'
                                    else:
                                        product_url = f'http://{domain}.{SEED_BASE_DOMAIN}/{href}'

                        region = region_text.replace('供应地区：', '').strip() if region_text else ''

                        company_link = p_tags[2].find('a')
                        company_name = p_tags[2].get_text(strip=True).replace('企业：', '').strip()
                        company_url = company_link['href'] if company_link and 'href' in company_link.attrs else ''

                        # 点击量：优先从 <strong> 标签提取，避免和日期数字粘连
                        strong_tags = cell2.find_all('strong')
                        click_count = ''
                        if strong_tags:
                            # 页面有“评论数”和“点击量”两个 strong，取最后一个
                            click_count = strong_tags[-1].get_text(strip=True)
                        else:
                            click_match = re.search(r'点击量[:：]\s*(\d+)', cell2.get_text())
                            if click_match:
                                click_count = click_match.group(1)

                        # 日期：单独正则匹配
                        cell2_text = cell2.get_text()
                        date_match = re.search(r'(\d{4}/\d{1,2}/\d{1,2}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?)',
                                               cell2_text)
                        date = date_match.group(1) if date_match else ''

                        if product_name:
                            data_list.append({
                                '产品名称': product_name,
                                '供应地区': region,
                                '企业名称': company_name,
                                '点击量': click_count,
                                '日期': date,
                                '产品链接': product_url,
                                '企业链接': company_url
                            })

        return data_list

    except Exception as e:
        return []


def scrape_category(category, progress):
    name = category['name']
    base_url = category['url']

    print_flush(f"\n{'=' * 60}")
    print_flush(f"开始爬取: {name}")
    print_flush(f"{'=' * 60}")

    category_progress = progress.get(name, {})

    if category_progress.get('completed'):
        print_flush(f"{name} 已完成，跳过...")
        return

    page_template, total_pages = get_pagination_info_from_home(base_url)
    print_flush(f"共 {total_pages} 页")
    if page_template:
        print_flush(f"分页模板: {page_template}")

    start_page = category_progress.get('last_page', 0) + 1

    if start_page > 1:
        print_flush(f"断点续爬: 从第 {start_page} 页开始")
    else:
        print_flush(f"全新爬取: 从第 1 页开始")

    need_header = not os.path.exists(OUTPUT_FILE)
    total_count = 0

    for page_num in range(start_page, total_pages + 1):
        if page_num == 1:
            page_url = base_url
        else:
            page_url = build_page_url(base_url, page_template, page_num)

        print_flush(f"\n正在爬取第 {page_num}/{total_pages} 页")

        page_data = scrape_page(page_url, category)

        if page_data:
            for item in page_data:
                item['分类'] = name

            df = pd.DataFrame(page_data)
            df.to_csv(OUTPUT_FILE, index=False, mode='a', header=need_header, encoding='utf-8-sig')
            need_header = False

            total_count += len(page_data)
            print_flush(f"  获取到 {len(page_data)} 条数据（累计: {total_count}）")

            progress[name] = {
                'last_page': page_num,
                'total_pages': total_pages,
                'completed': False
            }
            save_progress(progress)
        else:
            print_flush(f"  未获取到数据")

        sys.stdout.flush()

        if page_num < total_pages:
            delay = random.randint(2, 4)
            print_flush(f"  等待 {delay} 秒...")
            time.sleep(delay)

    progress[name] = {
        'last_page': total_pages,
        'total_pages': total_pages,
        'completed': True,
        'total_data': total_count
    }
    save_progress(progress)

    print_flush(f"\n【{name}】爬取完成，共 {total_count} 条数据")


def scrape_all_categories():
    progress = load_progress()

    print_flush("=" * 60)
    print_flush("种子供应数据爬取器")
    print_flush(f"共 {len(CATEGORIES)} 个分类")
    print_flush("=" * 60)

    if progress:
        completed = sum(1 for k, v in progress.items() if isinstance(v, dict) and v.get('completed'))
        print_flush(f"\n检测到历史进度: 已完成 {completed}/{len(CATEGORIES)} 个分类")

    for i, category in enumerate(CATEGORIES, 1):
        name = category['name']

        cat_progress = progress.get(name, {})
        if cat_progress.get('completed'):
            print_flush(f"\n\n[{i}/{len(CATEGORIES)}] {name} 已完成，跳过")
            continue

        print_flush(f"\n\n进度: {i}/{len(CATEGORIES)}")

        try:
            scrape_category(category, progress)
        except Exception as e:
            print_flush(f"\n错误: {name} 爬取失败 - {e}")

        if i < len(CATEGORIES):
            delay = random.randint(5, 10)
            print_flush(f"\n分类间等待 {delay} 秒...")
            time.sleep(delay)

    print_flush("\n" + "=" * 60)
    print_flush("全部爬取完成！")
    print_flush(f"数据已保存到: {OUTPUT_FILE}")
    print_flush("=" * 60)


def show_progress():
    progress = load_progress()

    print_flush("\n" + "=" * 60)
    print_flush("种子爬取进度")
    print_flush("=" * 60)

    if 'updated_at' in progress:
        print_flush(f"\n最后更新: {progress['updated_at']}")

    completed = 0
    for category in CATEGORIES:
        name = category['name']
        cat_prog = progress.get(name, {})
        if cat_prog.get('completed'):
            completed += 1
            print_flush(f"  [完成] {name}: {cat_prog.get('total_data', 0)} 条")
        else:
            last_page = cat_prog.get('last_page', 0)
            total = cat_prog.get('total_pages', '?')
            print_flush(f"  [进行中] {name}: 第 {last_page}/{total} 页")

    print_flush(f"\n进度: {completed}/{len(CATEGORIES)} 个分类已完成")


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--status':
        show_progress()
    else:
        scrape_all_categories()
