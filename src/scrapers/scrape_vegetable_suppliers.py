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

from config.categories import HEADERS, RAW_DATA_DIR, \
    get_supplier_categories, SUPPLIER_BASE_DOMAIN

DATA_DIR = os.path.join(RAW_DATA_DIR, 'suppliers')
PROGRESS_FILE = os.path.join(DATA_DIR, 'suppliers_progress.json')
OUTPUT_FILE = os.path.join(DATA_DIR, 'suppliers_all.csv')

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

SHUCAI_CATEGORIES = get_supplier_categories()


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


def get_sub_categories(base_url):
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []

        content = resp.content.decode('utf-8')
        soup = BeautifulSoup(content, 'html.parser')

        all_links = soup.find_all('a', href=True)
        sub_categories = set()

        for a in all_links:
            href = a.get('href', '')
            if '/shucai/' in href:
                match = re.search(r'/shucai/([^/]+)/', href)
                if match:
                    sub_cat = match.group(1)
                    if sub_cat not in ['p', 'buy']:
                        sub_categories.add(sub_cat)

        return sorted(list(sub_categories))
    except Exception as e:
        print_flush(f'  获取子分类失败: {e}')
        return []


def get_pagination_info(base_url, sub_cat_url):
    try:
        if sub_cat_url.startswith('/'):
            url = f'{base_url.rstrip("/")}{sub_cat_url}/'
        else:
            url = f'{base_url.rstrip("/")}/{sub_cat_url}/'

        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return None, 1, url

        content = resp.content.decode('utf-8')
        soup = BeautifulSoup(content, 'html.parser')

        all_links = soup.find_all('a')

        page_links = []
        for a in all_links:
            href = a.get('href', '')
            if '/p' in href and re.search(r'/p\d+\.html$', href):
                page_links.append(a)

        if page_links:
            first_link = page_links[0].get('href', '')

            match = re.search(r'(.+)/p\d+\.html', first_link)
            page_template = None
            if match:
                page_template = match.group(1) + '/'

            if not page_template:
                match = re.search(r'(.+/)p\d+\.html', first_link)
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

            return page_template, max_page, url

        return None, 1, url

    except Exception as e:
        return None, 1, base_url


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

        tables = soup.find_all('table', class_='m_t_5')

        data_list = []

        for table in tables:
            rows = table.find_all('tr')
            if len(rows) < 1:
                continue

            row = rows[0]
            cells = row.find_all('td')
            if len(cells) < 2:
                continue

            cell1 = cells[1]

            product_link = cell1.find('a', href=True)
            if not product_link:
                continue

            href = product_link.get('href')
            if '/sell/' not in href:
                continue

            product_name = product_link.get_text(strip=True)
            if not product_name:
                continue

            location = ''
            contact = ''
            contact_type = ''  # "个人" 或 "企业"
            click_count = ''
            date = ''

            # 从 <p> 标签提取产地、联系人和企业信息
            # 联系人格式: <p>个人：<a title="xxx">xxx</a></p>
            # 企业格式:   <p>企业：<a title="xxx">xxx</a></p>
            all_p = cell1.find_all('p')
            for p in all_p:
                p_text = p.get_text(strip=True)

                # 提取产地
                if '产地' in p_text:
                    location_match = re.search(r'产地[:：]\s*([^<\n]+)', p_text)
                    if location_match:
                        location = location_match.group(1).strip()

                # 提取联系人（个人）
                if '个人' in p_text and not contact:
                    a_tag = p.find('a', title=True)
                    if a_tag:
                        contact = a_tag.get('title', '').strip()
                        contact_type = '个人'

                # 提取联系人（企业）
                if '企业' in p_text and not contact:
                    a_tag = p.find('a', title=True)
                    if a_tag:
                        contact = a_tag.get('title', '').strip()
                        contact_type = '企业'

            # 从第3列提取点击量和日期
            if len(cells) >= 3:
                cell2 = cells[2]
                # 点击量：从 <strong> 标签提取，或从"点击量：xxx"文本提取
                strong_tag = cell2.find('strong')
                if strong_tag:
                    click_count = strong_tag.get_text(strip=True)
                else:
                    click_match = re.search(r'点击量[:：]\s*(\d+)', cell2.get_text())
                    if click_match:
                        click_count = click_match.group(1)
                # 日期：匹配 yyyy/m/d 格式
                date_match = re.search(r'(\d{4}/\d{1,2}/\d{1,2}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?)', cell2.get_text())
                if date_match:
                    date = date_match.group(1)

            if not href.startswith('http'):
                domain = category['domain']
                if href.startswith('/'):
                    if domain == 'm':
                        product_url = f'http://m.{SUPPLIER_BASE_DOMAIN}{href}'
                    else:
                        product_url = f'http://{domain}.{SUPPLIER_BASE_DOMAIN}{href}'
                else:
                    if domain == 'm':
                        product_url = f'http://m.{SUPPLIER_BASE_DOMAIN}/{href}'
                    else:
                        product_url = f'http://{domain}.{SUPPLIER_BASE_DOMAIN}/{href}'
            else:
                product_url = href

            data_list.append({
                '产品名称': product_name,
                '产地': location,
                '联系人': contact,
                '企业类型': contact_type,
                '点击量': click_count,
                '日期': date,
                '产品链接': product_url
            })

        return data_list

    except Exception as e:
        return []


def scrape_sub_category(category, sub_cat, progress):
    name = category['name']
    base_url = category['url']

    page_template, total_pages, sub_cat_base_url = get_pagination_info(base_url, sub_cat)

    sub_cat_progress = progress.get(name, {}).get('sub_categories', {}).get(sub_cat, {})

    if sub_cat_progress.get('completed'):
        print_flush(f"    [{sub_cat}] 已完成，跳过...")
        return 0

    start_page = sub_cat_progress.get('last_page', 0) + 1

    all_data = []
    need_header = not os.path.exists(OUTPUT_FILE)

    for page_num in range(start_page, total_pages + 1):
        if page_num == 1:
            page_url = sub_cat_base_url
        else:
            page_url = build_page_url(base_url, page_template, page_num)

        print_flush(f"    正在爬取第 {page_num}/{total_pages} 页: {page_url}")

        page_data = scrape_page(page_url, category)

        if page_data:
            for item in page_data:
                item['分类'] = name
                item['子分类'] = sub_cat

            df = pd.DataFrame(page_data)
            df.to_csv(OUTPUT_FILE, index=False, mode='a', header=need_header, encoding='utf-8-sig')
            need_header = False

            all_data.extend(page_data)
            print_flush(f"      获取到 {len(page_data)} 条数据（累计: {len(all_data)}）")

            if name not in progress:
                progress[name] = {}
            if 'sub_categories' not in progress[name]:
                progress[name]['sub_categories'] = {}
            progress[name]['sub_categories'][sub_cat] = {
                'last_page': page_num,
                'total_pages': total_pages,
                'completed': False
            }
            save_progress(progress)
        else:
            print_flush(f"      未获取到数据")

        sys.stdout.flush()

        if page_num < total_pages:
            delay = random.randint(1, 2)
            time.sleep(delay)

    if name not in progress:
        progress[name] = {}
    if 'sub_categories' not in progress[name]:
        progress[name]['sub_categories'] = {}
    progress[name]['sub_categories'][sub_cat] = {
        'last_page': total_pages,
        'total_pages': total_pages,
        'completed': True,
        'total_data': len(all_data)
    }
    save_progress(progress)

    print_flush(f"    [{sub_cat}] 爬取完成，共 {len(all_data)} 条数据")
    return len(all_data)


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

    sub_categories = get_sub_categories(base_url)
    print_flush(f"发现 {len(sub_categories)} 个子分类: {sub_categories}")

    total_data = 0
    completed_sub_cats = 0

    for i, sub_cat in enumerate(sub_categories, 1):
        print_flush(f"\n  子分类进度: {i}/{len(sub_categories)} - [{sub_cat}]")

        try:
            data_count = scrape_sub_category(category, sub_cat, progress)
            total_data += data_count

            sub_cat_progress = progress.get(name, {}).get('sub_categories', {}).get(sub_cat, {})
            if sub_cat_progress.get('completed'):
                completed_sub_cats += 1
        except Exception as e:
            print_flush(f"  错误: [{sub_cat}] 爬取失败 - {e}")

        if i < len(sub_categories):
            delay = random.randint(2, 4)
            print_flush(f"  子分类间等待 {delay} 秒...")
            time.sleep(delay)

    if completed_sub_cats == len(sub_categories):
        progress[name]['completed'] = True
        progress[name]['total_data'] = total_data
        save_progress(progress)

    print_flush(f"\n【{name}】爬取完成，共 {total_data} 条数据")


def scrape_all():
    progress = load_progress()

    print_flush("=" * 60)
    print_flush("蔬菜供应商数据爬取器（子分类版）")
    print_flush(f"共 {len(SHUCAI_CATEGORIES)} 个分类")
    print_flush("=" * 60)

    if progress:
        completed = sum(1 for k, v in progress.items() if isinstance(v, dict) and v.get('completed'))
        print_flush(f"\n检测到历史进度: 已完成 {completed}/{len(SHUCAI_CATEGORIES)} 个分类")

    for i, category in enumerate(SHUCAI_CATEGORIES, 1):
        name = category['name']

        cat_progress = progress.get(name, {})
        if cat_progress.get('completed'):
            print_flush(f"\n\n[{i}/{len(SHUCAI_CATEGORIES)}] {name} 已完成，跳过")
            continue

        print_flush(f"\n\n进度: {i}/{len(SHUCAI_CATEGORIES)}")

        try:
            scrape_category(category, progress)
        except Exception as e:
            print_flush(f"\n错误: {name} 爬取失败 - {e}")

        if i < len(SHUCAI_CATEGORIES):
            delay = random.randint(3, 6)
            print_flush(f"\n分类间等待 {delay} 秒...")
            time.sleep(delay)

    print_flush("\n" + "=" * 60)
    print_flush("全部爬取完成！")
    print_flush(f"数据已保存到: {OUTPUT_FILE}")
    print_flush("=" * 60)


def show_progress():
    progress = load_progress()

    print_flush("\n" + "=" * 60)
    print_flush("蔬菜供应商爬取进度")
    print_flush("=" * 60)

    if 'updated_at' in progress:
        print_flush(f"\n最后更新: {progress['updated_at']}")

    completed = 0
    for category in SHUCAI_CATEGORIES:
        name = category['name']
        cat_prog = progress.get(name, {})

        sub_cats = cat_prog.get('sub_categories', {})
        total_sub_cats = len(sub_cats)
        completed_sub_cats = sum(1 for sc in sub_cats.values() if sc.get('completed'))

        if cat_prog.get('completed'):
            completed += 1
            print_flush(f"  [完成] {name}: {cat_prog.get('total_data', 0)} 条")
        else:
            if total_sub_cats > 0:
                print_flush(f"  [进行中] {name}: {completed_sub_cats}/{total_sub_cats} 个子分类完成")
            else:
                print_flush(f"  [未开始] {name}")

    print_flush(f"\n进度: {completed}/{len(SHUCAI_CATEGORIES)} 个分类已完成")


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--status':
        show_progress()
    else:
        scrape_all()
