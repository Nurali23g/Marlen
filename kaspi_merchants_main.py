import requests
from bs4 import BeautifulSoup
import re
import json
import json5
from datetime import datetime, timedelta
# import psycopg2
import urllib.parse
from parsers.parser_funcs import headers, proxies, truncate_string, session_get #, login, password, password_dwh, password_cdc
# import cx_Oracle
requests.packages.urllib3.disable_warnings()
import csv


session = requests.Session()
session.headers.update(headers)
session.verify = False
session.proxies.update(proxies)

def save_to_csv(data, csv_filename):
    """Сохраняет данные в CSV-файл с разделителем | и без заголовков."""
    with open(csv_filename, 'a', encoding='utf-8', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter='|')
        for row in data:
            writer.writerow(row.values())


def categories_parser(category, categories_data, parse_date):
    # URL веб-сайта, который будем парсить
    url = f'https://kaspi.kz/shop/almaty/c/{category}/?q=%3AavailableInZones%3AMagnum_ZONE1%3Acategory%3A{category}&sort=relevance&sc='

    # response = requests.get(url, headers=headers, verify=False, proxies=proxies)
    response = session_get(session, url)
    soup = BeautifulSoup(response.content, 'html.parser')
    scripts = soup.find_all('script')

    target_script_content = None
    for script in scripts:
        script_content = script.get_text(strip=True)
        if script_content.startswith('BACKEND.components.catalog'):
            target_script_content = script_content
            break

    try:
        match = re.search(r'BACKEND\.components\.catalog\s*=\s*(\{.*?\})\s*;', target_script_content, re.DOTALL)

        catalog_data = match.group(1)
        # catalog_data = catalog_data.replace('filterQuery:', '"filterQuery":').replace('categoryInfo:', '"categoryInfo":')
        catalog_json = json5.loads(catalog_data) # поменять на json5
        product_list_data = catalog_json["productListData"]
        filters = product_list_data["filters"]

        # Найдите фильтр с id "allMerchants"
        all_merchants_filter = next((filter for filter in filters if filter["id"] == "allMerchants"), None)

        rows = all_merchants_filter["rows"]
        
    except Exception as e:
        error_message = f"Error processing category {category}: {str(e)}"
        # with open("kaspi_merchants/unsuccessful_categories.json", "a", encoding="utf-8") as f:
        #     f.write(json.dumps({"category": category, "error": error_message}, ensure_ascii=False, indent=4) + "\n")

        print(f"Ошибка: {error_message}")
        return None


    for row in rows:
        id_clean = row["id"].replace(":allMerchants:", "")
        category_decoded = urllib.parse.unquote(category)
        item = {
            "id": truncate_string(id_clean + '_' + category_decoded, 100),
            "merchant_id": truncate_string(id_clean, 50),
            "title": truncate_string(row["title"], 100),
            "name": truncate_string(row["name"], 100),
            "count": row["count"],
            "popularity": row["popularity"],
            "category": truncate_string(category_decoded, 50),
            "parse_date": parse_date
        }
        categories_data.append(item)

    return None


# Функция для извлечения ссылок
def extract_links(items, links):
    for item in items:
        if 'items' not in item: # Добавляем только ссылки конечных ветвей
            links.append(item.get('link').split('/')[2])
        if 'items' in item and isinstance(item['items'], list):
            extract_links(item['items'], links)


def for_categorie(cat, csv_filename):
    with open(f'home/airflowadmin/rsalimov/kaspi_merchants/{cat}.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    parse_date = datetime.now().strftime('%Y-%m-%d')
    # Извлечение ссылок
    links = []
    extract_links(data, links)

    categories_data = []

    # Запись ссылок в текстовый файл
    for link in links:
        # if link == "cpu":
        print(f'{cat} > {link}')
        categories_parser(link, categories_data, parse_date)


    save_to_csv(categories_data, csv_filename)
    print(f"✅ Данные сохранены в {csv_filename}")


def kaspi_start():
    csv_filename = 'home/airflowadmin/rsalimov/kaspi_merchants.csv'

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csvfile.truncate()

    categories = ['beauty_and_health', 'for_childrens', 'for_home', 'electronics', 'furnitures']
    # categories = ['electronics'] ###
    for cat in categories:
        print(f'Категория: {cat}')
        for_categorie(cat, csv_filename)


if __name__ == '__main__':
    paging_main()

# 60 мин за все

# def kaspi_start():
#     pass