from parsers.parser_funcs import headers, proxies, truncate_string, session_get
import requests
from bs4 import BeautifulSoup
import json
import json5
from datetime import datetime, timedelta
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
requests.packages.urllib3.disable_warnings()


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


def merchant_parser(merchant_id, merchant_data, parse_date):

    merchant_url = f'https://kaspi.kz/shop/info/merchant/{merchant_id}/address-tab/?merchantId={merchant_id}'

    # response = requests.get(merchant_url, headers=headers, verify=False, proxies=proxies)
    response = session_get(session, merchant_url)

    if response == "Max_retries":
        print(f"Skipping merchant {merchant_id} due to repeated failures.")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

    script_content = soup.find_all('script')[12].string.replace('BACKEND.components.merchant = ', '')


    catalog_json = json5.loads(script_content)

    merchant_data.append({
        "merchant_id": truncate_string(catalog_json.get("uid", None), 50),
        "name": truncate_string(catalog_json.get("name", None), 100),
        "phone": truncate_string(catalog_json.get("phone", None), 20),
        "create_date": truncate_string(catalog_json.get("create", None), 50),
        "salesCount": catalog_json.get("salesCount", None),
        "rating": catalog_json.get("rating", None),
        "ratingCount": catalog_json.get("summary", {}).get("ratingCount", None),
        "reviewsCount": catalog_json.get("summary", {}).get("reviewsCount", None),
        "parse_date": parse_date
    })

    return True


def kaspi_start_detailed(gp_conn_name):

    csv_filename = 'debug/kaspi_detailed.csv'

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csvfile.truncate()

    parse_date = datetime.now().strftime('%Y-%m-%d')

    rows = []

    with open('debug\kaspi.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter='|')
        for row in reader:
            data.append(row)

    number_of_ids = len(rows)
    print(f'Уникальных id {len(rows)}')

    merchant_data = []
    count = 1
    batch_size = 1000

    for row in rows:
        merchant_id = row[0]
        
        print(f'{count}/{number_of_ids}: {merchant_id}')
        merchant_parser(merchant_id, merchant_data, parse_date)
        count += 1

        if len(merchant_data) == batch_size:
            save_to_csv(merchant_data, csv_filename)
            print(f"✅ Данные сохранены в {csv_filename}")
            merchant_data.clear()

    if merchant_data:
        save_to_csv(merchant_data, csv_filename)
        print(f"✅ Данные сохранены в {csv_filename}")


if __name__ == '__main__':
    kaspi_start()

# 15769 секунд на 22177 записей