from parsers.parser_funcs import headers, proxies, truncate_string, session_get
import requests
from bs4 import BeautifulSoup
import json
import json5
from datetime import datetime, timedelta
import csv
import os
import re
import sys
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

def _dump_response(html_text: str, merchant_id, save_dir: str, tag: str = "merchant_error") -> str:
    os.makedirs(save_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_id = re.sub(r"[^A-Za-z0-9_-]", "_", str(merchant_id))
    path = os.path.join(save_dir, f"{tag}_{safe_id}_{ts}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
    return path

def extract_merchant_json(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script")

    # Ищем тот <script>, где есть BACKEND.components.merchant
    target = next(
        (s for s in scripts if s.string and "BACKEND.components.merchant" in s.string),
        None
    )
    if not target or not target.string:
        raise ValueError("merchant script not found or empty")

    raw = target.string

    # Вырезаем всё, что после закрывающей фигурной скобки объекта merchant
    # 1. убираем префикс 'BACKEND.components.merchant = '
    raw = raw.split("BACKEND.components.merchant", 1)[-1]
    raw = raw.lstrip(" =")

    # 2. находим первую { и соответствующую }
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        raise ValueError("No JSON object found after merchant assignment")

    json_text = match.group(0)

    # 3. Парсим JSON
    return json5.loads(json_text)

def merchant_parser(
    merchant_id,
    merchant_data,
    parse_date,
    save_html_on_error: bool = True,
    save_dir: str = "/home/airflowadmin/rsalimov/kaspi_errors_html"
):
    merchant_url = f"https://kaspi.kz/shop/info/merchant/{merchant_id}/address-tab/?merchantId={merchant_id}"

    # response = requests.get(merchant_url, headers=headers, verify=False, proxies=proxies)
    response = session_get(session, merchant_url)

    if response == "Max_retries":
        print(f"Skipping merchant {merchant_id} due to repeated failures.")
        sys.exit(1)

    try:
        # Проверка статуса, если он есть у объекта ответа
        status = getattr(response, "status_code", None)
        if status is not None and status != 200:
            raise RuntimeError(f"HTTP status {status} at {merchant_url}")

        html = response.text

        catalog_json = extract_merchant_json(html)

        merchant_data.append({
            "merchant_id": truncate_string(catalog_json.get("uid"), 50),
            "name": truncate_string(catalog_json.get("name"), 100),
            "phone": truncate_string(catalog_json.get("phone"), 20),
            "create_date": truncate_string(catalog_json.get("create"), 50),
            "salesCount": catalog_json.get("salesCount"),
            "rating": catalog_json.get("rating"),
            "ratingCount": catalog_json.get("summary", {}).get("ratingCount"),
            "reviewsCount": catalog_json.get("summary", {}).get("reviewsCount"),
            "parse_date": parse_date
        })
        return True

    except Exception as e:
        # Пишем проблемный ответ в файл
        if save_html_on_error and hasattr(response, "text"):
            try:
                path = _dump_response(response.text, merchant_id, save_dir, tag="merchant_error")
                print(f"[merchant_parser] Error for merchant {merchant_id}: {e}. HTML saved to: {path}")
            except Exception as dump_err:
                print(f"[merchant_parser] Error for merchant {merchant_id}: {e}. Failed to save HTML: {dump_err}")
        else:
            print(f"[merchant_parser] Error for merchant {merchant_id}: {e}")
        
        sys.exit(1)


def kaspi_start_detailed(gp_conn_name):

    csv_filename = '/home/airflowadmin/rsalimov/kaspi_merchants_detailed.csv'

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csvfile.truncate()

    parse_date = datetime.now().strftime('%Y-%m-%d')

    print("Connect to GP...")
    gp_hook = PostgresHook(postgres_conn_id=gp_conn_name)
    gp_conn = gp_hook.get_conn()
    gp_curs = gp_conn.cursor()
    print("Connected")

    sql_script = f'''
            SELECT km.merchant_id
            FROM (SELECT DISTINCT merchant_id FROM fgp_dsx_dm.kaspi_merchants) km
            LEFT JOIN fgp_dsx_dm.kaspi_merchants_detailed kmd
                ON km.merchant_id = kmd.merchant_id
            WHERE kmd.merchant_id IS null
        '''

    try:
        gp_curs.execute(sql_script)
    except Exception as e:
        print(f"Ошибка при запросе в kaspi_merchant: {e}")
        gp_conn.rollback()
        gp_curs.close()
        gp_conn.close()
        raise AirflowException(f"Ошибка при запросе в kaspi_merchant: {e}")

    rows = gp_curs.fetchall()
    gp_curs.close()
    gp_conn.close()

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