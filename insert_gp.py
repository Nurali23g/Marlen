import csv
import psycopg2
from psycopg2.extras import execute_batch
import os

CONN_PARAMS = {
    "host": "dl-gpdb-prod.fortebank.com",      # адрес хоста
    "port": 5432,                  # порт Greenplum
    "database": "gpdb",   # имя БД
    "user": "***",             # пользователь
    "password": "***"      # пароль
}
TABLE_NAME = "fgp_de_sandbox.test_bins"
CSV_FILE = "data_to_insert.csv"
COLUMNS = ["bin"]
def insert_csv_rows(
    csv_path: str,
    conn_params: dict,
    table: str,
    columns: list[str],
    batch_size: int = 1000,
    delimiter: str = ",",
    encoding: str = "utf-8",
):
    """
    Читает CSV без заголовка и вставляет данные в Greenplum пачками.
    """
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    
    try:
        n = len(columns)
        placeholders = ", ".join(["%s"] * n)
        cols_sql = ", ".join(columns)
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
        
        data = []
        with open(csv_path, "r", newline="", encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                if len(row) < n:
                    continue  
                data.append(tuple(row[i] for i in range(n)))
        
        if data:
            if TRUNC:
                cur.execute(f"TRUNCATE TABLE {table}")
            execute_batch(cur, sql, data, page_size=batch_size)
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def insert_csv_rows_executemany(
    csv_path: str,
    conn_params: dict,
    table: str,
    columns: list[str],
    batch_size: int = 1000,
    delimiter: str = ",",
    encoding: str = "utf-8",
    trunc: bool = False,
):
    """
    Читает CSV без заголовка и вставляет данные в Greenplum пакетами через cursor.executemany.
    """
    n = len(columns)
    placeholders = ", ".join(["%s"] * n)
    cols_sql = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"

    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    try:
        if trunc:
            cur.execute(f"TRUNCATE TABLE {table}")

        inserted = 0
        batch = []
        with open(csv_path, "r", encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                if len(row) < n:
                    continue
                batch.append(tuple(row[i] for i in range(n)))

                if len(batch) >= batch_size:
                    cur.executemany(sql, batch)
                    inserted += len(batch)
                    print(f"Вставлено {inserted} строк...")
                    batch.clear()

        if batch:
            cur.executemany(sql, batch)
            inserted += len(batch)
            print(f"Вставлено {inserted} строк...")

        conn.commit()
        print(f"Завершено. Всего вставлено {inserted} строк.")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    insert_csv_rows_executemany(
        csv_path=CSV_FILE,
        conn_params=CONN_PARAMS,
        table=TABLE_NAME,
        columns=COLUMNS,
        batch_size=10000,
        delimiter=",",  # <-- ваш файл разделен запятыми
        encoding="utf-8",
        trunc=TRUNC,
    )

    print("Готово: все строки вставлены.")
TRUNC = True


def insert_csv_rows(
    csv_path: str,
    conn_params: dict,
    table: str,
    columns: list[str],
    batch_size: int = 1000,
    delimiter: str = "|",
    encoding: str = "utf-8",
):
    """
    Читает CSV без заголовка и вставляет данные в Greenplum пачками.
    
    :param csv_path: путь к CSV-файлу
    :param conn_params: параметры подключения к БД
    :param table: имя таблицы
    :param columns: список колонок, например ['bin','name']
    :param batch_size: размер пакета для execute_batch
    :param delimiter: разделитель в CSV
    :param encoding: кодировка файла
    """
    # Подключаемся
    conn = psycopg2.connect(**conn_params)
    cur  = conn.cursor()
    
    try:
        n = len(columns)
        placeholders = ", ".join(["%s"] * n)
        cols_sql     = ", ".join(columns)
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
        
        data = []
        with open(csv_path, "r", newline="", encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                # если в строке меньше полей, пропускаем
                if len(row) < n:
                    continue  
                # собираем первые n полей в кортеж
                data.append(tuple(row[i] for i in range(n)))
        
        if data:
            if TRUNC:
                cur.execute(f"TRUNCATE TABLE {table}")
            execute_batch(cur, sql, data, page_size=batch_size)
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def insert_csv_rows_executemany(
    csv_path: str,
    conn_params: dict,
    table: str,
    columns: list[str],
    batch_size: int = 1000,
    delimiter: str = "|",
    encoding: str = "utf-8",
    trunc: bool = False,
):
    """
    Читает CSV без заголовка и вставляет данные в Greenplum пакетами через cursor.executemany.

    :param csv_path: путь к CSV-файлу
    :param conn_params: параметры подключения к БД
    :param table: имя таблицы (schema.table)
    :param columns: список колонок, например ['bin','name']
    :param batch_size: размер пакета для executemany
    :param delimiter: разделитель в CSV
    :param encoding: кодировка файла
    :param trunc: выполнить TRUNCATE перед вставкой
    """

    n = len(columns)
    placeholders = ", ".join(["%s"] * n)
    cols_sql     = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"

    conn = psycopg2.connect(**conn_params)
    cur  = conn.cursor()

    try:
        if trunc:
            cur.execute(f"TRUNCATE TABLE {table}")

        inserted = 0
        batch = []
        with open(csv_path, "r", encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                if len(row) < n:
                    continue
                batch.append(tuple(row[i] for i in range(n)))

                if len(batch) >= batch_size:
                    cur.executemany(sql, batch)
                    inserted += len(batch)
                    print(f"Вставлено {inserted} строк...")
                    batch.clear()

        if batch:
            cur.executemany(sql, batch)
            inserted += len(batch)
            print(f"Вставлено {inserted} строк...")

        conn.commit()
        print(f"Завершено. Всего вставлено {inserted} строк.")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # insert_csv_rows(
    #     csv_path   = CSV_FILE,
    #     conn_params= CONN_PARAMS,
    #     table      = TABLE_NAME,
    #     columns    = COLUMNS  # передаёте столько, сколько нужно
    # )

    insert_csv_rows_executemany(
        csv_path = CSV_FILE,
        conn_params = CONN_PARAMS,
        table = TABLE_NAME,
        columns = COLUMNS,
        batch_size = 10000,
        delimiter = "|",
        encoding = "utf-8",
        trunc = TRUNC,
    )

    print("Готово: все строки вставлены.")

