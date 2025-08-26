import csv
import psycopg2
from psycopg2.extras import execute_batch

CONN_PARAMS = {
    "host": "dl-gpdb-prod.fortebank.com",
    "port": 5432,
    "database": "gpdb",
    "user": "ngharifulla",
    "password": "n23062005G)"
}

TABLE_NAME = "fgp_de_sandbox.kill_kaspi_merchants_detailed"
CSV_FILE = "kaspi_merchants_detailed.csv"

COLUMNS = [
    "merchant_id",
    '"name"',       # name обязательно в кавычках
    "phone",
    "create_date",
    "salescount",
    "rating",
    "ratingcount",
    "reviewscount",
    "parse_date"
]

def insert_csv_rows(
    csv_path: str,
    conn_params: dict,
    table: str,
    columns: list[str],
    batch_size: int = 5000,
    delimiter: str = ",",
    encoding: str = "utf-8",
    trunc: bool = True,
):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    try:
        if trunc:
            cur.execute(f"TRUNCATE TABLE {table}")

        placeholders = ", ".join(["%s"] * len(columns))
        cols_sql     = ", ".join(columns)
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"

        batch = []
        inserted = 0

        with open(csv_path, "r", newline="", encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader, None)  # пропускаем заголовок

            for row in reader:
                # заменяем пустые строки на None
                values = [val if val != "" else None for val in row]
                batch.append(values)

                if len(batch) >= batch_size:
                    execute_batch(cur, sql, batch, page_size=batch_size)
                    inserted += len(batch)
                    print(f"Вставлено {inserted} строк...")
                    batch.clear()

        if batch:
            execute_batch(cur, sql, batch, page_size=batch_size)
            inserted += len(batch)

        conn.commit()
        print(f"✅ Завершено. Всего вставлено {inserted} строк.")

    except Exception as e:
        conn.rollback()
        print("Ошибка:", e)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    insert_csv_rows(
        csv_path=CSV_FILE,
        conn_params=CONN_PARAMS,
        table=TABLE_NAME,
        columns=COLUMNS,
        batch_size=10000,
        delimiter=",",
        encoding="utf-8",
        trunc=True,
    )
