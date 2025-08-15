TABLE_NAME = "fgp_de_sandbox.test_halyk_categories_all"
CSV_FILE = "halyk_all_categories.csv"
COLUMNS = ["id", "name", "external_id", "parent_id"]
TRUNC = True


select count(distinct category)
from fgp_de_sandbox.test_halyk_merchants_all a
where category not in (select distinct merchant_name
from fgp_de_sandbox.test_halyk_merchants_all
where category in ('1', '2', '6', '3', '4', '8', '9', '32626', '11', '5' )
)









WITH RECURSIVE cat_tree AS (
    -- seed: the root external_ids you want to exclude
    SELECT id, external_id, parent_id
    FROM fgp_de_sandbox.test_halyk_categories_all
    WHERE external_id IN ('1','2','3','4','5','6','8','9','11','32626')

    UNION ALL

    -- walk down to all descendants
    SELECT c.id, c.external_id, c.parent_id
    FROM fgp_de_sandbox.test_halyk_categories_all c
    JOIN cat_tree p ON c.parent_id = p.id
)
SELECT m.*
FROM fgp_de_sandbox.test_halyk_merchants_all m
WHERE NOT EXISTS (
    SELECT 1
    FROM cat_tree t
    WHERE m.category::text = t.external_id::text
);












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



