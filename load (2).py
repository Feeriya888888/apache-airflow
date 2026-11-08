"""Load: потоковая заливка CSV в raw-слой ClickHouse по HTTP.

Читает файл построчно и отправляет чанками — память не растёт вместе с данными.
Формат CSV без заголовков: порядок колонок фиксирован и совпадает со схемой raw.
"""

import argparse
import csv
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
from ch_utils import ch  # noqa: E402

CHUNK_SIZE = 200_000

TABLES = {
    "users.csv":       ("raw.users", ["user_id", "email", "signup_at", "traffic_source", "country"]),
    "events.csv":      ("raw.events", ["event_id", "user_id", "event_type", "page_url", "event_at"]),
    "orders.csv":      ("raw.orders", ["order_id", "user_id", "order_at", "total_amount", "status"]),
    "order_items.csv": ("raw.order_items", ["order_item_id", "order_id", "product_id", "quantity", "price"]),
    "products.csv":    ("raw.products", ["product_id", "product_name", "category", "price"]),
}


def load_file(path: str, table: str, cols: list) -> int:
    query = f"INSERT INTO {table} ({', '.join(cols)}) FORMAT CSV"
    loaded = 0
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # заголовок пропускаем — порядок колонок задан в INSERT
        while True:
            buf, n = io.StringIO(), 0
            writer = csv.writer(buf)
            for row in reader:
                writer.writerow(row)
                n += 1
                if n >= CHUNK_SIZE:
                    break
            if n == 0:
                break
            ch(query, data=buf.getvalue().encode("utf-8"))
            loaded += n
    return loaded


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="data")
    args = ap.parse_args()

    for filename, (table, cols) in TABLES.items():
        path = os.path.join(args.dir, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"не найден {path} — сначала запустите extract.py")
        n = load_file(path, table, cols)
        print(f"{table}: загружено {n} строк")


if __name__ == "__main__":
    main()
