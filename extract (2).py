"""Extract: детерминированная генерация данных магазина в CSV (data/).

Сид фиксированный, поэтому повторный запуск DAG даёт те же самые данные:
raw-слой накапливает "дубликаты", а stg-слой дедуплицирует их через
ReplacingMergeTree + FINAL. Так демо показывает идемпотентность ELT.

Только стандартная библиотека, никаких зависимостей.
"""

import argparse
import csv
import math
import os
import random
import uuid


def det_uuid(rng) -> str:
    """uuid, детерминированный от генератора: повторный запуск с тем же сидом даёт тот же id."""
    return str(uuid.UUID(int=rng.getrandbits(128)))
from datetime import datetime, timedelta

COUNTRY_WEIGHTS = [("RU", 55), ("KZ", 12), ("BY", 10), ("AM", 6), ("DE", 7), ("US", 10)]
SOURCE_WEIGHTS = [("organic", 40), ("paid", 25), ("email", 15), ("social", 12), ("referral", 8)]
CATEGORIES = ["electronics", "apparel", "home", "beauty", "sports", "books", "toys"]
STATUS_WEIGHTS = [("completed", 88), ("refunded", 7), ("cancelled", 5)]

PAGE_VIEWS = ["/", "/catalog", "/search", "/category/electronics", "/category/apparel", "/category/home", "/cart"]


def weighted_choice(rng, pairs):
    return rng.choices([p[0] for p in pairs], weights=[p[1] for p in pairs])[0]


def gen_products(rng, n):
    out = []
    for pid in range(1, n + 1):
        cat = rng.choice(CATEGORIES)
        out.append({
            "product_id": pid,
            "product_name": f"{cat.title()} Item #{pid}",
            "category": cat,
            "price": round(rng.uniform(5, 500), 2),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data")
    ap.add_argument("--users", type=int, default=30000)
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--sessions-per-user", type=float, default=8.0, help="среднее число сессий на пользователя")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    rng = random.Random(args.seed)
    start = datetime(2025, 1, 1)
    period = timedelta(days=args.days)
    products = gen_products(rng, 120)

    users, events, orders, items = [], [], [], []
    for i in range(1, args.users + 1):
        user_id = str(uuid.UUID(int=i))  # стабильные id при том же сиде
        signup = start + timedelta(seconds=rng.uniform(0, period.total_seconds() * 0.5))
        users.append({
            "user_id": user_id,
            "email": f"user_{i}@{rng.choice(['mail.ru', 'gmail.com', 'yandex.ru', 'example.com'])}",
            "signup_at": signup.strftime("%Y-%m-%d %H:%M:%S"),
            "traffic_source": weighted_choice(rng, SOURCE_WEIGHTS),
            "country": weighted_choice(rng, COUNTRY_WEIGHTS),
        })

        # число сессий — логнормальное вокруг среднего
        n_sessions = max(1, int(round(math.exp(rng.gauss(math.log(args.sessions_per_user), 0.6)))))
        for _ in range(n_sessions):
            sess_time = signup + timedelta(seconds=rng.uniform(0, (period - (signup - start)).total_seconds()))
            t = sess_time
            n_views = rng.randint(1, 4)
            for _ in range(n_views):
                events.append({
                    "event_id": det_uuid(rng),
                    "user_id": user_id,
                    "event_type": "page_view",
                    "page_url": rng.choice(PAGE_VIEWS),
                    "event_at": t.strftime("%Y-%m-%d %H:%M:%S"),
                })
                t += timedelta(seconds=rng.uniform(10, 600))
            if rng.random() < 0.40:  # добавил в корзину
                pid = rng.randint(1, len(products))
                events.append({
                    "event_id": det_uuid(rng),
                    "user_id": user_id,
                    "event_type": "add_to_cart",
                    "page_url": f"/product/{pid}",
                    "event_at": t.strftime("%Y-%m-%d %H:%M:%S"),
                })
                t += timedelta(seconds=rng.uniform(10, 600))
                if rng.random() < 0.35:  # купил
                    events.append({
                        "event_id": det_uuid(rng),
                        "user_id": user_id,
                        "event_type": "purchase",
                        "page_url": f"/product/{pid}",
                        "event_at": t.strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    order_id = det_uuid(rng)
                    total = 0.0
                    for _ in range(rng.randint(1, 3)):
                        prod = rng.choice(products)
                        qty = rng.randint(1, 3)
                        total += prod["price"] * qty
                        items.append({
                            "order_item_id": det_uuid(rng),
                            "order_id": order_id,
                            "product_id": prod["product_id"],
                            "quantity": qty,
                            "price": f"{prod['price']:.2f}",
                        })
                    orders.append({
                        "order_id": order_id,
                        "user_id": user_id,
                        "order_at": t.strftime("%Y-%m-%d %H:%M:%S"),
                        "total_amount": f"{total:.2f}",
                        "status": weighted_choice(rng, STATUS_WEIGHTS),
                    })

    events.sort(key=lambda e: e["event_at"])
    orders.sort(key=lambda o: o["order_at"])

    def dump(name, rows, cols):
        with open(f"{args.out}/{name}", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        print(f"{name}: {len(rows)} строк")

    dump("users.csv", users, ["user_id", "email", "signup_at", "traffic_source", "country"])
    dump("events.csv", events, ["event_id", "user_id", "event_type", "page_url", "event_at"])
    dump("orders.csv", orders, ["order_id", "user_id", "order_at", "total_amount", "status"])
    dump("order_items.csv", items, ["order_item_id", "order_id", "product_id", "quantity", "price"])
    dump("products.csv", products, ["product_id", "product_name", "category", "price"])


if __name__ == "__main__":
    main()
