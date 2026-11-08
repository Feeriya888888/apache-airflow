-- Слой raw: сырые данные "как есть" из источника.
-- Все поля String + technical поле loaded_at. Никакой трансформации.
-- Таблицы создаются через IF NOT EXISTS — raw накапливает историю запусков DAG.

CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.users
(
    user_id        String,
    email          String,
    signup_at      String,
    traffic_source String,
    country        String,
    loaded_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS raw.events
(
    event_id   String,
    user_id    String,
    event_type String,
    page_url   String,
    event_at   String,
    loaded_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS raw.orders
(
    order_id     String,
    user_id      String,
    order_at     String,
    total_amount String,
    status       String,
    loaded_at    DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS raw.order_items
(
    order_item_id String,
    order_id      String,
    product_id    String,
    quantity      String,
    price         String,
    loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS raw.products
(
    product_id   String,
    product_name String,
    category     String,
    price        String,
    loaded_at    DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY tuple();
