-- Слой stg: типизация, валидация, дедупликация.
-- ReplacingMergeTree(loaded_at) хранит последнюю версию строки по бизнес-ключу,
-- чтение всегда с FINAL, поэтому повторные запуски DAG безопасны.

-- ===== users =====
DROP TABLE IF EXISTS stg.users;
CREATE TABLE stg.users
(
    user_id        UUID,
    email          String,
    signup_at      DateTime,
    traffic_source LowCardinality(String),
    country        LowCardinality(String),
    loaded_at      DateTime
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY user_id
AS
SELECT
    toUUID(user_id)                                              AS user_id,
    if(position(email, '@') > 0, email, 'unknown@example.com')   AS email,
    parseDateTimeBestEffortOrNull(signup_at)                     AS signup_at,
    toLowCardinality(traffic_source)                             AS traffic_source,
    toLowCardinality(country)                                    AS country,
    now64()                                                      AS loaded_at
FROM raw.users;

-- ===== events =====
DROP TABLE IF EXISTS stg.events;
CREATE TABLE stg.events
(
    event_id   UUID,
    user_id    UUID,
    event_type LowCardinality(String),
    page_url   String,
    event_at   DateTime,
    loaded_at  DateTime
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (user_id, event_at, event_id)
AS
SELECT
    toUUID(event_id)                                                        AS event_id,
    toUUID(user_id)                                                         AS user_id,
    toLowCardinality(if(event_type IN ('page_view', 'add_to_cart', 'purchase'),
                        event_type, 'unknown'))                             AS event_type,
    page_url                                                                AS page_url,
    parseDateTimeBestEffortOrNull(event_at)                                 AS event_at,
    now64()                                                                 AS loaded_at
FROM raw.events
WHERE event_at != '' AND isNumeric(user_id) = 0;

-- ===== orders =====
DROP TABLE IF EXISTS stg.orders;
CREATE TABLE stg.orders
(
    order_id     UUID,
    user_id      UUID,
    order_at     DateTime,
    total_amount Decimal(12, 2),
    status       LowCardinality(String),
    loaded_at    DateTime
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY order_id
AS
SELECT
    toUUID(order_id)                                                        AS order_id,
    toUUID(user_id)                                                         AS user_id,
    parseDateTimeBestEffortOrNull(order_at)                                 AS order_at,
    toDecimal64OrZero(total_amount, 2)                                      AS total_amount,
    toLowCardinality(if(status IN ('completed', 'refunded', 'cancelled'),
                        status, 'unknown'))                                 AS status,
    now64()                                                                 AS loaded_at
FROM raw.orders
WHERE order_at != '';

-- ===== order_items =====
DROP TABLE IF EXISTS stg.order_items;
CREATE TABLE stg.order_items
(
    order_item_id UUID,
    order_id      UUID,
    product_id    UInt32,
    quantity      UInt16,
    price         Decimal(10, 2),
    loaded_at     DateTime
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY order_item_id
AS
SELECT
    toUUID(order_item_id)               AS order_item_id,
    toUUID(order_id)                    AS order_id,
    toUInt32OrZero(product_id)          AS product_id,
    toUInt16OrZero(quantity)            AS quantity,
    toDecimal64OrZero(price, 2)         AS price,
    now64()                             AS loaded_at
FROM raw.order_items;

-- ===== products =====
DROP TABLE IF EXISTS stg.products;
CREATE TABLE stg.products
(
    product_id   UInt32,
    product_name String,
    category     LowCardinality(String),
    price        Decimal(10, 2),
    loaded_at    DateTime
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY product_id
AS
SELECT
    toUInt32OrZero(product_id)      AS product_id,
    product_name                    AS product_name,
    toLowCardinality(category)      AS category,
    toDecimal64OrZero(price, 2)     AS price,
    now64()                         AS loaded_at
FROM raw.products;
