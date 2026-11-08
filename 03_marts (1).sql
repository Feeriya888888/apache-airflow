-- Слой mart: витрины.
-- Агрегаты — через AggregatingMergeTree (uniqState/sumState), чтение через uniqMerge/sumMerge.
-- Витрины с деталями (воронка, retention) — обычными VIEW.

-- ===== DAU по дням =====
DROP TABLE IF EXISTS mart.dau_daily;
CREATE TABLE mart.dau_daily
(
    event_date Date,
    user_count AggregateFunction(uniq, UUID)
)
ENGINE = AggregatingMergeTree
ORDER BY event_date
AS
SELECT
    toDate(event_at) AS event_date,
    uniqState(user_id) AS user_count
FROM stg.events
WHERE event_type = 'page_view' AND toDate(event_at) >= toDate(now()) - INTERVAL 400 DAY
GROUP BY event_date;

-- ===== Выручка и заказы по дням =====
DROP TABLE IF EXISTS mart.revenue_daily;
CREATE TABLE mart.revenue_daily
(
    order_date   Date,
    revenue      AggregateFunction(sum, Decimal64(2)),
    orders_count AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY order_date
AS
SELECT
    toDate(order_at) AS order_date,
    sumState(total_amount) AS revenue,
    sumState(toUInt64(1))  AS orders_count
FROM stg.orders
WHERE status = 'completed' AND toDate(order_at) >= toDate(now()) - INTERVAL 400 DAY
GROUP BY order_date;

-- ===== Воронка по дням =====
DROP VIEW IF EXISTS mart.v_funnel;
CREATE VIEW mart.v_funnel AS
SELECT
    toDate(event_at) AS day,
    uniqExactIf(user_id, event_type = 'page_view')    AS viewed,
    uniqExactIf(user_id, event_type = 'add_to_cart')  AS added_to_cart,
    uniqExactIf(user_id, event_type = 'purchase')     AS purchased
FROM stg.events
GROUP BY day;

-- ===== Когортный retention =====
DROP VIEW IF EXISTS mart.v_cohort_retention;
CREATE VIEW mart.v_cohort_retention AS
SELECT
    cohort_month,
    months_since,
    active_users,
    cohort_size,
    round(active_users / cohort_size * 100, 1) AS retention_pct
FROM
(
    SELECT
        u.cohort_month AS cohort_month,
        dateDiff('month', u.cohort_month, toStartOfMonth(e.event_at)) AS months_since,
        uniqExact(e.user_id) AS active_users
    FROM stg.events AS e
    INNER JOIN
    (
        SELECT user_id, toStartOfMonth(signup_at) AS cohort_month
        FROM stg.users
    ) AS u USING (user_id)
    WHERE e.event_type = 'page_view'
    GROUP BY cohort_month, months_since
) AS t
INNER JOIN
(
    SELECT toStartOfMonth(signup_at) AS cohort_month, count() AS cohort_size
    FROM stg.users
    GROUP BY cohort_month
) AS c USING (cohort_month);
