-- Аналитические запросы к витринам. Запускаются вручную или из BI.

-- 1. DAU по дням (агрегат из mart, чтение через uniqMerge)
SELECT
    event_date,
    uniqMerge(user_count) AS dau
FROM mart.dau_daily
GROUP BY event_date
ORDER BY event_date;

-- 2. Выручка и заказы по дням
SELECT
    order_date,
    round(sumMerge(revenue), 2) AS revenue,
    sumMerge(orders_count)      AS orders
FROM mart.revenue_daily
GROUP BY order_date
ORDER BY order_date;

-- 3. Воронка конверсии по дням
SELECT
    day,
    viewed,
    added_to_cart,
    purchased,
    round(purchased / viewed * 100, 2) AS cr_to_purchase_pct
FROM mart.v_funnel
ORDER BY day
LIMIT 30;

-- 4. Когортный retention по месяцам жизни когорты
SELECT
    cohort_month,
    months_since,
    cohort_size,
    active_users,
    retention_pct
FROM mart.v_cohort_retention
WHERE months_since <= 6
ORDER BY cohort_month, months_since;

-- 5. Топ-10 товаров по выручке
SELECT
    p.product_name,
    p.category,
    round(sum(i.price * i.quantity), 2) AS revenue,
    sum(i.quantity)                     AS units_sold
FROM stg.order_items AS i
INNER JOIN stg.orders AS o
    ON o.order_id = i.order_id AND o.status = 'completed'
INNER JOIN stg.products AS p
    ON p.product_id = i.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 10;

-- 6. Выручка по каналам привлечения
SELECT
    u.traffic_source,
    round(sum(o.total_amount), 2) AS revenue
FROM stg.orders AS o
INNER JOIN stg.users AS u USING (user_id)
WHERE o.status = 'completed'
GROUP BY u.traffic_source
ORDER BY revenue DESC;

-- 7. Демонстрация uniq vs uniqExact:
--    uniq — это HyperLogLog с погрешностью ~1% и O(1) памяти,
--    uniqExact — точный счёт через хеш-таблицу (дороже по памяти)
SELECT
    uniq(user_id)      AS dau_approx,
    uniqExact(user_id) AS dau_exact
FROM stg.events
WHERE event_type = 'page_view';
