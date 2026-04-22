DESCRIBE olist_orders_dataset;
DESCRIBE customer;
SELECT order_purchase_timestamp
FROM olist_orders_dataset
LIMIT 10;
CREATE OR REPLACE VIEW v_base_orders AS
SELECT
    o.order_id,
    c.customer_id,
    c.customer_unique_id,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp AS purchase_ts,
    CAST(DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m-01') AS DATE) AS order_month
FROM olist_orders_dataset o
JOIN customer c
    ON o.customer_id = c.customer_id
WHERE o.order_purchase_timestamp IS NOT NULL
  AND c.customer_unique_id IS NOT NULL
  AND o.order_status = 'delivered';
  
 -- Kiểm tra bảng
 SELECT *
FROM v_base_orders
LIMIT 20;
-- Kiểm tra giá trị 
SELECT COUNT(*) FROM v_base_orders;
SELECT
    MIN(purchase_ts) AS min_purchase_ts,
    MAX(purchase_ts) AS max_purchase_ts,
    MIN(order_month) AS min_order_month,
    MAX(order_month) AS max_order_month
FROM v_base_orders;
SELECT
    SUM(CASE WHEN purchase_ts IS NULL THEN 1 ELSE 0 END) AS null_purchase_ts,
    SUM(CASE WHEN order_month IS NULL THEN 1 ELSE 0 END) AS null_order_month,
    SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) AS null_customer_unique_id
FROM v_base_orders;

-- Một khách có thể mua nhiều đơn trong cùng 1 tháng nhưng chỉ cần biết tháng đó khách có active hay không =>> bớt trùng ở mức customer-month


CREATE OR REPLACE VIEW v_customer_months AS
SELECT DISTINCT
    customer_unique_id,
    customer_state,
    order_month
FROM v_base_orders;

-- Kiểm tra:
SELECT *
FROM v_customer_months
ORDER BY customer_unique_id, order_month
LIMIT 20;

-- Xac dinh thang mua dau tien cua khach
-- cohort_month = tháng mua đầu tiên
-- first_state = state tại lần mua đầu tiên
CREATE OR REPLACE VIEW v_first_purchase_detail AS
SELECT
    customer_unique_id,
    customer_state AS first_state,
    order_month AS cohort_month,
    purchase_ts AS first_purchase_ts
FROM (
    SELECT
        customer_unique_id,
        customer_state,
        order_month,
        purchase_ts,
        order_id,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY purchase_ts, order_id
        ) AS rn
    FROM v_base_orders
) t
WHERE rn = 1;
SELECT *
FROM v_first_purchase_detail
LIMIT 20;
-- Cohort retention long table
CREATE OR REPLACE VIEW v_cohort_retention_long AS
SELECT
    fp.cohort_month,
    fp.first_state AS customer_state,
    TIMESTAMPDIFF(MONTH, fp.cohort_month, cm.order_month) + 1 AS cohort_index,
    COUNT(DISTINCT cm.customer_unique_id) AS customers_retained
FROM v_customer_months cm
JOIN v_first_purchase_detail fp
    ON cm.customer_unique_id = fp.customer_unique_id
GROUP BY
    fp.cohort_month,
    fp.first_state,
    TIMESTAMPDIFF(MONTH, fp.cohort_month, cm.order_month) + 1;
-- Kiem tra
SELECT *
FROM v_cohort_retention_long
ORDER BY cohort_month, customer_state, cohort_index
LIMIT 100;

-- Retention %
CREATE OR REPLACE VIEW v_cohort_retention_pct AS
SELECT
    r.cohort_month,
    r.customer_state,
    r.cohort_index,
    r.customers_retained,
    cs.cohort_size,
    ROUND(100.0 * r.customers_retained / cs.cohort_size, 2) AS retention_pct
FROM v_cohort_retention_long r
JOIN (
    SELECT
        cohort_month,
        customer_state,
        customers_retained AS cohort_size
    FROM v_cohort_retention_long
    WHERE cohort_index = 1
) cs
    ON r.cohort_month = cs.cohort_month
   AND r.customer_state = cs.customer_state;

-- Kiểm tra:
SELECT *
FROM v_cohort_retention_pct
ORDER BY cohort_month, customer_state, cohort_index
LIMIT 100;

CREATE OR REPLACE VIEW v_monthly_new_repeat AS
SELECT
    cm.order_month,
    COUNT(DISTINCT CASE
        WHEN cm.order_month = fp.cohort_month THEN cm.customer_unique_id
    END) AS new_customers,
    COUNT(DISTINCT CASE
        WHEN cm.order_month > fp.cohort_month THEN cm.customer_unique_id
    END) AS repeat_customers,
    COUNT(DISTINCT cm.customer_unique_id) AS active_customers
FROM v_customer_months cm
JOIN v_first_purchase_detail fp
    ON cm.customer_unique_id = fp.customer_unique_id
GROUP BY cm.order_month;
-- Them ty le repeat
CREATE OR REPLACE VIEW v_monthly_new_repeat_pct AS
SELECT
    order_month,
    new_customers,
    repeat_customers,
    active_customers,
    ROUND(100.0 * repeat_customers / active_customers, 2) AS repeat_share_pct
FROM v_monthly_new_repeat;

-- đơn thứ 1 và đơn thứ 2 của mỗi khách

-- BO BO BO CREATE OR REPLACE VIEW v_customer_order_rank AS
SELECT
    customer_unique_id,
    customer_state,
    order_id,
    purchase_ts,
    ROW_NUMBER() OVER (
        PARTITION BY customer_unique_id
        ORDER BY purchase_ts, order_id
    ) AS order_rank
FROM v_base_orders;

-- bảng first-second purchase
CREATE OR REPLACE VIEW v_first_second_purchase AS
SELECT
    r1.customer_unique_id,
    r1.customer_state,
    CAST(DATE_FORMAT(r1.purchase_ts, '%Y-%m-01') AS DATE) AS cohort_month,
    r1.purchase_ts AS first_purchase_ts,
    r2.purchase_ts AS second_purchase_ts
FROM (
    SELECT * FROM v_customer_order_rank WHERE order_rank = 1
) r1
LEFT JOIN (
    SELECT * FROM v_customer_order_rank WHERE order_rank = 2
) r2
    ON r1.customer_unique_id = r2.customer_unique_id;
-- Tính repurchase windows
CREATE OR REPLACE VIEW v_repurchase_30_60_90 AS
SELECT
    cohort_month,
    customer_state,
    COUNT(*) AS total_new_customers,
    COUNT(CASE
        WHEN second_purchase_ts IS NOT NULL
         AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 30
        THEN 1
    END) AS repurchase_30d_customers,
    COUNT(CASE
        WHEN second_purchase_ts IS NOT NULL
         AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 60
        THEN 1
    END) AS repurchase_60d_customers,
    COUNT(CASE
        WHEN second_purchase_ts IS NOT NULL
         AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 90
        THEN 1
    END) AS repurchase_90d_customers,
    ROUND(
        100.0 * COUNT(CASE
            WHEN second_purchase_ts IS NOT NULL
             AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 30
            THEN 1
        END) / COUNT(*), 2
    ) AS repurchase_30d_pct,
    ROUND(
        100.0 * COUNT(CASE
            WHEN second_purchase_ts IS NOT NULL
             AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 60
            THEN 1
        END) / COUNT(*), 2
    ) AS repurchase_60d_pct,
    ROUND(
        100.0 * COUNT(CASE
            WHEN second_purchase_ts IS NOT NULL
             AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 90
            THEN 1
        END) / COUNT(*), 2
    ) AS repurchase_90d_pct
FROM v_first_second_purchase
GROUP BY cohort_month, customer_state;


 -- bảng rank 
DROP TABLE IF EXISTS t_customer_order_rank;

CREATE TABLE t_customer_order_rank AS
SELECT
    customer_unique_id,
    customer_state,
    order_id,
    purchase_ts,
    ROW_NUMBER() OVER (
        PARTITION BY customer_unique_id
        ORDER BY purchase_ts, order_id
    ) AS order_rank
FROM v_base_orders;

-- Tạo index:

CREATE INDEX idx_tcor_customer_rank
ON t_customer_order_rank (customer_unique_id(32), order_rank);

CREATE INDEX idx_tcor_purchase_ts
ON t_customer_order_rank (purchase_ts);

-- Test:

SELECT COUNT(*) FROM t_customer_order_rank;
SELECT * FROM t_customer_order_rank LIMIT 20;

-- Bảng first-second purchase
DROP TABLE IF EXISTS t_first_second_purchase;

CREATE TABLE t_first_second_purchase AS
SELECT
    customer_unique_id,
    MAX(CASE WHEN order_rank = 1 THEN customer_state END) AS customer_state,
    CAST(
        DATE_FORMAT(
            MAX(CASE WHEN order_rank = 1 THEN purchase_ts END),
            '%Y-%m-01'
        ) AS DATE
    ) AS cohort_month,
    MAX(CASE WHEN order_rank = 1 THEN purchase_ts END) AS first_purchase_ts,
    MAX(CASE WHEN order_rank = 2 THEN purchase_ts END) AS second_purchase_ts
FROM t_customer_order_rank
WHERE order_rank IN (1, 2)
GROUP BY customer_unique_id;

-- tao index
CREATE INDEX idx_tfsp_cohort_state
ON t_first_second_purchase (cohort_month, customer_state(2));

-- Tao view repurchase
CREATE OR REPLACE VIEW v_repurchase_30_60_90 AS
SELECT
    cohort_month,
    customer_state,
    COUNT(*) AS total_new_customers,
    COUNT(CASE
        WHEN second_purchase_ts IS NOT NULL
         AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 30
        THEN 1
    END) AS repurchase_30d_customers,
    COUNT(CASE
        WHEN second_purchase_ts IS NOT NULL
         AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 60
        THEN 1
    END) AS repurchase_60d_customers,
    COUNT(CASE
        WHEN second_purchase_ts IS NOT NULL
         AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 90
        THEN 1
    END) AS repurchase_90d_customers,
    ROUND(
        100.0 * COUNT(CASE
            WHEN second_purchase_ts IS NOT NULL
             AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 30
            THEN 1
        END) / COUNT(*), 2
    ) AS repurchase_30d_pct,
    ROUND(
        100.0 * COUNT(CASE
            WHEN second_purchase_ts IS NOT NULL
             AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 60
            THEN 1
        END) / COUNT(*), 2
    ) AS repurchase_60d_pct,
    ROUND(
        100.0 * COUNT(CASE
            WHEN second_purchase_ts IS NOT NULL
             AND DATEDIFF(second_purchase_ts, first_purchase_ts) <= 90
            THEN 1
        END) / COUNT(*), 2
    ) AS repurchase_90d_pct
FROM t_first_second_purchase
GROUP BY cohort_month, customer_state;

-- Test 
SELECT *
FROM v_repurchase_30_60_90
ORDER BY cohort_month, customer_state
LIMIT 100;

-- New vs Repeat theo state va theo month
CREATE OR REPLACE VIEW v_monthly_new_repeat_state AS
SELECT
    cm.order_month,
    fp.first_state AS customer_state,
    COUNT(DISTINCT CASE
        WHEN cm.order_month = fp.cohort_month THEN cm.customer_unique_id
    END) AS new_customers,
    COUNT(DISTINCT CASE
        WHEN cm.order_month > fp.cohort_month THEN cm.customer_unique_id
    END) AS repeat_customers,
    COUNT(DISTINCT cm.customer_unique_id) AS active_customers
FROM v_customer_months cm
JOIN v_first_purchase_detail fp
    ON cm.customer_unique_id = fp.customer_unique_id
GROUP BY
    cm.order_month,
    fp.first_state;
-- Test 
SELECT *
FROM v_monthly_new_repeat_state
ORDER BY order_month, customer_state
LIMIT 100;

-- Rentention theo state
SELECT *
FROM v_cohort_retention_pct
ORDER BY cohort_month, customer_state, cohort_index
LIMIT 100;


SELECT * FROM v_cohort_retention_pct;
SELECT * FROM v_monthly_new_repeat_pct;
SELECT * FROM v_repurchase_30_60_90;
SELECT * FROM v_monthly_new_repeat_state;





