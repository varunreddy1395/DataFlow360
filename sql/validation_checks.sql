-- DataFlow360 — Data Quality Validation Checks
-- All queries should return 0 to pass.

-- 1. Null customer IDs
SELECT 'null_customer_id' AS check_name, COUNT(*) AS failed_records
FROM staging.customer_sales_staging
WHERE customer_id IS NULL;

-- 2. Null transaction dates
SELECT 'null_transaction_date' AS check_name, COUNT(*) AS failed_records
FROM staging.customer_sales_staging
WHERE transaction_date IS NULL;

-- 3. Null amounts
SELECT 'null_amount' AS check_name, COUNT(*) AS failed_records
FROM staging.customer_sales_staging
WHERE amount IS NULL;

-- 4. Duplicate transactions
SELECT 'duplicate_transactions' AS check_name, COUNT(*) AS failed_records
FROM (
    SELECT customer_id, transaction_date, product_id, COUNT(*) AS cnt
    FROM staging.customer_sales_staging
    GROUP BY customer_id, transaction_date, product_id
    HAVING COUNT(*) > 1
) dupes;

-- 5. Negative or zero amounts
SELECT 'invalid_amount' AS check_name, COUNT(*) AS failed_records
FROM staging.customer_sales_staging
WHERE amount <= 0;

-- 6. Future transaction dates
SELECT 'future_dates' AS check_name, COUNT(*) AS failed_records
FROM staging.customer_sales_staging
WHERE transaction_date > CURRENT_DATE;

-- 7. Consolidated summary report
WITH checks AS (
    SELECT 'null_customer_id' AS check_name, COUNT(*) AS failed_count
    FROM staging.customer_sales_staging WHERE customer_id IS NULL
    UNION ALL
    SELECT 'null_transaction_date', COUNT(*)
    FROM staging.customer_sales_staging WHERE transaction_date IS NULL
    UNION ALL
    SELECT 'null_amount', COUNT(*)
    FROM staging.customer_sales_staging WHERE amount IS NULL
    UNION ALL
    SELECT 'negative_amount', COUNT(*)
    FROM staging.customer_sales_staging WHERE amount <= 0
    UNION ALL
    SELECT 'future_dates', COUNT(*)
    FROM staging.customer_sales_staging WHERE transaction_date > CURRENT_DATE
)
SELECT
    check_name,
    failed_count,
    CASE WHEN failed_count = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM checks
ORDER BY status DESC, check_name;
