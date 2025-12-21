-- =====================================================
-- COMPLETENESS CHECKS
-- =====================================================

-- NULL checks for mandatory fields
SELECT 'customers.email' AS field, COUNT(*) AS null_count
FROM production.customers WHERE email IS NULL;

SELECT 'products.price' AS field, COUNT(*) AS null_count
FROM production.products WHERE price IS NULL;

-- =====================================================
-- DUPLICATE CHECKS
-- =====================================================

-- Duplicate customer emails
SELECT email, COUNT(*)
FROM production.customers
GROUP BY email
HAVING COUNT(*) > 1;

-- Duplicate transactions (same customer, date, time, amount)
SELECT customer_id, transaction_date, transaction_time, total_amount, COUNT(*)
FROM production.transactions
GROUP BY customer_id, transaction_date, transaction_time, total_amount
HAVING COUNT(*) > 1;

-- =====================================================
-- REFERENTIAL INTEGRITY
-- =====================================================

-- Orphan transactions (customer missing)
SELECT COUNT(*) AS orphan_transactions
FROM production.transactions t
LEFT JOIN production.customers c
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction items (transaction missing)
SELECT COUNT(*) AS orphan_items_txn
FROM production.transaction_items ti
LEFT JOIN production.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction items (product missing)
SELECT COUNT(*) AS orphan_items_product
FROM production.transaction_items ti
LEFT JOIN production.products p
ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;

-- =====================================================
-- CONSISTENCY CHECKS
-- =====================================================

-- line_total mismatch
SELECT COUNT(*) AS line_total_mismatch
FROM production.transaction_items
WHERE ABS(
    line_total -
    (quantity * unit_price * (1 - discount_percentage/100))
) > 0.01;

-- transaction total mismatch
SELECT COUNT(*) AS txn_total_mismatch
FROM production.transactions t
JOIN production.transaction_items ti
ON t.transaction_id = ti.transaction_id
GROUP BY t.transaction_id, t.total_amount
HAVING ABS(t.total_amount - SUM(ti.line_total)) > 0.01;

-- =====================================================
-- BUSINESS RULES
-- =====================================================

-- cost >= price
SELECT COUNT(*) AS invalid_cost_price
FROM production.products
WHERE cost >= price;

-- discount out of range
SELECT COUNT(*) AS invalid_discount
FROM production.transaction_items
WHERE discount_percentage < 0 OR discount_percentage > 100;

-- future transactions
SELECT COUNT(*) AS future_txns
FROM production.transactions
WHERE transaction_date > CURRENT_DATE;
