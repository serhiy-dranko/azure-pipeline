-- Question 1: Which categories earn the most revenue?
SELECT category_group, main_category, SUM(revenue) AS revenue
FROM dbw_bigdata_dev_1234.ecommerce.revenue_by_category_brand
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 10;

-- Question 2: Which brands earn the most revenue (excluding unknown)?
SELECT brand, SUM(revenue) AS revenue
FROM dbw_bigdata_dev_1234.ecommerce.revenue_by_category_brand
WHERE brand <> 'unknown'
GROUP BY 1
ORDER BY revenue DESC
LIMIT 10;

-- Question 3: Which products are viewed a lot but rarely bought?
SELECT product_id, brand, main_category, views, purchases, view_to_purchase_rate
FROM dbw_bigdata_dev_1234.ecommerce.product_performance
ORDER BY views DESC
LIMIT 20;

-- Question 4: Busiest hours of the day (UTC)
SELECT event_hour, avg_events_per_day
FROM dbw_bigdata_dev_1234.ecommerce.hourly_traffic
ORDER BY avg_events_per_day DESC
LIMIT 5;