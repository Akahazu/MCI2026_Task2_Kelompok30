# Catatan eksplorasi tabel orders_top_products & tabel customer_ranking.
# Hasil agregasi Spark — seluruh produk yang muncul di snapshot order terakhir.
# Sumber API: http://96.9.212.102:8000/orders

# =========================================================
# NUMBER CARD 
# =========================================================

# Total order secara global
SELECT SUM(total_orders) FROM analytics.orders_top_products;

# Total produk yang tercatat
SELECT COUNT(*) FROM analytics.orders_top_products;

# Total reorder secara global
SELECT SUM(reorder_count) FROM analytics.orders_top_products;

# Rata-rata pesanan per produk
SELECT AVG(total_orders) FROM analytics.orders_top_products;

# =========================================================
# BAR CHART (Vertikal dan Horizontal)
# =========================================================

# Top Departement berdasarkan total order
SELECT department, SUM(total_orders)
FROM analytics.orders_top_products
GROUP BY department
ORDER BY SUM(total_orders) DESC;

# Top produk reorder terbanyak
SELECT product_name, reorder_count
FROM analytics.orders_top_products
ORDER BY reorder_count DESC
LIMIT 10;

# =========================================================
# PIE CHART
# =========================================================

# Segmentasi user berdasarkan shopping style
SELECT 
    CASE 
        WHEN isNaN(final_score) OR (reorder_rate = 0 AND days_since_prior_order < 15) THEN 'New Customer'
        WHEN final_score > 80 THEN 'The Champions'
        WHEN final_score > 60 THEN 'The Potential'
        WHEN final_score > 40 THEN 'Regular Customer'
        WHEN final_score > 20 THEN 'At Risk Customer'
        ELSE 'Lost Customer'
    END AS customer_type,
    count() AS total_users
FROM analytics.customer_ranking
GROUP BY customer_type
ORDER BY total_users DESC;

# Jenis Stocking User berdasarkan basket size dan reorder rate
SELECT 
    CASE 
        WHEN basket_size > 15 AND reorder_rate < 0.3 THEN 'Stockpiler'
        WHEN basket_size < 5 AND reorder_rate > 0.7 THEN 'Frequent Reorderer'
        ELSE 'Balanced'
    END AS shopping_style,
    count() AS total_users,
    round(avg(basket_size), 2) AS avg_basket_size,
    round(avg(reorder_rate) * 100, 2) AS avg_reorder_rate_pct
FROM analytics.customer_ranking
GROUP BY shopping_style
ORDER BY total_users DESC;

# =========================================================
# Scatter Plot
# =========================================================

# Peak hour user
SELECT 
    round(avg_order_hour) AS peak_hour,
    count() AS total_top_users
FROM analytics.customer_ranking
GROUP BY peak_hour
ORDER BY peak_hour ASC;

# =========================================================
# TABLE CARD 
* =========================================================

# Top 10 customer loyal
SELECT 
    user_id, 
    final_score
FROM analytics.customer_ranking
ORDER BY final_score DESC
LIMIT 10;






