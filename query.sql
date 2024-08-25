SELECT 
    country_code AS country, 
    COUNT(store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_no_stores DESC;

   SELECT 
    locality, 
    COUNT(store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
HAVING 
    COUNT(store_code) > 0
ORDER BY 
    total_no_stores DESC;
   
SELECT 
    SUM(o.product_quantity * p.product_price) AS total_sales,
    d.month::INTEGER AS month
FROM 
    orders_table o
JOIN 
    dim_date_times d ON o.date_uuid = d.date_uuid
JOIN 
    dim_products p ON o.product_code = p.product_code
GROUP BY 
    d.month
ORDER BY 
    total_sales DESC;

WITH sales_by_store_type AS (
    SELECT 
        s.store_type,
        SUM(o.product_quantity * p.product_price) AS total_sales
    FROM 
        orders_table o
    JOIN 
        dim_store_details s ON o.store_code = s.store_code
    JOIN 
        dim_products p ON o.product_code = p.product_code
    GROUP BY 
        s.store_type
)
SELECT 
    store_type,
    total_sales,
    ROUND((total_sales / SUM(total_sales) OVER ()) * 100, 2) AS percentage_total
FROM 
    sales_by_store_type
ORDER BY 
    total_sales DESC;
   
WITH monthly_sales AS (
    SELECT 
        d.year AS year,
        d.month AS month,
        SUM(o.product_quantity * p.product_price) AS total_sales
    FROM 
        orders_table o
    JOIN 
        dim_date_times d ON o.date_uuid = d.date_uuid
    JOIN 
        dim_products p ON o.product_code = p.product_code
    GROUP BY 
        d.year,
        d.month
)
SELECT 
    total_sales,
    year,
    month
FROM 
    monthly_sales
ORDER BY 
    total_sales DESC
LIMIT 10;

SELECT 
    country_code,
    SUM(staff_numbers) AS total_staff_numbers
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    country_code;
   
   SELECT 
    SUM(o.product_quantity * p.product_price) AS total_sales,
    s.store_type,
    s.country_code
FROM 
    orders_table o
JOIN 
    dim_store_details s ON o.store_code = s.store_code
JOIN 
    dim_products p ON o.product_code = p.product_code
WHERE 
    s.country_code = 'DE'
GROUP BY 
    s.store_type, s.country_code
ORDER BY 
    total_sales DESC;
   
WITH ordered_sales AS (
    SELECT 
        year,
        timestamp AS sale_time,
        LEAD(timestamp) OVER (PARTITION BY year ORDER BY date_uuid) AS next_sale_time
    FROM 
        dim_date_times
),
time_differences AS (
    SELECT 
        year,
        CASE 
            WHEN next_sale_time IS NOT NULL AND next_sale_time > sale_time 
            THEN EXTRACT(EPOCH FROM (next_sale_time - sale_time))
            ELSE NULL
        END AS duration_seconds
    FROM 
        ordered_sales
),
average_time AS (
    SELECT 
        year,
        AVG(duration_seconds) AS avg_duration_seconds
    FROM 
        time_differences
    WHERE 
        duration_seconds IS NOT NULL
    GROUP BY 
        year
)
SELECT 
    year,
    CONCAT(
        'hours: ', 
        FLOOR(avg_duration_seconds / 3600), 
        ', minutes: ', 
        FLOOR((avg_duration_seconds % 3600) / 60),
        ', seconds: ', 
        FLOOR(avg_duration_seconds % 60),
        ', milliseconds: ', 
        ROUND((avg_duration_seconds % 1) * 1000)
    ) AS actual_time_taken
FROM 
    average_time
ORDER BY 
    year;