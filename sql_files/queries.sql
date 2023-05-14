
-- Task_1_number_of_stores
SELECT country_code AS Country, count (*) total_no_stores
FROM dim_store_details 
GROUP BY country_code
ORDER BY count (*) DESC;

--Task_2_number_of_stores_in_region.sql
SELECT locality, count (*) 
FROM dim_store_details 
GROUP BY locality
ORDER BY COUNT(*) DESC
LIMIT 7;


--Task_3_number_of_sales_per_month
SELECT dim_date_times.month, ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS numeric), 2) AS total_sales
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY sum(orders_table.product_quantity*dim_products.product_price) DESC;

-- Task_4_online_Vs_offline
SELECT COUNT(orders_table.product_quantity) as numbers_of_sales, sum(orders_table.product_quantity) as product_quantity_count,
	case 
		when dim_store_details.store_code = 'WEB-1388012W' then 'Web'
		else 'Offline'
		end as product_location
FROM orders_table
JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code
group by product_location
ORDER BY sum(orders_table.product_quantity);


--Task_5_percent_of_saLes
SELECT dim_store_details.store_type, ROUND(CAST(SUM(orders_table.product_quantity*dim_products.product_price) AS numeric), 2) as revenue,
SUM(100.0*orders_table.product_quantity*dim_products.product_price)/(sum(sum(orders_table.product_quantity*dim_products.product_price)) over ()) AS percentage_total
FROM orders_table
JOIN dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products on  orders_table.product_code = dim_products.product_code
JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code
GROUP BY dim_store_details.store_type
ORDER BY percentage_total DESC;

--Task_6_revenue by month_year

SELECT dim_date_times.month, dim_date_times.year, round(sum(orders_table.product_quantity*dim_products.product_price)) as revenue
FROM orders_table
JOIN dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products on  orders_table.product_code = dim_products.product_code
JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY sum(orders_table.product_quantity*dim_products.product_price) DESC;

-- Task_7_Staff_count

SELECT SUM(dim_store_details.staff_numbers) AS total_staff_numbers, dim_store_details.country_code
FROM dim_store_details
GROUP BY dim_store_details.country_code
ORDER BY total_staff_numbers DESC

--Task_8_German_store_type

SELECT ROUND(CAST(SUM(orders_table.product_quantity*dim_products.product_price) AS numeric), 2) as sales, dim_store_details.store_type, dim_store_details.country_code
FROM orders_table
JOIN dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products on  orders_table.product_code = dim_products.product_code
JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type,dim_store_details.country_code
ORDER BY sum(orders_table.product_quantity*dim_products.product_price);

--Task_9_averge_time_to_get_an_order
ALTER TABLE dim_date_times
ADD COLUMN time_diff interval;

UPDATE dim_date_times
SET time_diff = x.time_diff
FROM (
  SELECT timestamp, LAG(timestamp) OVER (ORDER BY "day", "month", "year", timestamp DESC ) - timestamp AS time_diff
  FROM dim_date_times
  --order by "day", "month"
) AS x
WHERE dim_date_times.timestamp = x.timestamp;


With h AS (
SELECT "year", "month", timestamp, LAG(timestamp) OVER (ORDER BY "day", "month", "year", timestamp DESC ) - timestamp AS time_diff
FROM dim_date_times		 		  
group by dim_date_times.year, timestamp, "day", "month", "year") 
select "year", abs(AVG(EXTRACT(EPOCH FROM time_diff)::integer))AS avg_absolute_diff
from h
group by "year"
order by "year"