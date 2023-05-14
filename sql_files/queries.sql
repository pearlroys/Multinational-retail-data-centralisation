SELECT country_code AS Country, count (*) total_no_stores
FROM dim_store_details 
GROUP BY country_code
ORDER BY count (*) DESC;