# multinational-retail-data-centralisation
# Data Centralisation Project

In this project, I have created a powerful data management system using Postgres, AWS S3, boto3, REST API, CSV, and Python (Pandas). The main objectives were to create a local PostgreSQL database, extract data from different sources, clean and process it, and perform SQL queries.

Key technologies used: Postgres, AWS (s3), boto3, rest-API, csv, Python (Pandas). 

## Project Utils
To achieve this, I organized the project into utility files:

1. "data_extraction.py" handles data extraction from various sources into pandas data frames.
2. "data_cleaning.py" contains the "DataCleaning" class for cleaning the extracted tables.
3. "database_utils.py" has the "DatabaseConnector" class for easy data uploading to the database.
4. "main.py" serves as the entry point, allowing efficient data upload and execution of the code.
This project brings together the best technologies to simplify data management and analysis, providing a seamless experience for extracting, cleaning, and querying data in a local PostgreSQL database.

## Step by Step Data Processing

In this project, I explored six data sources:

1. Connected to a remote Postgres database in AWS Cloud, focusing on the "order_table" for sales data. Extracted relevant fields and transformed them into foreign keys, cleaning and ensuring data integrity.

2. Accessed user data from a remote Postgres database (AWS Cloud), using the "dim_users" table as a primary key reference.

3. Retrieved "dim_card_details" data from a public link in AWS Cloud, handling a PDF file, cleaning the card number, and converting it to a string format.

4. Downloaded "dim_product" table from an AWS S3 bucket, converted product price to float and adjusted weight to grams for consistency.

5. Accessed "dim_store_details" data via a RESTful API, converting the JSON response into a pandas dataframe using store codes as primary keys.

6. Retrieved "dim_date_times" data from a link, converting the JSON response into a pandas dataframe using date UUIDs as primary keys.

Throughout this project, my aim was to create a comprehensive and accurate database by efficiently extracting, cleaning, and integrating data from various sources.

#### General Data Cleaning Notes

1. To ensure the integrity of the "primary key" field, all data cleaning is performed with respect to it. Rows in the table are only removed if duplicates, NaNs, or missing values occur in this field. This precaution is necessary to prevent issues where the "foreign key" in the "orders_table" cannot be matched with the corresponding "primary key," potentially causing disruptions in the database schema.

2. The date transformation process addresses different time formats by implementing the following steps:
```
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
```
These conversions accommodate various date representations, allowing for proper handling of the data. Once the clean data is loaded into the database, appropriate formatting is applied, and additional columns are added to enhance the information associated with the data.
Let's consider a typical workflow

1. Convert data fields
```
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE float8 USING product_price::float8
ALTER TABLE dim_products
	ALTER COLUMN "weight (KG)" TYPE float8 USING "weight (KG)"::float8
ALTER TABLE dim_products
	ALTER COLUMN "EAN" TYPE VARCHAR(255) USING "EAN"::VARCHAR(255)
ALTER TABLE dim_products
	ALTER COLUMN product_code TYPE VARCHAR(255) USING product_code::VARCHAR(255)
ALTER TABLE dim_products
	ALTER COLUMN date_added TYPE date USING date_added::date
ALTER TABLE dim_products
	ALTER COLUMN uuid TYPE uuid USING uuid::uuid
ALTER TABLE dim_products
	ALTER COLUMN still_available TYPE bool USING still_available::bool
ALTER TABLE dim_products
	ALTER COLUMN weight_class TYPE VARCHAR(255) USING weight_class::VARCHAR(255)

```

2. Add foreign and primary keys in connected tables

```
ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid);

--- to create foreign key	
ALTER TABLE orders_table 
	ADD CONSTRAINT fk_product FOREIGN KEY(product_code) 
	REFERENCES dim_products(product_code);
	
```
3. To improve store logistics and analysis, we'll add conditional data segmentation columns based on product weight. We'll also convert string-based availability flags to boolean format for better representation.
```
ALTER TABLE dim_products
	ADD weight_class VARCHAR(30);
UPDATE dim_products
	SET weight_class = 
		CASE 
			when weight/1000 < 2 then 'Light'
			when weight/1000 between 2 and 40 then 'Mid_Sized'
			when weight/1000 between 41 and 140 then 'Heavy'
			when weight/1000 > 140 then 'Truck_Required'  
		else 'Invalid' 
		END;
  
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;
  
UPDATE dim_products
	SET still_available = 
		CASE 
			when still_available = 'Still_available' then True
			when still_available = 'Removed' then False
		END;
```

## SQL Queries

As our primary and foreign keys are settled and data are clean, we can start writing queries in our database. 

1. How many stores do the business have and in which countries?
```	
select country_code, 
	count (*) 
from dim_store_details 
group by country_code	
```
<img width="306" alt="Screenshot 2023-05-21 at 01 28 18" src="https://github.com/pearlroys/multinational-retail-data-centralisation/assets/103274172/ed6a5797-59f4-4e3b-91da-0cc810c580f1">



2. How many sales come online?

```
select 	count (orders_table.product_quantity) as numbers_of_sales,
	sum(orders_table.product_quantity) as product_quantity_count,
	case 
		when dim_store_details.store_code = 'WEB-1388012W' then 'Web'
	else 'Offline'
	end as product_location
from orders_table
	join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
	join dim_products on  orders_table.product_code = dim_products.product_code
	join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by product_location
ORDER BY sum(orders_table.product_quantity) ASC;
```
<img width="444" alt="Screenshot 2023-05-21 at 01 30 16" src="https://github.com/pearlroys/multinational-retail-data-centralisation/assets/103274172/cc8d758e-773d-4b20-a089-84202bae3c0a">




3. Which month in the year produced the most sales?

```
select  dim_date_times.year,
		dim_date_times.month, 
		round(sum(orders_table.product_quantity*dim_products.product_price)) as revenue
from orders_table
	join dim_date_times    on  orders_table.date_uuid    = dim_date_times.date_uuid
	join dim_products      on  orders_table.product_code = dim_products.product_code
	join dim_store_details on orders_table.store_code    = dim_store_details.store_code
group by 	dim_date_times.month,
			dim_date_times.year
ORDER BY    sum(orders_table.product_quantity*dim_products.product_price)  DESC;
```

<img width="460" alt="Screenshot 2023-05-21 at 01 31 39" src="https://github.com/pearlroys/multinational-retail-data-centralisation/assets/103274172/9477cc3f-7fd8-4086-b654-01554bb5b6c4">



4. Which German store saling the most?
```
SELECT ROUND(CAST(SUM(orders_table.product_quantity*dim_products.product_price) AS numeric), 2) as sales, dim_store_details.store_type, dim_store_details.country_code
FROM orders_table
JOIN dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products on  orders_table.product_code = dim_products.product_code
JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type,dim_store_details.country_code
ORDER BY sum(orders_table.product_quantity*dim_products.product_price);
```

<img width="427" alt="Screenshot 2023-05-21 at 01 32 58" src="https://github.com/pearlroys/multinational-retail-data-centralisation/assets/103274172/84a5dd02-0667-4470-99b9-1cd9240e4ef1">

9. How quickly company making sales?

```
SELECT
    year,
    JSON_BUILD_OBJECT(
        'hours', ROUND(AVG(EXTRACT(HOUR FROM time_diff))),
        'minutes', ROUND(AVG(EXTRACT(MINUTE FROM time_diff))),
        'seconds', ROUND(AVG(EXTRACT(SECOND FROM time_diff))),
        'milliseconds', ROUND(AVG(EXTRACT(MILLISECONDS FROM time_diff)))
    ) AS actual_time_taken

from (
SELECT "day", "month", "year", timestamp, LAG(timestamp) OVER (PARTITION BY year, month, day ORDER BY "day", "month", "year", timestamp DESC ) - timestamp AS time_diff
FROM dim_date_times	 		  
) as subquery
group by "year"
order by avg(time_diff) desc
```
<img width="562" alt="Screenshot 2023-05-21 at 01 35 07" src="https://github.com/pearlroys/multinational-retail-data-centralisation/assets/103274172/b78f64c7-6489-4539-9913-ec73fd997498">




