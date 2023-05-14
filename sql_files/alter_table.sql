-- alter order_table
ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid
ALTER TABLE orders_table
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid
ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(255) USING card_number::VARCHAR(255)
ALTER TABLE orders_table
	ALTER COLUMN store_code TYPE VARCHAR(255) USING store_code::VARCHAR(255)
ALTER TABLE orders_table
	ALTER COLUMN product_code TYPE VARCHAR(255) USING product_code::VARCHAR(255)
ALTER TABLE orders_table
	ALTER COLUMN product_quantity TYPE smallint USING product_quantity::smallint


-- alter users table
ALTER TABLE dim_users
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::VARCHAR(255)
ALTER TABLE dim_users
	ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::VARCHAR(255)
ALTER TABLE dim_users
	ALTER COLUMN country_code TYPE VARCHAR(255) USING country_code::VARCHAR(255)
ALTER TABLE dim_users
	ALTER COLUMN date_of_birth TYPE date USING date_of_birth::date
ALTER TABLE dim_users
	ALTER COLUMN join_date TYPE date USING join_date::date

--alter dim_store_details

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE float8 USING longitude::float8
ALTER TABLE dim_store_details
	ALTER COLUMN latitude TYPE float8 USING latitude::float8
ALTER TABLE dim_store_details
	ALTER COLUMN staff_numbers TYPE smallint USING staff_numbers::smallint
ALTER TABLE dim_store_details
	ALTER COLUMN locality TYPE VARCHAR(255) USING locality::VARCHAR(255)
ALTER TABLE dim_store_details
	ALTER COLUMN store_code TYPE VARCHAR(255) USING store_code::VARCHAR(255)
ALTER TABLE dim_store_details
	ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::VARCHAR(255)
ALTER TABLE dim_store_details
	ALTER COLUMN store_type DROP NOT NULL; -- to make nullable
ALTER TABLE dim_store_details
	ALTER COLUMN country_code TYPE VARCHAR(255) USING country_code::VARCHAR(255)
ALTER TABLE dim_store_details
	ALTER COLUMN opening_date TYPE date USING opening_date::date
ALTER TABLE dim_store_details
	ALTER COLUMN continent TYPE VARCHAR(255) USING continent::VARCHAR(255)


--alter dim_products
select "weight (KG)"
from dim_products
WHERE dim_products."weight (KG)" > 50

SELECT *
FROM dim_products

-- remove pound sign from column
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '%£%';

-- drop product_price pound column
ALTER TABLE dim_products DROP COLUMN "product_price (£)";

--create weight_class
ALTER TABLE dim_products
	ADD weight_class VARCHAR(30);
UPDATE dim_products
	SET weight_class = 
		CASE 
			when "weight (KG)" < 2 then 'Light'
			when "weight (KG)" between 2 and 40 then 'Mid_Sized'
			when "weight (KG)" between 41 and 140 then 'Heavy'
			when "weight (KG)" > 140 then 'Truck_Required'  
		else 'Invalid' 
		END;
-- rename column  
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;
--alter products table	
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

SELECT DISTINCT still_available
FROM dim_products;




UPDATE dim_products
	SET still_available = 
		CASE 
			when still_available = 'Still_avaliable' then True
			when still_available = 'Removed' then False
		END;

--- alter date time table

select *
from dim_date_times

--alter pdate table	
ALTER TABLE dim_date_times
	ALTER COLUMN "month" TYPE VARCHAR(255) USING "month"::VARCHAR(255)
ALTER TABLE dim_date_times
	ALTER COLUMN "year" TYPE VARCHAR(255) USING "year"::VARCHAR(255)
ALTER TABLE dim_date_times
	ALTER COLUMN "day" TYPE VARCHAR(255) USING "day"::VARCHAR(255)
ALTER TABLE dim_date_times
	ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid
ALTER TABLE dim_date_times
	ALTER COLUMN time_period TYPE VARCHAR(255) USING time_period::VARCHAR(255)

select *
from dim_card_details

--alter date table	
ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(255) USING card_number::VARCHAR(255)
ALTER TABLE dim_card_details
	ALTER COLUMN expiry_date TYPE VARCHAR(255) USING expiry_date::VARCHAR(255)
ALTER TABLE dim_card_details
	ALTER COLUMN date_payment_confirmed TYPE date USING date_payment_confirmed::date



-- CREATING THE STAR SCHEMA WITH DIMENSION AND FACT TABLES
SELECT *
FROM dim_card_details
where card_number = 'NULL'


-- deleting null values
DELETE FROM dim_card_details
where card_number = 'NULL'


-- cleaning rows with obvious mistakes
UPDATE dim_store_details
SET staff_numbers = 39
WHERE staff_numbers = '3n9';



-- to create primary key
ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);
ALTER TABLE dim_card_details
	ADD PRIMARY KEY (card_number);
ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid);

--- to create foreign key	
ALTER TABLE orders_table 
	ADD CONSTRAINT fk_product FOREIGN KEY(product_code) 
	REFERENCES dim_products(product_code);
	
	
ALTER TABLE orders_table 
	ADD CONSTRAINT fk_users FOREIGN KEY(user_uuid) 
	REFERENCES dim_users(user_uuid);
	
ALTER TABLE orders_table
	ADD CONSTRAINT fk_store FOREIGN KEY(store_code) 
	REFERENCES dim_store_details(store_code);
	
ALTER TABLE orders_table
	ADD CONSTRAINT fk_card FOREIGN KEY(card_number) 
	REFERENCES dim_card_details(card_number);
	
ALTER TABLE orders_table
	ADD CONSTRAINT fk_date FOREIGN KEY(date_uuid) 
	REFERENCES dim_date_times(date_uuid);

-- to find rows in the fact table(orders_table) that are not present in the dimension table

SELECT DISTINCT(COUNT(orders_table.store_code)) as diff
FROM orders_table
LEFT JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.store_code IS NULL;	

SELECT DISTINCT(orders_table.card_number) as diff
FROM orders_table
LEFT JOIN dim_card_details ON orders_table.card_number = dim_card_details.card_number
WHERE dim_card_details.card_number IS NULL;


FROM orders_table
LEFT JOIN dim_card_details ON orders_table.card_number = dim_card_details.card_number
WHERE dim_card_details.card_number IS NULL;	



