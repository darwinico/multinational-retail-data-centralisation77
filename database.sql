ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(255),
    ALTER COLUMN product_code TYPE VARCHAR(255),
    ALTER COLUMN product_quantity TYPE SMALLINT;

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat::FLOAT);

ALTER TABLE dim_store_details
DROP COLUMN lat;

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

INSERT INTO dim_store_details (store_code, longitude, locality, staff_numbers, opening_date, store_type, latitude, country_code, continent)
VALUES ('WEB-1388012W', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

DELETE FROM dim_products
WHERE removed NOT IN ('Still_avaliable', 'Removed');


ALTER TABLE dim_products
ADD COLUMN product_price_cleaned NUMERIC;

UPDATE dim_products
SET product_price_cleaned = REPLACE(product_price, '£', '')::NUMERIC;

ALTER TABLE dim_products
DROP COLUMN product_price;

ALTER TABLE dim_products
RENAME COLUMN product_price_cleaned TO product_price;

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight_kg < 2 THEN 'Light'
    WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
    WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
    WHEN weight_kg >= 140 THEN 'Truck_Required'
    ELSE 'Unknown'
END;

UPDATE dim_products
SET removed = 'Still_available'
WHERE removed = 'Still_avaliable';







ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available = CASE
    WHEN still_available = 'Still_available' THEN 'TRUE'
    WHEN still_available = 'Removed' THEN 'FALSE'
    ELSE 'FALSE'
END;

ALTER TABLE dim_products
DROP COLUMN IF EXISTS weight;

ALTER TABLE dim_products
RENAME COLUMN weight_kg TO weight;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(255);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(255);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL USING still_available::BOOLEAN;

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(255);

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(255),
    ALTER COLUMN year TYPE VARCHAR(255),
    ALTER COLUMN day TYPE VARCHAR(255),
    ALTER COLUMN time_period TYPE VARCHAR(255),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN timestamp TYPE TIME USING timestamp::TIME;

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE dim_users
    ADD CONSTRAINT pk_dim_users_user_uuid PRIMARY KEY (user_uuid);

ALTER TABLE dim_products
    ADD CONSTRAINT pk_dim_products_product_code PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
    ADD CONSTRAINT pk_dim_date_times_date_uuid PRIMARY KEY (date_uuid);

ALTER TABLE dim_store_details
    ADD CONSTRAINT pk_dim_store_details_store_code PRIMARY KEY (store_code);

ALTER TABLE dim_card_details
    ADD CONSTRAINT pk_dim_card_details_card_number PRIMARY KEY (card_number);



ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_user_uuid
    FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_product_code
    FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_date_uuid
    FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_store_code
    FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_card_number
    FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);