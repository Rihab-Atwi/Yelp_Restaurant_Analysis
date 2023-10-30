/* 1_Business dimension table */ 
CREATE TABLE IF NOT EXISTS target_schema.dim_business
(
    business_id TEXT PRIMARY KEY NOT NULL, 
    name TEXT, 
    address TEXT, 
    city TEXT, 
    state TEXT, 
    postal_code TEXT,
    latitude FLOAT,
    longitude FLOAT,
    stars FLOAT,
    review_count INT,
    is_open INT

);
CREATE INDEX IF NOT EXISTS "idx_business_id" ON target_schema.dim_business(business_id);
INSERT INTO target_schema.dim_business (business_id, name, address, city, state, postal_code,latitude,longitude, stars, review_count, is_open)
SELECT DISTINCT 
    src_table.business_id as business_id, 
    src_table.name, 
    src_table.address, 
    src_table.city, 
    src_table.state, 
    src_table.postal_code,
    src_table.latitude,
    src_table.longitude,
    src_table.stars,
    src_table.review_count,
    src_table.is_open
FROM target_schema.stg_business AS src_table
ON CONFLICT(business_id)
DO UPDATE SET 
    name = EXCLUDED.name, 
    address = EXCLUDED.address, 
    city = EXCLUDED.city, 
    state = EXCLUDED.state, 
    postal_code = EXCLUDED.postal_code,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    stars = EXCLUDED.stars,
    review_count = EXCLUDED.review_count,
    is_open = EXCLUDED.is_open;


    