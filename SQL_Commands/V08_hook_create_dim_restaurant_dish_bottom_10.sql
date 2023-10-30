/* dim_restaurant_dish_bottom_10 */
CREATE TABLE IF NOT EXISTS target_schema.dim_restaurant_dish_bottom_10
(
    business_id TEXT,
    restaurant_name TEXT,
    dish_id INT PRIMARY KEY NOT NULL,
    dish_name TEXT,
    dish_price TEXT,
    price_range INT,
    review_count INT

);
CREATE INDEX IF NOT EXISTS "idx_dish_id" ON target_schema.dim_restaurant_dish_bottom_10(dish_id);
INSERT INTO target_schema.dim_restaurant_dish_bottom_10 ( business_id, restaurant_name, dish_id, dish_name, dish_price, price_range, review_count)
SELECT DISTINCT 
    src_table.business_id as business_id, 
    src_table.restaurant_name,
    src_table.dish_id,
    src_table.dish_name,
    src_table.dish_price,
    src_table.price_range,
    src_table.review_count
FROM target_schema.stg_bottom_10_resto AS src_table
ON CONFLICT(dish_id)
DO UPDATE SET  
    business_id = EXCLUDED.business_id,
    restaurant_name = EXCLUDED.restaurant_name,  
    dish_name = EXCLUDED.dish_name,
    dish_price = EXCLUDED.dish_price,
    price_range = EXCLUDED.price_range,
    review_count = EXCLUDED.review_count;
