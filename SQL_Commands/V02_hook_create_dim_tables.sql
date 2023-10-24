/* Business dimension table */ 
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


    /* Attribute dimension table */ 
CREATE TABLE IF NOT EXISTS target_schema.dim_attribute
(
	attributes_id INTEGER PRIMARY KEY NOT NULL,
	attribute TEXT 
);
CREATE INDEX IF NOT EXISTS "idx_attributes_id" ON target_schema.dim_attribute(attributes_id);
INSERT INTO target_schema.dim_attribute (attributes_id, attribute)
SELECT DISTINCT
	src_table.attributes_id, 
	src_table.filtered_attributes
FROM target_schema.stg_attributes as src_table 
ON CONFLICT (attributes_id)
DO UPDATE SET 
    attribute = EXCLUDED.attribute; 

/* Category dimension table */ 
CREATE TABLE IF NOT EXISTS target_schema.dim_category
(
	category_id INTEGER PRIMARY KEY NOT NULL,
	category TEXT 
);
CREATE INDEX IF NOT EXISTS "idx_category_id" ON target_schema.dim_category(category_id);
INSERT INTO target_schema.dim_category (category_id, category)
SELECT DISTINCT
	src_table.categorie_id, 
	src_table.categories
FROM target_schema.stg_categories as src_table 
ON CONFLICT (category_id)
DO UPDATE SET 
    category = EXCLUDED.category; 
/* Elite YEAR dimension table */
CREATE TABLE IF NOT EXISTS target_schema.dim_elite_year
(
	elite_year_id INTEGER PRIMARY KEY NOT NULL,
	elite_year TEXT 
);
CREATE INDEX IF NOT EXISTS "idx_elite_year_id" ON target_schema.dim_elite_year(elite_year_id);
INSERT INTO target_schema.dim_elite_year (elite_year_id, elite_year)
SELECT DISTINCT
	src_table.elite_year_id, 
	src_table.elite_year
FROM target_schema.stg_elite_user as src_table 
ON CONFLICT (elite_year_id)
DO UPDATE SET 
    elite_year = EXCLUDED.elite_year; 

/* Checkin dimension table */
CREATE TABLE IF NOT EXISTS target_schema.dim_checkin
(
    business_id TEXT,
    checkin_time TIMESTAMP 
);
CREATE INDEX IF NOT EXISTS "idx_business_id" ON target_schema.dim_checkin(business_id);
INSERT INTO target_schema.dim_checkin (business_id, checkin_time)
SELECT DISTINCT
    src_table.business_id, 
    src_table.date
FROM target_schema.stg_checkin as src_table;
ON CONFLICT (business_id, checkin_time) 
DO UPDATE SET 
    checkin_time = EXCLUDED.checkin_time;

/* Business dimension table */
CREATE TABLE IF NOT EXISTS target_schema.dim_user
( 
    user_id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    review_count INT,
    date TIMESTAMP,
    useful INT,
    funny INT,
    cool INT,
    elite INT,
    fans INT,
    average_stars FLOAT,
    compliment_hot INT,
    compliment_more INT,
    compliment_profile INT,
    compliment_cute INT,
    compliment_list INT,
    compliment_note INT,
    compliment_plain INT,
    compliment_cool INT,
    compliment_funny INT,
    compliment_writer INT,
    compliment_photos INT,
    nb_friends INT
);

CREATE INDEX IF NOT EXISTS "idx_user_id" ON target_schema.dim_user(user_id);

INSERT INTO target_schema.dim_user (user_id, name, review_count, date, useful, funny, cool, 
                                        elite, fans, average_stars, compliment_hot, compliment_more, compliment_profile, 
                                        compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, 
                                        compliment_funny, compliment_writer, compliment_photos, nb_friends)
SELECT DISTINCT 
src_table.user_id as user_id,
src_table.name,
src_table.review_count,
src_table.date,
src_table.useful,
src_table.funny,
src_table.cool,
src_table.elite,
src_table.fans,
src_table.average_stars,
src_table.compliment_hot,
src_table.compliment_more,
src_table.compliment_profile,
src_table.compliment_cute,
src_table.compliment_list,
src_table.compliment_note,
src_table.compliment_plain,
src_table.compliment_cool,
src_table.compliment_funny,
src_table.compliment_writer,
src_table.compliment_photos,
src_table.nb_friends
FROM target_schema.stg_user AS src_table
ON CONFLICT(user_id)
DO UPDATE SET  
    name = EXCLUDED.name,
    review_count = EXCLUDED.review_count,
    date = EXCLUDED.date,
    useful = EXCLUDED.useful,
    funny = EXCLUDED.funny,
    cool = EXCLUDED.cool,
    elite = EXCLUDED.elite,
    fans = EXCLUDED.fans,
    average_stars = EXCLUDED.average_stars,
    compliment_hot = EXCLUDED.compliment_hot,
    compliment_more = EXCLUDED.compliment_more,
    compliment_profile = EXCLUDED.compliment_profile,
    compliment_cute = EXCLUDED.compliment_cute,
    compliment_list = EXCLUDED.compliment_list,
    compliment_note = EXCLUDED.compliment_note,
    compliment_plain = EXCLUDED.compliment_plain,
    compliment_cool = EXCLUDED.compliment_cool,
    compliment_funny = EXCLUDED.compliment_funny,
    compliment_writer = EXCLUDED.compliment_writer,
    compliment_photos = EXCLUDED.compliment_photos,
    nb_friends = EXCLUDED.nb_friends;

/* dim_Restaurant_dish_top_10 */
CREATE TABLE IF NOT EXISTS target_schema.dim_restaurant_dish_top_10
(
    business_id TEXT,
    restaurant_name TEXT,
    dish_id INT PRIMARY KEY NOT NULL,
    dish_name TEXT,
    dish_price TEXT,
    price_range INT,
    review_count INT

);
CREATE INDEX IF NOT EXISTS "idx_dish_id" ON target_schema.dim_restaurant_dish_top_10(dish_id);
INSERT INTO target_schema.dim_restaurant_dish_top_10 ( business_id, restaurant_name, dish_id, dish_name, dish_price, price_range, review_count)
SELECT DISTINCT 
    src_table.business_id as business_id, 
    src_table.restaurant_name,
    src_table.dish_id,
    src_table.dish_name,
    src_table.dish_price,
    src_table.price_range,
    src_table.review_count
FROM target_schema.stg_top_10_resto AS src_table
ON CONFLICT(dish_id)
DO UPDATE SET
    business_id = EXCLUDED.business_id, 
    restaurant_name = EXCLUDED.restaurant_name,  
    dish_name = EXCLUDED.dish_name,
    dish_price = EXCLUDED.dish_price,
    price_range = EXCLUDED.price_range,
    review_count = EXCLUDED.review_count;

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