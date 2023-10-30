-- 8-bottom resto view for top 10
CREATE OR REPLACE VIEW target_schema.bottom_restaurant_dish AS
SELECT
    dish.business_id,
    dish.restaurant_name,
    dish.dish_id,
    dish.dish_name,
    dish.dish_price,
    dish.price_range,
    dish.review_count,
    business.name,
    business.address,
    business.city,
    business.state,
    business.postal_code,
    business.latitude,
    business.longitude,
    business.stars AS business_stars,
    business.review_count As business_review_count,
    business.is_open,
    review.review_id,
    review.date,
    review.user_id,
    review.text,
    review.stars,
    review.useful,
    review.funny,
    review.cool,
    review.topic,
    review.sentiment,
    review.score,
    attribute.attribute,
    category.category
FROM target_schema.dim_restaurant_dish_bottom_10 dish
INNER JOIN target_schema.dim_business business 
    ON dish.business_id = business.business_id
LEFT JOIN target_schema.fct_review review 
    ON dish.business_id = review.business_id
LEFT JOIN target_schema.brdg_business_attribute bba 
    ON dish.business_id = bba.business_id
LEFT JOIN target_schema.dim_attribute attribute 
    ON bba.attributes_id = attribute.attributes_id
LEFT JOIN target_schema.brdg_business_category bbc 
    ON dish.business_id = bbc.business_id
LEFT JOIN target_schema.dim_category category 
    ON bbc.categorie_id = category.category_id;


-- 9-top resto (continued)
CREATE OR REPLACE VIEW target_schema.top_restaurant_dish AS
SELECT
    dish.business_id,
    dish.restaurant_name,
    dish.dish_id,
    dish.dish_name,
    dish.dish_price,
    dish.price_range,
    dish.review_count,
    business.name,
    business.state,
    business.latitude,
    business.longitude,
    business.stars AS business_stars,
    business.review_count As business_review_count,
    business.is_open,
    review.review_id,
    review.date,
    review.user_id,
    review.stars,
    attribute.attribute,
    category.category
FROM target_schema.dim_restaurant_dish_top_10 dish
INNER JOIN target_schema.dim_business business 
    ON dish.business_id = business.business_id
LEFT JOIN target_schema.fct_review review 
    ON dish.business_id = review.business_id
LEFT JOIN target_schema.brdg_business_attribute bba 
    ON dish.business_id = bba.business_id
LEFT JOIN target_schema.dim_attribute attribute 
    ON bba.attributes_id = attribute.attributes_id
LEFT JOIN target_schema.brdg_business_category bbc 
    ON dish.business_id = bbc.business_id
LEFT JOIN target_schema.dim_category category 
    ON bbc.categorie_id = category.category_id;