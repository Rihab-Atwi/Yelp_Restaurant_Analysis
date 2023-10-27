-- 1-checkin views
CREATE OR REPLACE VIEW target_schema.business_checkin_view AS
SELECT
    b.business_id,
    c.checkin_time,
    b.city,
    b.state,
    b.stars,
    b.review_count,
    b.is_open
FROM target_schema.dim_checkin c
INNER JOIN target_schema.dim_business b ON c.business_id = b.business_id;

-- 2-Category view
CREATE OR REPLACE VIEW target_schema.business_category_view AS
SELECT
    b.business_id,
    dc.category,
    b.city,
    b.state,
    b.stars,
    b.review_count
FROM target_schema.dim_business b
INNER JOIN target_schema.brdg_business_category bbc ON b.business_id = bbc.business_id
INNER JOIN target_schema.dim_category dc ON bbc.categorie_id = dc.category_id;

-- 3-attributes view
CREATE OR REPLACE VIEW target_schema.business_attribute_view AS
SELECT
    b.business_id,
    da.attribute,
    b.city,
    b.state,
    b.stars,
    b.review_count
FROM target_schema.dim_business b
INNER JOIN target_schema.brdg_business_attribute bba ON b.business_id = bba.business_id
INNER JOIN target_schema.dim_attribute da ON bba.attributes_id = da.attributes_id;

-- 4-hour view
CREATE OR REPLACE VIEW target_schema.view_business_checkin_hour AS
SELECT
    b.business_id,
    EXTRACT(HOUR FROM c.checkin_time) AS checkin_hour,
    b.city,
    b.state,
    b.stars,
    b.review_count,
    b.is_open
FROM target_schema.dim_checkin c
INNER JOIN target_schema.dim_business b ON c.business_id = b.business_id;

-- 5-view review
CREATE OR REPLACE VIEW target_schema.review_view AS
SELECT
    fr.review_id,
    fr.date,
    fr.business_id,
    fr.user_id,
    fr.text,
    fr.stars,
    fr.useful,
    fr.funny,
    fr.cool,
    fr.topic,
    fr.sentiment,
    fr.score,
    db.state AS business_state,
    db.is_open,
    du.average_stars
FROM target_schema.fct_review fr
INNER JOIN target_schema.dim_business db ON fr.business_id = db.business_id
INNER JOIN target_schema.dim_user du ON fr.user_id = du.user_id;

-- 6-negative review
CREATE OR REPLACE VIEW target_schema.negative_review_view AS
SELECT
    fr.review_id,
    fr.date,
    fr.business_id,
    fr.user_id,
    fr.text,
    fr.stars,
    fr.useful,
    fr.funny,
    fr.cool,
    fr.topic,
    fr.sentiment,
    fr.score,
    db.state AS business_state,
    db.is_open,
    CASE
        WHEN du.average_stars BETWEEN 0 AND 1 THEN '0-1'
        WHEN du.average_stars BETWEEN 1 AND 2 THEN '1-2'
        WHEN du.average_stars BETWEEN 2 AND 3 THEN '2-3'
        WHEN du.average_stars BETWEEN 3 AND 4 THEN '3-4'
        WHEN du.average_stars BETWEEN 4 AND 5 THEN '4-5'
        ELSE 'Unknown'
    END AS average_stars_bin
FROM target_schema.fct_review fr
INNER JOIN target_schema.dim_business db ON fr.business_id = db.business_id
INNER JOIN target_schema.dim_user du ON fr.user_id = du.user_id
WHERE fr.sentiment = 'Negative';

-- 7-positive review
CREATE OR REPLACE VIEW target_schema.positive_review_view AS
SELECT
    fr.review_id,
    fr.date,
    fr.business_id,
    fr.user_id,
    fr.text,
    fr.stars,
    fr.useful,
    fr.funny,
    fr.cool,
    fr.topic,
    fr.sentiment,
    fr.score,
    db.state AS business_state,
    db.is_open,
    CASE
        WHEN du.average_stars BETWEEN 0 AND 1 THEN '0-1'
        WHEN du.average_stars BETWEEN 1 AND 2 THEN '1-2'
        WHEN du.average_stars BETWEEN 2 AND 3 THEN '2-3'
        WHEN du.average_stars BETWEEN 3 AND 4 THEN '3-4'
        WHEN du.average_stars BETWEEN 4 AND 5 THEN '4-5'
        ELSE 'Unknown'
    END AS average_stars_bin
FROM target_schema.fct_review fr
INNER JOIN target_schema.dim_business db ON fr.business_id = db.business_id
INNER JOIN target_schema.dim_user du ON fr.user_id = du.user_id
WHERE fr.sentiment = 'Positive';

-- 8-bottom resto view for top 10
CREATE OR REPLACE VIEW target_schema.bottom_restaurant_dish AS
SELECT
    rd.business_id,
    rd.restaurant_name,
    rd.dish_id,
    rd.dish_name,
    rd.dish_price,
    rd.price_range,
    rd.review_count,
    db.name AS business_name,
    db.address,
    db.city,
    db.state,
    db.postal_code,
    db.latitude,
    db.longitude,
    db.stars,
    db.review_count AS business_review_count,
    db.is_open,
    fr.review_id,
    fr.date,
    fr.user_id,
    fr.text,
    fr.stars AS review_stars,
    fr.useful,
    fr.funny,
    fr.cool,
    fr.topic,
    fr.sentiment,
    fr.score,
    da.attribute AS attribute_name,
    dc.category AS category_name
FROM target_schema.dim_restaurant_dish_bottom_10 rd
INNER JOIN target_schema.dim_business db ON rd.business_id = db.business_id
LEFT JOIN target_schema.fct_review fr ON rd.business_id = fr.business_id
LEFT JOIN target_schema.brdg_business_attribute bba ON rd.business_id = bba.business_id
LEFT JOIN target_schema.dim_attribute da ON bba.attributes_id = da.attributes_id
LEFT JOIN target_schema.brdg_business_category bbc ON rd.business_id = bbc.business_id
LEFT JOIN target_schema.dim_category dc ON bbc.categorie_id = dc.category_id;

-- 9-top resto (continued)
CREATE OR REPLACE VIEW target_schema.top_restaurant_dish AS
SELECT
    rd.business_id,
    rd.restaurant_name,
    rd.dish_id,
    rd.dish_name,
    rd.dish_price,
    rd.price_range,
    rd.review_count,
    db.name AS business_name,
    db.state,
    db.latitude,
    db.longitude,
    db.stars,
    db.review_count AS business_review_count,
    db.is_open,
    fr.review_id,
    fr.date,
    fr.user_id,
    fr.stars AS review_stars,
    da.attribute AS attribute_name,
    dc.category AS category_name
FROM target_schema.dim_restaurant_dish_top_10 rd
INNER JOIN target_schema.dim_business db ON rd.business_id = db.business_id
LEFT JOIN target_schema.fct_review fr ON rd.business_id = fr.business_id
LEFT JOIN target_schema.brdg_business_attribute bba ON rd.business_id = bba.business_id
LEFT JOIN target_schema.dim_attribute da ON bba.attributes_id = da.attributes_id
LEFT JOIN target_schema.brdg_business_category bbc ON rd.business_id = bbc.business_id
LEFT JOIN target_schema.dim_category dc ON bbc.categorie_id = dc.category_id;
