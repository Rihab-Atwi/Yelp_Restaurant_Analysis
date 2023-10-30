-- 1-checkin views
CREATE OR REPLACE VIEW target_schema.business_checkin_view AS
SELECT
    business.business_id,
    checkin.checkin_time,
    business.city,
    business.state,
    business.stars,
    business.review_count,
    business.is_open
FROM target_schema.dim_checkin checkin
INNER JOIN target_schema.dim_business business 
    ON checkin.business_id = business.business_id;


-- 2-Category view
CREATE OR REPLACE VIEW target_schema.business_category_view AS
SELECT
    business.business_id,
    category.category,
    business.city,
    business.state,
    business.stars,
    business.review_count
FROM target_schema.dim_business business
INNER JOIN target_schema.brdg_business_category bbc 
    ON business.business_id = bbc.business_id
INNER JOIN target_schema.dim_category category 
    ON bbc.categorie_id = category.category_id;