-- 3-attributes view
CREATE OR REPLACE VIEW target_schema.business_attribute_view AS
SELECT
    business.business_id,
    attribute.attribute,
    business.city,
    business.state,
    business.stars,
    business.review_count
FROM target_schema.dim_business business
INNER JOIN target_schema.brdg_business_attribute bba 
    ON business.business_id = bba.business_id
INNER JOIN target_schema.dim_attribute attribute 
    ON bba.attributes_id = attribute.attributes_id;


-- 4-hour view
CREATE OR REPLACE VIEW target_schema.view_business_checkin_hour AS
SELECT
    business.business_id,
    EXTRACT(HOUR FROM checkin.checkin_time) AS checkin_hour,
    business.city,
    business.state,
    business.stars,
    business.review_count,
    business.is_open
FROM target_schema.dim_checkin checkin
INNER JOIN target_schema.dim_business business 
    ON checkin.business_id = business.business_id;