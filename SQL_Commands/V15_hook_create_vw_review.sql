-- 5-view review
CREATE OR REPLACE VIEW target_schema.review_view AS
SELECT
    review.review_id,
    review.date,
    review.business_id,
    review.user_id,
    review.text,
    review.stars,
    review.useful,
    review.funny,
    review.cool,
    review.topic,
    review.sentiment,
    review.score,
    business.state,
    business.is_open,
    "user"."average_stars"
FROM target_schema.fct_review AS review
INNER JOIN target_schema.dim_business AS business 
    ON review.business_id = business.business_id
INNER JOIN target_schema.dim_user AS "user"  -- Use AS to alias the table correctly
    ON review.user_id = "user".user_id;


-- 6-negative review
CREATE OR REPLACE VIEW target_schema.negative_review_view AS
SELECT
    review.review_id,
    review.date,
    review.business_id,
    review.user_id,
    review.text,
    review.stars,
    review.useful,
    review.funny,
    review.cool,
    review.topic,
    review.sentiment,
    review.score,
    business.state,
    business.is_open,
    CASE
        WHEN "user"."average_stars" BETWEEN 0 AND 1 THEN '0-1'
        WHEN "user"."average_stars" BETWEEN 1 AND 2 THEN '1-2'
        WHEN "user"."average_stars" BETWEEN 2 AND 3 THEN '2-3'
        WHEN "user"."average_stars" BETWEEN 3 AND 4 THEN '3-4'
        WHEN "user"."average_stars" BETWEEN 4 AND 5 THEN '4-5'
        ELSE 'Unknown'
    END AS average_stars_bin
FROM target_schema.fct_review review
INNER JOIN target_schema.dim_business business 
    ON review.business_id = business.business_id
INNER JOIN target_schema.dim_user "user"
    ON review.user_id = "user".user_id
WHERE review.sentiment = 'Negative';

-- 7-positive review
CREATE OR REPLACE VIEW target_schema.positive_review_view AS
SELECT
    review.review_id,
    review.date,
    review.business_id,
    review.user_id,
    review.text,
    review.stars,
    review.useful,
    review.funny,
    review.cool,
    review.topic,
    review.sentiment,
    review.score,
    business.state,
    business.is_open,
    CASE
        WHEN "user"."average_stars" BETWEEN 0 AND 1 THEN '0-1'
        WHEN "user"."average_stars" BETWEEN 1 AND 2 THEN '1-2'
        WHEN "user"."average_stars" BETWEEN 2 AND 3 THEN '2-3'
        WHEN "user"."average_stars" BETWEEN 3 AND 4 THEN '3-4'
        WHEN "user"."average_stars" BETWEEN 4 AND 5 THEN '4-5'
        ELSE 'Unknown'
    END AS average_stars_bin
FROM target_schema.fct_review AS review
INNER JOIN target_schema.dim_business AS business 
    ON review.business_id = business.business_id
INNER JOIN target_schema.dim_user AS "user"  -- Use AS to alias the table correctly
    ON review.user_id = "user".user_id
WHERE review.sentiment = 'Positive';