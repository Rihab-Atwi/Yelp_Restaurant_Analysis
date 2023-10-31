CREATE TABLE IF NOT EXISTS target_schema.agg_business_review_stats (
    business_id TEXT,
    business_name TEXT,
    avg_rating FLOAT,
    review_count INT,
    total_useful INT,
    total_funny INT,
    total_cool INT
);

INSERT INTO target_schema.agg_business_review_stats (
    business_id,
    business_name,
    avg_rating,
    review_count,
    total_useful,
    total_funny,
    total_cool
)
SELECT
    dim_business.business_id AS business_id,
    dim_business.name AS business_name,
    AVG(fct_review.stars) AS avg_rating,
    COUNT(fct_review.review_id) AS review_count,
    SUM(fct_review.useful) AS total_useful,
    SUM(fct_review.funny) AS total_funny,
    SUM(fct_review.cool) AS total_cool
FROM target_schema.dim_business AS dim_business
LEFT OUTER JOIN target_schema.fct_review AS fct_review  
	ON dim_business.business_id = fct_review.business_id
GROUP BY
     dim_business.business_id, dim_business.name;

UPDATE target_schema.agg_business_review_stats AS target
SET 
    avg_rating = subquery.avg_rating,
    review_count = subquery.review_count,
    total_useful = subquery.total_useful,
    total_funny = subquery.total_funny,
    total_cool = subquery.total_cool
FROM (
    SELECT
        dim_business.business_id AS business_id,
        AVG(fct_review.stars) AS avg_rating,
        COUNT(fct_review.review_id) AS review_count,
        SUM(fct_review.useful) AS total_useful,
        SUM(fct_review.funny) AS total_funny,
        SUM(fct_review.cool) AS total_cool
    FROM target_schema.dim_business AS dim_business
    LEFT OUTER JOIN target_schema.fct_review AS fct_review  
	    ON dim_business.business_id = fct_review.business_id
    GROUP BY
        dim_business.business_id
) AS subquery
WHERE target.business_id = subquery.business_id;
