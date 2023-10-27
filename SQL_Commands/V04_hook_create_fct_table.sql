CREATE TABLE IF NOT EXISTS target_schema.fct_review
(
	review_id TEXT PRIMARY KEY NOT NULL,
    date TIMESTAMP,
    business_id TEXT,
    user_id TEXT,
    text Text,
    stars INT,
    useful INT,
    funny INT,
    cool INT,
    topic TEXT,
    sentiment TEXT,
    score FLOAT

);

CREATE INDEX IF NOT EXISTS "idx_review_id" ON target_schema.fct_review (review_id);

INSERT INTO target_schema.fct_review(review_id, date, business_id, user_id, text, stars, useful, funny, cool,topic, sentiment, score)
SELECT DISTINCT
    src_table.review_id,
    src_table.date, 
    src_table.business_id,
    src_table.user_id,
    src_table.text, 
    src_table.stars, 
    src_table.useful, 
    src_table.funny, 
    src_table.cool,
    topic TEXT,
    sentiment TEXT,
    score FLOAT
FROM target_schema.stg_review AS src_table
ON CONFLICT (review_id)
DO UPDATE SET
    review_id = EXCLUDED.review_id,
    date = EXCLUDED.date,
    business_id = EXCLUDED.business_id,
    user_id = EXCLUDED.user_id,
    text = EXCLUDED.text,
    stars = EXCLUDED.stars,
    useful = EXCLUDED.useful,
    funny = EXCLUDED.funny,
    cool = EXCLUDED.cool,
    topic = EXCLUDED.topic,
    sentiment = EXCLUDED.sentiment,
    score = EXCLUDED.score
