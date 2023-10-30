
/* 3_Category dimension table */ 
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
