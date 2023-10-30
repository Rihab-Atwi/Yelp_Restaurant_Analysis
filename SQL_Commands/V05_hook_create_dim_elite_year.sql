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
