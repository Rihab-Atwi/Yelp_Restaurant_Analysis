/* Checkin dimension table */
CREATE TABLE IF NOT EXISTS target_schema.dim_checkin
(
    business_id TEXT,
    checkin_time TIMESTAMP ,
    id INTEGER PRIMARY KEY NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_id" ON target_schema.dim_checkin(id);
INSERT INTO target_schema.dim_checkin (business_id, checkin_time,id)
SELECT DISTINCT
    src_table.business_id, 
    src_table.date,
    src_table.id
FROM target_schema.stg_checkin as src_table
ON CONFLICT(id) 
DO UPDATE SET 
    checkin_time = EXCLUDED.checkin_time,
    business_id = EXCLUDED.business_id;