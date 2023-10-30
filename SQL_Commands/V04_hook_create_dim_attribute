/* Attribute dimension table */ 
CREATE TABLE IF NOT EXISTS target_schema.dim_attribute
(
	attributes_id INTEGER PRIMARY KEY NOT NULL,
	attribute TEXT 
);
CREATE INDEX IF NOT EXISTS "idx_attributes_id" ON target_schema.dim_attribute(attributes_id);
INSERT INTO target_schema.dim_attribute (attributes_id, attribute)
SELECT DISTINCT
	src_table.attributes_id, 
	src_table.filtered_attributes
FROM target_schema.stg_attributes as src_table 
ON CONFLICT (attributes_id)
DO UPDATE SET 
    attribute = EXCLUDED.attribute; 

