/* Business Category Bridge Table */
CREATE TABLE IF NOT EXISTS target_schema.brdg_business_category
(
    categorie_id INT,
    business_id TEXT,
    CONSTRAINT brdg_business_category_unique_constraint UNIQUE (categorie_id, business_id)
);
CREATE INDEX IF NOT EXISTS idx_brdg_business_category ON target_schema.brdg_business_category (categorie_id, business_id);
INSERT INTO target_schema.brdg_business_category (categorie_id,business_id)
    SELECT DISTINCT
        src_table.categorie_id,
        src_table.business_id
    FROM target_schema.stg_categories AS src_table
ON CONFLICT (categorie_id,business_id)
DO UPDATE SET 
    categorie_id = EXCLUDED.categorie_id,
    business_id = EXCLUDED.business_id;

/* Business Attribute Bridge Table */
CREATE TABLE IF NOT EXISTS target_schema.brdg_business_attribute
(
    attributes_id INT,
    business_id TEXT,
    CONSTRAINT brdg_business_attribute_unique_constraint UNIQUE (attributes_id, business_id)
);
CREATE INDEX IF NOT EXISTS idx_brdg_business_attribute ON target_schema.brdg_business_attribute (attributes_id, business_id);

INSERT INTO target_schema.brdg_business_attribute (attributes_id, business_id)
    SELECT DISTINCT
        src_table.attributes_id,
        src_table.business_id
    FROM target_schema.stg_attributes AS src_table
ON CONFLICT (attributes_id,business_id)
DO UPDATE SET 
    attributes_id = EXCLUDED.attributes_id,
    business_id = EXCLUDED.business_id;

/* Uesr elite user Bridge Table */
CREATE TABLE IF NOT EXISTS target_schema.brdg_elite_user
(
    elite_year_id INT,
    user_id TEXT,
    CONSTRAINT brdg_elite_user_unique_constraint UNIQUE (elite_year_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_brdg_elite_user ON target_schema.brdg_elite_user (elite_year_id, user_id);

INSERT INTO target_schema.brdg_elite_user (elite_year_id, user_id)
    SELECT DISTINCT
        src_table.elite_year_id,
        src_table.user_id
    FROM target_schema.stg_elite_user AS src_table
ON CONFLICT (elite_year_id,user_id)
DO UPDATE SET 
    elite_year_id = EXCLUDED.elite_year_id,
    user_id = EXCLUDED.user_id;


