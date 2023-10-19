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
        categorie_id,
        business_id
    FROM target_schema.stg_business
ON CONFLICT (categorie_id,business_id)
DO UPDATE SET 
    categorie_id = EXCLUDED.categorie_id,
    business_id = EXCLUDED.business_id;

/* Business Attribute Bridge Table */
CREATE TABLE IF NOT EXISTS target_schema.brdg_business_attribute
(
    attributes_id INT,
    business_id TEXT,
    CONSTRAINT language_movie_series_unique_constraint UNIQUE (attributes_id, business_id)
);
CREATE INDEX IF NOT EXISTS idx_lang_movie_series ON target_schema.brdg_business_attribute (attributes_id, business_id);

INSERT INTO target_schema.brdg_business_attribute (attributes_id, business_id)
    SELECT DISTINCT
        attributes_id,
        business_id
    FROM target_schema.stg_attributes
ON CONFLICT (attributes_id,business_id)
DO UPDATE SET 
    attributes_id = EXCLUDED.attributes_id,
    business_id = EXCLUDED.business_id;
