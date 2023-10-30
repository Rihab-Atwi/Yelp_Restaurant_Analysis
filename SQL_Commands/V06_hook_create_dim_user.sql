/* Business dimension table */
CREATE TABLE IF NOT EXISTS target_schema.dim_user
( 
    user_id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    review_count INT,
    date TIMESTAMP,
    useful INT,
    funny INT,
    cool INT,
    elite INT,
    fans INT,
    average_stars FLOAT,
    compliment_hot INT,
    compliment_more INT,
    compliment_profile INT,
    compliment_cute INT,
    compliment_list INT,
    compliment_note INT,
    compliment_plain INT,
    compliment_cool INT,
    compliment_funny INT,
    compliment_writer INT,
    compliment_photos INT,
    nb_friends INT
);

CREATE INDEX IF NOT EXISTS "idx_user_id" ON target_schema.dim_user(user_id);

INSERT INTO target_schema.dim_user (user_id, name, review_count, date, useful, funny, cool, 
                                        elite, fans, average_stars, compliment_hot, compliment_more, compliment_profile, 
                                        compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, 
                                        compliment_funny, compliment_writer, compliment_photos, nb_friends)
SELECT DISTINCT 
src_table.user_id as user_id,
src_table.name,
src_table.review_count,
src_table.date,
src_table.useful,
src_table.funny,
src_table.cool,
src_table.elite,
src_table.fans,
src_table.average_stars,
src_table.compliment_hot,
src_table.compliment_more,
src_table.compliment_profile,
src_table.compliment_cute,
src_table.compliment_list,
src_table.compliment_note,
src_table.compliment_plain,
src_table.compliment_cool,
src_table.compliment_funny,
src_table.compliment_writer,
src_table.compliment_photos,
src_table.nb_friends
FROM target_schema.stg_user AS src_table
ON CONFLICT(user_id)
DO UPDATE SET  
    name = EXCLUDED.name,
    review_count = EXCLUDED.review_count,
    date = EXCLUDED.date,
    useful = EXCLUDED.useful,
    funny = EXCLUDED.funny,
    cool = EXCLUDED.cool,
    elite = EXCLUDED.elite,
    fans = EXCLUDED.fans,
    average_stars = EXCLUDED.average_stars,
    compliment_hot = EXCLUDED.compliment_hot,
    compliment_more = EXCLUDED.compliment_more,
    compliment_profile = EXCLUDED.compliment_profile,
    compliment_cute = EXCLUDED.compliment_cute,
    compliment_list = EXCLUDED.compliment_list,
    compliment_note = EXCLUDED.compliment_note,
    compliment_plain = EXCLUDED.compliment_plain,
    compliment_cool = EXCLUDED.compliment_cool,
    compliment_funny = EXCLUDED.compliment_funny,
    compliment_writer = EXCLUDED.compliment_writer,
    compliment_photos = EXCLUDED.compliment_photos,
    nb_friends = EXCLUDED.nb_friends;

