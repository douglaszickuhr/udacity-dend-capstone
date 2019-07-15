DROP TABLE staging_tips;
CREATE TABLE staging_tips (user_id VARCHAR, business_id VARCHAR, text VARCHAR(65535), date timestamp, compliment_count INT) distkey(business_id) sortkey(date);

COPY staging_tips
FROM 's3://udacity-dend-capstone-douglas/yelp_academic_dataset_tip.json' 
ACCESS_KEY_ID ''
SECRET_ACCESS_KEY ''
FORMAT AS JSON 'auto';
