drop table if exists fact_review;

CREATE TABLE IF NOT EXISTS fact_review
(
	review_id VARCHAR(256) NOT NULL,
  user_id VARCHAR(256) NOT NULL,
	business_id VARCHAR(256) NOT NULL,
	stars DOUBLE PRECISION,
	useful INTEGER,
	funny INTEGER,
	cool INTEGER,
	text VARCHAR(65535),
	date TIMESTAMP,
  PRIMARY KEY (review_id)
)
DISTSTYLE EVEN;

insert into fact_review
select
  review_id,
  user_id,
  business_id,
  stars,
  useful,
  funny,
  cool,
  text,
  date
from
  staging_reviews;
