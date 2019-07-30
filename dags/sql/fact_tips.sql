drop table if exists fact_tip cascade;

CREATE TABLE IF NOT EXISTS fact_tip
(
	tip_id VARCHAR(32) NOT NULL,
  user_id VARCHAR(256) NOT NULL,
	business_id VARCHAR(256) NOT NULL,
	text VARCHAR(65535),
	compliment_count INTEGER,
  PRIMARY KEY (tip_id)
)
DISTSTYLE EVEN;

insert into fact_tip
select
  md5(user_id || business_id || date) as tip_id,
  user_id,
  business_id,
  text,
  compliment_count
from
  staging_tips;
