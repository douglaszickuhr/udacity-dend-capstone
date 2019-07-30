drop table if exists dim_cities cascade;  

CREATE TABLE IF NOT EXISTS dim_cities
(
	city_id VARCHAR(32) NOT NULL,
  state VARCHAR(256),
	city VARCHAR(256),
  PRIMARY KEY (city_id)
)
DISTSTYLE EVEN;

insert into dim_cities
select
  distinct(md5(state || city)) as city_id,
  state,
  city
from
  staging_business;
