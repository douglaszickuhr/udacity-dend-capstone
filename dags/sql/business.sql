drop table if exists dim_business cascade;

CREATE TABLE IF NOT EXISTS dim_business
(
	business_id VARCHAR(256) NOT NULL,
  name VARCHAR(256) NOT NULL,
	latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION,
	city_id VARCHAR(32) NOT NULL,
	full_address VARCHAR(65535),
  PRIMARY KEY (business_id)
)
DISTSTYLE EVEN;

insert into dim_business
select
  b.business_id,
  b.name,
  b.latitude,
  b.longitude,
  c.city_id,
  b.full_address
from
  staging_business b
  left join dim_cities c on b.state = c.state
  and b.city = c.city;
