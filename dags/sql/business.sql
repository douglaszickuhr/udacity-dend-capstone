drop table if exists dim_cities;
select
  distinct(
    md5(state || city)
  ) as city_id,
  state,
  city into dim_cities
from
  staging_business;
drop
  table if exists dim_business;
select
  b.business_id,
  b.name,
  b.latitude,
  b.longitude,
  c.city_id,
  b.full_address into dim_business
from
  staging_business b
  left join dim_cities c on b.state = c.state
  and b.city = c.city;
