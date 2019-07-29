drop table if exists dim_cities;

select
  distinct(
    md5(state || city)
  ) as city_id,
  state,
  city into dim_cities
from
  staging_business;
