drop table if exists dim_users;
select
  user_id,
  name,
  yelping_since
into dim_users
from
  staging_users;
