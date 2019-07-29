drop table if exists fact_tips;

select
  md5(user_id || business_id || date) as tip_id,
  user_id,
  business_id,
  text,
  compliment_count
into fact_tips
from
  staging_tips;
