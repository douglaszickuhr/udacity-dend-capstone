drop table if exists fact_review;

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
into fact_review
from
  staging_reviews;
