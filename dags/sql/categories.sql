drop
  table dim_category;
select
  md5(a.category) as category_id,
  category into dim_category
from
  (
    with NS AS (
      select
        1 as n
      union all
      select
        2
      union all
      select
        3
      union all
      select
        4
      union all
      select
        5
      union all
      select
        6
      union all
      select
        7
      union all
      select
        8
      union all
      select
        9
      union all
      select
        10
      union all
      select
        11
    )
    select
      TRIM(
        SPLIT_PART(
          staging_business.categories, ',',
          NS.n
        )
      ) AS category
    from
      NS
      inner join staging_business ON NS.n <= REGEXP_COUNT(
        staging_business.categories, ','
      ) + 1
    group by
      category
  ) a;
drop
  table bridge_business_category;
select
  a.business_id,
  dim_category.category_id into bridge_business_category
from
  (
    with NS AS (
      select
        1 as n
      union all
      select
        2
      union all
      select
        3
      union all
      select
        4
      union all
      select
        5
      union all
      select
        6
      union all
      select
        7
      union all
      select
        8
      union all
      select
        9
      union all
      select
        10
      union all
      select
        11
    )
    select
      staging_business.business_id,
      TRIM(
        SPLIT_PART(
          staging_business.categories, ',',
          NS.n
        )
      ) AS category
    from
      NS
      inner join staging_business ON NS.n <= REGEXP_COUNT(staging_business.categories, '') + 1
  ) a
  inner join dim_category on a.category = dim_category.category;
