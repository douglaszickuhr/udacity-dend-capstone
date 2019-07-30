-- Creating categories dimension
drop table if exists dim_category cascade;

CREATE TABLE IF NOT EXISTS dim_category
(
	category_id VARCHAR(32) NOT NULL,
	category VARCHAR(max) NOT NULL,
	PRIMARY KEY (category_id)
);

insert into dim_category
select
  md5(a.category) as category_id,
  category
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
      union all
      select
        12
      union all
      select
        13
      union all
      select
        14
      union all
      select
        15
      union all
      select
        16
      union all
      select
        17
      union all
      select
        18
      union all
      select
        19
      union all
      select
        20
      union all
      select
        21
      union all
      select
        22
      union all
      select
        23
      union all
      select
        24
      union all
      select
        25
      union all
      select
        26
      union all
      select
        27
      union all
      select
        28
      union all
      select
        29
      union all
      select
        30
      union all
      select
        31
      union all
      select
        32
      union all
      select
        33
      union all
      select
        34
      union all
      select
        35
      union all
      select
        36
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

-- Creating bridge between business and categories
drop table if exists bridge_business_category cascade;

CREATE TABLE IF NOT EXISTS bridge_business_category
(
	bridge_business_category_id VARCHAR(32) NOT NULL,
	business_id VARCHAR(256) NOT NULL,
	category_id VARCHAR(32) NOT NULL,
	PRIMARY KEY (bridge_business_category_id)
);

insert into bridge_business_category
select
  md5(a.business_id||dim_category.category_id) as bridge_business_category_id,
  a.business_id,
  dim_category.category_id
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
      union all
      select
        12
      union all
      select
        13
      union all
      select
        14
      union all
      select
        15
      union all
      select
        16
      union all
      select
        17
      union all
      select
        18
      union all
      select
        19
      union all
      select
        20
      union all
      select
        21
      union all
      select
        22
      union all
      select
        23
      union all
      select
        24
      union all
      select
        25
      union all
      select
        26
      union all
      select
        27
      union all
      select
        28
      union all
      select
        29
      union all
      select
        30
      union all
      select
        31
      union all
      select
        32
      union all
      select
        33
      union all
      select
        34
      union all
      select
        35
      union all
      select
        36
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
