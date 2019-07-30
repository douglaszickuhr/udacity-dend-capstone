drop table if exists dim_times cascade;

CREATE TABLE IF NOT EXISTS dim_times
(
	"datetime" TIMESTAMP NOT NULL,
	"hour" INTEGER NOT NULL,
	"minute" INTEGER NOT NULL,
	"day" INTEGER NOT NULL,
	"month" INTEGER NOT NULL,
	"year" INTEGER NOT NULL,
	quarter INTEGER NOT NULL,
	weekday INTEGER NOT NULL,
	yearday INTEGER NOT NULL,
  PRIMARY KEY (datetime)
)
DISTSTYLE EVEN;

insert into dim_times
select
  a.datetime,
  extract(
    hour
    from
      a.datetime
  ) as hour,
  extract(
    minute
    from
      a.datetime
  ) as minute,
  extract(
    day
    from
      a.datetime
  ) as day,
  extract(
    month
    from
      a.datetime
  ) as month,
  extract(
    year
    from
      a.datetime
  ) as year,
  extract(
    qtr
    from
      a.datetime
  ) as quarter,
  extract(
    weekday
    from
      a.datetime
  ) as weekday,
  extract(
    yearday
    from
      a.datetime
  ) as yearday
from
  (
    SELECT
      yelping_since as datetime
    from
      staging_users
    group by
      yelping_since
    union
    select
      date
    from
      staging_reviews
    group by
      date
    union
    select
      date
    from
      staging_tips
    group by
      date
  ) a;
