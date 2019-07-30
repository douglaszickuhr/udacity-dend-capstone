drop table if exists dim_users cascade;

CREATE TABLE IF NOT EXISTS dim_users
(
	user_id VARCHAR(256) NOT NULL,
  name VARCHAR(256),
	yelping_since TIMESTAMP,
  PRIMARY KEY (user_id)
)
DISTSTYLE EVEN;

insert into dim_users
select
  user_id,
  name,
  yelping_since
from
  staging_users;
