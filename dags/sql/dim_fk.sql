alter table dim_business
add foreign key (city_id) references dim_cities (city_id);

alter table bridge_business_category
add foreign key (business_id) references dim_business(business_id);
alter table bridge_business_category
add foreign key (category_id) references dim_category(category_id);
alter table dim_users
add foreign key (yelping_since) references dim_times(datetime);

alter table fact_review
add foreign key (user_id) references dim_users(user_id);
alter table fact_review
add foreign key (business_id) references dim_business(business_id);
alter table fact_review
add foreign key (date) references dim_times(datetime);

alter table fact_tip
add foreign key (user_id) references dim_users(user_id);
alter table fact_tip
add foreign key (business_id) references dim_business(business_id);
