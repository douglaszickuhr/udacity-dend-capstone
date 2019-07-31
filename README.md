# Data Engineering Nanodegree - Capstone Project

## Introduction
This is the final project for the Data Engineer Nanodegree. Udacity gives us the option to use their suggested project or pick one dataset and scope it by ourselves. In my case I went for the second option. The dataset I will use on this project is from a service called [Yelp](https://www.yelp.ie), which basically stores business reviews given by customers.

## Dataset
The dataset was found on [Kaggle](https://www.kaggle.com/yelp-dataset/yelp-dataset) and was uploaded by Yelp team for a competition called **Yelp Dataset Challenge** which they were looking users to analyse their data and find interesting patterns or insights using NLP techniques (sentiment analysis for instance) and graph mining.

According to the description, in total there are:

- 5,200,000 user reviews
- Information on 174,000 businesses
- The data spans 11 metropolitan areas

At the Kaggle page, there is a link for some [documentation](https://www.yelp.com/dataset/documentation/json), which unfortunately is not available anymore.

### Source Files
There are in total five JSON files included in the original data source:
- `yelp_academic_dataset_business.json`
- `yelp_academic_dataset_checkin.json`
- `yelp_academic_dataset_review.json`
- `yelp_academic_dataset_tip.json`
- `yelp_academic_dataset_user.json`

I eventually pre-process `yelp_academic_dataset_business.json` to make it a `csv` file, as the project request at least two different files format.

### Storage
The files were uploaded to a [S3 bucket](s3://udac-dend-capstone-dz/), which is open for access. The total space utilised on that bucket is approximately 8 gb, which is a considerable amount of data.


## Project Scope
The scope of this project is to read data from Amazon S3 and load it on Amazon Redshift, later process the data in order to create dimensions and facts.

Finally some data quality checks are applied.

The idea is to create dimensions and facts following the `Snowflake` schema as some of the relationships are many-to-many which is not supported by Star Schema.

The outcome is a set of tables that make easier complex queries and at the same time tidy the data.


## Tooling
The tools utilised on this project are the same as we have been learning during the course of this Nanodegree.

- `Amazon S3` for File Storage
- `Amazon Redshift` for Data Storage
- `Apache Airflow` as an Orchestration Tool

Those tools are widely utilised and considered industry standards. The community is massive and the tools provide support to several features.

Apache Airflow, in special, gives freedom to create new plugins and adapt it to any needs that we might have. There are also several plugins available to use.

## Data Model
The final data model include seven tables, being five of them dimensions and two facts.



## Scenarios
The following scenarios were requested to be addressed:

1. **The data was increased by 100x.** That wouldn't be a technical issue as both Amazon tools are commonly utilised in huge amount of data. Eventually the Redshift cluster would have to grow.

2. **The pipelines would be run on a daily basis by 7 am every day.** That's perfectly plausible and could be done utilising Airflow `DAG`· definitions.

3. **The database needed to be accessed by 100+ people.** That wouldn't be a problem as Redshift is highly scalable.

## Pipeline Execution

### Data Ingestion
The first step is read the data from S3 into Redshift. This is done through the `S3ToRedshift` Operator. In this project I've decided to use a plugin that is available on [Airflow-Plugins](https://github.com/airflow-plugins/redshift_plugin) Github page.

That plugin has some advantages over the `contrib` option.

![picture](https://i.ibb.co/GMNwWdR/Screenshot-2019-07-31-at-20-20-06.png)
