# -*- coding: utf-8 -*-
"""
Created on Wed Sep  8 14:50:55 2021

@author: Marie

"""

populate_facts_tags_query = '''

INSERT INTO datamart.facts_tags(video_id, tag_id)
VALUES(%s, %s)

'''



count_facts_tags_query = '''

SELECT COUNT(*) FROM datamart.facts_tags

'''


populate_dim_tags_query = '''
INSERT INTO datamart.DIM_tags(tag, tag_id)
VALUES(%s, %s)
'''

count_dim_tags_query = '''

SELECT COUNT(*) FROM datamart.DIM_tags

'''


create_jt_query = '''
CREATE TABLE IF NOT EXISTS datamart.facts_tags(
    tag_id INT references datamart.dim_tags(tag_id),
    video_id VARCHAR(25) references datamart.dim_videos(video_id),
    PRIMARY KEY(tag_id, video_id))
'''


create_facts_table_query = '''

CREATE TABLE IF NOT EXISTS datamart.FACTS_trending_youtube_data(
    video_id VARCHAR(25) references datamart.dim_videos(video_id),
    trend_date_id TEXT references datamart.dim_date(date),
    country_code VARCHAR(2) references datamart.DIM_countries(country_code),
    views INT,
    likes INT,
    dislikes INT,
    comment_count INT, 
    comments_disabled INT,
    ratings_disabled INT,
    video_error_or_removed INT, 
    days_until_trending INT,
    PRIMARY KEY(video_id, trend_date_id, country_code)
    )

'''


populate_dim_videos_table_query = '''
INSERT INTO datamart.DIM_videos(video_id, channel_title, title, description, pub_date, category_name)
VALUES(%s, %s, %s, %s, %s, %s)

'''


count_dim_video_query = '''

SELECT COUNT(*)
FROM datamart.DIM_videos

'''

create_dim_videos_query = '''

CREATE TABLE IF NOT EXISTS datamart.DIM_videos(
    video_id VARCHAR(25) PRIMARY KEY,
    channel_title TEXT,
    category_name VARCHAR(30),
    title TEXT, 
    description TEXT,
    pub_date TEXT
    )

'''



create_publish_date_view_query = '''
CREATE OR REPLACE VIEW datamart.VIEW_pub_date AS 
SELECT * FROM datamart.DIM_date
'''

create_trend_date_view_query = '''
CREATE OR REPLACE VIEW datamart.VIEW_trend_date AS 
SELECT * FROM datamart.DIM_date
WHERE year > 2016
'''

count_dim_countries_query = '''

SELECT COUNT(*)
FROM datamart.DIM_countries

'''

populate_dim_country_table_query = '''
INSERT INTO datamart.DIM_countries(country_code, country_name)
VALUES(%s, %s)

'''


create_dim_country_table_query = '''

CREATE TABLE IF NOT EXISTS datamart.DIM_countries(
    country_code varchar(2) PRIMARY KEY,
    country_name varchar(20)
    )

'''



populate_dim_date_table_query = '''
INSERT INTO datamart.DIM_date(date, year, month, day, weekday_num, is_holidayUS,
                              is_holidayCA, is_holidayIN, is_holidayGB, is_holidayFR,
                              is_holidayDE, is_holidayJP, is_holidayKR, is_holidayRU, 
                              is_holidayMX, is_weekend)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

'''



count_dim_date_query = '''

SELECT COUNT(*)
FROM datamart.DIM_date
'''




create_dim_date_table_query = '''
CREATE TABLE IF NOT EXISTS datamart.DIM_date(
date TEXT PRIMARY KEY,
year INT,
month INT, 
day INT, 
weekday_num INT, 
is_holidayUS INT,
is_holidayCA INT,
is_holidayIN INT, 
is_holidayGB INT,
is_holidayFR INT,
is_holidayDE INT,
is_holidayJP INT,
is_holidayKR INT, 
is_holidayRU INT,
is_holidayMX INT, 
is_weekend INT
);
'''

create_categories_table = """
CREATE TABLE IF NOT EXISTS categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(30)
    );"""

create_yt_all_countries_table = """
CREATE TABLE IF NOT EXISTS trending_data (
    video_id VARCHAR(20),
    trending_date TIMESTAMP,
    title TEXT,
    channel_title TEXT,
    category_id INT REFERENCES categories(category_id),
    publish_time TIMESTAMP,
    tags TEXT,
    views INT,
    likes INT,
    dislikes INT,
    comment_count INT,
    comments_disabled BOOL,
    ratings_disabled BOOL,
    video_error_or_removed BOOL,
    description TEXT,
    country CHAR(2),
    days_until_trending INT,
    publish_time_wd CHAR(3),
    trending_date_wd CHAR(3),
    like_per_view FLOAT,
    dislike_per_view FLOAT,
    comment_per_view FLOAT
    );"""

populate_category_table_query = '''
INSERT INTO categories (category_id, category_name)
VALUES (%s, %s);
'''

populate_trending_data_query = '''
INSERT INTO trending_data (video_id,trending_date, title, channel_title, category_id, publish_time,
                           tags, views, likes, dislikes, comment_count, comments_disabled, ratings_disabled, 
                           video_error_or_removed, description, country, days_until_trending, publish_time_wd, 
                           trending_date_wd, like_per_view, dislike_per_view, comment_per_view)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

