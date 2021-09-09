# -*- coding: utf-8 -*-
"""
Created on Wed Sep  8 14:50:55 2021

@author: Marie
"""

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

