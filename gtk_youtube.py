# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 14:50:21 2021

@author: Jacob
"""
# STD
import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
import time
from collections import Counter
# DB
import psycopg2


# FETCH FUNCTIONS AND QUERY-STRINGS
from help_functions import *

from queries import *

# TRANSLATOR
from multiprocessing import Pool, cpu_count
translator = Translator()

# loading datasets from 10 countries
yt_us = pd.read_csv('dataset/USvideos.csv') # 40 949 / nrows
yt_ca = pd.read_csv('dataset/CAvideos.csv') # 40 881
yt_de = pd.read_csv('dataset/DEvideos.csv') # 40 840
yt_fr = pd.read_csv('dataset/FRvideos.csv') # 40 724
yt_in = pd.read_csv('dataset/INvideos.csv') # 37 352
yt_gb = pd.read_csv('dataset/GBvideos.csv') # 38 916
yt_jp = pd.read_csv('dataset/JPvideos.csv') # 20 523
yt_kr = pd.read_csv('dataset/KRvideos.csv') # 34 567 
yt_mx = pd.read_csv('dataset/MXvideos.csv') # 40 451
yt_ru = pd.read_csv('dataset/RUvideos.csv') # 40 739

# # TOTAL = 338 320

# some useful lists

df_country_list = [yt_us, yt_ca, yt_de, yt_fr, yt_in, yt_gb,yt_jp, yt_kr,yt_mx, yt_ru]
country_codes = ['US','CA','DE','FR','IN','GB','JP','KR','MX','RU']

for country_df in df_country_list:
    del country_df['thumbnail_link']

# adding country-column to all datasets
yt_us['country'] = 'US' 
yt_ca['country'] = 'CA' 
yt_de['country'] = 'DE' 
yt_fr['country'] = 'FR' 
yt_in['country'] = 'IN' 
yt_gb['country'] = 'GB' 
yt_jp['country'] = 'JP' 
yt_kr['country'] = 'KR' 
yt_mx['country'] = 'MX' 
yt_ru['country'] = 'RU' 
    
# extracting categories from json from usa (us data set contains one more category compared to other countries)
def categories_to_csv(): 
    category_json = pd.read_json('dataset/US_category_id.json')
    identification = [d.get('id') for d in category_json['items']]
    items = [d.get('snippet') for d in category_json['items']]
    cat_names = [d.get('title') for d in items]
    category_df = pd.DataFrame()
    category_df['category_id'] = identification
    category_df['category_name'] = cat_names
    category_df['category_id'] = category_df['category_id'].astype(int)
    del identification
    del items
    del cat_names
    category_df.to_csv(r'prepped_data\categories.csv')
    return category_df

categories = categories_to_csv()



for country_df in df_country_list: 
    country_df.drop(country_df[country_df['video_id'] == '#NAME?'].index, inplace = True)
    country_df.drop(country_df[country_df['video_id'] == '#VALUE!'].index, inplace = True)
    
    country_df['trending_date'] = pd.to_datetime(country_df['trending_date'], format='%y.%d.%m')
    country_df['publish_date'] = country_df['publish_time'].str.slice(0, -5)
    del country_df['publish_time']
    country_df['publish_date'] = pd.to_datetime(country_df['publish_date'], format='%Y-%m-%dT%H:%M:%S')

    country_df['days_until_trending'] = -(country_df['publish_date'] - country_df['trending_date']).dt.days
    
    country_df['publish_date_wd_num'] = country_df['publish_date'].dt.dayofweek
    country_df['publish_date_wd'] = country_df['publish_date_wd_num'].map(weekday_mapping)
    
    country_df['trending_date_wd_num'] = country_df['trending_date'].dt.dayofweek
    country_df['trending_date_wd'] = country_df['trending_date_wd_num'].map(weekday_mapping)

    
    country_df['trending_date'] = pd.to_datetime(country_df['trending_date'], format='%y.%d.%m').dt.date
    country_df['publish_date'] = pd.to_datetime(country_df['publish_date'], format='%Y-%m-%dT%H:%M:%S').dt.date

    country_df["comments_disabled"] = country_df["comments_disabled"].astype(int)
    country_df["ratings_disabled"] = country_df["ratings_disabled"].astype(int)
    country_df["video_error_or_removed"] = country_df["video_error_or_removed"].astype(int)
    country_df['like per view'] = country_df['likes']/country_df['views']
    country_df['dislike per view'] = country_df['dislikes']/country_df['views']
    country_df['comment per view'] = country_df['comment_count']/country_df['views']
    country_df['tags'] = country_df['tags'].map(clean_tags)
    country_df['tag_count'] = country_df['tags'].map(count_tags)
    
    country_df.drop_duplicates(['video_id','trending_date', 'country'],keep= 'last')

    time1 = time.time()
    country_df['is_holiday'] = [holiday_mapping(*a) for a in tuple(zip(country_df['country'], country_df['trending_date']))]
    time2 = time.time()
    print(time2-time1)
    print(f'ferdig med {country_df["country"][0]}')

# TRANSLATOR 


# print('Beginning translation of US tags')
# time1 = time.time()
# yt_us['tags'] = yt_us['tags'].map(translate_to_english)
# time2 = time.time()
# print(f'Finished in {(time2-time1):.1f} seconds')

def tags_to_list(df):
    unique_tags_list = [] 
    for index, row in df.iterrows():
        tags = row['tags']
        for tag in tags:
            unique_tags_list.append(tag)
    return unique_tags_list

print('Executing tags-to-list...')
time1 = time.time()
all_tags = []
tags = []
for country_df in df_country_list:
    tags_list = tags_to_list(country_df)
    all_tags.append(tags_list)
for tag_list in all_tags:
    for tag in tag_list:
        tags.append(tag)

print(len(tags))
        
time2 = time.time()
print(f'Executed in {(time2-time1):1f} seconds')

counter = Counter(tags).most_common()
yt_all_countries = pd.concat(df_country_list, axis = 0)
yt_all_countries = yt_all_countries.drop_duplicates(['video_id','trending_date','country'],keep= 'last')


# alchemy

def stage_yt_data():
    
    print('Uploading YT-data to DB...')
    from sqlalchemy import create_engine
    time1 = time.time()
    engine_azure = create_engine('postgresql://marie@marie123:ProjectP5354@marie123.postgres.database.azure.com:5432/postgres')
    yt_all_countries.to_sql('trending_data',engine_azure)
    time2 = time.time()
    print(f'Executed in {(time2-time1):1f} seconds')
    
stage_yt_data()

# create video_dimension_data

vid_dim_df = yt_all_countries[['video_id', 'title', 'channel_title', 'category_id', 'description']]


vid_dim_df = vid_dim_df.merge(categories, on='category_id', how='left')

vid_dim_df.drop_duplicates(subset='video_id', keep='last', inplace=True)
del vid_dim_df['category_id']

movies = yt_all_countries.loc[yt_all_countries['category_id'] == 30]

vid_dim_df.to_csv(r'prepped_data\vid_dim_data.csv', index=False)

def extract_tags_to_df(df):
    a = 100000000
    list_of_tuples = []
    for index, row in df.iterrows():
        video_id = row['video_id']
        tags = row['tags']
        a -= 1
        for tag in tags:
            tag_video_tuple = (video_id, tag)
            list_of_tuples.append(tag_video_tuple)
        if a == 0:
            break
    return list_of_tuples
            
        
time1 = time.time()
print(extract_tags_to_df(yt_us))
time2 = time.time()
print(f'Executed in {time2-time1} seconds')






