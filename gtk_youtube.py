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
# DB
import psycopg2



# FETCH FUNCTIONS AND QUERY-STRINGS
from mapping_functions import *

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
    del identification
    del items
    del cat_names
    category_df.to_csv(r'prepped_data\categories.csv')

categories_to_csv()

US_holidays = holidays.UnitedStates()

date(2015, 1, 2) in US_holidays

for country_df in df_country_list: 
    
    country_df['trending_date'] = pd.to_datetime(country_df['trending_date'], format='%y.%d.%m')
    country_df['publish_time'] = country_df['publish_time'].str.slice(0, -5)
    country_df['publish_time'] = pd.to_datetime(country_df['publish_time'], format='%Y-%m-%dT%H:%M:%S')
    
    country_df['days_until_trending'] = -(country_df['publish_time'] - country_df['trending_date']).dt.days
    
    country_df['publish_time_wd_num'] = country_df['publish_time'].dt.dayofweek
    country_df['publish_time_wd'] = country_df['publish_time_wd_num'].map(weekday_mapping)
    
    country_df['trending_date_wd_num'] = country_df['trending_date'].dt.dayofweek
    country_df['trending_date_wd'] = country_df['trending_date_wd_num'].map(weekday_mapping)
    
    country_df['like per view'] = country_df['likes']/country_df['views']
    country_df['dislike per view'] = country_df['dislikes']/country_df['views']
    country_df['comment per view'] = country_df['comment_count']/country_df['views']
    country_df['tags'] = country_df['tags'].map(clean_tags)
    country_df['tag_count'] = country_df['tags'].map(count_tags)
    
    #country_df['is_holiday'] = country_df['trending_date'].map(holiday_mapping)
    #country_df['is_holiday'] = country_df.apply(lambda x: holiday_mapping(country = x['country'], date_input = x['trending_date']), axis=1)
    country_df['is_holiday'] = [holiday_mapping(*a) for a in tuple(zip(country_df['country'], country_df['trending_date']))]
    
   
    print(f'ferdig med {country_df["country"][0]}')

yt_all_countries = pd.concat(df_country_list, axis = 0)

dir(yt_kr)

# TRANSLATOR 

# time1 = time.time()
# yt_us['tags'] = yt_us['tags'].map(translate_to_english)
# time2 = time.time()

# def process_Pandas_data(func, df, num_processes=None):
    
#     # If num_processes is not specified, default to minimum(#columns, #machine-cores)
#     if num_processes==None:
#         num_processes = min(df.shape[1], cpu_count())
    
#     # 'with' context manager takes care of pool.close() and pool.join() for us
#     with Pool(num_processes) as pool:
        
#         # we need a sequence to pass pool.map; this line creates a generator (lazy iterator) of columns
#         seq = df['tags']
        
#         # pool.map returns results as a list
#         results_list = pd.DataFrame(pool.map(func, seq))
        
#         # return list of processed columns, concatenated together as a new dataframe
#         return pd.concat(results_list, axis=1)

# if __name__ == '__main__':
  
#     mp_result = process_Pandas_data(translate_to_english, yt_us, 8)



# yt_all_countries.to_csv(r'prepped_data\all_countries_data.csv')


# get unique video_id's


# create and populate database

# def getOnThatDB(host='marie123.postgres.database.azure.com', user='marie@marie123', password='ProjectP5354', dbname = 'postgres', sslmode='require'):
#     connection = psycopg2.connect(
#         user=user,
#         password=password,
#         host=host,
#         database=dbname,
#         sslmode = sslmode
#     )
#     return connection

# with getOnThatDB() as connection: 
#     cursor = connection.cursor()
#     cursor.execute(create_categories_table)
#     cursor.execute(create_yt_all_countries_table)

# def populate_category_table():
        
#     with getOnThatDB() as connection: 
#         cursor = connection.cursor()
#         cursor.executemany(populate_category_table_query,category_df.values.tolist())
           
# #populate_category_table()

# from queries import *

# yt_to_db = yt_all_countries.copy()

# def populate_trending_data_table():
#     with getOnThatDB() as connection: 
#         cursor = connection.cursor()
#         cursor.executemany(populate_trending_data_query,yt_to_db.values.tolist())
     
# populate_trending_data_table()

# yt_list = yt_all_countries.values.tolist()



