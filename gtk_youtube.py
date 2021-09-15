# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 14:50:21 2021

@author: Jacob
"""
import pandas as pd
import numpy as np
import datetime as dt
from sklearn.preprocessing import OneHotEncoder
import psycopg2
from deep_translator import GoogleTranslator
import time
from googletrans import Translator
from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger
polyglot_logger.setLevel("ERROR")

translator = Translator()


from queries import *

# loading datasets from 10 countries
yt_us = pd.read_csv('dataset/USvideos.csv', nrows=40) # 40 949
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
    
weekday_mapping = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}




# data prep

def count_tags(tag_list):
    return len(tag_list)

def clean_tags(tag_string):
    tag_list = tag_string.split('|')
    tag_list = [i.replace('"','') for i in tag_list]
    tag_list = [i.replace('“','') for i in tag_list]
    tag_list = [i.replace('”','') for i in tag_list]
    tag_list = [j.lower() for j in tag_list]
    for tag in tag_list:
        if tag == '[none]':
            tag_list.remove(tag)
    return tag_list

from deep_translator import GoogleTranslator
import time
from googletrans import Translator
from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger

polyglot_logger.setLevel("ERROR")

# pandas mapping progress bar
# ta ut utvalg av dataen og kjør mapping på
# drit i oversettelsen hvis det ikke går videre nå 

def translate_to_english(tag_list):
    print(f'New line in df with number of tags {len(tag_list)}')
    success = 0
    failure = 0
    for tag in tag_list:
        lang_obj = Detector(tag, quiet=True)
        lang_code = lang_obj.language.code
        lang_conf = lang_obj.language.confidence
      
        # print('tag:', tag)
        # print('language code:', lang_code)
        # print('confidence:', lang_conf)
        # print('\n')
        if (lang_code != 'en') and (lang_code != 'sco'):
            try:
                old_tag = tag
                tag = GoogleTranslator(source=lang_code, target = 'en').translate(tag)
                # print('new tag:', tag, '\n')
                # print(f'Successful translation from {lang_code} to english')
                # print(f'Old tag: {old_tag} to new tag: {tag}')
                # success += 1
            except:
                print(f'Unable to translate tag: {tag}')
                print(f'think language is {lang_code}')
                # failure += 1
    # try:
    #     failure_rate = float(failure/(failure+success))
    # except:
    #     failure_rate = 'no data'
    # # print(f'failure rate {failure_rate}')
     
    return tag_list


for country_df in df_country_list: 
    
    country_df['trending_date'] = pd.to_datetime(country_df['trending_date'], format='%y.%d.%m')
    country_df['publish_time'] = country_df['publish_time'].str.slice(0, -5)
    country_df['publish_time'] = pd.to_datetime(country_df['publish_time'], format='%Y-%m-%dT%H:%M:%S')
    
    country_df['days_until_trending'] = -(country_df['publish_time'] - country_df['trending_date']).dt.days
    
    country_df['publish_time_wd'] = country_df['publish_time'].dt.dayofweek
    country_df['publish_time_wd'] = country_df['publish_time_wd'].map(weekday_mapping)
    
    country_df['trending_date_wd'] = country_df['trending_date'].dt.dayofweek
    country_df['trending_date_wd'] = country_df['trending_date_wd'].map(weekday_mapping)
    
    country_df['like per view'] = country_df['likes']/country_df['views']
    country_df['dislike per view'] = country_df['dislikes']/country_df['views']
    country_df['comment per view'] = country_df['comment_count']/country_df['views']
    country_df['tags'] = country_df['tags'].map(clean_tags)
    country_df['tag_count'] = country_df['tags'].map(count_tags)
   
    print(f'ferdig med {country_df["country"][0]}')

time1 = time.time()
yt_us['tags'] = yt_us['tags'].map(translate_to_english)
time2 = time.time()

print((time2-time1)/40, 'sekunder per linje')

sum_tags = yt_us['tag_count'].mean()

# write to csv's

# for index, country_df in enumerate(df_country_list):
#     country_df.to_csv(rf'prepped_data\{country_codes[index]}_data.csv')
#     print(index)

yt_all_countries = pd.concat(df_country_list, axis = 0)

sum_all_tags = yt_all_countries['tag_count'].sum()




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



