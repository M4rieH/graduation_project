# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 14:50:21 2021

@author: Jacob
"""
import pandas as pd
import datetime as dt
from sklearn.preprocessing import OneHotEncoder
from googleapiclient.discovery import build
import grequests

def top_10(df, col, num=10):
    sort_df = df.sort_values(col, ascending=False).iloc[:num]
    ax = sort_df[col].plot.bar()
    labels = []
    for item in sort_df['title']:
        labels.append(item[:10] + '...')  
    ax.set_title(col.upper(), fontsize=16)
    ax.set_xticklabels(labels, rotation=45, fontsize=10)
    return sort_df[['video_id', 'title', 'channel_title', col]]

# #these are fine
yt_us = pd.read_csv('dataset/USvideos.csv') # 40 949
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

df_country_list = [yt_us, yt_ca, yt_de, yt_fr, yt_in, yt_gb,yt_jp, yt_kr,yt_mx, yt_ru]
country_codes = ['US','CA','DE','FR','IN','GB','JP','KR','MX','RU']


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
    
# categories from json from usa (us data set contains one more category compared to other countries)
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
    
weekday_mapping = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}

# data prep

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
    
    # trend_days = country_df.groupby(['video_id'])['video_id'].agg(total_trend_days=len).reset_index()
    # country_df = pd.merge(country_df, trend_days, on='video_id', how='left')    

# prints number of videos with error or remove flag True in each data set

for country_df in df_country_list:
    error_videos = country_df.loc[country_df['video_error_or_removed']]
    print(error_videos.shape[0])


# write to csv's

for index, country_df in enumerate(df_country_list):
    country_df.to_csv(rf'prepped_data\{country_codes[index]}_data.csv')
    print(index)
    
# get unique video_id's

unique_video_id_mx = yt_mx['video_id'].unique()


# Get data from Youtube API

api_key = 'AIzaSyB7PXfRpIlMKg7msCnCx0UUAP5Xl86ljvw'

youtube = build('youtube','v3',developerKey=(api_key))
def request(iden):
    request = youtube.videos().list(part = 'contentDetails', id={iden})
    response =request.execute()
    return response

request_test = request('SbOwzAl9ZfQ')
print(request_test)

