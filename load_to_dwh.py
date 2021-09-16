# -*- coding: utf-8 -*-
"""
Created on Mon Sep 13 12:45:58 2021

@author: Jacob
"""
from help_functions import *
from queries import *
import pandas as pd
import datetime as dt
from queries import *
from psycopg2.extras import execute_batch


def create_dim_date_df():
    dates = get_date_list('2006-07-23', '2018-06-14')
    date_lines = []
    for date in dates: 
        date_list = date.split('-')
        year = int(date_list[0])
        month = int(date_list[1])
        day = int(date_list[2])
        date_line = [date, year, month, day]
        date_lines.append(date_line)
    df = pd.DataFrame(date_lines, columns=['date', 'year', 'month', 'day'])
    df['date'] = pd.to_datetime(df['date'])
    df['weekday_num'] = df['date'].dt.dayofweek
    df['is_holidayUS'] = df['date'].map(holiday_mappingUS)
    df['is_holidayCA'] = df['date'].map(holiday_mappingCA)
    df['is_holidayIN'] = df['date'].map(holiday_mappingIN)
    df['is_holidayFR'] = df['date'].map(holiday_mappingFR)
    df['is_holidayDE'] = df['date'].map(holiday_mappingDE)
    df['is_holidayGB'] = df['date'].map(holiday_mappingGB)
    df['is_holidayMX'] = df['date'].map(holiday_mappingMX)
    df['is_holidayJP'] = df['date'].map(holiday_mappingJP)
    df['is_holidayKR'] = df['date'].map(holiday_mappingKR)
    df['is_holidayRU'] = df['date'].map(holiday_mappingRU)
    df['is_weekend'] = df['weekday_num'].map(weekend_mapping)
    df['date'] = df['date'].dt.date.astype(str)
    return df


def create_dim_date_table():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(create_dim_date_table_query)

create_dim_date_table()

def populate_dim_date_table():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(count_dim_date_query)
        count = my_cursor.fetchone()
        if count[0] == 0:
            df = create_dim_date_df()
            my_cursor.executemany(populate_dim_date_table_query, df.values.tolist())


populate_dim_date_table()

def create_dim_country_table():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(create_dim_country_table_query)

create_dim_country_table()
        
def create_country_df():
    country_data = [['US', 'United States'],
                    ['FR', 'France'],
                    ['IN', 'India'],
                    ['MX', 'Mexico'],
                    ['RU', 'Russia'],
                    ['KR', 'Korea'], 
                    ['JP', 'Japan'],
                    ['DE', 'Germany'], 
                    ['GB', 'Great Britain'],
                    ['CA', 'Canada']]
    
    country_df = pd.DataFrame(country_data, columns= ['country_code', 'country_name'])
    return country_df

def populate_dim_country_table():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(count_dim_countries_query)
        count = my_cursor.fetchone()
        print(count)
        if count[0] == 0:
            df = create_country_df()
            print(df)
            my_cursor.executemany(populate_dim_country_table_query, df.values.tolist())

populate_dim_country_table()

def create_date_views():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(create_trend_date_view_query)
        my_cursor.execute(create_publish_date_view_query)
        

def create_dim_video_table():
       with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(create_dim_videos_query)
create_dim_video_table()

create_dim_video_table()

def populate_dim_video_table():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(count_dim_video_query)
        count = my_cursor.fetchone()
        print(count)
        if count[0] == 0:
            df = pd.read_csv('prepped_data/vid_dim_data.csv')
            print(df)
            execute_batch(my_cursor, populate_dim_videos_table_query, df.values.tolist())

time1 = time.time()
populate_dim_video_table()
time2 = time.time()
print(f'Executed in {time2-time1} seconds')      

def create_facts_table():
     with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(create_facts_table_query)
        
create_facts_table()


def create_jt_tags_table():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(create_jt_query)
        
create_jt_tags_table()

def populate_dim_tags():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(count_dim_tags_query)
        count = my_cursor.fetchone()
        print(count)
        if count[0] == 0:
            df = pd.read_csv('prepped_data\all_tags_alone.csv')
            print(df)
            execute_batch(my_cursor, populate_dim_tags_query, df.values.tolist())

populate_dim_tags()

def populate_facts_tags():
    with getOnThatDataMart() as connection:
        my_cursor = connection.cursor()
        my_cursor.execute(count_facts_tags_query)
        count = my_cursor.fetchone()
        print(count)
        if count[0] == 0:
            df = pd.read_csv(r'prepped_data\tags_df.csv')
            print(df)
            execute_batch(my_cursor, populate_facts_tags_query, df.values.tolist())

populate_facts_tags()            
            
            
            
            
            
            
            
            
            