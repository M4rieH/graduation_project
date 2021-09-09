# -*- coding: utf-8 -*-
"""
Created on Wed Sep  8 13:15:27 2021

@author: Jacob
"""
import itertools
from itertools import permutations
import pandas as pd

minutes = list(range(0,60))
minutes_str = []
if len(minutes_str)==0:
    for m in minutes:
        if m < 10:
            minutes_str.append('0'+str(m))
        else:
            minutes_str.append(str(m))

hours = list(range(0,24))
hours_str = []
if len(hours_str)==0:
    for h in hours:
        if h < 10:
            hours_str.append('0'+str(h))
        else:
            hours_str.append(str(h))
       
timestamp = []

if len(timestamp) == 0:
    for h in hours_str:
        for m in minutes_str:
            timestamp.append(h+':'+m)

ts_index = list(range(0,len(timestamp)))
        
timestamp_pd = pd.DataFrame(ts_index, columns = ['index'])
timestamp_pd['timestamp'] = timestamp

def get_date_list(date_from, date_end):
    import datetime
    sdate = date_from
    edate = date_end
    start = datetime.datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.datetime.strptime(date_end, "%Y-%m-%d")
    dates = []
    while start <= end:
        dates.append(str(start.date()))
        start += datetime.timedelta(days=1)
    return dates

dates = get_date_list('2017-11-14', '2018-06-14')

d_index = list(range(0, len(dates)))

del 

dates_pd = pd.DataFrame(d_index, columns=['index'])
dates_pd['date'] = dates

dates_pd.to_csv(r'prepped_data\dates.csv')
timestamp_pd.to_csv(r'prepped_data\timestamps.csv')
