# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 14:50:21 2021

@author: Jacob
"""
import pandas as pd
import numpy as np


#these are fine
us_yt = pd.read_csv('archive/USvideos.csv')
ca_yt = pd.read_csv('archive/CAvideos.csv')
de_yt = pd.read_csv('archive/DEvideos.csv')
fr_yt = pd.read_csv('archive/FRvideos.csv')
in_yt = pd.read_csv('archive/INvideos.csv')
gb_yt = pd.read_csv('archive/GBvideos.csv')

#these have encoding issues
jp_yt = pd.read_csv('archive/JPvideos.csv', nrows = 500) 
# kr_yt = pd.read_csv('archive/KRvideos.csv')
# mx_yt = pd.read_csv('archive/MXvideos.csv')



# categories from json
us_category_json = pd.read_json('archive/US_category_id.json')
ca_category_json = pd.read_json('archive/CA_category_id.json')
de_category_json = pd.read_json('archive/DE_category_id.json')
fr_category_json = pd.read_json('archive/FR_category_id.json')
in_category_json = pd.read_json('archive/IN_category_id.json')
gb_category_json = pd.read_json('archive/GB_category_id.json')
jp_category_json = pd.read_json('archive/JP_category_id.json')
kr_category_json = pd.read_json('archive/KR_category_id.json')
mx_category_json = pd.read_json('archive/MX_category_id.json')

category_json_list = [us_category_json,
                      ca_category_json,
                      de_category_json,
                      fr_category_json,
                      in_category_json,
                      gb_category_json,
                      jp_category_json,
                      kr_category_json,
                      mx_category_json
                      ]

# extracting id and title for canada (can be implemented for the rest later)


identification = [d.get('id') for d in ca_category_json['items']]
items = [d.get('snippet') for d in ca_category_json['items']]
cat_names = [d.get('title') for d in items]

ca_category_df = pd.DataFrame()
ca_category_df['category_id'] = identification
ca_category_df['category_name'] = cat_names

# find unique categories present in canada-set. It's only 17 out of the 31 from ca_category_df
unike = ca_yt['category_id'].unique()


identification = [d.get('id') for d in jp_category_json['items']]
items = [d.get('snippet') for d in jp_category_json['items']]
cat_names = [d.get('title') for d in items]

jp_category_df = pd.DataFrame()
jp_category_df['category_id'] = identification
jp_category_df['category_name'] = cat_names

# find unique categories present in canada-set. It's only 17 out of the 31 from ca_category_df
unike = jp_yt['category_id'].unique()


identification = [d.get('id') for d in kr_category_json['items']]
items = [d.get('snippet') for d in kr_category_json['items']]
cat_names = [d.get('title') for d in items]

kr_category_df = pd.DataFrame()
kr_category_df['category_id'] = identification
kr_category_df['category_name'] = cat_names

# find unique categories present in canada-set. It's only 17 out of the 31 from ca_category_df



identification = [d.get('id') for d in in_category_json['items']]
items = [d.get('snippet') for d in in_category_json['items']]
cat_names = [d.get('title') for d in items]

in_category_df = pd.DataFrame()
in_category_df['category_id'] = identification
in_category_df['category_name'] = cat_names