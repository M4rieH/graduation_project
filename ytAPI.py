# -*- coding: utf-8 -*-
"""
Created on Tue Sep  7 08:03:50 2021

@author: Marie
"""

from googleapiclient.discovery import build
import grequests

api_key = 'AIzaSyB7PXfRpIlMKg7msCnCx0UUAP5Xl86ljvw'

youtube = build('youtube','v3',developerKey=(api_key))
request = youtube.videos().list(part = 'contentDetails', id='th5_9woFJmk')
response =request.execute()
print(response)

url = https://date.nager.at/api/v3/publicholidays/2017/AT
GET = /api/v3/PublicHolidays/{Year}/{CountryCode}

rs = (grequests.get(u) for c in urls)
grequests.map(rs)