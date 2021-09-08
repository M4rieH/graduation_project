# -*- coding: utf-8 -*-
"""
Created on Wed Sep  8 14:31:59 2021

@author: Marie
"""
from googleapiclient.discovery import build

# Get data from Youtube API

api_key = 'AIzaSyB7PXfRpIlMKg7msCnCx0UUAP5Xl86ljvw'

youtube = build('youtube','v3',developerKey=(api_key))
def request(iden):
    request = youtube.videos().list(part = 'contentDetails', id={iden})
    # response =request.execute()
    # return response

request_test = request('SbOwzAl9ZfQ')
print(request_test)
