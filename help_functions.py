# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 21:41:37 2021

@author: Jacob
"""
# STD

from datetime import date

# DB

import psycopg2


#TRANSLATOR
from deep_translator import GoogleTranslator
import time
from googletrans import Translator
from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger
polyglot_logger.setLevel("ERROR")

#HOLIDAY
import holidays

#BoW

from sklearn.feature_extraction.text import CountVectorizer
from bs4 import BeautifulSoup   
import nltk
from nltk.corpus import stopwords # Import the stop word list
import re

def clean_sentence(sentence):
    # Function to convert a raw review to a string of words
    # The input is a single string (a raw movie review), and 
    # the output is a single string (a preprocessed movie review)
    #
    # 1. Remove HTML
    this_sentence = BeautifulSoup(sentence, features="lxml").get_text() 
    #
    # 2. Remove non-letters        
    #letters_only = re.sub("[^a-zA-Z]", " ", this_sentence) 
    #
    letter_only = this_sentence
    # 3. Convert to lower case, split into individual words
    words = letters_only.lower().split()                             
    #
    # 4. In Python, searching a set is much faster than searching
    #   a list, so convert the stop words to a set
    stops = set(stopwords.words("english"))                  
    # 
    # 5. Remove stop words
    meaningful_words = [w for w in words if not w in stops]   
    #
    # 6. Join the words back into one string separated by space, 
    # and return the result.
    return( " ".join( meaningful_words)) 



def space_to_underscore(list_of_tags):
    the_list = []
    for tag in list_of_tags:
        tag = tag.replace(' ', '_')
        the_list.append(tag)
    return the_list

list_of_tags =['jeg heter jacob', 'folke musikk', '32fLFAWw^*____^w\335 aeg']


def list_to_sent(tags):
    tags = space_to_underscore(tags)
    tag_as_sentence = " ".join(tags)
    tag_as_sentence = clean_sentence(tag_as_sentence)
    return tag_as_sentence
print(list_to_sent(list_of_tags))




def lang_detector(word):
    lang_obj = Detector(word, quiet=True)
    lang_code = lang_obj.language.code
    lang_conf = lang_obj.language.confidence
    return lang_code, lang_conf 


def translate_to_english(word_list):
    for index, word in enumerate(word_list):
        lang_code, lang_conf = lang_detector(word)
        if (lang_code != 'en') and (lang_code != 'sco'):
            try:
                word = GoogleTranslator(source=lang_code, target = 'en').translate(word)
                word_list[index] = word
            except:
                pass
    return word_list



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

def weekend_mapping(wd_num):
    if wd_num == 5 or wd_num == 6:
        return 1
    else:
        return 0

weekday_mapping = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}

def holiday_mapping(country, date_input):
    if country == 'KR':
        country = 'US'
    try:  
        local_holidays = holidays.CountryHoliday(country)
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0
    
def holiday_mappingUS(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('US')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0    

def holiday_mappingCA(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('CA')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0 

def holiday_mappingDE(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('DE')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0 
    
def holiday_mappingFR(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('FR')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0 
    
def holiday_mappingIN(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('IN')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0 
    
def holiday_mappingJP(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('JP')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0 
    
def holiday_mappingMX(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('MX')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0 
    
def holiday_mappingRU(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('RU')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0 

def holiday_mappingKR(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('KR')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0
    
def holiday_mappingGB(date_input):
    try:  
        local_holidays = holidays.CountryHoliday('GB')
        year = date_input.year
        month = date_input.month
        day = date_input.day
        if date(year, month, day) in local_holidays:
            return 1
        else:
            return 0 
    except:
        return 0 
        

    

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

def getOnThatDataMart(database='postgres', user='marie@marie123', password='ProjectP5354'):
    connection = psycopg2.connect(
        user=user,
        password=password,
        host='marie123.postgres.database.azure.com',
        port='5432',
        database=database,
        sslmode = 'require'
    )
    return connection



