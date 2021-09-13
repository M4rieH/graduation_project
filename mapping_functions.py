# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 21:41:37 2021

@author: Jacob
"""
# STD

from datetime import date

#TRANSLATOR
from deep_translator import GoogleTranslator
import time
from googletrans import Translator
from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger
polyglot_logger.setLevel("ERROR")

#HOLIDAY
import holidays

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

