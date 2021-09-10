# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 21:41:37 2021

@author: Jacob
"""
from deep_translator import GoogleTranslator
import time
from googletrans import Translator
from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger

polyglot_logger.setLevel("ERROR")
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
