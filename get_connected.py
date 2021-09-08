# -*- coding: utf-8 -*-
"""
Created on Thu Jul 15 14:53:17 2021

@author: Marie
"""

import psycopg2

dbname='postgres' 
user='marie@marie123' 
host='marie123.postgres.database.azure.com' 
password='ProjectP5354' 
port='5432' 
sslmode='require'

conn_string = "host = {0} user = {1} dbname = {2} password = {3} sslmode = {4}".format(host, user, dbname, password, sslmode)
connection = psycopg2.connect(conn_string)

cursor = connection.cursor()
cursor.close()
connection.close()


