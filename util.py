#%%
try:
    import mysql.connector as ms
except:
    try:
        import MySQLdb as ms
    except:
        import pymysql as ms
import sys
import os
import pandas as pd
import numpy as np
from decimal import *
from datetime import datetime,date,timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from pandas.io import gbq
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import time
import requests
from sqlalchemy import column, true
import pytz
  
home_directory = '/home/'
# home_directory = "C:/Users/magicpin/Downloads/"
# home_directory = "C:/Users/Admin/Downloads/"
bq_auth = f'{home_directory}magicpin-analytics-growth-team.json'
gsheet_auth = f'{home_directory}gsheet_creds.json'  # brainstormv2 service account
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
sheet_url="https://docs.google.com/spreadsheets/d/17RRNrKYuEMUaeNxZFXaS1xmMaIs8JSjzKz6BIgpVJeg"
gurl="https://docs.google.com/spreadsheets/d/"



tz = pytz.timezone('Asia/Kolkata')

start_date = datetime.now(tz).replace(day=1).strftime('%Y-%m-%d')
report_date = (datetime.now(tz)- timedelta(days=1)).replace(day=2).strftime('%Y-%m-%d')
yesterday = (datetime.now(tz)- timedelta(days=1)).strftime('%Y-%m-%d')
day_7 = (datetime.now(tz)- timedelta(days=7)).strftime('%Y-%m-%d')
today = (datetime.now(tz)).strftime('%Y-%m-%d')
month = (datetime.now(tz)- timedelta(days=1)).strftime('%b')
year = (datetime.now(tz)- timedelta(days=1)).strftime('%y')
next_month = (datetime.now(tz)+ timedelta(days=31))
next_month = next_month.replace(day=1)
next_month = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')

#---------------------------------------------------------------------------
#=================================================

SOMONTH =start_date
EOMONTH =next_month

today = (datetime.now(tz)).strftime('%Y-%m-%d')
yesterday = (datetime.now(tz)- timedelta(days=0)).strftime('%Y-%m-%d')
today=date.today()
today = today - timedelta(days=0)
yesterday = today - timedelta(days=1)
print(today)
print(yesterday)
#yesterday = "2023-07-01"
#today = "2023-07-09"

yesterday_dates = pd.date_range(yesterday,today,freq='d')
yesterday_dates = [a.strftime('%Y-%m-%d') for a in yesterday_dates]
print (yesterday_dates)



credentials_bq = service_account.Credentials.from_service_account_file(bq_auth)
def big_qu_df(query):
  rdf = pd.DataFrame(pandas_gbq.read_gbq(query, project_id='magicpin-14cba',credentials=credentials_bq))
  return rdf

def upload_to_bq(df,if_exists='append',table_name='merchant_growth.monitisation'):
          pd.set_option("display.max_columns",100)
          project_id_bq = 'magicpin-14cba'
          credentials = service_account.Credentials.from_service_account_file(bq_auth)
          pandas_gbq.to_gbq(df, table_name,project_id_bq, chunksize=90000,credentials=credentials, if_exists=if_exists)#For First Time replace append 


def connectDB(query,DBname,encode = False):

    WALLET = os.environ.get('WALLET_PASSWORD')
    ARYAN = os.environ.get('ARYAN_PASSWORD')
    DELIVERY = os.environ.get('DELIVERY_RO_DB_PASSWORD')

    if DBname == 'wallet':
        conn = ms.connect(host = 'paydbro.gc.magicpin.in',user = 'sherlock',passwd = WALLET,db = DBname)
    if DBname != 'wallet' and DBname != 'delivery':
        conn = ms.connect(host = 'dbro.gc.magicpin.in',user = 'aryan_phpmyadmin',passwd = ARYAN,db = DBname)
    if DBname == 'delivery':
        conn = ms.connect(host = 'oms-ro-db.gc.magicpin.in',user = 'delivery',passwd = DELIVERY,db = DBname)
    if not encode:
        return(pd.read_sql(query,conn))
    else:
        data = pd.read_sql(query,conn)
        return (pd.DataFrame(([[str(data[j][i]) for j in list(data.columns)] for i in range(len(data))]),columns = list(data.columns)))


def gs_writer(sheet_name,dataframe,sheet_url, resize_bit, start_column):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
    gc = gspread.authorize(credentials)
    sht2 = gc.open_by_url(sheet_url)
    sht = sht2.worksheet(sheet_name)
    set_with_dataframe(sht,dataframe,resize = resize_bit, col =start_column )

def gs_reader(sheet_name,col,sheet_url):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
    gc = gspread.authorize(credentials)
    sht2 = gc.open_by_url(sheet_url)
    sht = sht2.worksheet(sheet_name)
    if col ==0:
        c= get_as_dataframe(sht,evaluate_formulas=True)
        return c
    else:
        c= get_as_dataframe(sht,usecols=col,evaluate_formulas=True)
        return c
