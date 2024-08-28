import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import pandas_gbq
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas_gbq
from google.oauth2 import service_account
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np


#----------------------BQ & Ghseet Credentials------------------------------------#
project_id = 'magicpin-14cba'
home_directory = '/home/'
# home_directory = "C:/Users/magicpin/Downloads/"
bq_auth_file = f'{home_directory}magicpin-14cba-e67b877a7ead.json'
gsheet_auth_file = f'{home_directory}magicpin-analytics-growth-team.json'
# gsheet_auth_file = f'{home_directory}gsheet_creds.json'
credentials_bq = service_account.Credentials.from_service_account_file(bq_auth_file)

scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth_file, scope)
client = gspread.authorize(creds)

#------------------MySQL Passwords---------------------------------------#

import os
def fetch_aryan_password():
    if "ARYAN_PASSWORD" not in os.environ.keys():
        raise ValueError("env var ARYAN_PASSWORD not defined")
    return os.environ.get("ARYAN_PASSWORD")
def fetch_wallet_password():
    if "WALLET_PASSWORD" not in os.environ.keys():
        raise ValueError("env var WALLET_PASSWORD not defined")
    return os.environ.get("WALLET_PASSWORD")

def fetch_delivery_password():
    if "DELIVERY_PASSWORD" not in os.environ.keys():
        raise ValueError("env var DELIVERY_PASSWORD not defined")
    return os.environ.get("DELIVERY_PASSWORD")

def fetch_aryan_mad_password():
    if "ARYAN_MAD_PASSWORD" not in os.environ.keys():
        raise ValueError("env var ARYAN_MAD_PASSWORD not defined")
    return os.environ.get("ARYAN_MAD_PASSWORD")


#---------------------------Function Definition----------------------------------------------------------#
def big_qu_df(query, auth_file = bq_auth_file):
  credentials = service_account.Credentials.from_service_account_file(auth_file)
  rdf = pd.DataFrame(pandas_gbq.read_gbq(query, project_id = 'magicpin-14cba', credentials = credentials))
  print(datetime.now())
  return rdf

#print(fetch_aryan_mad_password())

def aryan(query):
    try:
        db_aryan = mysql.connector.connect(user='analytics', password='an@lytix10ca1', host='mad.gc.magicpin.in', database='aryan')
        aryancursor = db_aryan.cursor()
        aryancursor.execute(query)
        result = pd.DataFrame(aryancursor.fetchall(), columns = list(aryancursor.column_names))
        db_aryan.close()
        return result
    except:
        return 'There is an error'

def crm(query):
    try:
        db_crm = mysql.connector.connect(user='analytics', password='an@lytix10ca1', host='mad.gc.magicpin.in', database='crm')
        crmcursor = db_crm.cursor()
        crmcursor.execute(query)
        result = pd.DataFrame(crmcursor.fetchall(), columns = list(crmcursor.column_names))
        db_crm.close()
        return result
    except:
        return 'There is an error'

def wallet(query):
    try:
        db_crm = mysql.connector.connect(user='sherlock', password='solvem@gicpin', host='paydbro.gc.magicpin.in', database='wallet')
        crmcursor = db_crm.cursor()
        crmcursor.execute(query)
        result = pd.DataFrame(crmcursor.fetchall(), columns = list(crmcursor.column_names))
        db_crm.close()
        return result
    except:
        return 'There is an error'



def gs_writer(sheet_name,dataframe,sheet_url, resize_bit, start_column):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth_file, scope)
    gc = gspread.authorize(credentials)
    sht2 = gc.open_by_url(sheet_url)
    sht = sht2.worksheet(sheet_name)
    set_with_dataframe(sht,dataframe,resize = resize_bit, col =start_column )

def gs_reader(sheet_name,col,sheet_url):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth_file, scope)
    gc = gspread.authorize(credentials)
    sht2 = gc.open_by_url(sheet_url)
    sht = sht2.worksheet(sheet_name)
    if col ==0:
        c= get_as_dataframe(sht,evaluate_formulas=True)
        return c
    else:
        c= get_as_dataframe(sht,usecols=col,evaluate_formulas=True)
        return c

def gs_clear(sheet_name,first_column_withstart,last_column_with_end,sheet_url):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth_file, scope)
    gc = gspread.authorize(credentials)
    sht2 = gc.open_by_url(sheet_url)
    sht2.values_clear(sheet_name+"!"+str(first_column_withstart)+""+":"+""+str(last_column_with_end)+"")

