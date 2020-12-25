#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 18 14:32:22 2020

@author: Mohammed Kamal

GSA and VA repopulating to ensure corrent record numbers
and then all other agencies to the usaspending schema in csv_database

"""

import os
import pandas as pd
import setuprefvar as st
import linuxpostgres_cred as creds

from sqlalchemy import create_engine


#### Did not work
#import psycopg2.extras
####

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
#engine = create_engine(conn_string)
#or
#engine = create_engine(conn_string, use_batch_mode=True)
#or
#engine = create_engine(conn_string, executemany_mode='batch')
#or
engine = create_engine(conn_string, executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500)
#or
#engine = create_engine(conn_string, executemany_mode='batch', executemany_batch_page_size=x)
#or
#engine =  create_engine(conn_string, executemany_mode='values', executemany_values_page_size=x)


#### Did not work
# from sqlalchemy import event
# @event.listens_for(engine, 'before_cursor_execute')
# def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
#     print("Listen before_cursor_execute - executemany: %s" % str(executemany))
#     if executemany:
#         cursor.fast_executemany = True
#         cursor.commit()
####

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

#m = 0
#m = 1
#m = 2
#m = 3
#m = 4
#m = 5
#m = 6
#m = 7
#m = 8
#m = 9


Cnt_table_name = agency_abb[m]+"_Cnt1"

path1 = '/data/backups/.spyder-py3/data_noflt/2008_OctToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
dirs = os.listdir(path1)

for i in range(len(dirs)):
    filename = path1+dirs[i]
    print(filename)            
    
    df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
    print(Cnt_table_name)
    #df.to_sql(Cnt_table_name, engine, if_exists='append')
    df.to_sql(Cnt_table_name, engine, index=False, if_exists="append", schema="usaspending")
    
for l in range(len(agency_year)):
   path1 = '/data/backups/.spyder-py3/data_noflt/' + agency_year[l] + '_JanToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
   dirs = os.listdir(path1)
   for i in range(len(dirs)):
      filename = path1+dirs[i]
      print(filename)            
        
      df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
      print(Cnt_table_name)
      #df.to_sql(Cnt_table_name, engine, if_exists='append')
      df.to_sql(Cnt_table_name, engine, index=False, if_exists="append", schema="usaspending")