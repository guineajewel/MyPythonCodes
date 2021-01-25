#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 22:02:10 2020

@author: Mohammed kamal


"""

import pandas as pd
import linuxpostgres_cred as creds
import sqlstmnts as sqst
import setuprefvar as st
#import numpy as np
#import matplotlib.pylab as plt
#from scipy import stats 

from sqlalchemy import create_engine

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string, executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

def repeatvendor(sqlstmt) :
  df = pd.read_sql_query(sqlstmt, engine)
  return df


try:
  
  basefy = '2009'
  for m in range(len(agency_abb)):
     sqlstmt = sqst.sqlst44.format(agency_abb[m], basefy)
     df = repeatvendor(sqlstmt)
     print(agency_abb[m]+' Total base for 2009: ' +str(len(df)))
     totcnt = 0
     for n in range(1, len(agency_year)):
       sqlstmt = sqst.sqlst45.format(agency_abb[m], basefy, agency_year[n])
       df = repeatvendor(sqlstmt)
       print(agency_year[n] + ": " +str(len(df)))
       totcnt += len(df)
     
        
     print(agency_abb[m]+' Total Repeat: ' +str(totcnt))
      
  
 
  #     table_name = agency_abb[m]+"_"+agency_year[n]
  #     df.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")

  


except (AttributeError, TypeError) as e:
    print("Error occurred:", e)