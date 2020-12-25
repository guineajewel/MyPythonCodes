#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 18 12:37:00 2020

@author: Mohammed kamal

 [Read the csv files from /data/backups/.spyder-py3/data_noflt/*.csv 
  and create table name ends withh _Cnt and append for all agencies]
 1. Populate “allagency1” table based on allaward and filteredaward
 2. Populate “vendors1” table based on 

"""

import pandas as pd
import setuprefvar as st
import linuxpostgres_cred as creds
import sqlstmnts as sqst

from sqlalchemy import create_engine

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode


for m in range(len(agency_abb)):    
    table_name = 'allagency1'
    agencyabb=[agency_abb[m]]
    sqlstmt = sqst.sqlst13.format(agency_abb[m])
    df = pd.read_sql_query(sqlstmt, engine)
    sqlstmt = sqst.sqlst14.format(agency_abb[m])
    df1 = pd.read_sql_query(sqlstmt, engine)
    df['allaward'] = df1['allaward']
    df['allawardtotal'] = df1['allawardtotal']
    df['filteredaward'] = df1['filteredaward']
    df['filteredawardtotal'] = df1['filteredawardtotal']
    
    for i in range(10):
        agencyabb.append(agency_abb[m])
    df['agency_abb'] = agencyabb
    df.to_sql(table_name, engine, if_exists='append')
    
table_name = 'vendors1'
for m in range(len(agency_abb)):
    sqlst = sqst.sqlst6.format(agency_abb[m])
    df = pd.read_sql_query(sqlst, engine)
    df[df.columns[1]] = df[df.columns[1]].replace('[\$,]', '', regex=True).astype(float)
    df['agency_abb'] = agency_abb[m]
    df['index'] = m
    df.to_sql(table_name, engine, if_exists='append')