#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 12:25:44 2020

@author: Mohammed kamal

 [Read the _analysis_n1 and _Cnt1 tables append for the agencies DHS, DOJ, GSA, HHS, TREAS, VA]
 1. Populate “selectedagency1” table based on allaward and filteredaward
 2. Populate “selectedvendors1” table based on fiscal years 2017, 2018 and 2019 

"""

import pandas as pd
import linuxpostgres_cred as creds
import sqlstmnts as sqst

from sqlalchemy import create_engine

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string, executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500)

agency_year = ['2017', '2018', '2019']
agency_abb  = ['DHS', 'DOJ', 'GSA', 'HHS', 'TREAS', 'USDA', 'VA']

for m in range(len(agency_abb)):    
    table_name = 'selectedagency1'
    agencyabb=[agency_abb[m]]
    sqlstmt = sqst.sqlst22.format(agency_abb[m])
    df = pd.read_sql_query(sqlstmt, engine)
    sqlstmt = sqst.sqlst23.format(agency_abb[m])
    df1 = pd.read_sql_query(sqlstmt, engine)
    df['allaward'] = df1['allaward']
    df['allawardtotal'] = df1['allawardtotal']
    df['filteredaward'] = df1['filteredaward']
    df['filteredawardtotal'] = df1['filteredawardtotal']
    
    for i in range(2): 
        agencyabb.append(agency_abb[m])         
    df['agency_abb'] = agencyabb
    df.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")
    df.to_csv (agency_abb[m]+'_selectedagency1.csv', header=True)
    
table_name = 'selectedvendors1'
for m in range(len(agency_abb)):
    sqlst = sqst.sqlst24.format(agency_abb[m])
    df = pd.read_sql_query(sqlst, engine)
    df[df.columns[1]] = df[df.columns[1]].replace('[\$,]', '', regex=True).astype(float)
    df['agency_abb'] = agency_abb[m]
    df['index'] = m
    df.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")
    
    if m == 0:
        df.to_csv (table_name+'.csv', header=True)
    else:
        df.to_csv (table_name+'.csv', mode='a', header=False)
    