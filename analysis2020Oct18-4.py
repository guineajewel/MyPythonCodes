#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 18 20:26:14 2020

@author: Mohammed Kamal

1. Populate “_Cnt1” and “_analysis_n1“ tables in the csv_database for 
   csv_database and usaspending schema
   October 2008 thru December 2008 and for January 2009 thru December 2019
2.   [Read the csv files from ./data_noflt/*.csv and create table names ends with "_Cnt1" 
   and  “_analysis_n1“ and append for all agencies]
3. Populate “allagency1” table to show on allaward and filteredaward
4. Populate “vendors1” table to show filteredaward and number of vendors and award value 

"""

import os
import pandas as pd
import setuprefvar as st
import linuxpostgres_cred as creds
import sqlstmnts as sqst

from sqlalchemy import create_engine

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string, executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

for m in range(len(agency_abb)):
    Cnt_table_name = agency_abb[m]+"_Cnt1"
    ana_table_name = agency_abb[m]+"_analysis_n1"   
    
    path1 = '/data/backups/.spyder-py3/data_noflt/2008_OctToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
    dirs = os.listdir(path1)
    for i in range(len(dirs)):
        filename = path1+dirs[i]
        print(filename)            
        
        df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
        print(Cnt_table_name)
        df.to_sql(Cnt_table_name, engine, index=False, if_exists="append", schema="usaspending")

        print(ana_table_name)
        df = pd.read_csv(filename, usecols=st.forana1, dtype = st.foranadtype, encoding='utf-8')
        df = df[(df['naics_code'].isin(st.selected_naics)) & \
         (df['primary_place_of_performance_country_code']=="USA")]
    
        df['period_of_performance_start_date']=df['period_of_performance_start_date'].str[:10]
        df['period_of_performance_current_end_date']=df['period_of_performance_current_end_date'].str[:10]
        df['period_of_performance_potential_end_date']=df['period_of_performance_potential_end_date'].str[:10]
        df.to_sql(ana_table_name, engine, index=False, if_exists="append", schema="usaspending")
        

for m in range(len(agency_abb)):
    Cnt_table_name = agency_abb[m]+"_Cnt1"
    ana_table_name = agency_abb[m]+"_analysis_n1"
     
    for l in range(len(agency_year)):
        path1 = '/data/backups/.spyder-py3/data_noflt/' + agency_year[l] + '_JanToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
        dirs = os.listdir(path1)
        
        for i in range(len(dirs)):
            filename = path1+dirs[i]
            print(filename)            
            
            df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
            print(Cnt_table_name)
            df.to_sql(Cnt_table_name, engine, index=False, if_exists="append", schema="usaspending")
    
            print(ana_table_name)
            df = pd.read_csv(filename, usecols=st.forana1, dtype = st.foranadtype, encoding='utf-8')
            df = df[(df['naics_code'].isin(st.selected_naics)) & \
             (df['primary_place_of_performance_country_code']=="USA")]
        
            df['period_of_performance_start_date']=df['period_of_performance_start_date'].str[:10]
            df['period_of_performance_current_end_date']=df['period_of_performance_current_end_date'].str[:10]
            df['period_of_performance_potential_end_date']=df['period_of_performance_potential_end_date'].str[:10]
            df.to_sql(ana_table_name, engine, index=False, if_exists="append", schema="usaspending")
            
table_name = 'allagency1'
for m in range(len(agency_abb)):    
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
    df.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")
    
table_name = 'vendors1'
for m in range(len(agency_abb)):
    sqlst = sqst.sqlst6.format(agency_abb[m])
    df = pd.read_sql_query(sqlst, engine)
    df[df.columns[1]] = df[df.columns[1]].replace('[\$,]', '', regex=True).astype(float)
    df['agency_abb'] = agency_abb[m]
    df['index'] = m
    df.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")