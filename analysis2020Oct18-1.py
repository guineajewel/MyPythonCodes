#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 18 12:32:01 2020

@author: Mohammed Kamal

This program was generated because the analysis2020Oct17-2 has failed at GSA 2018
So this is populating the rest o fthe data
YES 1. Populate “_Cnt1” table in the csv_database for January 2009 thru December 2019
    
First for GSA 2018 and GSA 2019 and then start with HHS of the follwoing list
    agencyabb = ["DHS","DOC","DOD","DOJ","GSA","HHS","SEC","TREAS","USDA","VA"]
   
   [Read the csv files from /data/backups/.spyder-py3/data_noflt/*.csv 
    and create table name ends withh _Cnt and append for all agencies]

"""

import os
import pandas as pd
import setuprefvar as st
import linuxpostgres_cred as creds

from sqlalchemy import create_engine

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

# only for the GSA 2018 and GSA 2019
m = 4
Cnt_table_name = agency_abb[m]+"_Cnt1"
for l in (9, 10):
    path1 = '/data/backups/.spyder-py3/data_noflt/' + agency_year[l] + '_JanToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
    dirs = os.listdir(path1)
    
    for i in range(len(dirs)):
        filename = path1+dirs[i]
        print(filename)            
        
        df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
        print(Cnt_table_name)
        df.to_sql(Cnt_table_name, engine, if_exists='append')


# here we then start with HHS off of the follwoing list
#agencyabb = ["DHS","DOC","DOD","DOJ","GSA","HHS","SEC","TREAS","USDA","VA"]        
#               0     1     2     3     4     5     6     7       8     9

for m in range (5, 10):
    Cnt_table_name = agency_abb[m]+"_Cnt1"
     
    for l in range(len(agency_year)):
        path1 = '/data/backups/.spyder-py3/data_noflt/' + agency_year[l] + '_JanToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
        dirs = os.listdir(path1)
        
        for i in range(len(dirs)):
            filename = path1+dirs[i]
            print(filename)            
            
            df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
            print(Cnt_table_name)
            df.to_sql(Cnt_table_name, engine, if_exists='append')