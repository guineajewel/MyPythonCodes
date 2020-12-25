#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 12:34:44 2020

@author: Mohammed Kamal

Populate "_Top15Vendors_detail" for for the agencies DHS, DOJ, GSA, HHS, TREAS, VA 
generate "_Top15Vendors.csv" CSVs as well
ased on fiscal years 2017, 2018 and 2019 
"""

import pandas as pd
from sqlalchemy import create_engine
import linuxpostgres_cred as creds
import sqlstmnts as sqst


conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string, executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500)

agency_year = ['2017', '2018', '2019']
agency_abb  = ['DHS', 'DOJ', 'GSA', 'HHS', 'TREAS', 'USDA', 'VA']

try:
    for m in range(len(agency_abb)):
        df1 = pd.read_sql_query(sqst.sqlst31.format(agency_abb[m]), engine)
        df1.to_csv (agency_abb[m]+'_Top15Vendors2.csv', header=True)

        table_name = agency_abb[m]+"_Top15Vendors2_detail"
        print(table_name)    
        #x=0
        for x in range(15):
            df2 = pd.read_sql_query(sqst.sqlst32.format(agency_abb[m],df1['recipient_duns'][x]), engine)
            df2['rank'] = x #+ 1    
            print('rank written..')
            if x == 0:
                df2.to_csv (table_name+'.csv', header=True)
            else:
                df2.to_csv (table_name+'.csv', mode='a', header=False)
            
            df2.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")

except (AttributeError, TypeError) as e:
    print("Error occurred:", e)