# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 18:41:33 2020

@author: Mohammed Kamal

Populate "_TopVendors_detail" for all 10 agencies 
generate CSVs as well
"""

import pandas as pd
import setuprefvar as st
from sqlalchemy import create_engine
import linuxpostgres_cred as creds
import sqlstmnts as sqst



conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string, executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

#m = 3
agency_abb = st.agencyabb

try:
    for m in range(len(agency_abb)):
        df1 = pd.read_sql_query(sqst.sqlst7.format(agency_abb[m]), engine)
        df1.to_csv (agency_abb[m]+'_TopVendors.csv', header=True)

        table_name = agency_abb[m]+"_TopVendors_detail"
        print(table_name)    
        #x=0
        for x in range(3):
            df2 = pd.read_sql_query(sqst.sqlst15.format(agency_abb[m],df1['recipient_duns'][x]), engine)
            df2['rank'] = x #+ 1    
            print('rank written..')
            if x == 0:
                df2.to_csv (table_name+'.csv', header=True)
            else:
                df2.to_csv (table_name+'.csv', mode='a', header=False)
            
            df2.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")

except (AttributeError, TypeError) as e:
    print("Error occurred:", e)