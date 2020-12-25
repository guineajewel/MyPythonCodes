#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 20 16:01:29 2020

@author: Mohammed kamal

 1. Read the _analysis_n1 and _Cnt1 tables for all agencies 
  ["DHS","DOC","DOD","DOJ","GSA","HHS","SEC","TREAS","USDA","VA"]
  and for all fiscal years
  ["2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019"]
 2. Write to '_forhistogram.csv' for each agency for award value histogram analysis
 3. Write to '_forhistogrambyyear.csv' for each agency for award value histogram analysis
    group by per year
 4. Populate “allagency2” table to show allaward and filteredaward
 5. Populate “vendors2” table to show filteredaward and number of vendors and award value   

"""

import pandas as pd
import linuxpostgres_cred as creds
import sqlstmnts as sqst
import setuprefvar as st

from sqlalchemy import create_engine

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string, executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

try:

    for m in range(len(agency_abb)):    
        table_name = 'allagency2'
        
        agencyabb=[agency_abb[m]]
        
        sqlstmt = sqst.sqlst29.format(agency_abb[m])
        df = pd.read_sql_query(sqlstmt, engine)
    
        for i in range(len(df)-1): 
            agencyabb.append(agency_abb[m])         
        df['agency_abb'] = agencyabb
        df.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")
    
        if m == 0:
            df.to_csv (table_name+'.csv', header=True)
        else:
            df.to_csv (table_name+'.csv', mode='a', header=False)
    
        df1 = pd.read_sql_query(sqst.sqlst34.format(agency_abb[m]), engine)
        df1.to_csv (agency_abb[m]+'_forhistogram.csv', header=True)    
        print(agency_abb[m]+'_forhistogram.csv')

        df2 = pd.read_sql_query(sqst.sqlst35.format(agency_abb[m]), engine)
        df2.to_csv (agency_abb[m]+'_forhistogrambyyear.csv', header=True)    


    table_name = 'vendors2'
    for m in range(len(agency_abb)):
        sqlst = sqst.sqlst30.format(agency_abb[m])
        df = pd.read_sql_query(sqlst, engine)
        df[df.columns[1]] = df[df.columns[1]].replace('[\$,]', '', regex=True).astype(float)
        df['agency_abb'] = agency_abb[m]
        df['index'] = m
        df.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")
        
        if m == 0:
            df.to_csv (table_name+'.csv', header=True)
        else:
            df.to_csv (table_name+'.csv', mode='a', header=False)
    
    for m in range(len(agency_abb)):
        df1 = pd.read_sql_query(sqst.sqlst31.format(agency_abb[m]), engine)
        df1.to_csv (agency_abb[m]+'_Top15Vendors2.csv', header=True)

        table_name = agency_abb[m]+"_Top15Vendors2_detail"
        print(table_name)    
        #x=0
        for x in range(len(df1)-1):
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