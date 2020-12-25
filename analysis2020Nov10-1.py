#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 21:20:41 2020

@author: Mohammed Kamal

"""

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

#m = 0
# table_name = "allstates1"
# for m in range(len(agency_abb)):
#     df1 = pd.read_sql_query(sqst.sqlst19.format(agency_abb[m]), engine)
#     df1.to_sql(table_name, engine, schema='usaspending', if_exists='append')

df2 = pd.read_sql_query(sqst.sqlst20, engine)
df2.to_csv ('allstates1.csv', header=True)