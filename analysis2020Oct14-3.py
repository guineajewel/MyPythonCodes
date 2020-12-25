# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 14:16:53 2020

@author: Mohammed Kamal

Plot Vendors.png

"""

import matplotlib.pyplot as plt
import setuprefvar as st
import linuxpostgres_cred as creds
import pandas as pd
import sqlstmnts as sqst

fig, ax = plt.subplots(figsize=(10,5))

from sqlalchemy import create_engine
conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string)

agency_abb= st.agencyabb
table_name = 'vendors'
sqlst = sqst.sqlst9
df = pd.read_sql_query(sqlst, engine)
#df[df.columns[1]] = df[df.columns[1]].replace('[\$,]', '', regex=True).astype(float)
ax = df.plot.bar(rot=15, title="No of Vendors, $ value in Mil & No of Awards");
ax.set_xticklabels(df['agency_abb'])
ax.set_xlabel('Agency')
ax.set_ylabel('Counts/Values')


#plt.legend(args, kwargs)

ax.legend(['No of Award','Award Value (mil)','No of Vendors'], bbox_to_anchor=(0.18, 0.35, 0.5, 0.5))


plt.savefig('vendors.png', dpi=300)