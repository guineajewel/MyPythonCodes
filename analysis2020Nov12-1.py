#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 14:43:49 2020

@author: kamalm
"""

import setuprefvar as st
import linuxpostgres_cred as creds
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import sqlstmnts as sqst


y_format = mtick.FuncFormatter(lambda x, p: format(int(x), ','))  # make formatter

fig= plt.figure(figsize=(14,8))
#ax = plt.axes([1., 1., 0., 0.], frameon=False, xticks=[],yticks=[])

from sqlalchemy import create_engine
conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

df = pd.read_sql_query(sqst.sqlst21, engine)
#df1.to_csv ('allstates1.csv', header=True)

ax = df.plot(x='recipient_state_code', y="totalawardvalue", legend=False, linewidth=2, color='r',
    marker='o', markersize=7) 

plt.yticks(fontsize=4)

plt.xlabel('States')
plt.ylabel('Award Value', fontsize=4)
plt.gca().yaxis.set_major_formatter(y_format)

ax2 = ax.twinx()
df.plot(x='recipient_state_code', y="noofaward", ax=ax2, legend=False, linewidth=2, color='b',
    marker='x', markersize=7) 
   
plt.yticks(fontsize=6)

plt.xticks(range(0,10,1),df['recipient_state_code'], fontsize=6)

plt.title('Award Values and Counts for States')

plt.ylabel('Award Count', fontsize=6)
plt.gca().yaxis.set_major_formatter(y_format)

plt.grid(True)

ax2.figure.legend(['Award Value','Award Count'], bbox_to_anchor=(0.05, 0.25, 0.5, 0.5))
plt.savefig('awardcountvaluestate.png', dpi=300)
plt.show()