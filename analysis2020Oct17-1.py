# -*- coding: utf-8 -*-
"""
Created on Sat Oct 17 11:01:06 2020

@author: Mohammed Kamal

Read all the agencyabb as  ["DHS","DOC","DOD","DOJ","GSA","HHS","SEC","TREAS","USDA","VA"] from 10 files 
and display double y axes line plot to show total award and filtered award between 2009 and 2019

"""

import setuprefvar as st
import linuxpostgres_cred as creds
import pandas as pd
import matplotlib.pyplot as plt
import time
import sqlstmnts as sqst

#fig= plt.figure(figsize=(11,8))

from sqlalchemy import create_engine
conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string)

agency_abb= st.agencyabb
for m in range(len(agency_abb)):
    
    sqlst = sqst.sqlst8.format(agency_abb[m])
    
    df = pd.read_sql_query(sqlst, engine)
    ax = df.plot(x='action_date_fiscal_year', y="allawardinthou", legend=False, linewidth=2, color='r',
        marker='o', markersize=7) 
    
    plt.xlabel('Fiscal Year')
    plt.ylabel('All Award Count (x1000)')
    
    ax2 = ax.twinx()
    df.plot(x='action_date_fiscal_year', y="filteredawardinhund", ax=ax2, legend=False, linewidth=2, color='b',
        marker='x', markersize=7) 
   
    plt.xticks(range(0,11,1),df['action_date_fiscal_year'])
    
    plt.title(agency_abb[m]+': Award Counts')
    plt.ylabel('Filtered Award Count (x100)')
    plt.grid(True)
    
    if m == 0:
        ax2.figure.legend(['All Award','Filtered Award'], bbox_to_anchor=(0.05, 0.25, 0.5, 0.5))
    if m == 2:
        ax2.figure.legend(['All Award','Filtered Award'], bbox_to_anchor=(0.28, -0.20, 0.5, 0.5))
    if m == 4:
        ax2.figure.legend(['All Award','Filtered Award'], bbox_to_anchor=(0.25, 0.05, 0.5, 0.5))    
    if m == 7:
        ax2.figure.legend(['All Award','Filtered Award'], bbox_to_anchor=(0.15, -0.15, 0.5, 0.5))
    if m == 9:
        ax2.figure.legend(['All Award','Filtered Award'], bbox_to_anchor=(0.20, -0.20, 0.5, 0.5))        
    if m in (1,3,5,6,8):
        ax2.figure.legend(['All Award','Filtered Award'], bbox_to_anchor=(0.25, -0.15, 0.5, 0.5))
        
    #if m == 0:
    
    time.sleep(2)
    plt.savefig(agency_abb[m]+'awardcount2009to2019.png', dpi=300)
    plt.show()

    agencyabb=[agency_abb[m]]
    for i in range(10):
        agencyabb.append(agency_abb[m])
    df['agency_abb'] = agencyabb
    df.to_csv (agency_abb[m]+'awardcount2009to2019.csv', header=True)