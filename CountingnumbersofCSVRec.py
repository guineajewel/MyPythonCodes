# -*- coding: utf-8 -*-
"""
Created on Sun Oct 18 14:32:22 2020

@author: Mohammed Kamal
"""


import os
import pandas as pd
import setuprefvar as st
#import linuxpostgres_cred as creds
#import sqlstmnts as sqst

#from sqlalchemy import create_engine

#conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
#engine = create_engine(conn_string)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

m = 4
#m = 9
xlen = 0
ylen = 0
path1 = '/data/backups/.spyder-py3/data_noflt/2008_OctToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
dirs = os.listdir(path1)

for i in range(len(dirs)):
    filename = path1+dirs[i]
    print(filename)            
    
    df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
    xlen += len(df)
    print("xlen="+str(len(df)))
    
for l in range(len(agency_year)):
   path1 = '/data/backups/.spyder-py3/data_noflt/' + agency_year[l] + '_JanToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
   dirs = os.listdir(path1)
   for i in range(len(dirs)):
      filename = path1+dirs[i]
      print(filename)            
        
      df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
      ylen += len(df)
      print("ylen="+str(len(df)))

dimension = xlen + ylen
print(dimension)