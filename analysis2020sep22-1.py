# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 10:52:40 2020

@author: Kamal
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

for m in range(len(agency_abb)):
    #table_name = agency_abb[m]+"_Cnt"
    table_name = agency_abb[m]+"_Plt"
    print(table_name)
    path1 = './data_noflt/2008_OctToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
    dirs = os.listdir(path1)
    #print(path1)
    #print(dirs)
    for i in range(len(dirs)):
        filename = path1+dirs[i]
        print(filename)            
        
        #use this df only for "_Cnt"
        #df = pd.read_csv(filename, usecols=st.forcount1, dtype=st.forcountdtype, encoding='utf-8')
        
        #use this df only for "_Plt"
        df = pd.read_csv(filename, usecols=st.forplot1, dtype=st.forplotdtype, encoding='utf-8')
        #use this filter only for "_Plt" -- do not use filder for "_Cnt"
        df = df[  (df['naics_code'].isin(st.selected_naics)) & \
              (df['primary_place_of_performance_country_code']=="USA") 
             ]
        
        df.to_sql(table_name, engine, if_exists='append')
        

#for m in range(len(agency_abb)):
#    table_name = agency_abb[m]+"_analysis_n1"   
#    print(table_name)
#    path1 = './data_noflt/2008_OctToDec_noflt_'+ agency_abb[m] +'_'+ agency_code[m] +'_Prime/'
#    dirs = os.listdir(path1)
#    #print(path1)
#    #print(dirs)
#    for i in range(len(dirs)):
#        filename = path1+dirs[i]
#        print(filename)
#        df = pd.read_csv(filename, usecols=st.forana1, dtype = st.foranadtype, encoding='utf-8')
#        df = df[(df['naics_code'].isin(st.selected_naics)) & \
#         (df['primary_place_of_performance_country_code']=="USA")]
#    
#        df['period_of_performance_start_date']=df['period_of_performance_start_date'].str[:10]
#        df['period_of_performance_current_end_date']=df['period_of_performance_current_end_date'].str[:10]
#        df['period_of_performance_potential_end_date']=df['period_of_performance_potential_end_date'].str[:10]
#        df.to_sql(table_name, engine, if_exists='append')
