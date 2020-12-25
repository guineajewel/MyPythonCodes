# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 10:18:32 2020

@author: Mohammed Kamal

Regression and Correlation of top 1000 awards 
For ten agencies 10*1000 records find relationship between 
NoOfAward and TotalAwardValue
 
Please read this before analyzing
https://realpython.com/linear-regression-in-python/

"""


import numpy as np
from sklearn.linear_model import LinearRegression

#import os
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

#m = 3

for m in range(len(agency_abb)):
    df1 = pd.read_sql_query(sqst.sqlst16.format(agency_abb[m]), engine)
    df2 = pd.read_sql_query(sqst.sqlst18.format(agency_abb[m]), engine)
    
    x = np.array(df1["noofaward"]).reshape((-1,1))
    y = np.array(df1["totalawardvalue"])
    
    
    model = LinearRegression().fit(x,y)
    
    #print('Agency: ' + agency_abb[m])
    r_sq = model.score(x, y)
    print('Agency: ' + agency_abb[m] + ': coefficient of determination r_sq [#ofAward (x), totalawardvalue (y)] for top 1000:', r_sq)
    df1.to_csv (agency_abb[m]+'_Top1000Vendors.csv', header=True)
    df2.to_csv (agency_abb[m]+'_TopRecipients_inStates.csv', header=True)

#print('intercept:', model.intercept_)
#print('slope:', model.coef_)

#new_model = LinearRegression().fit(x, y.reshape((-1, 1)))
#print('intercept:', new_model.intercept_)
#print('slope:', new_model.coef_)


#y_pred = model.predict(x)
#print('predicted response:', y_pred, sep='\n')

#y_pred = model.intercept_ + model.coef_ * x
#print('predicted response:', y_pred, sep='\n')



