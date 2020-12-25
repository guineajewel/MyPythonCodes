#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 20 22:20:12 2020

@author: Mohammed kamal

 1. Read the _analysis_n1 table for all agencies 
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
import numpy as np
import matplotlib.pylab as plt
from scipy import stats 

from sqlalchemy import create_engine

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string, executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

try:
  
  # for m in range(len(agency_abb)):    
  #   for n in range(len(agency_year)):
  #     sqlstmt = sqst.sqlst26.format(agency_abb[m],agency_year[n])
  #     df = pd.read_sql_query(sqlstmt, engine)
  #     table_name = agency_abb[m]+"_"+agency_year[n]
  #     df.to_sql(table_name, engine, index=False, if_exists="append", schema="usaspending")
  sqlstmt = sqst.sqlst36.format(agency_abb[0],agency_year[0])  
  df2009 = pd.read_sql_query(sqlstmt, engine)
  ar2009 = np.array(df2009['recipient_duns'], dtype=float)
  sqlstmt = sqst.sqlst36.format(agency_abb[0],agency_year[1])
  df2010 = pd.read_sql_query(sqlstmt, engine)
  ar2010 = np.array(df2010['recipient_duns'], dtype=float)
  common1 = np.intersect1d(ar2009, ar2010)
  #print(np.intersect1d(ar2009, ar2010))
  
 
  
  # plot histogram
  plt.hist(ar2009, bins = 15, density=1, alpha=0.5)

  # find minimum and maximum of xticks, so we know
  # where we should compute theoretical distribution
  xt = plt.xticks()[0]  
  xmin, xmax = min(xt), max(xt)  
  lnspc = np.linspace(xmin, xmax, len(ar2009))
    
  # lets try the normal distribution first
  m, s = stats.norm.fit(ar2009) # get mean and standard deviation  
  pdf_g = stats.norm.pdf(lnspc, m, s) # now get theoretical values in our interval  
  plt.plot(lnspc, pdf_g, label="Norm") # plot it
    
  # exactly same as above
  ag,bg,cg = stats.gamma.fit(ar2009)  
  pdf_gamma = stats.gamma.pdf(lnspc, ag, bg,cg)  
  plt.plot(lnspc, pdf_gamma, label="Gamma")
    
  # guess what :) 
  ab,bb,cb,db = stats.beta.fit(ar2009)  
  pdf_beta = stats.beta.pdf(lnspc, ab, bb,cb, db)  
  plt.plot(lnspc, pdf_beta, label="Beta")
    
  plt.show()  
  
  
  
  
  
  
except (AttributeError, TypeError) as e:
    print("Error occurred:", e)