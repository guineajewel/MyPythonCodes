# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 14:13:00 2020

@author: Mohammed Kamal

Populate "TopThreeVendors_detail" for all 10 agencies 

"""

import pandas as pd
import setuprefvar as st
from sqlalchemy import create_engine
import linuxpostgres_cred as creds
import sqlstmnts as sqst


import requests
import json


conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string)

agency_year = st.years
agency_abb  = st.agencyabb
agency_code = st.agencycode

#m = 0
agency_abb = st.agencyabb

try:
    for m in range(len(agency_abb)):
        df1 = pd.read_sql_query(sqst.sqlst7.format(agency_abb[m]), engine)
        df1.to_csv (agency_abb[m]+'TopThreeVendors.csv', header=True)

        table_name = agency_abb[m]+"TopThreeVendors_detail"
        print(table_name)    
        #x=0
        for x in range(3):
            df2 = pd.read_sql_query(sqst.sqlst10.format(agency_abb[m],df1['recipient_duns'][x]), engine)
            
            #also look at
            # https://www.usaspending.gov/award/CONT_AWD_70B04C18F00000071_7014_GS00Q09BGD0060_4735
            
            rank = []
            baseexercisedoptions = []
            numberofoffers = []
            startdate = []
            enddate = []
            extentcompeteddescription = []
            potentialenddate = []
            productorservicecode = []
            productorservicedescription = []
            naics = []
            naicsdescription = []
            solicitationprocedures = []
            web_page = []
            
            #n = 0
            for n in range(len(df2)):
                api_url = 'https://api.usaspending.gov/api/v2/awards/'+df2['contract_award_unique_key'][n]
                webpage = 'https://www.usaspending.gov/award/'+df2['contract_award_unique_key'][n]
                resp = requests.get(api_url)
                if resp.status_code == 200:
                    #print('success')
                    resp.encoding = 'utf-8'
                    json_data = resp.content            
                    parsed_json = (json.loads(json_data))
                    df3 = pd.json_normalize(parsed_json)
                    
                    
                    rank.append(x) 
                    baseexercisedoptions.append(df3['base_exercised_options'][0])
                    numberofoffers.append(df3['latest_transaction_contract_data.number_of_offers_received'][0])
                    startdate.append(df3['period_of_performance.start_date'][0])
                    enddate.append(df3['period_of_performance.end_date'][0])
                    extentcompeteddescription.append(df3['latest_transaction_contract_data.extent_competed_description'][0])
                    potentialenddate.append(df3['period_of_performance.potential_end_date'][0])
                    productorservicecode.append(df3['latest_transaction_contract_data.product_or_service_code'][0])
                    productorservicedescription.append(df3['latest_transaction_contract_data.product_or_service_description'][0])
                    naics.append(df3['latest_transaction_contract_data.naics'][0])
                    naicsdescription.append(df3['latest_transaction_contract_data.naics_description'][0])
                    solicitationprocedures.append(df3['latest_transaction_contract_data.solicitation_procedures'][0])
                    web_page.append(webpage)
                    
                else:
                    print('there is a problem with the response')
                    rank.append(x) 
                    baseexercisedoptions.append('0')
                    numberofoffers.append('0')
                    startdate.append('none')
                    enddate.append('none')
                    extentcompeteddescription.append('none')
                    potentialenddate.append('none')
                    productorservicecode.append('none')
                    productorservicedescription.append('none')
                    naics.append('none')
                    naicsdescription.append('none')
                    solicitationprocedures.append('none')
                    web_page.append(webpage)
                    
            print('rank written..')
            df2['rank'] = rank
            df2['recipient_duns'] = df1['recipient_duns'][x]
            df2['base_exercised_options'] = baseexercisedoptions
            df2['number_of_offers_received'] = numberofoffers
            df2['start_date'] = startdate
            df2['end_date'] = enddate
            df2['extent_competed_description'] = extentcompeteddescription
            df2['potential_end_date'] = potentialenddate
            df2['product_or_service_code'] = productorservicecode
            df2['product_or_service_description'] = productorservicedescription
            df2['naics'] = naics
            df2['naics_description'] = naicsdescription
            df2['solicitation_procedures'] = solicitationprocedures
            df2['web_page'] = web_page
            if x == 0:
                df2.to_csv (table_name+'.csv', header=True)
            else:
                df2.to_csv (table_name+'.csv', mode='a', header=False)

            df2.to_sql(table_name, engine, if_exists='append')

except (AttributeError, TypeError) as e:
    print("Error occurred:", e)