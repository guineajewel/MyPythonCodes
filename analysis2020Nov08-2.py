# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 14:24:49 2020

@author: Mohammed Kamal

Plot filtered and cumulative data as the agencyabb = ["DHS","DOC","DOD","DOJ","GSA",
                                                      "HHS","SEC","TREAS","USDA","VA"] 
"""

#import setuprefvar as st
import pandas as pd
from sqlalchemy import create_engine
#import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib
import linuxpostgres_cred as creds



y_format = mtick.FuncFormatter(lambda x, p: format(int(x), ','))  # make formatter

conn_string = "postgresql://"+creds.PGUSER+":"+creds.PGPASSWORD+"@"+creds.PGHOST+":"+creds.PORT+"/"+creds.PGDATABASE
engine = create_engine(conn_string)


colorWheel =['#329932',
            '#ff6961',
            '#6a3d9a',
            '#fb9a99',
            '#e31a1c',
            '#fdbf6f',
            '#ff7f00',
            '#cab2d6',
            '#d1e5f0',
            '#ffff99']

#agency_abb= st.agencyabb


df = pd.read_sql_query('SELECT * FROM usaspending."allagency1" where action_date_fiscal_year::int <> 2020 \
                       order by agency_abb, action_date_fiscal_year', engine)

df[df.columns[8]] = df[df.columns[8]].replace('[\$,]', '', regex=True).astype(float)


#pivot_df = df.pivot(index='action_date_fiscal_year', columns='agency_abb', values='allaward')
#pivot_df = df.pivot(index='action_date_fiscal_year', columns='agency_abb', values='filteredaward')
pivot_df = df.pivot(index='action_date_fiscal_year', columns='agency_abb', values='filteredawardtotal')

#pivot_df.to_csv ('AllCumulativeCounts1.csv', header=True)
#pivot_df.to_csv ('FilteredCumulativeCounts1.csv', header=True)
pivot_df.to_csv ('AwardTotal1.csv', header=True)

matplotlib.style.use('ggplot')
pivot_df.loc[:,['DHS', 'DOC', 'DOD', 'DOJ', 'GSA', 'HHS', 'SEC', 'TREAS', 'USDA', 'VA']] \
.plot.bar(stacked=True, figsize=(14,7), edgecolor='black',legend=False, color=colorWheel)


plt.xlabel('Fiscal Year')
#plt.ylabel('Unfiltered Award Count - Cumulative', fontsize=8)
#plt.ylabel('Filtered Award Count - Cumulative')
plt.ylabel('Filtered Award $ Value - Cumulative')
plt.title('Cumulative Award Data 2009-2019 FY', fontsize=18)


#plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%.0f')) 


plt.gca().yaxis.set_major_formatter(y_format) 

# 10 column legend on top of the title [no need any more]
#plt.legend(loc="lower right", bbox_to_anchor=(1., 1.02) , borderaxespad=0., ncol=10)

# 10 column legend insite the plot [use t from now on]
plt.legend(loc="lower right", bbox_to_anchor=(.5, .75) , borderaxespad=0., ncol=2, )


#filename = 'AllCumulativeCounts1.png'
#filename = 'FilteredCumulativeCounts1.png'
filename = 'FilteredAwardCumulativeValue1.png'
plt.savefig(filename, dpi=300)

plt.show()