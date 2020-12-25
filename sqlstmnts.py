# -*- coding: utf-8 -*-
"""
Created on Sat Sep  5 11:09:49 2020

@author: Kamal
"""

sqltst1 = ['SELECT * FROM ', ' order by action_date_fiscal_year']
sqltst2 = 'select recipient_name, sum(base_and_all_options_value)::float8::numeric::money as total \
    from public."DOD_analysis_n1" where action_date_fiscal_year::int = 2015 group by recipient_name \
        order by total desc limit 10'
sqltst3 = 'select * from public."DOD_analysis_n1" where action_date_fiscal_year::int = 2015 limit 10'

# sqlst1 = 'Select contract_award_unique_key, count(*) as NoOfAward, \
# sum(base_and_all_options_value)::float8::numeric::money as total \
# , recipient_duns , recipient_name from public."DHS_analysis"\
# group by contract_award_unique_key, recipient_duns ,recipient_name \
# order by total desc limit 10'



sqlst1 = """Select contract_award_unique_key, count(*) as NoOfAward, 
sum(base_and_all_options_value)::float8::numeric::money as total 
, recipient_duns , recipient_name from public."{0}_n1"
group by contract_award_unique_key, recipient_duns ,recipient_name 
order by total desc limit 10"""


sqlst2 = """SELECT x.action_date_fiscal_year, y.allaward, x.filteredaward FROM 
(SELECT action_date_fiscal_year, count(action_date_fiscal_year) as filteredaward FROM public."{0}_Plt"
group by action_date_fiscal_year) as x LEFT JOIN
(SELECT action_date_fiscal_year, count(action_date_fiscal_year) as allaward FROM public."{0}_Cnt"
group by action_date_fiscal_year) as y 
ON x.action_date_fiscal_year = y.action_date_fiscal_year::int"""




sqlst3 = """Select contract_award_unique_key, modification_number, 
base_and_all_options_value::float8::numeric::money,
federal_action_obligation::float8::numeric::money, 
base_and_exercised_options_value::float8::numeric::money,
action_date, action_date_fiscal_year::text, period_of_performance_start_date, 
period_of_performance_current_end_date,
period_of_performance_potential_end_date, award_description, product_or_service_code, 
product_or_service_code_description, naics_code, naics_description, extent_competed,
solicitation_procedures, multi_year_contract, awarding_agency_name, awarding_sub_agency_name, 
funding_office_name, recipient_duns ,recipient_name  
from public."{0}_analysis_n1" where contract_award_unique_key = '{1}'"""


#'select * from public."transaction_fpds" ORDER BY transaction_id ASC LIMIT 10'

# piid
# parent_award_id
# awarding_sub_tier_agency_c
# naics
# naics_description
# awardee_or_recipient_uniqu
# awardee_or_recipient_legal
# ultimate_parent_legal_enti
# ultimate_parent_unique_ide
# award_description
# action_date
# action_type
# action_type_description
# federal_action_obligation
# current_total_value_award
# potential_total_value_awar
# total_obligated_amount
# base_exercised_options_val
# base_and_all_options_value
# awarding_office_code
# awarding_office_name
# extent_competed
# extent_compete_description
# multi_year_contract
# multi_year_contract_desc
# number_of_actions
# number_of_offers_received
# price_evaluation_adjustmen
# product_or_service_code
# product_or_service_co_desc
# other_than_full_and_open_c
# other_than_full_and_o_desc
# small_business_competitive
# solicitation_identifier
# solicitation_procedures
# solicitation_procedur_desc
# fair_opportunity_limited_s
# fair_opportunity_limi_desc
# transaction_number
# type_set_aside
# type_set_aside_description
# contracts
# solicitation_date
# organizational_type
# unique_award_key
# place_of_perform_country_n
# place_of_perform_state_nam
# period_of_performance_star
# period_of_performance_curr
# period_of_perf_potential_e
# ordering_period_end_date



sqldrop1 = 'Drop table public."DHS_analysis_n1";\
Drop table public."DOC_analysis_n1";\
Drop table public."DOD_analysis_n1";\
Drop table public."DOJ_analysis_n1";\
Drop table public."GSA_analysis_n1";\
Drop table public."HHS_analysis_n1";\
Drop table public."SEC_analysis_n1";\
Drop table public."TREAS_analysis_n1";\
Drop table public."USDA_analysis_n1";\
Drop table public."VA_analysis_n1";'


sqldrop2= 'Drop table public."DHS_Plt";\
Drop table public."DOC_Plt";\
Drop table public."DOD_Plt";\
Drop table public."DOJ_Plt";\
Drop table public."GSA_Plt";\
Drop table public."HHS_Plt";\
Drop table public."SEC_Plt";\
Drop table public."TREAS_Plt";\
Drop table public."USDA_Plt";\
Drop table public."VA_Plt";\
Drop table public."DHS_Cnt";\
Drop table public."DOC_Cnt";\
Drop table public."DOD_Cnt";\
Drop table public."DOJ_Cnt";\
Drop table public."GSA_Cnt";\
Drop table public."HHS_Cnt";\
Drop table public."SEC_Cnt";\
Drop table public."TREAS_Cnt";\
Drop table public."USDA_Cnt";\
Drop table public."VA_Cnt";'


sqldrop3 = 'Drop table public."allagency";'


sqldrop4 = 'Drop table usaspending."DHS_TopVendors_detail";\
Drop table usaspending."DOC_TopVendors_detail";\
Drop table usaspending."DOD_TopVendors_detail";\
Drop table usaspending."DOJ_TopVendors_detail";\
Drop table usaspending."GSA_TopVendors_detail";\
Drop table usaspending."HHS_TopVendors_detail";\
Drop table usaspending."SEC_TopVendors_detail";\
Drop table usaspending."TREAS_TopVendors_detail";\
Drop table usaspending."USDA_TopVendors_detail";\
Drop table usaspending."VA_TopVendors_detail"'



sqlst4 = """SELECT x.action_date_fiscal_year, y.alltransaction, x.filteredtransaction, x.transactiontotal 
FROM 
(SELECT action_date_fiscal_year, count(action_date_fiscal_year) as filteredtransaction,
 sum(base_and_all_options_value)::float8::numeric::money as transactiontotal 
 FROM public."{0}_analysis_n1" group by action_date_fiscal_year) as x
LEFT JOIN
(SELECT action_date_fiscal_year, count(action_date_fiscal_year) as alltransaction
 FROM public."{0}_Cnt" group by action_date_fiscal_year) as y
 ON x.action_date_fiscal_year = y.action_date_fiscal_year
 where x.action_date_fiscal_year <> '2020'"""


sqlst5 = """SELECT x.action_date_fiscal_year, y.allaward, x.filteredaward, x.awardtotal 
FROM 
(SELECT count(distinct contract_award_unique_key) as filteredaward, action_date_fiscal_year,
  sum(base_and_all_options_value)::float8::numeric::money as awardtotal
 FROM public."{0}_analysis_n1" group by action_date_fiscal_year) as x
LEFT JOIN
(SELECT count(distinct contract_award_unique_key) as allaward, action_date_fiscal_year
 FROM public."{0}_Cnt" group by action_date_fiscal_year) as y
 ON -- x.contract_award_unique_key = y.contract_award_unique_key 
 x.action_date_fiscal_year = y.action_date_fiscal_year
 where x.action_date_fiscal_year <> '2020'"""


sqlst6 = """Select count(distinct x.contract_award_unique_key) as nooffilteredaward, 
sum(x.total) as awardvalue, count(distinct x.recipient_duns) as noofvendors 
FROM (Select count(*) as noofactions, contract_award_unique_key, 
sum(base_and_all_options_value)::float8::numeric::money as total
, recipient_duns , recipient_name 
from usaspending."{0}_analysis_n1"
where action_date_fiscal_year <> '2020'
group by contract_award_unique_key, recipient_duns ,recipient_name order by total desc
) as x
"""

sqlst7 = """Select count(distinct contract_award_unique_key) as NoOfAward, 
sum(base_and_all_options_value)::float8::numeric::money as total, 
recipient_duns, recipient_name from usaspending."{0}_analysis_n1" 
group by recipient_duns, recipient_name order by total desc limit 3"""


sqlst8 = """SELECT action_date_fiscal_year::text, allaward/1000 allawardinthou, 
filteredaward/100 as filteredawardinhund FROM usaspending."allagency1" 
where action_date_fiscal_year::int <> 2020
and agency_abb = '{0}' order by action_date_fiscal_year"""

sqlst9 = """SELECT nooffilteredaward, awardvalue/10^6 as awardvaluemil, 
noofvendors, agency_abb FROM usaspending."vendors1" """


sqlst10 = """Select contract_award_unique_key, sum(base_and_exercised_options_value) as total
from usaspending."{0}_analysis_n1" where recipient_duns='{1}'
Group by contract_award_unique_key"""


sqlst11 = """(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."DHS_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."DOC_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."DOD_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."DOJ_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."GSA_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."HHS_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."SEC_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."TREAS_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."USDA_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)
UNION
(Select count(distinct contract_award_unique_key), sum(base_and_all_options_value) total, recipient_state_code, awarding_agency_code
from usaspending."VA_analysis_n1"
group by recipient_state_code, awarding_agency_code
order by total desc limit 5)"""

sqlst12 = """Select naics_code, count(distinct contract_award_unique_key) as count1, 
                       sum(base_and_all_options_value) total, action_date_fiscal_year 
                    from public."DHS_analysis" group by naics_code, action_date_fiscal_year 
                    order by action_date_fiscal_year, naics_code"""
                   
sqlst13 = """SELECT x.action_date_fiscal_year, y.alltransaction, y.alltransactiontotal,
x.filteredtransaction, x.filteredtransactiontotal
FROM 
(SELECT action_date_fiscal_year, count(action_date_fiscal_year) as filteredtransaction,
 sum(base_and_all_options_value)::float8::numeric::money as filteredtransactiontotal 
 FROM usaspending."{0}_analysis_n1" group by action_date_fiscal_year) as x
LEFT JOIN
(SELECT action_date_fiscal_year, count(action_date_fiscal_year) as alltransaction,
 sum(base_and_all_options_value)::float8::numeric::money as alltransactiontotal
 FROM usaspending."{0}_Cnt1" group by action_date_fiscal_year) as y
 ON x.action_date_fiscal_year = y.action_date_fiscal_year
 where x.action_date_fiscal_year <> '2020'"""


sqlst14 = """SELECT x.action_date_fiscal_year, y.allaward, y.allawardtotal, x.filteredaward, x.filteredawardtotal 
FROM 
(SELECT count(distinct contract_award_unique_key) as filteredaward, action_date_fiscal_year,
  sum(base_and_all_options_value)::float8::numeric::money as filteredawardtotal 
 FROM usaspending."{0}_analysis_n1" group by action_date_fiscal_year) as x
LEFT JOIN
(SELECT count(distinct contract_award_unique_key) as allaward, action_date_fiscal_year,
 sum(base_and_all_options_value)::float8::numeric::money as allawardtotal
 FROM usaspending."{0}_Cnt1" group by action_date_fiscal_year) as y
 ON x.action_date_fiscal_year = y.action_date_fiscal_year
 where x.action_date_fiscal_year <> '2020'"""
 
  
sqlst15 = """Select contract_award_unique_key, base_and_exercised_options_value,
action_date_fiscal_year, period_of_performance_start_date, 
period_of_performance_current_end_date, product_or_service_code,
product_or_service_code_description,
recipient_duns, recipient_name, extent_competed, period_of_performance_potential_end_date,
solicitation_procedures, naics_code, naics_description, usaspending_permalink, 
number_of_offers_received::int
from usaspending."{0}_analysis_n1" where recipient_duns='{1}'
-- Group by contract_award_unique_key
"""


sqlst16 = """(Select count(distinct contract_award_unique_key) as NoOfAward, 
sum(base_and_all_options_value) as TotalAwardValue, 
recipient_duns from usaspending."{0}_analysis_n1" 
group by recipient_duns order by TotalAwardValue 
desc limit 1000)"""
    
    
sqlst17 = """SELECT action_date_fiscal_year::text, allaward, 
filteredaward FROM usaspending."allagency1" 
where action_date_fiscal_year::int <> 2020
and agency_abb = '{0}' order by action_date_fiscal_year"""

sqlst18 = """Select count(distinct contract_award_unique_key) as NoOfAward, 
sum(base_and_all_options_value) as TotalAwardValue, 
recipient_state_code from usaspending."{0}_analysis_n1" 
group by recipient_state_code order by TotalAwardValue desc"""

# Select recipient_duns, recipient_name, recipient_city_name, recipient_state_code 
# from usaspending."DOD_analysis_n1" limit 100

# Select count(distinct contract_award_unique_key) as NoOfAward, 
# sum(base_and_all_options_value) as TotalAwardValue, 
# recipient_state_code, recipient_city_name, recipient_duns, recipient_name 
# from usaspending."DOD_analysis_n1" 
# group by recipient_state_code, recipient_city_name, recipient_duns, 
# recipient_name order by TotalAwardValue desc -- limit 1000


# Select count(distinct contract_award_unique_key) as NoOfAward, 
# sum(base_and_all_options_value) as TotalAwardValue, 
# recipient_state_code, recipient_city_name from usaspending."DOD_analysis_n1" 
# group by recipient_state_code, recipient_city_name order by TotalAwardValue desc -- limit 1000

sqlst19 = """Select count(distinct contract_award_unique_key) as NoOfAward, 
sum(base_and_all_options_value) as TotalAwardValue, 
recipient_state_code from usaspending."{0}_analysis_n1" 
group by recipient_state_code"""

sqlst20 = """Select sum(NoOfAward) as NoOfAward, 
sum(TotalAwardValue) as TotalAwardValue, 
recipient_state_code from usaspending."allstates1" 
where recipient_state_code IS NOT NULL
group by recipient_state_code order by TotalAwardValue desc"""

sqlst21 = """Select sum(NoOfAward) as NoOfAward, 
sum(TotalAwardValue) as TotalAwardValue, 
recipient_state_code from usaspending."allstates1" 
where recipient_state_code IS NOT NULL
group by recipient_state_code order by TotalAwardValue desc limit 10"""


sqlst22 = """SELECT x.action_date_fiscal_year, y.alltransaction, y.alltransactiontotal,
x.filteredtransaction, x.filteredtransactiontotal
FROM 
(SELECT action_date_fiscal_year, count(action_date_fiscal_year) as filteredtransaction,
 sum(base_and_all_options_value)::float8::numeric::money as filteredtransactiontotal
 FROM usaspending."{0}_analysis_n1" 
 where solicitation_procedures_code = 'NP'and action_date_fiscal_year in ('2017','2018','2019')
 group by action_date_fiscal_year) as x
LEFT JOIN
(SELECT action_date_fiscal_year, count(action_date_fiscal_year) as alltransaction,
 sum(base_and_all_options_value)::float8::numeric::money as alltransactiontotal
 FROM usaspending."{0}_Cnt1" group by action_date_fiscal_year) as y
 ON x.action_date_fiscal_year = y.action_date_fiscal_year"""
 
 
sqlst23 = """SELECT x.action_date_fiscal_year, y.allaward, y.allawardtotal, x.filteredaward, x.filteredawardtotal 
FROM 
(SELECT count(distinct contract_award_unique_key) as filteredaward, action_date_fiscal_year,
  sum(base_and_all_options_value)::float8::numeric::money as filteredawardtotal 
 FROM usaspending."{0}_analysis_n1" 
 where solicitation_procedures_code = 'NP'and action_date_fiscal_year in ('2017','2018','2019')
 group by action_date_fiscal_year) as x
LEFT JOIN
(SELECT count(distinct contract_award_unique_key) as allaward, action_date_fiscal_year,
 sum(base_and_all_options_value)::float8::numeric::money as allawardtotal
 FROM usaspending."{0}_Cnt1" group by action_date_fiscal_year) as y
 ON x.action_date_fiscal_year = y.action_date_fiscal_year """
 
 
sqlst24 = """Select count(distinct x.contract_award_unique_key) as nooffilteredaward, 
sum(x.total) as awardvalue, count(distinct x.recipient_duns) as noofvendors 
FROM (Select count(*) as noofactions, contract_award_unique_key, 
sum(base_and_all_options_value)::float8::numeric::money as total
, recipient_duns , recipient_name 
from usaspending."{0}_analysis_n1"
where solicitation_procedures_code = 'NP'and action_date_fiscal_year in ('2017','2018','2019')
group by contract_award_unique_key, recipient_duns ,recipient_name order by total desc
) as x
"""

sqlst25 = """Select count(distinct contract_award_unique_key) as NoOfAward, 
sum(base_and_all_options_value)::float8::numeric::money as total, 
recipient_duns, recipient_name from usaspending."{0}_analysis_n1"
where solicitation_procedures_code = 'NP'and action_date_fiscal_year in ('2017','2018','2019')
group by recipient_duns, recipient_name order by total desc limit 15"""


sqlst26 = """Select contract_award_unique_key, base_and_exercised_options_value,
action_date_fiscal_year, period_of_performance_start_date, 
period_of_performance_current_end_date, product_or_service_code,
product_or_service_code_description,
recipient_duns, recipient_name, extent_competed, period_of_performance_potential_end_date,
solicitation_procedures, naics_code, naics_description, usaspending_permalink, 
number_of_offers_received::int from usaspending."{0}_analysis_n1" 
where recipient_duns='{1}' and solicitation_procedures_code = 'NP'
and action_date_fiscal_year in ('2017','2018','2019')
"""


sqlst27 = """Select x.contract_award_unique_key, sum(x.base_and_exercised_options_value) as total
--, x.action_date_fiscal_year 
from (Select contract_award_unique_key, base_and_exercised_options_value,
action_date_fiscal_year, period_of_performance_start_date, 
period_of_performance_current_end_date, product_or_service_code,
product_or_service_code_description,
recipient_duns, recipient_name, extent_competed, period_of_performance_potential_end_date,
solicitation_procedures, naics_code, naics_description, usaspending_permalink, 
number_of_offers_received::int from usaspending."{0}_analysis_n1" 
where recipient_duns='{1}' and solicitation_procedures_code = 'NP'
and action_date_fiscal_year in ('2017','2018','2019')) as x
group by x.contract_award_unique_key --, x.action_date_fiscal_year
"""

sqlst28 = """Select x.contract_award_unique_key, sum(x.base_and_exercised_options_value) as total
, x.action_date_fiscal_year 
from (Select contract_award_unique_key, base_and_exercised_options_value,
action_date_fiscal_year, period_of_performance_start_date, 
period_of_performance_current_end_date, product_or_service_code,
product_or_service_code_description,
recipient_duns, recipient_name, extent_competed, period_of_performance_potential_end_date,
solicitation_procedures, naics_code, naics_description, usaspending_permalink, 
number_of_offers_received::int from usaspending."{0}_analysis_n1" 
where recipient_duns='{1}' and solicitation_procedures_code = 'NP'
and action_date_fiscal_year in ('2017','2018','2019')) as x
group by x.contract_award_unique_key , x.action_date_fiscal_year
"""

sqlst29 = """SELECT x.action_date_fiscal_year, y.allaward, y.allawardtotal, x.filteredaward, x.filteredawardtotal 
FROM 
(SELECT count(distinct contract_award_unique_key) as filteredaward, action_date_fiscal_year,
  sum(base_and_all_options_value)::float8::numeric::money as filteredawardtotal 
 FROM usaspending."{0}_analysis_n1" 
 where solicitation_procedures_code = 'NP'
 -- and action_date_fiscal_year in ('2017','2018','2019') -- uncomment this line if you only consider these years
  and modification_number ='0'
and action_date_fiscal_year::int <> 2020
 group by action_date_fiscal_year) as x
LEFT JOIN
(SELECT count(distinct contract_award_unique_key) as allaward, action_date_fiscal_year,
 sum(base_and_all_options_value)::float8::numeric::money as allawardtotal
 FROM usaspending."{0}_Cnt1" group by action_date_fiscal_year) as y
 ON x.action_date_fiscal_year = y.action_date_fiscal_year """

sqlst30 = """Select count(distinct x.contract_award_unique_key) as nooffilteredaward, 
sum(x.total) as awardvalue, count(distinct x.recipient_duns) as noofvendors 
FROM (Select count(*) as noofactions, contract_award_unique_key, 
sum(base_and_all_options_value)::float8::numeric::money as total
, recipient_duns , recipient_name 
from usaspending."{0}_analysis_n1"
where solicitation_procedures_code = 'NP' 
-- and action_date_fiscal_year in ('2017','2018','2019') -- uncomment this line if you only consider these years
and modification_number ='0'
and action_date_fiscal_year::int <> 2020
group by contract_award_unique_key, recipient_duns ,recipient_name order by total desc
) as x
"""

sqlst31 = """Select count(distinct contract_award_unique_key) as NoOfAward, 
sum(base_and_all_options_value)::float8::numeric::money as total, 
recipient_duns, recipient_name from usaspending."{0}_analysis_n1"
where solicitation_procedures_code = 'NP'
-- and action_date_fiscal_year in ('2017','2018','2019') -- uncomment this line if you only consider these years
and modification_number ='0'
and action_date_fiscal_year::int <> 2020
group by recipient_duns, recipient_name order by total desc limit 15"""

sqlst32 = """Select contract_award_unique_key, base_and_exercised_options_value,
action_date_fiscal_year, period_of_performance_start_date, 
period_of_performance_current_end_date, product_or_service_code,
product_or_service_code_description,
recipient_duns, recipient_name, extent_competed, period_of_performance_potential_end_date,
solicitation_procedures, naics_code, naics_description, usaspending_permalink, 
number_of_offers_received::int from usaspending."{0}_analysis_n1" 
where recipient_duns='{1}' and solicitation_procedures_code = 'NP'
-- and action_date_fiscal_year in ('2017','2018','2019') -- uncomment this line if you only consider these years
and action_date_fiscal_year::int <> 2020
and modification_number ='0'
"""

sqlst33 = """SELECT action_date_fiscal_year, awarding_agency_name, count(distinct contract_award_unique_key) as noofaward
, sum(base_and_all_options_value)::float8::numeric::money as awardvalue
, count(distinct recipient_duns) as noofvendors 
FROM usaspending."{0}_analysis_n1" 
where solicitation_procedures_code = 'NP' 
-- and action_date_fiscal_year in ('2017','2018','2019') -- uncomment this line if you only consider these years
 and modification_number ='0'
and action_date_fiscal_year::int <> 2020
 group by action_date_fiscal_year, awarding_agency_name
 """

sqlst34 = """SELECT action_date_fiscal_year, base_and_exercised_options_value, federal_action_obligation,
base_and_all_options_value, action_date, recipient_duns, recipient_name
FROM usaspending."{0}_analysis_n1" 
where solicitation_procedures_code = 'NP' and action_date_fiscal_year <> '2020' and modification_number ='0'
--and base_and_exercised_options_value >0
order by action_date_fiscal_year
"""

sqlst35 = """SELECT action_date_fiscal_year, count(contract_award_unique_key) as noofaward,
count(distinct recipient_duns) as vendors, sum(base_and_exercised_options_value) as awardvalue
FROM usaspending."{0}_analysis_n1" 
where solicitation_procedures_code = 'NP' and action_date_fiscal_year <> '2020'and modification_number ='0'
--and base_and_exercised_options_value >0
group by action_date_fiscal_year
order by action_date_fiscal_year
"""

sqlst36 = """SELECT action_date_fiscal_year, base_and_exercised_options_value, 
federal_action_obligation, base_and_all_options_value, action_date,
recipient_duns, recipient_name
FROM usaspending."{0}_analysis_n1" 
where solicitation_procedures_code = 'NP' and 
action_date_fiscal_year = '{1}' and modification_number ='0'
"""

sqlst37 = """SELECT action_date_fiscal_year::text, allaward, 
filteredaward FROM usaspending."allagency2" 
where action_date_fiscal_year::int <> 2020
and agency_abb = '{0}' order by action_date_fiscal_year"""

