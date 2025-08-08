#Use Case Scenario Analysis

# Required Libraries
from sqlalchemy import create_engine
import psycopg2
import pandas as pd 
import plotly.express as px

#SQL DATABASE CONNECTION

url= "postgresql://neondb_owner:npg_bpCADu5V4PsM@ep-purple-bird-a1wedpb9.ap-southeast-1.aws.neon.tech/phonepe_pulse?sslmode=require&channel_binding=require"
engine=create_engine(url)

# -----------------------Scenario 1-----------------------------

# Query 1:Transaction Dynamics Across States

q1="SELECT states,SUM(transaction_count) AS total_transaction_count,SUM(transaction_amount ) AS total_transaction_amount FROM agg_trans GROUP BY states ORDER BY total_transaction_amount DESC";
query=pd.read_sql_query(q1,engine)

# Query 2: Transaction Dynamics Over Quarters

q2='SELECT years, quarter, SUM(transaction_count) AS total_transaction_count,SUM(transaction_amount) AS total_transaction_amount FROM agg_trans GROUP BY years, quarter ORDER BY  years, quarter';
query=pd.read_sql_query(q2,engine)

# Query 3: Transaction Dynamics by Payment Category

q3='SELECT transaction_type, SUM(transaction_count) AS total_trans_count, SUM(transaction_amount) AS total_amount FROM agg_trans GROUP BY transaction_type ORDER BY total_amount DESC';
query=pd.read_sql_query(q3,engine)

# Query 4: Consistent Growth, Stagnation, or Decline Across States

q4=query='SELECT states, years,quarter,SUM(transaction_amount) AS total_amount FROM agg_trans GROUP BY states, years,quarter ORDER BY states, years,quarter';
q4=pd.read_sql_query(query,engine)

# Query 5: Consistent Growth, Stagnation, or Decline by Transaction Type

query='SELECT transaction_type , years, quarter,SUM(transaction_amount) AS transaction_Amount  FROM agg_trans GROUP BY transaction_type, years ,quarter ORDER BY transaction_type, years ,quarter ';
q5=pd.read_sql_query(query,engine)

#-------------------------- Scenario 2-------------------------

# Query 6 : - Device Dominance

q6='SELECT brand, SUM(registered_users) AS total_users FROM agg_user GROUP BY brand ORDER BY total_users DESC';
query=pd.read_sql_query(q6,engine)

# Query 7 : User Engagement

q7='SELECT brand, SUM(registered_users) AS total_users, SUM(app_opens) AS total_opens, ROUND(SUM(app_opens) * 1.0 / NULLIF(SUM(registered_users), 0), 2) AS engagement_rate FROM agg_user GROUP BY brand ORDER BY engagement_rate DESC';
query=pd.read_sql_query(q7,engine)

# Query 8 : Underutilized Devices

q8='SELECT brand, states, SUM(registered_users) AS total_users, SUM(app_opens) AS total_opens, ROUND(SUM(app_opens) * 1.0 / NULLIF(SUM(registered_users), 0), 2) AS engagement_rate FROM agg_user GROUP BY brand, states HAVING ROUND(SUM(app_opens) * 1.0 / NULLIF(SUM(registered_users), 0), 2) < 0.3 AND SUM(registered_users) > 500000 ORDER BY engagement_rate ASC';
query=pd.read_sql_query(q8,engine)
print(query)

# Query 9 : Region-Wise Engagement 

q9='SELECT states, brand, SUM(registered_users) AS total_users, SUM(app_opens) AS total_opens, ROUND(SUM(app_opens) * 1.0 / NULLIF(SUM(registered_users), 0), 2) AS engagement_rate FROM agg_user GROUP BY states, brand ORDER BY states, engagement_rate DESC';
query=pd.read_sql_query(q9,engine)
print(query)

# Query 10 : Quarterly Trends

q10='SELECT years, quarter, brand, SUM(registered_users) AS total_users, SUM(app_opens) AS total_opens, ROUND(SUM(app_opens) * 1.0 / NULLIF(SUM(registered_users), 0), 2) AS engagement_rate FROM agg_user GROUP BY years, quarter, brand ORDER BY years, quarter, engagement_rate DESC';
query=pd.read_sql_query(q10,engine)
print(query)

#----------------------------- Scenario 3---------------------------

# Query 11 : State Ranking by Insurance Volume & Value

q11='SELECT states, SUM(transaction_count) AS total_ins_count, SUM(transaction_amount) AS total_ins_amount FROM agg_trans GROUP BY states ORDER BY total_ins_amount DESC';
query=pd.read_sql_query(q11,engine)

# Query 12 : Quarterly & Yearly Growth Trajectory 

q12='SELECT states, years, quarter, cnt, amt, ROUND(((amt - LAG(amt) OVER (PARTITION BY states ORDER BY years, quarter)) / NULLIF(LAG(amt) OVER (PARTITION BY states ORDER BY years, quarter),0))::numeric,4) AS qoq_growth_pct, ROUND(((amt - LAG(amt) OVER (PARTITION BY states ORDER BY years)) / NULLIF(LAG(amt) OVER (PARTITION BY states ORDER BY years),0))::numeric,4) AS yoy_growth_pct FROM (SELECT states, years, quarter, SUM(transaction_count) AS cnt, SUM(transaction_amount) AS amt FROM agg_ins GROUP BY states, years, quarter) AS subq ORDER BY states, years, quarter';
query=pd.read_sql_query(q12,engine)

# Query 13 : Fastest-Growing Insurance Markets

q13='SELECT states, years, quarter, ROUND(((amt - prev_amt) / NULLIF(prev_amt, 0))::numeric, 4) AS growth_pct FROM (SELECT states, years, quarter, SUM(transaction_amount) AS amt, LAG(SUM(transaction_amount)) OVER (PARTITION BY states ORDER BY years, quarter) AS prev_amt FROM agg_ins GROUP BY states, years, quarter) AS growth WHERE prev_amt IS NOT NULL ORDER BY growth_pct DESC LIMIT 10';
query=pd.read_sql_query(q13,engine)

# ------------------------------Scenario 4------------------------------------

# Query 14 : Transaction analysis by top-performing states

q14='SELECT states, SUM(transaction_count) AS total_txn, SUM(transaction_amount) AS total_value FROM agg_trans GROUP BY states ORDER BY total_value DESC, total_txn DESC LIMIT 1';
query=pd.read_sql_query(q14,engine)

# Query 15 : Transaction analysis by top-performing Districts

q15='SELECT districts, SUM(transaction_count) AS total_txn, SUM(transaction_amount) AS total_value FROM map_trans GROUP BY districts ORDER BY total_value DESC, total_txn DESC ';
query=pd.read_sql_query(q15,engine)

# Query 16 : Transaction analysis by top-performing Pincodes

q16='SELECT pincodes, SUM(transaction_count) AS total_txn, SUM(transaction_amount) AS total_value FROM top_trans GROUP BY pincodes ORDER BY total_value DESC, total_txn DESC LIMIT 10';
query=pd.read_sql_query(q16,engine)

# ---------------------------Scenario 5------------------------------------------

# Query 17 :  User Registration Analysis by top states
# WHERE - For Specific Years Checking 

q17='SELECT states, SUM(registered_users) AS total_users FROM agg_user GROUP BY states ORDER BY total_users DESC LIMIT 10';
#WHERE years = 2024 AND quarter = 4 
query=pd.read_sql_query(q17,engine)

# Query 18 :  User Registration Analysis by top districts

q18='SELECT districts, SUM(registered_users) AS total_users FROM map_user GROUP BY districts ORDER BY total_users DESC LIMIT 1';
#WHERE years = 2024 AND quarter = 4
query=pd.read_sql_query(q18,engine)

# Query 19 :  User Registration Analysis by top pincodes

q19='SELECT pincodes, SUM(registered_users) AS total_users FROM top_user GROUP BY pincodes ORDER BY total_users DESC LIMIT 10';
#WHERE years = 2024 AND quarter = 4
query=pd.read_sql_query(q19,engine)

# -----------------------------scenario 6-----------------------

# Query 20 : Insurance Transactions Analysis Top States 

q20='SELECT states, SUM(transaction_count) AS total_txn, SUM(transaction_amount) AS total_value FROM agg_ins GROUP BY states ORDER BY total_value DESC, total_txn DESC LIMIT 10';
#WHERE years = 2024 AND quarter = 4 
query=pd.read_sql_query(q20,engine)

# Query 21 : Insurance Transactions Analysis Top districts

q21='SELECT districts, SUM(transaction_count) AS total_txn, SUM(transaction_amount) AS total_value FROM map_ins GROUP BY districts ORDER BY total_value DESC, total_txn DESC LIMIT 10';
#WHERE years = 2024 AND quarter = 4
query=pd.read_sql_query(q21,engine)

# Query 22 : Insurance Transactions Analysis Top Pincodes

q22='SELECT pincodes, SUM(transaction_count) AS total_txn, SUM(transaction_amount) AS total_value FROM top_ins GROUP BY pincodes ORDER BY total_value DESC, total_txn DESC LIMIT 10';
#WHERE years = 2024 AND quarter = 4
query=pd.read_sql_query(q22,engine)

#----------------------------------------------------------------------------
#Developed By : 

# Naveen Kumar R 
#email:naveenramu2003@gmail.com
#Linked In : https://www.linkedin.com/in/naveenkumar-ds
