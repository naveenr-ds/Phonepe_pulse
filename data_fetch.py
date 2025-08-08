#Data Fetch

# Required Libraries

import os
import json
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
#------------------Table 1--------------------------
agg_ins_path=r"D:\DS-Projects\Phonepe\Final\pulse\data\aggregated\insurance\country\india\state"
agg_ins_list=os.listdir(agg_ins_path)

table_1=[]

for i in agg_ins_list:
    state1 = os.path.join(agg_ins_path, i)
    state1_list = os.listdir(state1)

   
    for j in state1_list:
        years1 = os.path.join(state1, j)
        years1_list = os.listdir(years1)

        
        for k in years1_list:
            qtr1 = os.path.join(years1, k)
            data=open(qtr1,"r")

            J=json.load(data)
            
            for n in J["data"]["transactionData"]:
                    insurance = n["name"]
                    count = n['paymentInstruments'][0]['count']
                    amount = n['paymentInstruments'][0]["amount"]
                    

                   
                    table_1.append({
                        'states': i,
                        'years': j,
                        'quarter':int(k.strip('.json')),
                        'transaction_type': insurance,
                        'transaction_count': count,
                        'transaction_amount': amount
                    })

df_table1=pd.DataFrame(table_1)

df_table1['states']=df_table1['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table1['states']=df_table1['states'].str.replace('-',' ')
df_table1['states']=df_table1['states'].str.title()
df_table1['states']=df_table1['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table1.to_csv('table1.csv',index=False)

#SQL DATA BASE CONNECTION

url= "postgresql://neondb_owner:npg_bpCADu5V4PsM@ep-purple-bird-a1wedpb9.ap-southeast-1.aws.neon.tech/phonepe_pulse?sslmode=require&channel_binding=require"
engine=create_engine(url)

df_table1.to_sql('agg_ins', engine, if_exists='replace', index=False)

#----------------------------Table 2--------------------------------
agg_trans_path = r'D:\DS-Projects\Phonepe\Exercise\pulse\data\aggregated\transaction\country\india\state'
agg_trans_list = os.listdir(agg_trans_path)


table_2 = []

for i in agg_trans_list:
    state2 = os.path.join(agg_trans_path, i)
    state2_list = os.listdir(state2)

   
    for j in state2_list:
        years2 = os.path.join(state2, j)
        years2_list = os.listdir(years2)

        
        for k in years2_list:
            qtr2 = os.path.join(years2, k)
            data=open(qtr2,"r")

            J=json.load(data)



                # 4. Step 2 of ETL: Transform 
            for m in J["data"]["transactionData"]:
                        transaction = m["name"]
                        count = m['paymentInstruments'][0]['count']
                        amount = m['paymentInstruments'][0]["amount"]
                        

                    
                        table_2.append({
                            'states': i,
                            'years': j,
                            'quarter':int(k.strip('.json')),
                            'transaction_type': transaction,
                            'transaction_count': count,
                            'transaction_amount': amount
                        })

#5 . Step 3 of ETL: Load 
df_table2 = pd.DataFrame(table_2)

df_table2['states']=df_table2['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table2['states']=df_table2['states'].str.replace('-',' ')
df_table2['states']=df_table2['states'].str.title()
df_table2['states']=df_table2['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table2.to_csv('table2.csv',index=False)
df_table2.to_sql('agg_trans', engine, if_exists='replace', index=False)

#-------------------Table 3 ---------------------------------
agg_user_path = r'D:\DS-Projects\Phonepe\Final\pulse\data\aggregated\user\country\india\state'
agg_user_list = os.listdir(agg_user_path)


table_3 = []

for i in agg_user_list:
    state3 = os.path.join(agg_user_path, i)
    state3_list = os.listdir(state3)

    for j in state3_list:
        years3 = os.path.join(state3, j)
        years3_list = os.listdir(years3)

        for k in years3_list:
            qtr3 = os.path.join(years3, k)
            data=open(qtr3,"r")

            J=json.load(data)

            user = J["data"]["aggregated"].get("registeredUsers")
            app = J["data"]["aggregated"].get("appOpens")

            try:
                for n in J['data']['usersByDevice']:
                    brand=n['brand']
                    count=n['count']
                    percent=n['percentage']

                    table_3.append({
                            'states': i,
                            'years': j,
                            'quarter':int(k.strip('.json')),
                            'registered_users': user,
                            'app_opens': app,
                            'brand': brand,
                            'transaction_count': count,
                            'percentage': percent
                        })
            except:
                pass

df_table3=pd.DataFrame(table_3)

df_table3['states']=df_table3['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table3['states']=df_table3['states'].str.replace('-',' ')
df_table3['states']=df_table3['states'].str.title()
df_table3['states']=df_table3['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table3.to_csv('table3.csv',index=False)
df_table3.to_sql('agg_user', engine, if_exists='replace', index=False)
        
#---------------------------Table 4-----------------------------------------------------
map_ins_path = r"D:\DS-Projects\Phonepe\Final\pulse\data\map\insurance\hover\country\india\state"
map_ins_list = os.listdir(map_ins_path)

table_4= []

for i in map_ins_list:
    state4 = os.path.join(map_ins_path, i)
    state4_list = os.listdir(state4)

   
    for j in state4_list:
        years4 = os.path.join(state4, j)
        years4_list = os.listdir(years4)

        
        for k in years4_list:
            qtr4 = os.path.join(years4, k)

            with open(qtr4, 'r') as f:
                data = json.load(f)
                
                for n in data['data']['hoverDataList']:
                    name=n['name']
                    count=n['metric'][0]['count']
                    amount=n['metric'][0]['amount']
                    table_4.append({
                        'states':i,
                        "years":j,
                        'quarter':int(k.strip('.json')),
                        'districts':name,
                        'transaction_count':count,
                        'transaction_amount':amount
                    })

df_table4=pd.DataFrame(table_4)

df_table4['states']=df_table4['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table4['states']=df_table4['states'].str.replace('-',' ')
df_table4['states']=df_table4['states'].str.title()
df_table4['states']=df_table4['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table4.to_csv('table4.csv',index=False)
df_table4.to_sql('map_ins', engine, if_exists='replace', index=False)

#---------------------------Table 5----------------------------

map_trans_path = r"D:\DS-Projects\Phonepe\Final\pulse\data\map\transaction\hover\country\india\state"
map_trans_list = os.listdir(map_trans_path)

table_5= []

for i in map_trans_list:
    state5 = os.path.join(map_trans_path, i)
    state5_list = os.listdir(state5)

   
    for j in state5_list:
        years5 = os.path.join(state5, j)
        years5_list = os.listdir(years5)

        
        for k in years5_list:
            qtr5 = os.path.join(years5, k)

            with open(qtr5, 'r') as f:
                data = json.load(f)
                
                for n in data['data']['hoverDataList']:
                    name=n['name']
                    count=n['metric'][0]['count']
                    amount=n['metric'][0]['amount']
                    table_5.append({
                        'states':i,
                        "years":j,
                        'quarter':int(k.strip('.json')),
                        'districts':name,
                        'transaction_count':count,
                        'transaction_amount':amount
                    })
                                    

               
df_table5=pd.DataFrame(table_5)

df_table5['states']=df_table5['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table5['states']=df_table5['states'].str.replace('-',' ')
df_table5['states']=df_table5['states'].str.title()
df_table5['states']=df_table5['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table5.to_csv('table5.csv',index=False)
df_table5.to_sql('map_trans', engine, if_exists='replace', index=False)

#-----------------------Table 6-----------------------------------

map_user_path = r"D:\DS-Projects\Phonepe\Final\pulse\data\map\user\hover\country\india\state"
map_user_list = os.listdir(map_user_path)

table_6= []
for i in map_user_list:
    state6 = os.path.join(map_user_path, i)
    state6_list = os.listdir(state6)

   
    for j in state6_list:
        years6 = os.path.join(state6, j)
        years6_list = os.listdir(years6)

        
        for k in years6_list:
            qtr6 = os.path.join(years6, k)
            data=open(qtr6,"r")

            J=json.load(data)

            try:
                for n in J['data']['hoverData'].items():
                    districts=n[0]
                    regusers=n[1]['registeredUsers']
                    appopens=n[1]['appOpens']

                    table_6.append({
                        'states':i,
                        "years":j,
                        'quarter':int(k.strip('.json')),
                        'districts':districts,
                        'registered_users':regusers,
                        'app_opens':appopens,
                    })
            except:
                pass

df_table6=pd.DataFrame(table_6)

df_table6['states']=df_table6['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table6['states']=df_table6['states'].str.replace('-',' ')
df_table6['states']=df_table6['states'].str.title()
df_table6['states']=df_table6['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table6.to_csv('table6.csv',index=False)
df_table6.to_sql('map_user', engine, if_exists='replace', index=False)

#---------------------------Table 7------------------------------------

top_ins_path = r"D:\DS-Projects\Phonepe\Final\pulse\data\top\insurance\country\india\state"
top_ins_list = os.listdir(top_ins_path)

table_7= []

for i in top_ins_list:
    state7 = os.path.join(top_ins_path, i)
    state7_list = os.listdir(state7)

   
    for j in state7_list:
        years7 = os.path.join(state7, j)
        years7_list = os.listdir(years7)

        
        for k in years7_list:
            qtr7 = os.path.join(years7, k)
            data=open(qtr7,"r")

            J=json.load(data)

            for n in J["data"]["pincodes"]:
                name=n['entityName']
                count=n['metric']['count']
                amount=n['metric']['amount']

                table_7.append({
                        'states':i,
                        "years":j,
                        'quarter':int(k.strip('.json')),
                        'pincodes':name,
                        'transaction_count':count,
                        'transaction_amount':amount
                    })

df_table7=pd.DataFrame(table_7)

df_table7['states']=df_table7['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table7['states']=df_table7['states'].str.replace('-',' ')
df_table7['states']=df_table7['states'].str.title()
df_table7['states']=df_table7['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table7.to_csv('table7.csv',index=False)
df_table7.to_sql('top_ins', engine, if_exists='replace', index=False)

#-------------------------------Table 8-------------------------

top_trans_path = r"D:\DS-Projects\Phonepe\Final\pulse\data\top\transaction\country\india\state"
top_trans_list = os.listdir(top_trans_path)

table_8= []

for i in top_trans_list:
    state8 = os.path.join(top_trans_path, i)
    state8_list = os.listdir(state8)

   
    for j in state8_list:
        years8 = os.path.join(state8, j)
        years8_list = os.listdir(years8)

        
        for k in years8_list:
            qtr8 = os.path.join(years8, k)
            data=open(qtr8,"r")

            J=json.load(data)

            for n in J["data"]["pincodes"]:
                name=n['entityName']
                count=n['metric']['count']
                amount=n['metric']['amount']

                table_8.append({
                        'states':i,
                        "years":j,
                        'quarter':int(k.strip('.json')),
                        'pincodes':name,
                        'transaction_count':count,
                        'transaction_amount':amount
                    })

df_table8=pd.DataFrame(table_8)

df_table8['states']=df_table8['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table8['states']=df_table8['states'].str.replace('-',' ')
df_table8['states']=df_table8['states'].str.title()
df_table8['states']=df_table8['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table8.to_csv('table8.csv',index=False)
df_table8.to_sql('top_trans', engine, if_exists='replace', index=False)

#-----------------------Table 9--------------------

top_user_path = r"D:\DS-Projects\Phonepe\Final\pulse\data\top\user\country\india\state"
top_user_list = os.listdir(top_user_path )

table_9= []

for i in top_user_list:
    state9 = os.path.join(top_user_path , i)
    state9_list = os.listdir(state9)

   
    for j in state9_list:
        years9= os.path.join(state9, j)
        years9_list = os.listdir(years9)

        
        for k in years9_list:
            qtr9 = os.path.join(years9, k)
            data=open(qtr9,"r")

            J=json.load(data)

            for n in J["data"]["pincodes"]:
                name=n['name']
                regusers=n['registeredUsers']

                table_9.append({
                        'states':i,
                        "years":j,
                        'quarter':int(k.strip('.json')),
                        'pincodes':name,
                        'registered_users':regusers
                    })


df_table9=pd.DataFrame(table_9)

df_table9['states']=df_table9['states'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_table9['states']=df_table9['states'].str.replace('-',' ')
df_table9['states']=df_table9['states'].str.title()
df_table9['states']=df_table9['states'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

df_table9.to_csv('table9.csv',index=False)
df_table9.to_sql('top_user', engine, if_exists='replace', index=False)

#--------------------------Its Completetd-----------------------------------
#Developed By : Naveen Kumar R 
#email:naveenramu2003@gmail.com
#Linked In : https://www.linkedin.com/in/naveenkumar-ds

