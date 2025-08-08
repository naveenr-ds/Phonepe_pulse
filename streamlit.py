#libraries

import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_option_menu import option_menu
from sqlalchemy import create_engine
import psycopg2
import requests
import json


# Connect to your Neon PostgreSQL DB
url = "postgresql://neondb_owner:npg_bpCADu5V4PsM@ep-purple-bird-a1wedpb9.ap-southeast-1.aws.neon.tech/phonepe_pulse?sslmode=require&channel_binding=require"
engine = create_engine(url)

# Page setup
st.set_page_config(page_title="PhonePe Pulse Dashboard", layout="wide")

# Top navigation bar
selected = option_menu(
    menu_title='Welcome To Phonepepulse',
    options=["🏠 Home", "📊 Pulse Insights", "📄 Docs"],
    icons=["house", "bar-chart-line", "file-earmark-text"],
    orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": "#3d1a6e"},
        "nav-link": {"color": "white", "font-size": "16px", "margin": "0 20px"},
        "nav-link-selected": {"color": "#a370f7", "font-weight": "bold"},
    }
)

# Section 1 - Home
if selected == "🏠 Home":
    
    st.title("📱PhonePe")
    




    # About PhonePe
    st.markdown("""
    **PhonePe** is one of India’s leading digital payment platforms that allows users to:
    
    - Send and receive money instantly via UPI 💸  
    - Recharge mobile phones, pay utility bills ⚡📱  
    - Shop online and offline with QR code scanning 🛒  
    - Invest in mutual funds, insurance, and more 📊  
      
    It powers **millions of transactions every day** and provides **insightful data**, which we explore here in this dashboard.
    """)

    st.success("Explore how India transacts with PhonePe Pulse 📊")

    st.markdown("---")
    st.markdown("🔍 Use the top menu to start exploring data or view documentation.")


# Section 2 - Data Analysis Dashboards
elif selected == "📊 Pulse Insights":
    st.title("📊 Explore Transaction Data Insights")

    # Sidebar - Choose Scenario
    scenario = st.sidebar.selectbox("📌 Select Scenario", [
        "1. Transaction Dynamics on PhonePe",
        "2. Device Dominance and User Engagement Analysis",
        "3. Transaction Analysis",
        "4. User Registration Analysis",
        "5. Insurance Transactions Analysis"
    ])

    # Scenario 1
    if scenario == "1. Transaction Dynamics on PhonePe":
        q = st.selectbox("Choose a Question", [
            "I. Transaction Dynamics Across States",
            "II. Transaction Dynamics Over Quarters",
            "III. Transaction Dynamics by Payment Category",
            "IV. Consistent Growth, Stagnation, or Decline Across States",
            "V. Consistent Growth, Stagnation, or Decline by Transaction Type"

        ])

        # Question I
        if q == "I. Transaction Dynamics Across States":
            st.subheader("📌 Transaction Dynamics Across States")

            q1 = """
            SELECT states, 
                   SUM(transaction_count) AS total_transaction_count, 
                   SUM(transaction_amount) AS total_transaction_amount 
            FROM agg_trans 
            GROUP BY states 
            ORDER BY total_transaction_amount DESC
            """
            query = pd.read_sql_query(q1, engine)

            fig = px.bar(
                query,
                x='states',
                y='total_transaction_amount',
                title='Total Transaction Amount by State',
                labels={'total_transaction_amount': 'Transaction Amount (₹)', 'states': 'State'},
                hover_data=['total_transaction_count']
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query)

        # Question II
        elif q == "II. Transaction Dynamics Over Quarters":
            st.subheader("📌 Transaction Dynamics Over Quarters")

            q2 = """
            SELECT 
                years, 
                quarter, 
                SUM(transaction_count) AS total_transaction_count,
                SUM(transaction_amount) AS total_transaction_amount
            FROM agg_trans
            GROUP BY years, quarter
            ORDER BY years, quarter
            """
            query = pd.read_sql_query(q2, engine)

            # Create 'period' column
            query['period'] = query['years'].astype(str) + ' Q' + query['quarter'].astype(str)

            fig = px.bar(
                query,
                x='period',
                y='total_transaction_amount',
                title='Total Transaction Amount by Quarter',
                labels={'total_transaction_amount': 'Transaction Amount (₹)', 'period': 'Quarter'},
                hover_data=['total_transaction_count']
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query)

        #Question III
        elif q == "III. Transaction Dynamics by Payment Category":
            st.subheader("📌 Transaction Dynamics by Payment Category")

            q3 = """
            SELECT 
                transaction_type, 
                SUM(transaction_count) AS total_trans_count, 
                SUM(transaction_amount) AS total_amount 
            FROM agg_trans 
            GROUP BY transaction_type 
            ORDER BY total_amount DESC
            """
            query = pd.read_sql_query(q3, engine)

            st.write("This chart shows which payment categories (like Recharge, Bills, Peer-to-Peer, etc.) drive the highest transaction amounts and volumes on PhonePe.")

            fig = px.bar(
                query,
                x='transaction_type',
                y='total_amount',
                title='Total Transaction Amount by Payment Category',
                labels={'total_amount': 'Transaction Amount (₹)', 'transaction_type': 'Payment Category'},
                hover_data=['total_trans_count']
            )

            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query)

        #Question IV    
        elif q == "IV. Consistent Growth, Stagnation, or Decline Across States":
            st.subheader("📌 Consistent Growth, Stagnation, or Decline Across States")

            query = """
            SELECT 
                states, 
                years,
                quarter,
                SUM(transaction_amount) AS total_amount 
            FROM agg_trans 
            GROUP BY states, years, quarter 
            ORDER BY states, years, quarter
            """
            q4 = pd.read_sql_query(query, engine)

            # Create 'period' column for time series (e.g., "2021 Q1")
            q4['period'] = q4['years'].astype(str) + ' Q' + q4['quarter'].astype(str)

            st.write("This line chart shows how the total transaction amount in each state changes quarter by quarter. You can spot which states are growing consistently, and which ones are stagnant or declining.")

            fig = px.line(
                q4,
                x='period',
                y='total_amount',
                color='states',
                title='Transaction Growth Trend Across States (Quarterly)',
                labels={'total_amount': 'Transaction Amount (₹)', 'period': 'Quarter'},
                markers=True
            )

            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(q4)

        #Question V
        elif q == "V. Consistent Growth, Stagnation, or Decline by Transaction Type":
            st.subheader("📌 Consistent Growth, Stagnation, or Decline by Transaction Type")

            query = """
            SELECT 
                transaction_type, 
                years, 
                quarter,
                SUM(transaction_amount) AS transaction_amount  
            FROM agg_trans 
            GROUP BY transaction_type, years, quarter 
            ORDER BY transaction_type, years, quarter
            """
            q5 = pd.read_sql_query(query, engine)

            # Create a time period column
            q5['period'] = q5['years'].astype(str) + ' Q' + q5['quarter'].astype(str)

            st.write("This chart shows how transaction amounts vary over time across different transaction types, helping us identify which categories are growing, stable, or declining.")

            fig = px.line(
                q5,
                x='period',
                y='transaction_amount',
                color='transaction_type',
                title='Transaction Trend by Payment Category Over Time',
                labels={'transaction_Amount': 'Transaction Amount (₹)', 'period': 'Quarter'},
                markers=True
            )

            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(q5)

    elif scenario == "2. Device Dominance and User Engagement Analysis":
        q = st.selectbox("Choose a Question", [
            "I. Underutilized Devices",
            "II. Device Dominance",
            "III. Region-Wise Engagement",
        ])

        if q == "I. Underutilized Devices":
            st.subheader("📌 Underutilized Devices by State")

            q1 = """
            SELECT 
                brand, 
                states, 
                SUM(registered_users) AS total_users, 
                SUM(app_opens) AS total_opens,
                ROUND(SUM(app_opens) * 1.0 / NULLIF(SUM(registered_users), 0), 2) AS engagement_rate 
            FROM agg_user 
            GROUP BY brand, states 
            HAVING 
                (SUM(app_opens) * 1.0 / NULLIF(SUM(registered_users), 0)) < 0.3
                AND SUM(registered_users) > 500000 
            ORDER BY engagement_rate ASC;
            """
            query = pd.read_sql_query(q1, engine)

            st.write("Devices with large user bases but low app engagement. These could be optimized through targeted marketing or app performance improvements.")

            fig = px.bar(
                query,
                x='engagement_rate',
                y=query['brand'] + " (" + query['states'] + ")",  # Combine on the fly
                orientation='h',
                title='Underutilized Devices by State (Engagement Rate < 30%)',
                labels={'engagement_rate': 'Engagement Rate'},
                hover_data=['total_users', 'total_opens']
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query)

        elif q == "II. Device Dominance":
            st.subheader("📌 Top Device Brands by Registered Users")

            q2 = """
            SELECT 
                brand, 
                SUM(registered_users) AS total_users 
            FROM agg_user 
            GROUP BY brand 
            ORDER BY total_users DESC;
            """
            query = pd.read_sql_query(q2, engine)

            st.write("This chart highlights which device brands dominate PhonePe's user base.")

            fig = px.bar(
                query,
                x='brand',
                y='total_users',
                title='Top Device Brands by Registered Users',
                labels={'brand': 'Device Brand', 'total_users': 'Registered Users'},
                hover_data=['total_users']
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query)

        elif q == "III. Region-Wise Engagement":
            st.subheader("📌 Region-Wise Engagement Rate by Device Brand")

            q3 = """
            SELECT 
                states, 
                brand, 
                SUM(registered_users) AS total_users, 
                SUM(app_opens) AS total_opens, 
                ROUND(SUM(app_opens) * 1.0 / NULLIF(SUM(registered_users), 0), 2) AS engagement_rate 
            FROM agg_user 
            GROUP BY states, brand 
            ORDER BY states, engagement_rate DESC;
            """
            query = pd.read_sql_query(q3, engine)

            # Combine brand and state for y-axis label
            query['brand_state'] = query['brand'] + " (" + query['states'] + ")"

            st.write("This chart displays engagement rate by state and device brand. High engagement rates may indicate strong user loyalty or better app performance on specific devices.")

            fig = px.bar(
                query,
                x='engagement_rate',
                y='brand_state',
                orientation='h',
                title='Region-Wise Engagement Rate by Brand',
                labels={'engagement_rate': 'Engagement Rate'},
                hover_data=['states', 'brand', 'total_users', 'total_opens']
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query)

    
    elif scenario == "3. Transaction Analysis":
        q = st.selectbox(
            "Choose a Question",
            ["I. Transaction Analysis by Top-Performing States",
             "II. Transaction Analysis by Top-Performing Districts",
             "III. Transaction Analysis by Top-Performing Pincodes"]
        )

        if q == "I. Transaction Analysis by Top-Performing States":
            st.subheader("📊 Transaction Analysis by Top-Performing States")

            # 1. Load data from your database
            sql = """
            SELECT
                states,
                SUM(transaction_count)  AS total_txn,
                SUM(transaction_amount) AS total_value
            FROM agg_trans
            GROUP BY states
            ORDER BY total_value DESC, total_txn DESC
            """
            df = pd.read_sql_query(sql, engine)

            # 2. Map your raw state codes to the exact names in the GeoJSON
            state_name_map = {
                'Andaman & Nicobar': 'Andaman & Nicobar Islands',
                'Andhra Pradesh': 'Andhra Pradesh',
                'Arunachal Pradesh': 'Arunachal Pradesh',
                'Assam': 'Assam',
                'Bihar': 'Bihar',
                'Chandigarh': 'Chandigarh',
                'Chhattisgarh': 'Chhattisgarh',
                'Dadra and Nagar Haveli and Daman and Diu': 'Dadra and Nagar Haveli and Daman and Diu',
                'Delhi': 'Delhi',
                'Goa': 'Goa',
                'Gujarat': 'Gujarat',
                'Haryana': 'Haryana',
                'Himachal Pradesh': 'Himachal Pradesh',
                'Jammu & Kashmir': 'Jammu and Kashmir',
                'Jharkhand': 'Jharkhand',
                'Karnataka': 'Karnataka',
                'Kerala': 'Kerala',
                'Ladakh': 'Ladakh',
                'Lakshadweep': 'Lakshadweep',
                'Madhya Pradesh': 'Madhya Pradesh',
                'Maharashtra': 'Maharashtra',
                'Manipur': 'Manipur',
                'Meghalaya': 'Meghalaya',
                'Mizoram': 'Mizoram',
                'Nagaland': 'Nagaland',
                'Odisha': 'Odisha',
                'Puducherry': 'Puducherry',
                'Punjab': 'Punjab',
                'Rajasthan': 'Rajasthan',
                'Sikkim': 'Sikkim',
                'Tamil Nadu': 'Tamil Nadu',
                'Telangana': 'Telangana',
                'Tripura': 'Tripura',
                'Uttar Pradesh': 'Uttar Pradesh',
                'Uttarakhand': 'Uttarakhand',
                'West Bengal': 'West Bengal'
            }
            df['state_clean'] = df['states'].map(state_name_map)
            df = df.dropna(subset=['state_clean'])

            # 3. Fetch the GeoJSON from JBrobst’s gist
            geojson_url = (
                "https://gist.githubusercontent.com/jbrobst/"
                "56c13bbbf9d97d187fea01ca62ea5112/raw/"
                "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            )
            resp = requests.get(geojson_url)
            resp.raise_for_status()
            india_geojson = resp.json()

            # 4. Build the choropleth
            fig = px.choropleth(
                df,
                geojson=india_geojson,
                featureidkey="properties.ST_NM",  # matches the Gist’s “ST_NM”
                locations="state_clean",
                color="total_value",
                color_continuous_scale="Viridis",
                title="Top Performing States by Transaction Value",
                hover_data={"total_txn": True, "total_value": True}
            )

            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(margin={"r":0,"t":50,"l":0,"b":0})

            # 5. Render in Streamlit
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df.head(10))
        

        elif q == "II. Transaction Analysis by Top-Performing Districts":
            st.subheader("🏙️ Transaction Analysis by Top-Performing Districts")

            q15 = """
            SELECT 
                districts, 
                SUM(transaction_count) AS total_txn, 
                SUM(transaction_amount) AS total_value 
            FROM map_trans 
            GROUP BY districts 
            ORDER BY total_value DESC, total_txn DESC
            LIMIT 10
            """
            query = pd.read_sql_query(q15, engine)

            st.write("✅ Loaded top 10 districts:")
            

            fig = px.bar(
                query,
                x='districts',
                y='total_value',
                title='Top 10 Districts by Total Transaction Value',
                labels={'total_value': 'Transaction Value (₹)', 'districts': 'District'},
                hover_data=['total_txn']
            )

            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query)

        elif q == "III. Transaction Analysis by Top-Performing Pincodes":
            st.subheader("📮 Transaction Analysis by Top-Performing Pincodes")

            # SQL query
            q16 = """
            SELECT 
                pincodes, 
                SUM(transaction_count) AS total_txn, 
                SUM(transaction_amount) AS total_value 
            FROM top_trans 
            GROUP BY pincodes 
            ORDER BY total_value DESC, total_txn DESC 
            LIMIT 10
            """
            query = pd.read_sql_query(q16, engine)

            # Display top 10 pincodes in a table
            st.write("🔢 Top 10 Pincodes by Total Transaction Value:")
            

            # Create bar chart
            fig = px.bar(
                query,
                x='pincodes',
                y='total_value',
                title='Top 10 Pincodes by Transaction Value',
                labels={'pincodes': 'Pincode', 'total_value': 'Transaction Value (₹)'},
                hover_data=['total_txn']
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query)


    elif scenario == "4. User Registration Analysis":
        q = st.selectbox("Choose a Question", [
            "I. User Registration Analysis by Top States",
            "II. User Registration Analysis by Top Districts",
            "III. User Registration Analysis by Top Pincodes"
        ])

        if q == "I. User Registration Analysis by Top States":
            st.subheader("📈 User Registration Analysis by Top States")

            # 1. Year and Quarter selection
            selected_year = st.selectbox("Select Year", [2018, 2019, 2020, 2021, 2022, 2023, 2024])
            selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

            # 2. SQL Query with string casting for compatibility
            sql = f"""
            SELECT
                states,
                SUM(registered_users) AS total_users
            FROM agg_user
            WHERE years = '{selected_year}' AND quarter = '{selected_quarter}'
            GROUP BY states
            ORDER BY total_users DESC;
            """
            df = pd.read_sql_query(sql, engine)
            

            # 3. Clean state names
            state_name_map = {
                'Andaman & Nicobar': 'Andaman & Nicobar Islands',
                'Andhra Pradesh': 'Andhra Pradesh',
                'Arunachal Pradesh': 'Arunachal Pradesh',
                'Assam': 'Assam',
                'Bihar': 'Bihar',
                'Chandigarh': 'Chandigarh',
                'Chhattisgarh': 'Chhattisgarh',
                'Dadra and Nagar Haveli and Daman and Diu': 'Dadra and Nagar Haveli and Daman and Diu',
                'Delhi': 'Delhi',
                'Goa': 'Goa',
                'Gujarat': 'Gujarat',
                'Haryana': 'Haryana',
                'Himachal Pradesh': 'Himachal Pradesh',
                'Jammu & Kashmir': 'Jammu and Kashmir',
                'Jharkhand': 'Jharkhand',
                'Karnataka': 'Karnataka',
                'Kerala': 'Kerala',
                'Ladakh': 'Ladakh',
                'Lakshadweep': 'Lakshadweep',
                'Madhya Pradesh': 'Madhya Pradesh',
                'Maharashtra': 'Maharashtra',
                'Manipur': 'Manipur',
                'Meghalaya': 'Meghalaya',
                'Mizoram': 'Mizoram',
                'Nagaland': 'Nagaland',
                'Odisha': 'Odisha',
                'Puducherry': 'Puducherry',
                'Punjab': 'Punjab',
                'Rajasthan': 'Rajasthan',
                'Sikkim': 'Sikkim',
                'Tamil Nadu': 'Tamil Nadu',
                'Telangana': 'Telangana',
                'Tripura': 'Tripura',
                'Uttar Pradesh': 'Uttar Pradesh',
                'Uttarakhand': 'Uttarakhand',
                'West Bengal': 'West Bengal'
            }
            df['state_clean'] = df['states'].map(state_name_map)
            df = df.dropna(subset=['state_clean'])

            # 4. Load India GeoJSON
            geojson_url = (
                "https://gist.githubusercontent.com/jbrobst/"
                "56c13bbbf9d97d187fea01ca62ea5112/raw/"
                "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            )
            try:
                resp = requests.get(geojson_url)
                resp.raise_for_status()
                india_geojson = resp.json()
            except Exception as e:
                st.error(f"Error loading GeoJSON: {e}")
                st.stop()

            # 5. Render the visualization
            if not df.empty:
                fig = px.choropleth(
                    df,
                    geojson=india_geojson,
                    featureidkey="properties.ST_NM",
                    locations="state_clean",
                    color="total_users",
                    color_continuous_scale="Blues",
                    title=f"Top 10 States by User Registrations (Year: {selected_year}, Q{selected_quarter})",
                    hover_data={"total_users": True}
                )
                fig.update_geos(fitbounds="locations", visible=False)
                fig.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 0})

                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df.head(10))
            else:
                st.warning("No data available for the selected year and quarter.")


        elif q == "II. User Registration Analysis by Top Districts":
            st.subheader("🏙️ User Registration Analysis by Top Districts")

            # Filter selections for year and quarter
            year = st.selectbox("Select Year", [2021, 2022, 2023, 2024])
            quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

            # SQL Query
            q18 = f"""
            SELECT 
                districts, 
                SUM(registered_users) AS total_users 
            FROM map_user 
            WHERE years ='{year}'AND quarter = '{quarter}'
            GROUP BY districts 
            ORDER BY total_users DESC ;

            """
            query = pd.read_sql_query(q18, engine)

            st.write(f"✅ Top 10 Districts for {year} Q{quarter}:")


            # Plotly Bar Chart
            fig = px.bar(
                query,
                x='districts',
                y='total_users',
                title=f"Top 10 Districts by Registered Users ({year} Q{quarter})",
                labels={'total_users': 'Registered Users', 'districts': 'District'},
                text='total_users'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(xaxis_tickangle=-45)

            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query.head(10))

        elif q == "III. User Registration Analysis by Top Pincodes":
            st.subheader("📍 User Registration Analysis by Top Pincodes")

            # Filter selections for year and quarter
            year = st.selectbox("Select Year", [2021, 2022, 2023, 2024])
            quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

            # SQL Query
            q19 = f"""
            SELECT 
                pincodes, 
                SUM(registered_users) AS total_users 
            FROM top_user 
            WHERE years = '{year}' AND quarter = '{quarter}'
            GROUP BY pincodes 
            ORDER BY total_users DESC ;
            """
            query = pd.read_sql_query(q19, engine)

            st.write(f"✅ Top 10 Pincodes for {year} Q{quarter}:")

            # Plotly Bar Chart
            fig = px.bar(
                query,
                x='pincodes',
                y='total_users',
                title=f"Top 10 Pincodes by Registered Users ({year} Q{quarter})",
                labels={'total_users': 'Registered Users', 'pincodes': 'Pincode'},
                text='total_users'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(xaxis_tickangle=-45)

            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query.head(10))


    elif scenario == "5. Insurance Transactions Analysis":
        q = st.selectbox("Choose a Question", [
            "I. Insurance Transactions Analysis Top States",
            "II. Insurance Transactions Analysis by Top Districts",
            "III. Insurance Transactions Analysis by Top Pincodes"
        ])

        if q == "I. Insurance Transactions Analysis Top States":
            st.subheader("🏥 Insurance Transactions Analysis by Top States")

            # 1. Year and Quarter selection
            selected_year = st.selectbox("Select Year", [ 2020, 2021, 2022, 2023, 2024])
            selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

            # 2. SQL Query for Insurance Transactions
            q20 = f"""
            SELECT
                states,
                SUM(transaction_count) AS total_txn,
                SUM(transaction_amount) AS total_value
            FROM agg_ins
            WHERE years = '{selected_year}' AND quarter = '{selected_quarter}'
            GROUP BY states
            ORDER BY total_value DESC, total_txn DESC;
            """
            df = pd.read_sql_query(q20, engine)

            # 3. Clean state names
            state_name_map = {
                'Andaman & Nicobar': 'Andaman & Nicobar Islands',
                'Andhra Pradesh': 'Andhra Pradesh',
                'Arunachal Pradesh': 'Arunachal Pradesh',
                'Assam': 'Assam',
                'Bihar': 'Bihar',
                'Chandigarh': 'Chandigarh',
                'Chhattisgarh': 'Chhattisgarh',
                'Dadra and Nagar Haveli and Daman and Diu': 'Dadra and Nagar Haveli and Daman and Diu',
                'Delhi': 'Delhi',
                'Goa': 'Goa',
                'Gujarat': 'Gujarat',
                'Haryana': 'Haryana',
                'Himachal Pradesh': 'Himachal Pradesh',
                'Jammu & Kashmir': 'Jammu and Kashmir',
                'Jharkhand': 'Jharkhand',
                'Karnataka': 'Karnataka',
                'Kerala': 'Kerala',
                'Ladakh': 'Ladakh',
                'Lakshadweep': 'Lakshadweep',
                'Madhya Pradesh': 'Madhya Pradesh',
                'Maharashtra': 'Maharashtra',
                'Manipur': 'Manipur',
                'Meghalaya': 'Meghalaya',
                'Mizoram': 'Mizoram',
                'Nagaland': 'Nagaland',
                'Odisha': 'Odisha',
                'Puducherry': 'Puducherry',
                'Punjab': 'Punjab',
                'Rajasthan': 'Rajasthan',
                'Sikkim': 'Sikkim',
                'Tamil Nadu': 'Tamil Nadu',
                'Telangana': 'Telangana',
                'Tripura': 'Tripura',
                'Uttar Pradesh': 'Uttar Pradesh',
                'Uttarakhand': 'Uttarakhand',
                'West Bengal': 'West Bengal'
            }
            df['state_clean'] = df['states'].map(state_name_map)
            df = df.dropna(subset=['state_clean'])

            # 4. Load GeoJSON for India states
            geojson_url = (
                "https://gist.githubusercontent.com/jbrobst/"
                "56c13bbbf9d97d187fea01ca62ea5112/raw/"
                "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            )
            try:
                resp = requests.get(geojson_url)
                resp.raise_for_status()
                india_geojson = resp.json()
            except Exception as e:
                st.error(f"Error loading GeoJSON: {e}")
                st.stop()

            # 5. Render Choropleth Map
            if not df.empty:
                fig = px.choropleth(
                    df,
                    geojson=india_geojson,
                    featureidkey="properties.ST_NM",
                    locations="state_clean",
                    color="total_value",
                    color_continuous_scale="OrRd",
                    title=f"Top 10 States by Insurance Transaction Value ({selected_year}, Q{selected_quarter})",
                    hover_data={"total_txn": True, "total_value": True}
                )
                fig.update_geos(fitbounds="locations", visible=False)
                fig.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 0})
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df.head(10))
            else:
                st.warning("No data available for the selected year and quarter.")


        elif q == "II. Insurance Transactions Analysis by Top Districts":
            st.subheader("🏥 Insurance Transactions Analysis by Top Districts")

            # Filter selections for year and quarter
            year = st.selectbox("Select Year", [2020, 2021, 2022, 2023, 2024])
            quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

            # SQL Query
            q21 = f"""
            SELECT 
                districts, 
                SUM(transaction_count) AS total_txn, 
                SUM(transaction_amount) AS total_value 
            FROM map_ins 
            WHERE years = '{year}' AND quarter = '{quarter}'
            GROUP BY districts 
            ORDER BY total_value DESC, total_txn DESC ;
            """
            query = pd.read_sql_query(q21, engine)

            st.write(f"✅ Top 10 Districts for Insurance Transactions in {year} Q{quarter}:")

            # Plotly Bar Chart
            fig = px.bar(
                query,
                x='districts',
                y='total_value',
                title=f"Top 10 Districts by Insurance Transaction Value ({year} Q{quarter})",
                labels={'total_value': 'Total Transaction Value (₹)', 'districts': 'District'},
                text='total_value'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(xaxis_tickangle=-45)

            st.plotly_chart(fig, use_container_width=True)

            # Show top 10 table
            st.dataframe(query.head(10))


        elif q == "III. Insurance Transactions Analysis by Top Pincodes":
            st.subheader("🏥 Insurance Transactions Analysis by Top Pincodes")

            # Filter selections for year and quarter
            year = st.selectbox("Select Year", [2018, 2019, 2020, 2021, 2022, 2023, 2024])
            quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

            # Clean SQL Query — no stray spaces
            q22 = f"""
            SELECT 
                pincodes, 
                SUM(transaction_count) AS total_txn, 
                SUM(transaction_amount) AS total_value 
            FROM top_ins 
            WHERE years = '{year}' AND quarter = '{quarter}'
            GROUP BY pincodes 
            ORDER BY total_value DESC, total_txn DESC;
            """

            query = pd.read_sql_query(q22, engine)

            st.write(f"✅ Insurance Transactions for {year} Q{quarter}:")

            if not query.empty:
                # Plotly Bar Chart
                fig = px.bar(
                    query.head(10),  # limit to top 10 in chart
                    x='pincodes',
                    y='total_value',
                    title=f"Top 10 Pincodes by Insurance Transaction Value ({year} Q{quarter})",
                    labels={'total_value': 'Total Transaction Value (₹)', 'pincodes': 'Pincode'},
                    text='total_value'
                )
                fig.update_traces(textposition='outside')
                fig.update_layout(xaxis_tickangle=-45)

                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(query.head(10))
            else:
                st.warning(f"No data found for Insurance Transactions in {year} Q{quarter}.")
                st.text("Query preview:")
                st.code(q22, language="sql")


        

#docs
elif selected == "📄 Docs":
    st.title("📄 Project Documentation")
    st.markdown("""
    ### 📦 Dataset Info
    - **Source:** PhonePe Pulse GitHub (https://github.com/PhonePe/pulse)
    - **Data Format:** JSON
    - **Tools Used for ETL:** Python, Pandas
    - **Database:** PostgreSQL (via Neon Cloud)
    - **Visualization:** Plotly, Streamlit
    
    ### 🧩 Tables Used
    - `agg_trans` – Aggregated Transactions
    - `agg_user` – Aggregated User Metrics
    - `map_ins` – Insurance Metrics (District)
    - `top_user` – User by Top Pincodes
    - `top_ins` – Insurance by Top Pincodes

    ### 🧠 SQL Queries Executed
    - State-wise Transaction Aggregation
    - Device Usage Trends
    - Quarterly Growth Comparisons
    - Year-on-Year Trends
    - Top Pincode and District Analysis

    ### 📊 Dashboards Covered
    1. **Transaction Dynamics on PhonePe**
        - Transaction behavior across States, Quarters, Categories
        - Regional Performance Trends
        - Year-on-Year Insights

    2. **Device Dominance & User Engagement**
        - Top device brands used
        - App open vs user registration insights

    3. **User Registration Analysis**
        - Top states, districts, and pincodes by user registration

    4. **Transaction Analysis Dashboard**
        - Total transactions and values
        - Insights by states and districts
        - Growth and drop zones based on region
        - Comparative heatmap and bar chart views

    ---

    **📌 Developed by:** Naveen Kumar R  
    **📧 Email:** naveenramu2003@gmail.com
    **   Linked In : **https://www.linkedin.com/in/naveenkumar-ds

    ---

    Built with ❤️ using **Python, Pandas, Plotly, Streamlit, PostgreSQL**.
    """)
    
    st.success("Thanks for checking the 📄 Project Documentation!")

#-------------------------------------------------------------------
#Developed By : 

# Naveen Kumar R 
#email:naveenramu2003@gmail.com
#Linked In : https://www.linkedin.com/in/naveenkumar-ds
