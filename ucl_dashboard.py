from pyspark.sql import SparkSession
import psycopg2  
from pyspark.sql.functions import col, lit, coalesce, when, desc, asc
import matplotlib.pyplot as plt
import streamlit as st
import pandas as pd
import plotly.express as px


spark = SparkSession.builder \
    .appName("Load PostgreSQL data to Spark") \
    .getOrCreate()

# PostgreSQL JDBC connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/ucl2122"
properties = {
    "user": "postgres",
    "password": "Jeet@6291",
    "driver": "org.postgresql.Driver"
}

# Load data from PostgreSQL to Spark DataFrame
try:

    df = spark.read.jdbc(url=jdbc_url, table="player_stats", properties=properties)
    df.show()
    pandas_df = df.toPandas()

    
    # DASHBOARD BUIDING ###############################
    st.title("UCL 21-22 Player Performance Dashboard")
    
    clubs = pandas_df['club'].unique()
    positions = pandas_df['position'].unique()
    
    selected_club = st.sidebar.selectbox("Select Club", clubs)
    selected_position = st.sidebar.multiselect("Select Position", positions, default=positions)
    
    filtered_df = pandas_df[(pandas_df['club'] == selected_club) & (pandas_df['position'].isin(selected_position))]
    
    # Display filtered data
    st.write("Player Stats", filtered_df)

    # Example plot: Goals vs Assists
    fig1 = px.scatter(filtered_df, x="match_played", y="goals", color="player_name",
                    title="Goals vs  Matches Played")
    st.plotly_chart(fig1)

    fig2 = px.scatter(filtered_df, x="match_played", y="assists", color="player_name",
                 title="Assists vs Matches Played")
    st.plotly_chart(fig2)
    
    total_goals = filtered_df['goals'].sum()

    # Display the total goals
    st.markdown(f"### Total Goals Scored by {selected_club}: {total_goals}")
    
    
    # ---- New Plots for Top 10 Stats ---- #

    # 1. Top 10 Goal Scorers
    top_goal_scorers = pandas_df.nlargest(10, 'goals')
    fig_goal_scorers = px.bar(top_goal_scorers, x="player_name", y="goals", color="club",
                            title="Top 10 Goal Scorers")
    st.plotly_chart(fig_goal_scorers)

    # 2. Top 10 Assist Providers
    top_assist_providers = pandas_df.nlargest(10, 'assists')
    fig_assist_providers = px.bar(top_assist_providers, x="player_name", y="assists", color="club",
                                title="Top 10 Assist Providers")
    st.plotly_chart(fig_assist_providers)

    # 3. Top Clean Sheets (assuming the 'cleansheets' column holds clean sheet data)
    top_clean_sheets = pandas_df.nlargest(10, 'cleansheets')
    fig_clean_sheets = px.bar(top_clean_sheets, x="player_name", y="cleansheets", color="club",
                            title="Top 10 Clean Sheets")
    st.plotly_chart(fig_clean_sheets)


except Exception as e:
    print(f"Error: {e}")
