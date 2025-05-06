import pandas as pd
import psycopg2
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import plotly.express as px
import streamlit as st
import os

st.title("🍽️ Chicago Food Inspections Dashboard")
st.write("This app helps you explore and visualize food inspection data from the City of Chicago.")


DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_NAME"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
}


@st.cache_resource
def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
            f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
        )
        conn = engine.connect()
        return conn
    except Exception as e:
        st.error(f"❌ Error connecting to database: {e}")
        return None

conn = get_db_connection()

if conn:
    st.success("✅ Successfully connected to PostgreSQL database!")
else:
    st.error("❌ Failed to connect. Check your credentials.")


def fetch_data():
    """Fetch sample data from a PostgreSQL table and handle errors."""
    query = "SELECT * FROM \"Facility\";" 
    try:
        data = pd.read_sql(query, conn)
        return data
    except Exception as e:
        st.error(f"❌ Error fetching data: {e}")
        return pd.DataFrame()

df = fetch_data()

if df.empty:
    st.warning("⚠️ No data retrieved. Check your table name.")
    st.stop()

# --- Sample Preview ---
st.write("### 🔍 Sample of the Data")
st.dataframe(df.head())

# --- Inspection Result Distribution ---
st.write("### 📋 Inspection Results")
results = df['Results'].value_counts().reset_index()
results.columns = ['Result', 'Count']
fig1 = px.bar(results, x='Result', y='Count', title="Results of Inspections")
st.plotly_chart(fig1)

# --- Risk Levels ---
st.write("### ⚠ Risk Levels")
risk = df['Risk'].value_counts().reset_index()
risk.columns = ['Risk Level', 'Count']
fig2 = px.pie(risk, names='Risk Level', values='Count', title="Distribution of Risk Levels")
st.plotly_chart(fig2)

# --- Facility Type ---
st.write("### 🏢 Top 10 Facility Types")
facility = df['Facility Type'].value_counts().nlargest(10).reset_index()
facility.columns = ['Facility Type', 'Count']
fig3 = px.bar(facility, x='Count', y='Facility Type', orientation='h', title="Most Common Facility Types")
st.plotly_chart(fig3)

# --- Map of Inspections ---
st.write("### 🗺️ Inspection Locations Map")
df_map = df.dropna(subset=['Latitude', 'Longitude'])
st.map(df_map[['Latitude', 'Longitude']])

# --- Filter by City ---
st.write("### 🏙️ Filter by City")
if df['City'].notna().sum() > 0:
    city = st.selectbox("Select a City", sorted(df['City'].dropna().unique()))
    filtered = df[df['City'] == city]
    st.write(f"Showing {len(filtered)} records in {city}")
    st.dataframe(filtered[['DBA Name', 'Address', 'Inspection Date', 'Results', 'Risk']])
else:
    st.warning("No city data available.")

# --- Optional: Close Connection ---
conn.close()