import pandas as pd
import psycopg2
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import plotly.express as px
import streamlit as st
import os
import pydeck as pdk

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



# --- Load joined data ---
@st.cache_data
def fetch_data():
    query = """
    SELECT
        i.inspection_id,
        i.inspection_date,
        i.inspection_type,
        i.results,
        i.violation_text,
        f.dba_name,
        f.facility_type,
        f.risk,
        f.city,
        f.state,
        f.zip_code,
        f.latitude,
        f.longitude
    FROM "Inspections" i
    JOIN "Facility" f ON i.license_id = f.license_id
    WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL;
    """
    return pd.read_sql(query, conn)


df = fetch_data()

if df.empty:
    st.warning("⚠️ No data retrieved. Check your table name.")
    st.stop()

# --- Sample Preview ---
st.write("### 🔍 Sample of the Data")
st.dataframe(df.head())

# Pie Chart with any categorical column
"""Generate a pie chart for a column selected by the user."""
# Get the list of columns to select from
selected_col = st.selectbox("📊 Select column to visualize", ['risk', 'results'])

# Generate and display pie chart
if selected_col in df.columns:
    counts = df[selected_col].value_counts()
    fig = px.pie(
        counts, 
        names=counts.index, 
        values=counts.values, 
        title=f"Distribution of {selected_col.title()}"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning(f"⚠️ '{selected_col}' column not found in the DataFrame.")

# --- Map of Inspections ---
st.write("### Facilities organized by Results")

# Create a copy and clean up
df_map = df.dropna(subset=["latitude", "longitude"]).copy()
df_map["results"] = df_map["results"].astype(str).str.strip()

def get_color(result):
    return {
        "Pass": [0, 200, 0],
        "Fail": [255, 0, 0],
        "Out of Business": [0, 0, 0],
        "No Entry": [169, 169, 169],
        "Pass w/ Conditions": [255, 255, 0],  
        "Not Ready": [135, 206, 235],  
    }.get(result, [100, 100, 255])

# Apply color and filter columns
df_map["color"] = df_map["results"].apply(get_color)

# Select only columns that pydeck needs
df_deck = df_map[["dba_name", "results", "risk", "latitude", "longitude", "color"]].copy()

# Convert all types to ensure serialization
df_deck["latitude"] = df_deck["latitude"].astype(float)
df_deck["longitude"] = df_deck["longitude"].astype(float)
df_deck["color"] = df_deck["color"].apply(lambda x: list(map(int, x)))

# Create columns for map and legend
col1, col2 = st.columns([4, 1])

with col1:
    st.write("### 🗺️ Color-Coded Inspection Map")
    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=pdk.ViewState(
            latitude=df_deck["latitude"].mean(),
            longitude=df_deck["longitude"].mean(),
            zoom=10,
            pitch=0,
        ),
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=df_deck,
                get_position='[longitude, latitude]',
                get_fill_color='color',
                get_radius=60,
                pickable=True,
            )
        ],
        tooltip={"text": "{dba_name}\nResult: {results}\nRisk: {risk}"}
    ))

with col2:
    st.markdown("###  Legend", unsafe_allow_html=True)
    # Dynamically generate the legend based on unique results
    color_mapping = {
        "Pass": [0, 200, 0],
        "Fail": [255, 0, 0],
        "Out of Business": [0, 0, 0],
        "No Entry": [169, 169, 169],
        "Pass w/ Conditions": [255, 255, 0],
        "Not Ready": [135, 206, 235],
    }

    for result, color in color_mapping.items():
        color_box = f"rgb({color[0]}, {color[1]}, {color[2]})"
        st.markdown(f"""
        <div style="display: flex; align-items: center;">
            <div style="width: 20px; height: 20px; background-color: {color_box}; margin-right: 10px;"></div>
            <span><strong>{result}</strong></span>
        </div>
        """, unsafe_allow_html=True)

def get_time_series(df):
    df = df.copy()
    df['inspection_date'] = pd.to_datetime(df['inspection_date'])
    ts = (
        df.groupby(df['inspection_date'].dt.to_period('M'))
          .size()
          .reset_index(name='Count')
    )
    # Fix: Convert period to timestamp for Plotly compatibility
    ts['inspection_date'] = ts['inspection_date'].dt.to_timestamp()
    return ts

with st.expander("📅 View Inspections Over Time", expanded=True):
    st.write("### 📈 Inspections Over Time")
    df['inspection_date'] = pd.to_datetime(df['inspection_date'])
    time_df = get_time_series(df)

    fig_time = px.line(time_df, x='inspection_date', y='Count',
        title="Number of Inspections Over Time",
        markers=True
    )

    st.plotly_chart(fig_time, use_container_width=True)

# --- Filter by City ---
st.write("### 🏙️ Filter by City")
if df['city'].notna().sum() > 0:
    city = st.selectbox("Select a City", sorted(df['city'].dropna().unique()))
    filtered = df[df['city'] == city]
    st.write(f"Showing {len(filtered)} records in {city}")
    st.dataframe(filtered[['dba_name', 'inspection_date', 'results', 'risk']])
else:
    st.warning("No city data available.")

sunburst_df = df[['risk', 'results', 'inspection_type']].dropna()
fig = px.sunburst(sunburst_df, path=['risk', 'results','inspection_type'], title="Risk → Result → Inspection Type Breakdown",
    color='risk',
        color_discrete_map={
        'Risk 1 (High)': 'red',
        'Risk 2 (Medium)': 'orange',
        'Risk 3 (Low)': 'green'
    },

)
st.plotly_chart(fig, use_container_width=True)


# --- Optional: Close Connection ---
conn.close()