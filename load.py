import import_ipynb
import extract 
import transform
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

#get api credentials from .env
load_dotenv()
api_url = os.getenv("API_KEY").strip()

#CALL EXTRACT & TRANSFORM
if __name__ == "__main__":
    data = extract.fetch_api_data(api_url, batch_size=1000, num_records=100000, restart=True)
    df = pd.DataFrame(data)
    df = transform.clean_all(df)

# LOAD: push data to postgres
#get database credentials from .env
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")  
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_NAME")
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#create tables script
def create_tables(engine):
    with engine.connect() as conn:
        create_sql = """
        BEGIN;
        
        DROP TABLE IF EXISTS public."Inspections" CASCADE;
        DROP TABLE IF EXISTS public."Violations" CASCADE;
        DROP TABLE IF EXISTS public."Facility" CASCADE;
        
        
        CREATE TABLE IF NOT EXISTS public."Facility"
        (
            license_id character varying,
            dba_name character varying,
            aka_name character varying,
            facility_type character varying,
            risk character varying,
            address character varying,
            city character varying,
            state character varying(2),
            zip_code character varying(5),
            latitude numeric,
            longitude numeric,
            PRIMARY KEY (license_id)
        );

        CREATE TABLE IF NOT EXISTS public."Inspections"
        (
            inspection_id character varying NOT NULL,
            license_id character varying,
            inspection_date date,
            inspection_type character varying,
            results character varying,
            violation_ids character varying,
            violation_text text,
            PRIMARY KEY (inspection_id)
        );

        CREATE TABLE IF NOT EXISTS public."Violations"
        (
            violation_id serial NOT NULL,
            violation_description character varying,
            PRIMARY KEY (violation_id)
        );

        ALTER TABLE IF EXISTS public."Inspections"
            ADD FOREIGN KEY (license_id)
            REFERENCES public."Facility" (license_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;

        END;
                """
        conn.execute(text(create_sql))
        print("Tables created.")

#create connection
if __name__ == "__main__":
    try:
        engine = create_engine(DATABASE_URL)
        print("Connected to the database successfully.")
        create_tables(engine)
    except OperationalError as e:
        print("Failed to connect to the database.")
        print("Error:", e)

# create smaller df's for easier push
if __name__ == "__main__":
    facility_df = df[[
        'license_id', 'dba_name', 'aka_name', 'facility_type',
        'risk', 'address', 'city', 'state', 'zip_code',
        'latitude', 'longitude'
    ]].copy()
    facility_df = facility_df.drop_duplicates()

    inspections_df = df[[
        'inspection_id', 'license_id', 'inspection_date',
        'inspection_type', 'results', 'violation_ids', 'violations'
    ]].copy()
    inspections_df.rename(columns={'violations': 'violation_text'}, inplace=True)

    duplicated_rows = facility_df[facility_df.duplicated(subset='license_id', keep=False)]
    duplicated_rows = duplicated_rows.sort_values(by='license_id').reset_index(drop=True)
    duplicated_rows['null_count'] = duplicated_rows.isnull().sum(axis=1)
    cleaned_duplicates = duplicated_rows.sort_values(by=['license_id', 'null_count']).drop_duplicates(subset='license_id', keep='first')
    facility_df = facility_df[~facility_df['license_id'].isin(duplicated_rows['license_id'])]
    facility_df = pd.concat([facility_df, cleaned_duplicates.drop(columns='null_count')], ignore_index=True)

#separation for violations table
def extract_violations(df):
    violations_series = df['violations'].dropna()
    all_violations = violations_series.str.split('|').explode().str.strip()
    all_violations = all_violations[all_violations != ''].drop_duplicates()
    # Extract the ID vs. description
    extracted = all_violations.str.extract(r'^\s*(\d+)\.\s*(.*?)(?:\s*-\s*comments:.*)?$')
    extracted.columns = ['violation_id', 'violation_description']
    extracted = extracted.dropna(subset=['violation_id', 'violation_description'])
    # Clean up whitespace/special characters in description
    extracted['violation_description'] = (
        extracted['violation_description']
        .str.replace(r'^[\s\.\-\–\|]+', '', regex=True)   # Leading
        .str.replace(r'[\s\.\-\–\|]+$', '', regex=True)   # Trailing
        .str.strip()
    )
    # Group by violation_id and select the most common description
    violations_df = (
        extracted.groupby('violation_id')['violation_description']
        .agg(lambda x: x.mode().iloc[0])
        .reset_index()
        .sort_values(by='violation_id')
    )

    return violations_df.reset_index(drop=True)

if __name__ == "__main__":
    violations_df = extract_violations(df)
    df_dict = {'Facility': facility_df, 'Inspections': inspections_df, 'Violations': violations_df}

def push_to_sql(df_dict, engine):
    try:
        for table_name, df in df_dict.items():
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"{table_name} data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")

# CALLING LOAD
if __name__ == "__main__":
    push_to_sql(df_dict, engine)