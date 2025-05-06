import pandas as pd
import numpy as np
import uuid

# Facility Type Column
def clean_facility(row):
    keyword_map = {
    'restaurant': ['restaurant', 'taqueria', 'pizza', 'grill', 'burger', 'deli', 'cafe', 'bbq', 'taco', 'kitchen', 'ramen', 'chicken', 'fries', 'cuisine', 'noodle', 'sushi', 'spice', 'barbecue', 'eatery', 'food', 'juice', 'salad', 'cocina'],
    'school': ['school', 'academy', 'elementary', 'high school', 'college', 'university'],
    'daycare': ['daycare', 'child care', 'preschool'],
    'grocery store': ['grocery', 'market', 'foods', 'supermarket'],
    'gas station': ['gas', 'shell', 'bp', '7-eleven', 'station'],
    'bar': ['bar', 'pub', 'tavern', 'elbo room'],
    'pharmacy': ['pharmacy', 'walgreens', 'cvs'],
    'bakery': ['bakery', 'bake', 'funnel cakes', 'chocolat', 'sweet'],
    'salon/spa': ['salon', 'spa'],
    'hotel': ['hotel', 'sheraton'],
    'coffee shop': ['coffee', 'sip & savor', 'tea', 'te\'amo'],
    'health/nutrition': ['nutrition', 'nutricion', 'wellness', 'fitlife', 'health'],
}

    
    if pd.isna(row['facility_type']):
        name = str(row['dba_name']).lower().strip()
        for facility, keywords in keyword_map.items():
            if any(keyword in name for keyword in keywords):
                return facility.title()
        return 'Unknown/Other'
    else:
        row['facility_type'] = str(row['facility_type']).strip().lower()
    return row['facility_type']

# City Column
def clean_city(row):
    chicago_aliases = [
        'cchicago',
        'chicagoo',
        '312chicago',
        'chicagochicago',
        'chicago.',
        'ch',
        'chicagoc',
        'chicagobedford park',
        'chcicago',
        'charles a hayes',
        'chchicago',
        'chicagoi',
        'inactive'
    ]
    if pd.isna(row['city']):
        row['city'] = 'chicago'
    row['city'] = row['city'].strip().lower()
    if row['state'] == 'IL':
        if row['city'] == 'niles niles':
            row['city'] = 'niles'
        if row['city'] == 'oolympia fields':
            row['city'] = 'olympia fields'
        if row['city'] == 'bannockburndeerfield':
            row['city'] = 'bannockburn'
        if row['city'] in chicago_aliases:
            row['city'] = 'chicago'
    if row['city'] == 'merriville':
        row['city'] = 'merrillville'
    return row['city']

# Violations Column
def clean_violations(row):
    if pd.isna(row['violations']):
        if row['results'] == 'Pass':
            row['violations'] = 'no violations found'
        elif row['results'] == 'Fail' or row['results'] == 'Pass w/ Conditions':
            row['violations'] = 'unlisted violations'
        elif row['results'] == 'Out of Business':
            row['violations'] = 'not applicable (business closed)'
        elif row['results'] == 'No Entry':
            row['violations'] = 'not applicable (no entry given)'
        elif row['results'] == 'Not Ready':
            row['violations'] = 'not applicable (business not ready)'
        else:
            row['violations'] = 'Not applicable (business not located)'
    row['violations'] = row['violations'].strip().lower()
    return row['violations']

# dba name and aka_name
def fill_missing_aka_with_dba(df):
    df['aka_name'] = df['aka_name'].fillna(df['dba_name'])
    return df
#smart capitalization
import re
def smart_title(text):
    if pd.isnull(text):
        return text
    text = text.strip()  # Trim leading/trailing whitespace
    return ' '.join([
        word[0].upper() + word[1:].lower() if word else ''
        for word in re.split(r'(\s+)', text)
    ])

#inspection types
def clean_inspection_types(df, col='inspection_type'):

    df[col] = df[col].str.lower().str.strip()


    inspection_mapping = {
        'canvass': 'canvass',
        'canvas': 'canvass',
        'canvass re-inspection': 'canvass re-inspection',
        'canvass re inspection of close up': 'canvass re-inspection',
        'canvass re inspection': 'canvass re-inspection',
        'canvass/special event': 'canvass special event',
        'canvass special events': 'canvass special event',
        'canvass school/special event': 'canvass special event',

        'license': 'license',
        'license re-inspection': 'license re-inspection',
        'license consultation': 'license consultation',
        'license- task force': 'license task force',
        'license task force / not -for-profit clu': 'license task force',
        'license task force / not -for-profit club': 'license task force',
        'license request': 'license',
        'license daycare 1586': 'license',
        'license renewal inspection for daycare': 'license renewal daycare',
        'license renewal for daycare': 'license renewal daycare',
        'day care license renewal': 'license renewal daycare',

        'complaint': 'complaint',
        'short form complaint': 'complaint',
        'short form fire-complaint': 'complaint',
        'complaint re-inspection': 'complaint re-inspection',
        'complaint-fire': 'complaint fire',
        'complaint-fire re-inspection': 'complaint fire',

        'fire': 'fire',
        'fire complaint': 'fire',
        'fire/complain': 'fire',
        'reinspection of 48 hour notice': 're-inspection',
        're-inspection of close-up': 're-inspection',
        'reinspection': 're-inspection',

        'suspected food poisoning': 'food poisoning',
        'suspected food poisoning re-inspection': 'food poisoning re-inspection',

        'recent inspection': 'recent inspection',
        'sfp recently inspected': 'recent inspection',
        'sfp': 'complaint',
        'sfp/complaint': 'complaint',

        'not ready': 'not ready',
        'license/not ready': 'not ready',
        'liquour task force not ready': 'not ready',
        'task force not ready': 'not ready',

        'task force liquor 1474': 'task force liquor',
        'task force for liquor 1474': 'task force liquor',
        'task force liquor catering': 'task force liquor',
        'task force liquor (1481)': 'task force liquor',
        'task force package liquor': 'task force liquor',
        'package liquor 1474': 'task force liquor',
        'task force night': 'task force liquor',
        'task force': 'task force liquor',
        'special task force': 'task force liquor',
        'task force package goods 1474': 'task force liquor',
        'task force(1470) liquor tavern': 'task force liquor',

        'consultation': 'consultation',
        'pre-license consultation': 'consultation',

        'no entry': 'no entry',
        'no entry-short complaint)': 'no entry',

        'out of business': 'out of business',
        'o.b.': 'out of business',

        'illegal operation': 'violation',
        'citation re-issued': 'violation',
        'corrective action': 'violation',
        'license canceled by owner': 'violation',
        'owner suspended operation/license': 'violation',

        'covid complaint': 'complaint',
        'smoking complaint': 'complaint',
        'kids cafe': 'special program',
        "kids cafe'": 'special program',
        'summer feeding': 'special program',
        'taste of chicago': 'special event',
        'canvass for rib fest': 'special event',
        'special events (festivals)': 'special event',

        'sample collection': 'sample collection',
        'recall inspection': 'recall inspection',
        'addendum': 'other',
        'duplicated': 'other',
        'error save': 'other',
        'changed court date': 'other',
        'expansion': 'other',
        'business not located': 'other',
        'finish complaint inspection from 5-18-10': 'other',
        'haccp questionaire': 'other',
        'possible fbi': 'other',
        'assessment': 'other',
    }

    df[col] = df[col].replace(inspection_mapping)
    df[col] = df[col].fillna('unknown')
    return df

def clean_inspection_date(df):
    df['inspection_date'] = df['inspection_date'].fillna("00/00/0000")
    return df



def clean_and_deduplicate_licenses(df, license_col='license_id', name_col='dba_name'):
    #df = df.copy()

    # Step 1: Handle missing license numbers
    existing_licenses = set(df[license_col].dropna().astype(str))

    null_license_dbas = df[df[license_col].isnull()][name_col].unique()
    new_licenses = {}
    
    for dba in null_license_dbas:
        # Generate a unique license ID
        while True:
            new_license = f"gen_{str(uuid.uuid4())[:8]}"
            if new_license not in existing_licenses:
                existing_licenses.add(new_license)
                new_licenses[dba] = new_license
                break

    # Fill in missing licenses using the new license numbers
    df[license_col] = df.apply(
        lambda row: new_licenses[row[name_col]] if pd.isnull(row[license_col]) else str(int(row[license_col])),
        axis=1
    )

    # Step 2: Resolve conflicts where the same license is used by multiple different dba_names
    license_to_dba_counts = df.groupby(license_col)[name_col].nunique()
    conflicting_licenses = license_to_dba_counts[license_to_dba_counts > 1].index

    for license_num in conflicting_licenses:
        dba_counts = df[df[license_col] == license_num][name_col].value_counts()
        primary_dba = dba_counts.idxmax()  # Most frequent one keeps the license

        for dba in dba_counts.index:
            if dba != primary_dba:
                # Assign new unique license
                while True:
                    new_license = f"{license_num}_{str(uuid.uuid4())[:8]}"
                    if new_license not in existing_licenses:
                        existing_licenses.add(new_license)
                        break
                df.loc[(df[license_col] == license_num) & (df[name_col] == dba), license_col] = new_license

    return df

def clean_inspectionID(df):
    seen = set()
    unique_rows = []

    for _, row in df.iterrows():
        inspection_id = row['inspection_id']
        if inspection_id not in seen and pd.notnull(inspection_id):
            try:
                inspection_id = int(inspection_id)
            except ValueError:
                print(inspection_id, "is not a valid number")
                continue
            seen.add(inspection_id)
            unique_rows.append(row)

    return pd.DataFrame(unique_rows).reset_index(drop=True)

def clean_risk(row):
    """
    Check if the risk level is valid.
    """
    valid_risks = ['Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)', 'All']

    if pd.isnull(row['risk']) or row['risk'] in valid_risks:
        return row['risk']
    else:
        return np.nan

def clean_zip(row):
    """
    Check if the zip code is valid.
    """
    zip_code = row['zip_code']

    if pd.isnull(zip_code):
        return np.nan

    # Convert to string and remove any decimal points if it's a float like 60614.0
    zip_str = str(zip_code).strip().split('.')[0]

    if len(zip_str) == 5 and zip_str.isdigit():
        return zip_str
    else:
        return np.nan

def clean_lat_long(row):
    """
    Check if the latitude and longitude are valid.
    """
    lat = float(row['latitude'])
    long = float(row['longitude'])

    if pd.isnull(lat) or pd.isnull(long):
        return np.nan

    # Check if latitude and longitude are within valid ranges
    if -90 <= lat <= 90 and -180 <= long <= 180:
        return pd.Series([lat, long])
    else:
        return pd.Series([np.nan, np.nan])

def extract_violation_ids(df, source_col='violations', target_col='violation_ids'):
    def parse_ids(violation_text):
        if pd.isna(violation_text):
            return None
        if 'not applicable' in violation_text.lower():
            return 'N/a'
        
        # Split by "|" and match leading number
        entries = violation_text.split('|')
        ids = []
        for entry in entries:
            match = re.match(r'\s*(\d+)\.', entry.strip())
            if match:
                ids.append(match.group(1))
        return '|'.join(ids) if ids else None

    df[target_col] = df[source_col].apply(parse_ids)
    return df

def clean_all(df):
    columns_to_drop = [col for col in df.columns if col.startswith(':@computed_region')]
    columns_to_drop.append('location')
    df = df.drop(columns=columns_to_drop)
    df.rename(columns={'zip': 'zip_code'}, inplace=True)
    df.rename(columns={'license_': 'license_id'}, inplace=True)
    df['facility_type'] = df.apply(clean_facility, axis=1)
    df['facility_type'] = df['facility_type'].apply(smart_title)
    df['city'] = df.apply(clean_city, axis=1).astype(str).str.title()
    df['violations'] = df.apply(clean_violations, axis=1)
    df['dba_name'] = df['dba_name'].apply(smart_title)
    df['aka_name'] = df['aka_name'].apply(smart_title)
    df['aka_name'] = df['aka_name'].fillna(df['dba_name'])
    df['address'] = df['address'].apply(smart_title)
    df = clean_inspection_types(df)
    df['inspection_type'] = df['inspection_type'].apply(smart_title)
    df = clean_inspection_date(df)
    df = extract_violation_ids(df)
    df.loc[df['violations'].str.lower() == 'no violations found', 'violation_ids'] = 'No violations'
    df.loc[df['violations'] == 'unlisted violations', 'violation_ids'] = 'Unlisted violations'
    df = clean_and_deduplicate_licenses(df)
    df = clean_inspectionID(df)
    df['risk'] = df.apply(clean_risk, axis=1)
    df['zip_code'] = df.apply(clean_zip, axis=1)
    #df['zip_code'] = df['zip_code'].astype(int)
    df[['latitude', 'longitude']] = df.apply(clean_lat_long, axis=1)
    df.loc[df['city'] == 'chicago', 'state'] = 'IL'
    df['inspection_date'] = pd.to_datetime(df['inspection_date']).dt.date
    df = df.drop_duplicates()
    


    return df 

