import streamlit as st
import pandas as pd
import psycopg2
import os
from datetime import datetime
import requests
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
TABLE_NAME = 'data'
MAINTAINED_TABLE = 'interested_items1'
STATIC_EXCEL_FILE = 'test.xlsx'  # Set this to your Excel file path, e.g., 'mydata.xlsx'
ALLOWED_STATUSES = ['interested', 'contacted', 'reviewed', 'visited', 'confirmed', 'delete']
ZURICH_HB_COORDS = (47.378177, 8.540192)  # Zurich HB lat, lon
ORS_API_KEY = '5b3ce3597851110001cf62485d56c2c7ec274494b89bfae6e9b20178'  # Set your OpenRouteService API key as an environment variable

# Supabase Database Configuration
SUPABASE_URL = "https://obwwflnrapkulpakklab.supabase.co"

# Get database URL from environment variable. Use the direct connection for local dev, and the pooler for deployment.
# On Streamlit Cloud, set the DATABASE_URL secret to your *Connection Pooling* URI from Supabase.
DATABASE_URL = os.getenv('DATABASE_URL', "postgresql://postgres.obwwflnrapkulpakklab:Nejri0-xyxwoh-cygtus@aws-0-eu-central-2.pooler.supabase.com:6543/postgres?gssencmode=disable")
#print(DATABASE_URL)postgresql://postgres.obwwflnrapkulpakklab:[YOUR-PASSWORD]@aws-0-eu-central-2.pooler.supabase.com:6543/postgres
# Check if password placeholder needs to be replaced
if '[YOUR-PASSWORD]' in DATABASE_URL:
    st.error("Please set your Supabase database password in the DATABASE_URL environment variable or update the code with your actual password.")
    st.stop()

st.info("""
**Note for Streamlit Cloud Deployment:**
This app is configured to connect to a Supabase database. For successful deployment, you must set the `DATABASE_URL` secret in your Streamlit Cloud settings to the **Connection Pooling** URI provided by Supabase.

You can find this in your Supabase Dashboard under:
**Project Settings -> Database -> Connection string -> URI (while "Use connection pooling" is checked).**

The pooling URI typically uses port **6543**.
""")

# Helper: Clear add form fields
def clear_add_fields():
    st.session_state['add_link'] = ''
    st.session_state['add_status'] = ALLOWED_STATUSES[0]
    st.session_state['add_notes'] = ''

# Helper: Connect to PostgreSQL
def get_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Helper: Initialize database tables
def init_database():
    conn = get_connection()
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        
        # Create data table if it doesn't exist
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                "Canton" TEXT,
                "Gemeinde" TEXT,
                "MoreTaxPerMonth" TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create interested_items table if it doesn't exist
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {MAINTAINED_TABLE} (
                id SERIAL PRIMARY KEY,
                "Canton" TEXT,
                "Gemeinde" TEXT,
                "MoreTaxPerMonth" TEXT,
                link TEXT,
                notes TEXT,
                status TEXT,
                traveltime TEXT,
                "Buy Price" TEXT,
                "Rooms" TEXT,
                "Living Space" TEXT,
                "Land Area" TEXT,
                "Year Built" TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cursor.close()
    except Exception as e:
        st.error(f"Database initialization failed: {e}")
    finally:
        conn.close()

# Helper: Load data from PostgreSQL
def load_data():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql_query(f'SELECT * FROM {TABLE_NAME}', conn)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

# Helper: Save data to PostgreSQL
def save_data(df):
    conn = get_connection()
    if conn is None:
        return
    
    try:
        # Clear existing data and insert new data
        cursor = conn.cursor()
        cursor.execute(f'DELETE FROM {TABLE_NAME}')
        
        if not df.empty:
            # Convert DataFrame to list of tuples for insertion
            data_to_insert = []
            for _, row in df.iterrows():
                data_to_insert.append((
                    row.get('Canton', ''),
                    row.get('Gemeinde', ''),
                    row.get('MoreTaxPerMonth', '')
                ))
            
            cursor.executemany(
                f'INSERT INTO {TABLE_NAME} ("Canton", "Gemeinde", "MoreTaxPerMonth") VALUES (%s, %s, %s)',
                data_to_insert
            )
        
        conn.commit()
        cursor.close()
    except Exception as e:
        st.error(f"Error saving data: {e}")
    finally:
        conn.close()

# Helper: Load maintained table
def load_maintained():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql_query(f"SELECT * FROM {MAINTAINED_TABLE}", conn)
        # Ensure all property columns are treated as strings to avoid dtype issues
        property_columns = ["Buy Price", "MoreTaxPerMonth", "Rooms", "Living Space", "Land Area", "Year Built"]
        for col in property_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
    except Exception as e:
        st.error(f"Error loading maintained data: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

# Helper: Save maintained table
def save_maintained(df):
    conn = get_connection()
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        
        # Clear existing data
        cursor.execute(f"DELETE FROM {MAINTAINED_TABLE}")
        
        if not df.empty:
            # Convert all property fields to strings to avoid dtype issues
            df_copy = df.copy()
            property_columns = ["Buy Price", "MoreTaxPerMonth", "Rooms", "Living Space", "Land Area", "Year Built"]
            for col in property_columns:
                if col in df_copy.columns:
                    df_copy[col] = df_copy[col].astype(str)
            
            # Insert new data
            for _, row in df_copy.iterrows():
                cursor.execute(f"""
                    INSERT INTO {MAINTAINED_TABLE} (
                        "Canton", "Gemeinde", "MoreTaxPerMonth", link, notes, status, 
                        traveltime, "Buy Price", "Rooms", "Living Space", "Land Area", "Year Built", added_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row.get('Canton', ''),
                    row.get('Gemeinde', ''),
                    row.get('MoreTaxPerMonth', ''),
                    row.get('link', ''),
                    row.get('notes', ''),
                    row.get('status', ''),
                    row.get('traveltime'),
                    row.get('Buy Price', ''),
                    row.get('Rooms', ''),
                    row.get('Living Space', ''),
                    row.get('Land Area', ''),
                    row.get('Year Built', ''),
                    row.get('added_at', datetime.now().isoformat())
                ))
        
        conn.commit()
        cursor.close()
    except Exception as e:
        st.error(f"Error saving maintained data: {e}")
    finally:
        conn.close()

# Helper: Update a single row in maintained table
def update_maintained_row(row_id, updates):
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Build dynamic UPDATE query
        set_clauses = []
        values = []
        
        for key, value in updates.items():
            if key in ['link', 'notes', 'status', 'Buy Price', 'Rooms', 'Living Space', 'Land Area', 'Year Built']:
                set_clauses.append(f'"{key}" = %s')
                values.append(value)
        
        # Add updated_at timestamp
        set_clauses.append('updated_at = CURRENT_TIMESTAMP')
        
        # Add row_id to values
        values.append(row_id)
        
        query = f"UPDATE {MAINTAINED_TABLE} SET {', '.join(set_clauses)} WHERE id = %s"
        cursor.execute(query, values)
        
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        st.error(f"Error updating row: {e}")
        return False
    finally:
        conn.close()

# Helper: Geocode Gemeinde to coordinates (using OpenRouteService geocode API)
def geocode_location(place_name):
    if not ORS_API_KEY:
        return None
    url = 'https://api.openrouteservice.org/geocode/search'
    params = {
        'api_key': ORS_API_KEY,
        'text': f'{place_name}, Switzerland',
        'boundary.country': 'CH',
        'size': 1
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        features = resp.json().get('features', [])
        if features:
            coords = features[0]['geometry']['coordinates']  # [lon, lat]
            return coords[1], coords[0]
    except Exception:
        return None
    return None

# Helper: Get driving time from Gemeinde to Zurich HB (using OpenRouteService directions API)
def get_driving_time(from_coords, to_coords=ZURICH_HB_COORDS):
    if not ORS_API_KEY or not from_coords:
        return None
    url = 'https://api.openrouteservice.org/v2/directions/driving-car'
    headers = {'Authorization': ORS_API_KEY}
    body = {
        'coordinates': [
            [from_coords[1], from_coords[0]],  # [lon, lat]
            [to_coords[1], to_coords[0]]
        ]
    }
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=15)
        resp.raise_for_status()
        routes = resp.json().get('routes', [])
        if routes:
            seconds = routes[0]['summary']['duration']
            return round(seconds / 60, 1)  # return in minutes
    except Exception:
        return None
    return None

def fetch_property_details(link):
    """Fetch Buy Price, Rooms, Living Space, Land Area, Year Built from the given link (if possible)."""
    details = {
        'Buy Price': None,
        'Rooms': None,
        'Living Space': None,
        'Land Area': None,
        'Year Built': None
    }
    if not link or not str(link).startswith('http'):
        return details
    try:
        resp = requests.get(link, timeout=10)
        resp.raise_for_status()
        html = resp.text
        
        # Extract using quoted property names
        price_match = re.search(r'"price"\s*:\s*([\d\'\."]+)', html)
        rooms_match = re.search(r'"numberOfRooms"\s*:\s*([\d\.,"]+)', html)
        living_match = re.search(r'"livingSpace"\s*:\s*([\d\.,"]+)', html)
        land_match = re.search(r'"lotSize"\s*:\s*([\d\.,"]+)', html)
        year_match = re.search(r'"yearBuilt"\s*:\s*([\d\.,"]+)', html)
        if price_match:
            details['Buy Price'] = price_match.group(1).replace('"', '')
        if rooms_match:
            details['Rooms'] = rooms_match.group(1).replace('"', '')
        if living_match:
            details['Living Space'] = living_match.group(1).replace('"', '')
        if land_match:
            details['Land Area'] = land_match.group(1).replace('"', '')
        if year_match:
            details['Year Built'] = year_match.group(1).replace('"', '')
    except Exception:
        pass
    return details

# Initialize database on startup
if 'db_initialized' not in st.session_state:
    init_database()
    st.session_state['db_initialized'] = True

# Streamlit UI
st.title('Swiss House Search Helper')
st.set_page_config(page_title='Swiss House Search Helper', layout='wide')

# Load reference data from static Excel file
if 'reference_data' not in st.session_state:
    try:
        ref_df = pd.read_excel(STATIC_EXCEL_FILE)
        st.session_state['reference_data'] = ref_df
    except Exception as e:
        st.session_state['reference_data'] = pd.DataFrame()
        st.error(f'Could not load static Excel file: {e}')
else:
    ref_df = st.session_state['reference_data']

if ref_df.empty:
    st.warning('No reference data loaded. Please check the static Excel file.')
else:
    # Use tabs for Reference Table and Interested Items
    tab1, tab2 = st.tabs(["Reference Table", "Interested Items"])

    with tab1:
        st.subheader('Reference Table (from Excel)')
        # Add row selection to the reference table, showing only Canton and Gemeinde
        if not ref_df.empty and 'Canton' in ref_df.columns and 'Gemeinde' in ref_df.columns:
            ref_options = [
                (i, f"{ref_df.at[i, 'Canton']} - {ref_df.at[i, 'Gemeinde']}")
                for i in ref_df.index
            ]
            selected_ref_idx = st.selectbox('Select a row in Reference Table', ref_options, format_func=lambda x: x[1])
            selected_ref_idx = selected_ref_idx[0]
        else:
            selected_ref_idx = st.selectbox('Select a row in Reference Table', ref_df.index) if not ref_df.empty else None
        # st.dataframe(ref_df, hide_index=True)

        # Filter by Canton and Gemeinde (commune)
        cantons = ref_df['Canton'].unique() if 'Canton' in ref_df.columns else []
        gemeinden = ref_df['Gemeinde'].unique() if 'Gemeinde' in ref_df.columns else []
        selected_canton = ref_df.at[selected_ref_idx, 'Canton'] if selected_ref_idx is not None and 'Canton' in ref_df.columns else (st.selectbox('Select Canton', cantons) if len(cantons) > 0 else None)
        filtered_gemeinden = ref_df[ref_df['Canton'] == selected_canton]['Gemeinde'].unique() if selected_canton else gemeinden
        selected_gemeinde = ref_df.at[selected_ref_idx, 'Gemeinde'] if selected_ref_idx is not None and 'Gemeinde' in ref_df.columns else (st.selectbox('Select Gemeinde', filtered_gemeinden) if len(filtered_gemeinden) > 0 else None)

        # Filtered reference
        if selected_canton and selected_gemeinde:
            filtered_ref = ref_df[(ref_df['Canton'] == selected_canton) & (ref_df['Gemeinde'] == selected_gemeinde)]
        else:
            filtered_ref = ref_df

        st.write('Filtered Reference Table:')
        st.dataframe(filtered_ref, hide_index=True)

        # Add to maintained table
        st.subheader('Add to Interested Items')
        if not filtered_ref.empty:
            # If a row is selected, pre-fill the add form with its values
            if selected_ref_idx is not None:
                prefill_canton = ref_df.at[selected_ref_idx, 'Canton'] if 'Canton' in ref_df.columns else ''
                prefill_gemeinde = ref_df.at[selected_ref_idx, 'Gemeinde'] if 'Gemeinde' in ref_df.columns else ''
                row_options = [
                    (i, f"{ref_df.at[i, 'Canton']} - {ref_df.at[i, 'Gemeinde']}")
                    for i in filtered_ref.index
                ]
            else:
                prefill_canton = ''
                prefill_gemeinde = ''
                row_options = [
                    (i, f"{filtered_ref.at[i, 'Canton']} - {filtered_ref.at[i, 'Gemeinde']}")
                    for i in filtered_ref.index
                ]
            row_idx_option = st.selectbox('Select row to add', row_options, format_func=lambda x: x[1], key='add_row_select')
            row_idx = row_idx_option[0]
            # Use session state for form fields
            if 'add_link' not in st.session_state:
                st.session_state['add_link'] = ''
            if 'add_status' not in st.session_state:
                st.session_state['add_status'] = ALLOWED_STATUSES[0]
            if 'add_notes' not in st.session_state:
                st.session_state['add_notes'] = ''
            link = st.text_input('Link', value=st.session_state['add_link'], key='add_link')
            status = st.selectbox('Status', ALLOWED_STATUSES[:-1], index=ALLOWED_STATUSES.index(st.session_state['add_status']) if st.session_state['add_status'] in ALLOWED_STATUSES else 0, key='add_status')
            notes = st.text_area('Notes', value=st.session_state['add_notes'], key='add_notes', height=100)
            st.write(f"Canton: {prefill_canton}")
            st.write(f"Gemeinde: {prefill_gemeinde}")
            # Calculate time
            travel_time_min = None
            if st.button('Add to Interested Items', key='add_btn', on_click=clear_add_fields):
                # Insert directly into the database
                row_data = ref_df.loc[row_idx, ['Canton', 'Gemeinde']] if all(col in ref_df.columns for col in ['Canton', 'Gemeinde']) else ref_df.loc[row_idx]
                new_row = row_data.to_dict()
                new_row['link'] = link
                new_row['notes'] = notes
                if 'MoreTaxPerMonth' in ref_df.columns:
                    new_row['MoreTaxPerMonth'] = ref_df.at[row_idx, 'MoreTaxPerMonth']
                else:
                    new_row['MoreTaxPerMonth'] = None
                new_row['status'] = status
                new_row['added_at'] = datetime.now().isoformat()
                gemeinde_name = new_row.get('Gemeinde', '')
                coords = geocode_location(gemeinde_name)
                if coords:
                    travel_time_min = get_driving_time(coords)
                    new_row['traveltime'] = travel_time_min
                else:
                    new_row['traveltime'] = None
                prop_details = fetch_property_details(link)
                for k, v in prop_details.items():
                    new_row[k] = v
                # Insert into DB
                conn = get_connection()
                try:
                    cursor = conn.cursor()
                    # Convert all values to string (TEXT columns)
                    def safe_str(val):
                        if val is None:
                            return None
                        if isinstance(val, float) and (val != val):  # NaN check
                            return None
                        return str(val)
                    cursor.execute(f"""
                        INSERT INTO {MAINTAINED_TABLE} (
                            "Canton", "Gemeinde", "MoreTaxPerMonth", link, notes, status, traveltime, "Buy Price", "Rooms", "Living Space", "Land Area", "Year Built", added_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        safe_str(new_row.get('Canton', '')),
                        safe_str(new_row.get('Gemeinde', '')),
                        safe_str(new_row.get('MoreTaxPerMonth', '')),
                        safe_str(new_row.get('link', '')),
                        safe_str(new_row.get('notes', '')),
                        safe_str(new_row.get('status', '')),
                        safe_str(new_row.get('traveltime')),
                        safe_str(new_row.get('Buy Price', '')),
                        safe_str(new_row.get('Rooms', '')),
                        safe_str(new_row.get('Living Space', '')),
                        safe_str(new_row.get('Land Area', '')),
                        safe_str(new_row.get('Year Built', '')),
                        safe_str(new_row.get('added_at', datetime.now().isoformat()))
                    ))
                    conn.commit()
                    cursor.close()
                    st.success('Row added to interested items!')
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to add row: {e}")
                finally:
                    conn.close()

    with tab2:
        # Show maintained table with edit and filter options
        st.subheader('Interested Items (Maintained Table)')
        # Interest rate input
        interest_rate = st.number_input('Interest Rate (%)', min_value=0.01, max_value=10.0, value=1.5, step=0.01, format="%.2f")
        interest_rate_decimal = interest_rate / 100.0
        maintained_df = load_maintained()

        # Calculate LoanAmountEstimate using EMI formula for 30-year mortgage
        def calc_loan_amount(row):
            try:
                emi = float(row.get('MoreTaxPerMonth', '0').replace("'", '').replace(',', '').replace(' ', ''))
                n = 30 * 12  # 30 years
                r = interest_rate_decimal / 12  # monthly rate
                if r > 0 and emi > 0:
                    numerator = emi * ((1 + r) ** n - 1)
                    denominator = r * (1 + r) ** n
                    return round(numerator / denominator)
                else:
                    return None
            except Exception:
                return None
        if not maintained_df.empty and 'MoreTaxPerMonth' in maintained_df.columns:
            maintained_df['LoanAmountEstimate'] = maintained_df.apply(calc_loan_amount, axis=1)
        else:
            maintained_df['LoanAmountEstimate'] = None

        # Calculate Price per Square Meter
        def calc_price_per_sqm(row):
            try:
                price = float(str(row.get('Buy Price', '0')).replace("'", "").replace(",", "").strip())
                space = float(str(row.get('Living Space', '0')).replace("'", "").replace(",", "").strip())
                if space > 0:
                    return round(price / space)
                else:
                    return None
            except (ValueError, TypeError):
                return None
        if not maintained_df.empty and 'Buy Price' in maintained_df.columns and 'Living Space' in maintained_df.columns:
            maintained_df['PricePerSqm'] = maintained_df.apply(calc_price_per_sqm, axis=1)
        else:
            maintained_df['PricePerSqm'] = None

        # Option to show deleted
        show_deleted = st.checkbox('Show deleted items', value=False)
        filtered_maintained = maintained_df.copy()
        if not show_deleted and 'status' in filtered_maintained.columns:
            filtered_maintained = filtered_maintained[filtered_maintained['status'] != 'delete']

        # Column filters (only for Canton and Status)
        st.markdown('**Filter Maintained Table**')
        filter_cols = [col for col in ['Canton', 'status'] if col in filtered_maintained.columns]
        filter_values = {}
        for col in filter_cols:
            unique_vals = filtered_maintained[col].dropna().unique()
            if len(unique_vals) > 0 and len(unique_vals) < 20:
                val = st.multiselect(f'Filter {col}', unique_vals, default=unique_vals)
                filter_values[col] = val
        for col, vals in filter_values.items():
            filtered_maintained = filtered_maintained[filtered_maintained[col].isin(vals)]

        # Display table at the top
        all_cols = list(filtered_maintained.columns)
        desired_order = ['Canton', 'Gemeinde', 'traveltime', 'MoreTaxPerMonth', 'PricePerSqm', 'LoanAmountEstimate', 'link']
        
        # Start with desired columns that exist in the DataFrame
        ordered_cols = [col for col in desired_order if col in all_cols]
        
        # Add all other columns that are not already in the list and are not 'id' or 'index'
        remaining_cols = [col for col in all_cols if col not in ordered_cols and col not in ['id', 'index']]
        ordered_cols.extend(remaining_cols)
        
        df_display = filtered_maintained[ordered_cols].copy()
        
        st.write('Click a row below to edit its details:')
        selected_edit_id = None
        if not df_display.empty:
            # Use id from the database, not DataFrame index
            row_options = [
                (int(filtered_maintained.at[i, 'id']), f"{filtered_maintained.at[i, 'Canton']} - {filtered_maintained.at[i, 'Gemeinde']} - {filtered_maintained.at[i, 'link']}")
                for i in filtered_maintained.index
            ]
            selected_row = st.selectbox('Select a row to edit', row_options, format_func=lambda x: x[1], key='edit_row_select')
            selected_edit_id = selected_row[0]
            st.dataframe(df_display, hide_index=True)
        else:
            st.info('No items to display.')

        # If a row is selected, show editable fields below
        if selected_edit_id is not None:
            # Find the row in the DataFrame by id
            row_idx = maintained_df.index[maintained_df['id'] == selected_edit_id][0]
            st.subheader('Edit Selected Item')
            edit_link = st.text_input('Edit Link', value=str(maintained_df.at[row_idx, 'link']), key='edit_link')
            edit_status = st.selectbox('Edit Status', ALLOWED_STATUSES, index=ALLOWED_STATUSES.index(maintained_df.at[row_idx, 'status']) if maintained_df.at[row_idx, 'status'] in ALLOWED_STATUSES else 0, key='edit_status')
            edit_notes = st.text_area('Edit Notes', value=str(maintained_df.at[row_idx, 'notes']) if 'notes' in maintained_df.columns else '', key='edit_notes', height=100)
            edit_extras = {}
            for field in ['Buy Price', 'Rooms', 'Living Space', 'Land Area', 'Year Built']:
                edit_extras[field] = st.text_input(f'Edit {field}', value=str(maintained_df.at[row_idx, field]) if field in maintained_df.columns else '', key=f'edit_{field}')
            if st.button('Save Changes', key='save_edit_btn'):
                updates = {
                    'link': edit_link,
                    'status': edit_status,
                    'notes': edit_notes
                }
                for field in ['Buy Price', 'Rooms', 'Living Space', 'Land Area', 'Year Built']:
                    updates[field] = edit_extras[field]
                if update_maintained_row(selected_edit_id, updates):
                    st.success('Row updated!')
                    st.rerun()
                else:
                    st.error('Failed to update row')

        # Ensure new columns are shown in the table
        for col in ['Buy Price', 'Rooms', 'Living Space', 'Land Area', 'Year Built', 'notes']:
            if col not in ordered_cols and col in df_display.columns:
                ordered_cols.append(col)
        df_display = df_display.reindex(columns=ordered_cols) 