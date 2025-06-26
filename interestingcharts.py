import streamlit as st
import pandas as pd
import duckdb
import os
from datetime import datetime
import requests
import re
from bs4 import BeautifulSoup

# Constants
DB_FILE = 'interestingcharts.duckdb'
TABLE_NAME = 'data'
MAINTAINED_TABLE = 'interested_items'
STATIC_EXCEL_FILE = 'test.xlsx'  # Set this to your Excel file path, e.g., 'mydata.xlsx'
ALLOWED_STATUSES = ['interested', 'contacted', 'reviewed', 'visited', 'confirmed', 'delete']
ZURICH_HB_COORDS = (47.378177, 8.540192)  # Zurich HB lat, lon
ORS_API_KEY = '5b3ce3597851110001cf62485d56c2c7ec274494b89bfae6e9b20178'  # Set your OpenRouteService API key as an environment variable

# Helper: Clear add form fields
def clear_add_fields():
    st.session_state['add_link'] = ''
    st.session_state['add_status'] = ALLOWED_STATUSES[0]
    st.session_state['add_notes'] = ''

# Helper: Connect to DuckDB
def get_connection():
    return duckdb.connect(DB_FILE)

# Helper: Load data from DuckDB
def load_data():
    conn = get_connection()
    try:
        df = conn.execute(f'SELECT * FROM {TABLE_NAME}').df()
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

# Helper: Save data to DuckDB
def save_data(df):
    conn = get_connection()
    #conn.execute(f'DROP TABLE IF EXISTS {TABLE_NAME}')
    conn.execute(f'CREATE TABLE {TABLE_NAME} AS SELECT * FROM df')
    conn.close()

# Helper: Load maintained table
# Helper: Load maintained table
def load_maintained():
    conn = get_connection()
    try:
        df = conn.execute(f"SELECT * FROM {MAINTAINED_TABLE}").df()
        # Ensure all property columns are treated as strings to avoid dtype issues
        property_columns = ["Buy Price", "MoreTaxPerMonth", "Rooms", "Living Space", "Land Area", "Year Built"]
        for col in property_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

# Helper: Save maintained table
def save_maintained(df):
    conn = get_connection()
    conn.execute(f"DROP TABLE IF EXISTS {MAINTAINED_TABLE}")
    
    # Convert all property fields to strings to avoid dtype issues
    if not df.empty:
        df_copy = df.copy()
        property_columns = ["Buy Price", "MoreTaxPerMonth", "Rooms", "Living Space", "Land Area", "Year Built"]
        for col in property_columns:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].astype(str)
        conn.execute(f"CREATE TABLE {MAINTAINED_TABLE} AS SELECT * FROM df_copy")
    else:
        conn.execute(f"CREATE TABLE {MAINTAINED_TABLE} AS SELECT * FROM df")
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

# Streamlit UI
st.title('Interesting Charts Data Manager')

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
                maintained_df = load_maintained()
                # Only keep Canton, Gemeinde, plus link and status
                row_data = ref_df.loc[row_idx, ['Canton', 'Gemeinde']] if all(col in ref_df.columns for col in ['Canton', 'Gemeinde']) else ref_df.loc[row_idx]
                new_row = row_data.to_dict()
                new_row['link'] = link
                new_row['notes'] = notes
                # Pick MoreTaxPerMonth from reference table if available
                if 'MoreTaxPerMonth' in ref_df.columns:
                    new_row['MoreTaxPerMonth'] = ref_df.at[row_idx, 'MoreTaxPerMonth']
                else:
                    new_row['MoreTaxPerMonth'] = None
                new_row['status'] = status
                new_row['added_at'] = datetime.now().isoformat()
                # Get travel time
                gemeinde_name = new_row.get('Gemeinde', '')
                coords = geocode_location(gemeinde_name)
                if coords:
                    travel_time_min = get_driving_time(coords)
                    new_row['traveltime'] = travel_time_min
                else:
                    new_row['traveltime'] = None
                # Fetch property details from link
                prop_details = fetch_property_details(link)
                for k, v in prop_details.items():
                    new_row[k] = v
                maintained_df = pd.concat([maintained_df, pd.DataFrame([new_row])], ignore_index=True)
                save_maintained(maintained_df)
                st.success('Row added to interested items!')
                st.rerun()

    with tab2:
        # Show maintained table with edit and filter options
        st.subheader('Interested Items (Maintained Table)')
        maintained_df = load_maintained()

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
        display_cols = list(filtered_maintained.columns)
        if 'link' in display_cols and 'traveltime' in display_cols:
            link_idx = display_cols.index('link')
            travel_idx = display_cols.index('traveltime')
            # Move traveltime right after link
            if travel_idx != link_idx + 1:
                col = display_cols.pop(travel_idx)
                display_cols.insert(link_idx + 1, col)
        if 'traveltime' in display_cols and 'MoreTaxPerMonth' in display_cols:
            travel_idx = display_cols.index('traveltime')
            tax_idx = display_cols.index('MoreTaxPerMonth')
            # Move MoreTaxPerMonth right after traveltime
            if tax_idx != travel_idx + 1:
                col = display_cols.pop(tax_idx)
                display_cols.insert(travel_idx + 1, col)
        df_display = filtered_maintained[display_cols].copy()
        st.write('Click a row below to edit its details:')
        selected_edit_idx = None
        if not df_display.empty:
            # Show clickable table using selectbox for row selection
            row_options = [
                (i, f"{df_display.at[i, 'Canton']} - {df_display.at[i, 'Gemeinde']} - {df_display.at[i, 'link']}")
                for i in df_display.index
            ]
            selected_row = st.selectbox('Select a row to edit', row_options, format_func=lambda x: x[1], key='edit_row_select')
            selected_edit_idx = selected_row[0]
            st.dataframe(df_display, hide_index=True)
        else:
            st.info('No items to display.')

        # If a row is selected, show editable fields below
        if selected_edit_idx is not None:
            st.subheader('Edit Selected Item')
            edit_link = st.text_input('Edit Link', value=str(maintained_df.at[selected_edit_idx, 'link']), key='edit_link')
            edit_status = st.selectbox('Edit Status', ALLOWED_STATUSES, index=ALLOWED_STATUSES.index(maintained_df.at[selected_edit_idx, 'status']) if maintained_df.at[selected_edit_idx, 'status'] in ALLOWED_STATUSES else 0, key='edit_status')
            edit_notes = st.text_area('Edit Notes', value=str(maintained_df.at[selected_edit_idx, 'notes']) if 'notes' in maintained_df.columns else '', key='edit_notes', height=100)
            # Show property fields as text inputs for manual correction
            edit_extras = {}
            for field in ['Buy Price', 'Rooms', 'Living Space', 'Land Area', 'Year Built']:
                edit_extras[field] = st.text_input(f'Edit {field}', value=str(maintained_df.at[selected_edit_idx, field]) if field in maintained_df.columns else '', key=f'edit_{field}')
            if st.button('Save Changes', key='save_edit_btn'):
                maintained_df.at[selected_edit_idx, 'link'] = edit_link
                maintained_df.at[selected_edit_idx, 'status'] = edit_status
                maintained_df.at[selected_edit_idx, 'notes'] = edit_notes
                for field in ['Buy Price', 'Rooms', 'Living Space', 'Land Area', 'Year Built']:
                    maintained_df.at[selected_edit_idx, field] = edit_extras[field]
                maintained_df.at[selected_edit_idx, 'updated_at'] = datetime.now().isoformat()
                save_maintained(maintained_df)
                st.success('Row updated!')
                st.rerun()

        # Ensure new columns are shown in the table
        for col in ['Buy Price', 'Rooms', 'Living Space', 'Land Area', 'Year Built', 'notes']:
            if col not in display_cols and col in df_display.columns:
                display_cols.append(col)
        df_display = df_display.reindex(columns=display_cols) 