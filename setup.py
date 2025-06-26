#!/usr/bin/env python3
"""
Setup script for Interesting Charts Data Manager
Helps with database initialization and migration from DuckDB to Supabase
"""

import os
import sys
import psycopg2
import pandas as pd
import duckdb

def test_connection(database_url):
    """Test the database connection"""
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Successfully connected to PostgreSQL: {version[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def migrate_from_duckdb(duckdb_file, database_url):
    """Migrate data from DuckDB to PostgreSQL"""
    if not os.path.exists(duckdb_file):
        print(f"❌ DuckDB file {duckdb_file} not found")
        return False
    
    try:
        # Connect to DuckDB and read data
        duck_conn = duckdb.connect(duckdb_file)
        
        # Read interested_items table
        try:
            interested_df = duck_conn.execute("SELECT * FROM interested_items").df()
            print(f"📊 Found {len(interested_df)} records in interested_items table")
        except:
            print("ℹ️ No interested_items table found in DuckDB")
            interested_df = pd.DataFrame()
        
        # Read data table
        try:
            data_df = duck_conn.execute("SELECT * FROM data").df()
            print(f"📊 Found {len(data_df)} records in data table")
        except:
            print("ℹ️ No data table found in DuckDB")
            data_df = pd.DataFrame()
        
        duck_conn.close()
        
        # Connect to PostgreSQL and migrate data
        pg_conn = psycopg2.connect(database_url)
        cursor = pg_conn.cursor()
        
        # Migrate interested_items if exists
        if not interested_df.empty:
            print("🔄 Migrating interested_items table...")
            for _, row in interested_df.iterrows():
                cursor.execute("""
                    INSERT INTO interested_items (
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
                    row.get('added_at', '')
                ))
            print("✅ interested_items migration completed")
        
        # Migrate data table if exists
        if not data_df.empty:
            print("🔄 Migrating data table...")
            for _, row in data_df.iterrows():
                cursor.execute("""
                    INSERT INTO data ("Canton", "Gemeinde", "MoreTaxPerMonth") 
                    VALUES (%s, %s, %s)
                """, (
                    row.get('Canton', ''),
                    row.get('Gemeinde', ''),
                    row.get('MoreTaxPerMonth', '')
                ))
            print("✅ data migration completed")
        
        pg_conn.commit()
        cursor.close()
        pg_conn.close()
        
        print("🎉 Migration completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

def main():
    print("🚀 Interesting Charts Data Manager Setup")
    print("=" * 50)
    
    # Get database URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL environment variable not set")
        print("Please set it with: export DATABASE_URL='your_connection_string'")
        return
    
    if '[YOUR-PASSWORD]' in database_url:
        print("❌ Please replace [YOUR-PASSWORD] with your actual Supabase password")
        return
    
    # Test connection
    print("🔍 Testing database connection...")
    if not test_connection(database_url):
        return
    
    # Check for DuckDB file
    duckdb_file = 'interestingcharts.duckdb'
    if os.path.exists(duckdb_file):
        print(f"📁 Found existing DuckDB file: {duckdb_file}")
        migrate = input("Do you want to migrate data from DuckDB? (y/n): ").lower().strip()
        if migrate == 'y':
            migrate_from_duckdb(duckdb_file, database_url)
    
    print("\n✅ Setup completed! You can now run:")
    print("streamlit run interestingcharts.py")

if __name__ == "__main__":
    main() 