# Interesting Charts Data Manager

A Streamlit application for managing property data with Supabase PostgreSQL database integration.

## Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Database Connection
You need to set up your Supabase database connection. Replace `[YOUR-PASSWORD]` with your actual Supabase database password.

**Option A: Environment Variable (Recommended)**
```bash
export DATABASE_URL="postgresql://postgres:YOUR_ACTUAL_PASSWORD@db.obwwflnrapkulpakklab.supabase.co:5432/postgres"
```

**Option B: Direct Code Update**
Edit `interestingcharts.py` and replace the DATABASE_URL line with your actual password:
```python
DATABASE_URL = "postgresql://postgres:YOUR_ACTUAL_PASSWORD@db.obwwflnrapkulpakklab.supabase.co:5432/postgres"
```

### 3. Run the Application
```bash
streamlit run interestingcharts.py
```

## Features
- Reference table management from Excel files
- Interested items tracking with status management
- Property details extraction from links
- Travel time calculations to Zurich HB
- Database persistence with Supabase PostgreSQL

## Database Schema
The application automatically creates two tables:
- `data`: Reference data from Excel files
- `interested_items`: User-maintained property interest list

## Migration from DuckDB
This version has been migrated from DuckDB to Supabase PostgreSQL for better data persistence and scalability.
