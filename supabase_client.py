# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
import traceback

# !!! IMPORTANT: Replace these with Environment Variables in production !!!
# Use os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_SERVICE_KEY")
# SUPABASE_URL = "https://njluqaekknpssqcygmvz.supabase.co"
# SUPABASE_SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5qbHVxYWVra25wc3NxY3lnbXZ6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NjQ3NTUyNiwiZXhwIjoyMDYyMDUxNTI2fQ.NmQn4HppxhRXS1gGwyG83CBXHS-i09iIerqFlRdQBeg"

# Use environment variables - provide defaults for local testing if needed
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://njluqaekknpssqcygmvz.supabase.co")
# Use the SERVICE ROLE KEY for backend operations (inserting/updating data)
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5qbHVxYWVra25wc3NxY3lnbXZ6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NjQ3NTUyNiwiZXhwIjoyMDYyMDUxNTI2fQ.NmQn4HppxhRXS1gGwyG83CBXHS-i09iIerqFlRdQBeg")

TICKETS_TABLE = "tickets" # Define the table name
LOGS_TABLE = "dashboard_logs" # Optional table for logging

supabase: Client | None = None

def init_supabase_client():
    """Initializes the Supabase client."""
    global supabase
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables are required.")
        return None
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Supabase client initialized successfully.")
        return supabase
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        print(traceback.format_exc())
        supabase = None
        return None

def get_supabase_client():
    """Returns the initialized Supabase client, initializing if needed."""
    if supabase is None:
        return init_supabase_client()
    return supabase

def upsert_tickets_data(df: pd.DataFrame):
    """Upserts DataFrame rows into the Supabase tickets table."""
    client = get_supabase_client()
    if not client or df.empty:
        print("Supabase client not initialized or DataFrame is empty. Skipping upsert.")
        return False

    # Prepare data for Supabase (convert types, handle NaN/NaT)
    df_upsert = df.copy()
    # Convert datetime to ISO 8601 strings
    for col in df_upsert.select_dtypes(include=["datetime64[ns]"]).columns:
        # Check if timezone-aware, convert if necessary
        if df_upsert[col].dt.tz is not None:
            df_upsert[col] = df_upsert[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            # Supabase prefers ISO 8601 with timezone offset
        else:
            df_upsert[col] = df_upsert[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%f") # Naive datetime

    # Replace NaN/NaT with None for JSON compatibility
    df_upsert = df_upsert.replace({pd.NaT: None})
    df_upsert = df_upsert.replace([np.inf, -np.inf], np.nan).fillna(value=None)

    # Convert boolean explicitly if needed (often handled by JSON conversion)
    for col in df_upsert.select_dtypes(include=["bool"]).columns:
        df_upsert[col] = df_upsert[col].astype(bool)

    # Convert DataFrame to list of dictionaries
    records = df_upsert.to_dict(orient="records")

    print(f"Attempting to upsert {len(records)} records to 	'{TICKETS_TABLE}	' table...")
    try:
        # Upsert data - assumes 	'Chave	' is the primary key or unique identifier
        # Use 	'ignore_duplicates=False	' and rely on primary key conflict or RLS
        # Or use 	'on_conflict=	'Chave	'	' if your table is set up for it (requires PostgreSQL 15+ for native merge)
        # Simpler approach: Upsert handles conflicts based on primary key
        response = client.table(TICKETS_TABLE).upsert(records).execute()
        print(f"Upsert response: {response}")
        if hasattr(response, 'error') and response.error:
             print(f"Error during upsert: {response.error}")
             log_event("error", f"Supabase upsert failed: {response.error}")
             return False
        print(f"Successfully upserted {len(records)} records.")
        return True
    except Exception as e:
        print(f"Exception during Supabase upsert: {e}")
        print(traceback.format_exc())
        log_event("error", f"Supabase upsert exception: {e}")
        return False

def fetch_all_tickets_data() -> pd.DataFrame:
    """Fetches all data from the Supabase tickets table."""
    client = get_supabase_client()
    if not client:
        print("Supabase client not initialized. Cannot fetch data.")
        return pd.DataFrame() # Return empty DataFrame

    print(f"Attempting to fetch all records from 	'{TICKETS_TABLE}	' table...")
    try:
        response = client.table(TICKETS_TABLE).select("*").execute()
        print(f"Fetch response received.") # Don't print full response data for brevity/security
        if hasattr(response, 'data') and response.data:
            df = pd.DataFrame(response.data)
            print(f"Successfully fetched {len(df)} records.")
            # Convert data types back if necessary (e.g., dates)
            for col in df.columns:
                 # Attempt to convert columns that look like dates/timestamps
                 if df[col].dtype == 'object':
                     try:
                         # Try converting to datetime, coercing errors
                         converted_col = pd.to_datetime(df[col], errors='coerce')
                         # Check if conversion was successful for a significant portion
                         if converted_col.notna().sum() > len(df) * 0.5: # Heuristic
                             df[col] = converted_col
                             print(f"Converted column 	'{col}	' back to datetime.")
                     except Exception:
                         pass # Ignore columns that fail conversion
            return df
        elif hasattr(response, 'error') and response.error:
             print(f"Error fetching data: {response.error}")
             log_event("error", f"Supabase fetch failed: {response.error}")
             return pd.DataFrame()
        else:
            print("No data found in table or unexpected response.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Exception during Supabase fetch: {e}")
        print(traceback.format_exc())
        log_event("error", f"Supabase fetch exception: {e}")
        return pd.DataFrame()

def log_event(log_level: str, message: str, details: dict = None):
    """Logs an event to the Supabase logs table (optional)."""
    client = get_supabase_client()
    if not client:
        print(f"Log (Supabase client not init): [{log_level}] {message}")
        return

    log_record = {
        "timestamp": datetime.now().isoformat(),
        "level": log_level,
        "message": message,
        "details": details or {}
    }

    try:
        response = client.table(LOGS_TABLE).insert(log_record).execute()
        if hasattr(response, 'error') and response.error:
            print(f"Error logging to Supabase: {response.error}")
            # Fallback log to console
            print(f"Log (Supabase failed): [{log_level}] {message} {details or ''}")
    except Exception as e:
        print(f"Exception logging to Supabase: {e}")
        # Fallback log to console
        print(f"Log (Supabase exception): [{log_level}] {message} {details or ''}")

# Initialize client on import
# init_supabase_client()

