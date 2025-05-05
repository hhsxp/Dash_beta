# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
from datetime import datetime
import traceback

# Environment variables for Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://njluqaekknpssqcygmvz.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "YOUR_SERVICE_ROLE_KEY_HERE")

# Table names
TICKETS_TABLE = "tickets"
LOGS_TABLE = "dashboard_logs"

supabase: Client | None = None


def init_supabase_client() -> Client | None:
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


def get_supabase_client() -> Client | None:
    """Returns the initialized Supabase client, initializing if needed."""
    if supabase is None:
        return init_supabase_client()
    return supabase


def upsert_tickets_data(df: pd.DataFrame) -> bool:
    """Upserts DataFrame rows into the Supabase tickets table."""
    client = get_supabase_client()
    if not client or df.empty:
        print("Supabase client not initialized or DataFrame is empty. Skipping upsert.")
        return False

    df_upsert = df.copy()
    # Convert datetime columns to ISO strings
    for col in df_upsert.select_dtypes(include=["datetime64[ns]"]):
        if df_upsert[col].dt.tz is not None:
            df_upsert[col] = df_upsert[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        else:
            df_upsert[col] = df_upsert[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    # Replace NaN/NaT and infinities
    df_upsert = df_upsert.replace({pd.NaT: None})
    df_upsert = df_upsert.replace([np.inf, -np.inf], np.nan).fillna(value=None)

    # Ensure boolean types
    for col in df_upsert.select_dtypes(include=["bool"]):
        df_upsert[col] = df_upsert[col].astype(bool)

    records = df_upsert.to_dict(orient="records")
    print(f"Attempting to upsert {len(records)} records to '{TICKETS_TABLE}' table...")
    try:
        response = client.table(TICKETS_TABLE).upsert(records).execute()
        print(f"Upsert response: {response}")
        if getattr(response, 'error', None):
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
        return pd.DataFrame()

    print(f"Attempting to fetch all records from '{TICKETS_TABLE}' table...")
    try:
        response = client.table(TICKETS_TABLE).select("*").execute()
        print("Fetch response received.")
        if getattr(response, 'data', None):
            df = pd.DataFrame(response.data)
            print(f"Successfully fetched {len(df)} records.")
            # Convert date-like columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    converted = pd.to_datetime(df[col], errors='coerce')
                    if converted.notna().sum() > len(df) * 0.5:
                        df[col] = converted
                        print(f"Converted column '{col}' back to datetime.")
            return df
        if getattr(response, 'error', None):
            print(f"Error fetching data: {response.error}")
            log_event("error", f"Supabase fetch failed: {response.error}")
        else:
            print("No data found in table or unexpected response.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Exception during Supabase fetch: {e}")
        print(traceback.format_exc())
        log_event("error", f"Supabase fetch exception: {e}")
        return pd.DataFrame()


def log_event(log_level: str, message: str, details: dict = None) -> None:
    """Logs an event to the Supabase logs table."""
    client = get_supabase_client()
    if not client:
        print(f"Log (client not init): [{log_level}] {message}")
        return

    log_record = {
        "timestamp": datetime.now().isoformat(),
        "level": log_level,
        "message": message,
        "details": details or {}
    }
    try:
        response = client.table(LOGS_TABLE).insert(log_record).execute()
        if getattr(response, 'error', None):
            print(f"Error logging to Supabase: {response.error}")
            print(f"Fallback log: [{log_level}] {message} {details or ''}")
    except Exception as e:
        print(f"Exception logging to Supabase: {e}")
        print(f"Fallback log: [{log_level}] {message} {details or ''}")

# Optionally initialize on import
# init_supabase_client()
