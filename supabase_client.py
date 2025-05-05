# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
import traceback
from datetime import datetime
from supabase import create_client, Client

# Environment variables for Supabase
# SUPABASE_URL must be set, and SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SERVICE_KEY for service access
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_SERVICE_KEY")

# Table names
TICKETS_TABLE = "tickets"
LOGS_TABLE = "dashboard_logs"

# Supabase client placeholder
supabase_client: Client | None = None


def init_supabase_client() -> Client | None:
    """Initializes the Supabase client using environment variables."""
    global supabase_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        print(
            "Error: SUPABASE_URL and Supabase service role key (SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SERVICE_KEY) are required."
        )
        return None
    try:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Supabase client initialized successfully.")
        return supabase_client
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        print(traceback.format_exc())
        supabase_client = None
        return None


def get_supabase_client() -> Client | None:
    """Returns the initialized Supabase client, initializing if needed."""
    global supabase_client
    if supabase_client is None:
        return init_supabase_client()
    return supabase_client


def upsert_tickets_data(df: pd.DataFrame) -> bool:
    """Upserts DataFrame rows into the Supabase tickets table."""
    client = get_supabase_client()
    if not client:
        print("Supabase client not initialized. Skipping upsert.")
        return False
    if df.empty:
        print("DataFrame is empty. Skipping upsert.")
        return False

    df_clean = df.copy()
    # Convert datetime columns to ISO strings
    for col in df_clean.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]):
        df_clean[col] = df_clean[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    # Replace NaN/NaT and infinities
    df_clean = df_clean.replace({pd.NaT: None})
    df_clean = df_clean.replace([np.inf, -np.inf], np.nan).fillna(value=None)
    # Ensure booleans
    for col in df_clean.select_dtypes(include=["bool"]):
        df_clean[col] = df_clean[col].astype(bool)

    records = df_clean.to_dict(orient="records")
    print(f"Attempting to upsert {len(records)} records to '{TICKETS_TABLE}' table...")
    try:
        response = client.table(TICKETS_TABLE).upsert(records).execute()
        error = getattr(response, 'error', None)
        if error:
            print(f"Error during upsert: {error}")
            log_event("error", f"Upsert failed: {error}")
            return False
        print(f"Successfully upserted {len(records)} records.")
        return True
    except Exception as e:
        print(f"Exception during Supabase upsert: {e}")
        print(traceback.format_exc())
        log_event("error", f"Upsert exception: {e}")
        return False


def fetch_all_tickets_data() -> pd.DataFrame:
    """Fetches all rows from the Supabase tickets table."""
    client = get_supabase_client()
    if not client:
        print("Supabase client not initialized. Cannot fetch data.")
        return pd.DataFrame()

    print(f"Fetching all records from '{TICKETS_TABLE}'...")
    try:
        response = client.table(TICKETS_TABLE).select("*").execute()
        data = getattr(response, 'data', None)
        if data:
            df = pd.DataFrame(data)
            print(f"Fetched {len(df)} records.")
            # Attempt to parse date strings
            for col in df.columns:
                if df[col].dtype == object:
                    parsed = pd.to_datetime(df[col], errors='coerce')
                    if parsed.notna().sum() > len(df) * 0.5:
                        df[col] = parsed
            return df
        error = getattr(response, 'error', None)
        if error:
            print(f"Error fetching data: {error}")
            log_event("error", f"Fetch failed: {error}")
        else:
            print("No data found or unexpected response.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Exception during fetch: {e}")
        print(traceback.format_exc())
        log_event("error", f"Fetch exception: {e}")
        return pd.DataFrame()


def log_event(level: str, message: str, details: dict = None) -> None:
    """Logs events to the Supabase dashboard_logs table or prints fallback."""
    client = get_supabase_client()
    record = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": message,
        "details": details or {}
    }
    if not client:
        print(f"[LOG {level}] {message} {details or ''}")
        return
    try:
        resp = client.table(LOGS_TABLE).insert(record).execute()
        err = getattr(resp, 'error', None)
        if err:
            print(f"Error logging event: {err}")
            print(f"Fallback: [LOG {level}] {message} {details or ''}")
    except Exception as e:
        print(f"Exception during log insert: {e}")
        print(f"Fallback: [LOG {level}] {message} {details or ''}")
