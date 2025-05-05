# supabase_client.py
# -*- coding: utf-8 -*-

from datetime import datetime
import os
import traceback
import pandas as pd
import numpy as np
from supabase import create_client, Client

# Supabase configuration – read from env vars or use defaults for local testing
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://njluqaekknpssqcygymvz.supabase.co"
)
SUPABASE_KEY = os.environ.get(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJub2xnbHlwZXclMjBzZXJ2aWNlIiwiaXNzIjoibm9jZWxpZXJzIiwiaWF0IjoxNjc3OTMzNTI2LCJleHAiOjE5MzQ1NDE1MjZ9.NmQn4HppxhRXS1gGwyG83CBXHS-i09iIerqFlRdQBeg"
)

# Table names
TICKETS_TABLE = "tickets"
LOGS_TABLE    = "dashboard_logs"

# Global client handle
supabase: Client | None = None


def init_supabase_client() -> Client | None:
    """
    Initializes the Supabase client and returns it.
    """
    global supabase
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY.")
        return None
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Supabase client initialized successfully.")
        return supabase
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        return None


def get_supabase_client() -> Client | None:
    """
    Returns the initialized Supabase client, initializing on first call.
    """
    global supabase
    if supabase is None:
        return init_supabase_client()
    return supabase


def upsert_tickets_data(df: pd.DataFrame) -> bool:
    """
    Inserts all rows of the DataFrame into the tickets table.
    (Option 2: use pure INSERT instead of UPSERT.)
    """
    client = get_supabase_client()
    if client is None:
        print("Supabase client not initialized. Cannot insert data.")
        return False
    if df.empty:
        print("DataFrame is empty. Skipping insert.")
        return False

    records = df.to_dict("records")
    try:
        resp = client.table(TICKETS_TABLE).insert(records).execute()
        if hasattr(resp, "error") and resp.error:
            print("Error inserting tickets:", resp.error)
            return False
        return True
    except Exception as e:
        print("Exception on insert_tickets_data:", e)
        traceback.print_exc()
        return False


def fetch_all_tickets_data() -> pd.DataFrame | None:
    """
    Fetches all tickets from Supabase and returns as a DataFrame.
    """
    client = get_supabase_client()
    if client is None:
        print("Supabase client not initialized. Cannot fetch data.")
        return None

    try:
        resp = client.table(TICKETS_TABLE).select("*").order("Criado", desc=False).execute()
        if hasattr(resp, "error") and resp.error:
            print("Error fetching tickets:", resp.error)
            return None
        data = resp.data or []
        return pd.DataFrame(data)
    except Exception as e:
        print("Exception on fetch_all_tickets_data:", e)
        traceback.print_exc()
        return None


def log_event(level: str, message: str, details: dict | None = None) -> None:
    """
    Logs an event into the dashboard_logs table (or prints on failure).
    """
    client = get_supabase_client()
    log_record = {
        "level":     level,
        "message":   message,
        "details":   details or {},
        "timestamp": datetime.now().isoformat()
    }

    # Always print locally
    print(f"[{level.upper()}] {message}", details or "")

    if client is None:
        return

    try:
        resp = client.table(LOGS_TABLE).insert(log_record).execute()
        if hasattr(resp, "error") and resp.error:
            print("Error logging to Supabase:", resp.error)
    except Exception as e:
        print("Exception logging to Supabase:", e)
        traceback.print_exc()
