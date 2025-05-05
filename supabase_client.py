# supabase_client.py
# -*- coding: utf-8 -*-

from datetime import datetime
import os
import traceback
import pandas as pd
import numpy as np
from supabase import create_client, Client

# Supabase configuration – read from env vars ou defaults locais
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://seu-projeto.supabase.co"
)
SUPABASE_KEY = os.environ.get(
    "SUPABASE_SERVICE_KEY",
    "seu_service_key_aqui"
)

TICKETS_TABLE = "tickets"
LOGS_TABLE    = "dashboard_logs"

supabase: Client | None = None

def init_supabase_client() -> Client | None:
    global supabase
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: falta SUPABASE_URL ou SUPABASE_SERVICE_KEY.")
        return None
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Supabase client initialized successfully.")
        return supabase
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        return None

def get_supabase_client() -> Client | None:
    global supabase
    if supabase is None:
        return init_supabase_client()
    return supabase

def upsert_tickets_data(df: pd.DataFrame) -> bool:
    """
    Insere todas as linhas em tickets via INSERT (não depende de PK).
    """
    client = get_supabase_client()
    if client is None:
        print("Cliente Supabase não inicializado.")
        return False
    if df.empty:
        print("DataFrame vazio. Pulando insert.")
        return False

    records = df.to_dict("records")
    try:
        resp = client.table(TICKETS_TABLE).insert(records).execute()
        if hasattr(resp, "error") and resp.error:
            print("Erro ao inserir tickets:", resp.error)
            return False
        return True
    except Exception as e:
        print("Exception on insert_tickets_data:", e)
        traceback.print_exc()
        return False

def fetch_all_tickets_data() -> pd.DataFrame | None:
    client = get_supabase_client()
    if client is None:
        print("Cliente Supabase não inicializado.")
        return None
    try:
        resp = client.table(TICKETS_TABLE).select("*").order("Criado", desc=False).execute()
        if hasattr(resp, "error") and resp.error:
            print("Erro ao buscar tickets:", resp.error)
            return None
        data = resp.data or []
        return pd.DataFrame(data)
    except Exception as e:
        print("Exception on fetch_all_tickets_data:", e)
        traceback.print_exc()
        return None

def log_event(level: str, message: str, details: dict | None = None) -> None:
    client = get_supabase_client()
    log_record = {
        "level":     level,
        "message":   message,
        "details":   details or {},
        "timestamp": datetime.now().isoformat()
    }
    print(f"[{level.upper()}] {message}", details or "")
    if client is None:
        return
    try:
        resp = client.table(LOGS_TABLE).insert(log_record).execute()
        if hasattr(resp, "error") and resp.error:
            print("Erro ao logar evento:", resp.error)
    except Exception as e:
        print("Exception logging to Supabase:", e)
        traceback.print_exc()
