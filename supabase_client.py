# supabase_client.py
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import traceback
from datetime import datetime
from supabase import create_client, Client

# Carregue pelas variáveis de ambiente em produção
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_SERVICE_KEY")

# Nomes das tabelas
TICKETS_TABLE = "tickets"
LOGS_TABLE = "dashboard_logs"

# Cliente Supabase (singleton)
_supabase: Client | None = None

def init_supabase_client() -> Client | None:
    """Inicializa o cliente Supabase usando variáveis de ambiente."""
    global _supabase
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL e SUPABASE_SERVICE_KEY (ou _SERVICE_ROLE_KEY) são obrigatórios.")
        return None
    try:
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Supabase client initialized successfully.")
        return _supabase
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        print(traceback.format_exc())
        _supabase = None
        return None

def get_supabase_client() -> Client | None:
    """Retorna o cliente Supabase, inicializando-o se ainda não tiver sido."""
    global _supabase
    if _supabase is None:
        return init_supabase_client()
    return _supabase

def upsert_tickets_data(df: pd.DataFrame) -> bool:
    """Upsert de um DataFrame na tabela de tickets."""
    client = get_supabase_client()
    if not client:
        print("Supabase client not initialized. Skipping upsert.")
        return False
    if df.empty:
        print("DataFrame is empty. Skipping upsert.")
        return False

    # Limpeza / conversão
    df_clean = df.copy()
    # Converte datetimes para ISO strings
    for col in df_clean.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]):
        df_clean[col] = df_clean[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    # Substitui NaN/NaT e infinitos
    df_clean = df_clean.replace({pd.NaT: None})
    df_clean = df_clean.replace([np.inf, -np.inf], np.nan).fillna(value=None)
    # Garante boolean
    for col in df_clean.select_dtypes(include=["bool"]):
        df_clean[col] = df_clean[col].astype(bool)

    records = df_clean.to_dict(orient="records")
    print(f"Attempting to upsert {len(records)} records to '{TICKETS_TABLE}' table...")
    try:
        resp = client.table(TICKETS_TABLE).upsert(records).execute()
        err = getattr(resp, 'error', None)
        if err:
            print(f"Error during upsert: {err}")
            log_event("error", f"Upsert failed: {err}")
            return False
        print(f"Successfully upserted {len(records)} records.")
        return True
    except Exception as e:
        print(f"Exception during Supabase upsert: {e}")
        print(traceback.format_exc())
        log_event("error", f"Upsert exception: {e}")
        return False

def fetch_all_tickets_data() -> pd.DataFrame:
    """Busca todos os registros da tabela de tickets e devolve um DataFrame."""
    client = get_supabase_client()
    if not client:
        print("Supabase client not initialized. Cannot fetch data.")
        return pd.DataFrame()

    print(f"Fetching all records from '{TICKETS_TABLE}'...")
    try:
        resp = client.table(TICKETS_TABLE).select("*").execute()
        data = getattr(resp, 'data', None)
        if data:
            df = pd.DataFrame(data)
            print(f"Fetched {len(df)} records.")
            # Tenta parse de datas
            for col in df.columns:
                if df[col].dtype == object:
                    parsed = pd.to_datetime(df[col], errors='coerce')
                    if parsed.notna().sum() > len(df) * 0.5:
                        df[col] = parsed
            return df
        err = getattr(resp, 'error', None)
        if err:
            print(f"Error fetching data: {err}")
            log_event("error", f"Fetch failed: {err}")
        else:
            print("No data found or unexpected response.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Exception during fetch: {e}")
        print(traceback.format_exc())
        log_event("error", f"Fetch exception: {e}")
        return pd.DataFrame()

def log_event(level: str, message: str, details: dict | None = None) -> None:
    """Grava um log na tabela dashboard_logs (ou imprime como fallback)."""
    client = get_supabase_client()
    record = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": message,
        "details": details or {}
    }
    if not client:
        print(f"[LOG {level.upper()}] {message} {details or ''}")
        return

    try:
        resp = client.table(LOGS_TABLE).insert(record).execute()
        err = getattr(resp, 'error', None)
        if err:
            print(f"Error logging event: {err}")
            print(f"[LOG {level.upper()}] {message} {details or ''}")
    except Exception as e:
        print(f"Exception during log insert: {e}")
        print(f"[LOG {level.upper()}] {message} {details or ''}")
