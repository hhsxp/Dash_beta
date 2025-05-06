## supabase_client.py
```python
import os
import pandas as pd
import numpy as np
import traceback
from datetime import datetime
from supabase import create_client, Client

# Configurações de ambiente
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_SERVICE_KEY")

# Nome das tabelas
TICKETS_TABLE = "tickets"
LOGS_TABLE = "dashboard_logs"

_supabase: Client | None = None

def init_supabase_client() -> Client | None:
    global _supabase
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL e SUPABASE_KEY são obrigatórios.")
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
    global _supabase
    if _supabase is None:
        return init_supabase_client()
    return _supabase


def upsert_tickets_data(df: pd.DataFrame) -> bool:
    client = get_supabase_client()
    if not client or df.empty:
        return False

    df_clean = df.copy()
    # Normaliza datas
    for col in df_clean.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]):
        df_clean[col] = df_clean[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    df_clean = df_clean.replace({pd.NaT: None})
    df_clean = df_clean.replace([np.inf, -np.inf], np.nan).fillna(None)

    records = df_clean.to_dict(orient="records")
    try:
        resp = client.table(TICKETS_TABLE).upsert(records).execute()
        err = getattr(resp, 'error', None)
        if err:
            log_event("error", f"Upsert failed: {err}")
            return False
        return True
    except Exception as e:
        log_event("error", f"Upsert exception: {e}")
        return False


def fetch_all_tickets_data() -> pd.DataFrame:
    client = get_supabase_client()
    if not client:
        return pd.DataFrame()
    try:
        resp = client.table(TICKETS_TABLE).select("*").execute()
        data = getattr(resp, 'data', None)
        if data:
            df = pd.DataFrame(data)
            # Tenta parse de datas
            for col in df.columns:
                parsed = pd.to_datetime(df[col], errors='coerce')
                if parsed.notna().sum() > len(df) / 2:
                    df[col] = parsed
            return df
    except Exception as e:
        log_event("error", f"Fetch exception: {e}")
    return pd.DataFrame()


def log_event(level: str, message: str, details: dict | None = None) -> None:
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
    except Exception as e:
        print(f"Exception during log insert: {e}")
```
