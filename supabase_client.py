import os
import logging
from datetime import datetime
from supabase import create_client, Client

# ----------------------------------------------------------------------
# Inicialização do cliente Supabase
# ----------------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = None

def init_supabase_client() -> None:
    global supabase
    if supabase is None:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logging.info("Supabase client inicializado.")

# ----------------------------------------------------------------------
# Upsert e fetch de SLA
# ----------------------------------------------------------------------
def upsert_sla(df):
    # 1) Cria nova versão
    rv = supabase.table("versions").insert({"name": "upload_sla"}).select("id, created_at").execute()
    if rv.error:
        raise Exception(rv.error.message)
    version_id = rv.data[0]["id"]

    # 2) Prepara registros e insere
    df["version_id"] = version_id
    records = df.to_dict(orient="records")
    res = supabase.table("sla_tickets").insert(records).execute()
    if res.error:
        raise Exception(res.error.message)

    return version_id

def fetch_sla_versions():
    rv = supabase.table("versions").select("id, created_at").order("created_at", desc=True).execute()
    if rv.error:
        logging.error(rv.error.message)
        return []
    return rv.data

def fetch_sla_data(version_id):
    rv = supabase.table("sla_tickets").select("*").eq("version_id", version_id).execute()
    if rv.error:
        logging.error(rv.error.message)
        return []
    import pandas as pd
    return pd.DataFrame(rv.data)
