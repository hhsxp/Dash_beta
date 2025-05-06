# supabase_client.py
import os
from supabase import create_client, Client
import logging
from datetime import datetime

# Inicialização do client Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = None

def init_supabase():
    global supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def log_event(level: str, message: str, details: dict = None):
    """
    Registra um evento na tabela 'dashboard_logs'.
    """
    if supabase is None:
        logging.warning("Supabase client not initialized. Cannot log event.")
        return
    event = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": message,
        "details": details or {}
    }
    resp = supabase.table("dashboard_logs").insert(event).execute()
    if resp.status_code >= 300:
        logging.error(f"Failed to log event: {resp.data}")


def fetch_all_tickets_data():
    """
    Busca todos os registros da tabela 'tickets'.
    """
    if supabase is None:
        logging.warning("Supabase client not initialized. Cannot fetch data.")
        return []
    resp = supabase.table("tickets").select("*").execute()
    return resp.data


def insert_tickets_data(rows: list):
    """
    Insere uma lista de dicionários na tabela 'tickets'.
    """
    if supabase is None:
        logging.warning("Supabase client not initialized. Cannot insert data.")
        return
    resp = supabase.table("tickets").insert(rows).execute()
    if resp.status_code >= 300:
        logging.error(f"Failed to insert tickets: {resp.data}")