# supabase_client.py

import os
import logging
from datetime import datetime
from supabase import create_client, Client
from typing import List, Dict, Any

# URL e KEY via vars de ambiente
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client | None = None

def init_supabase_client() -> None:
    """
    Inicializa o client Supabase se as variáveis de ambiente estiverem presentes.
    """
    global supabase
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("SUPABASE_URL ou SUPABASE_KEY não definidos nas env vars.")
        return
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Supabase client initialized successfully.")

def fetch_all_tickets_data() -> List[Dict[str, Any]]:
    """
    Busca todos os registros da tabela `tickets`. 
    Retorna lista vazia em casos de erro ou client não inicializado.
    """
    if supabase is None:
        logging.warning("Supabase client não inicializado. fetch_all_tickets_data retorna vazio.")
        return []
    try:
        resp = supabase.table("tickets").select("*").execute()
        data = getattr(resp, "data", None)
        if data is None:
            logging.warning("fetch_all_tickets_data: nenhum dado retornado.")
            return []
        return data
    except Exception as e:
        logging.error(f"Erro ao buscar dados iniciais: {e}")
        return []

def log_event(level: str, message: str, details: Dict[str, Any] | None = None) -> None:
    """
    Insere um log na tabela `dashboard_logs`.  
    Não levanta erro em caso de falha de inserção: apenas loga localmente.
    """
    if supabase is None:
        logging.error(f"Falha ao logar evento ({level}): Supabase client não inicializado.")
        return
    try:
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "details": details or {}
        }
        supabase.table("dashboard_logs").insert(record).execute()
    except Exception as e:
        logging.error(f"Falha ao logar evento ({level}): {e}")
