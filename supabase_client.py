import os
import logging
from datetime import datetime
from supabase import create_client, Client

# Carrega URL e KEY do Supabase das variáveis de ambiente
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Cliente global
supabase_client: Client | None = None

def init_supabase_client() -> None:
    """
    Inicializa o supabase_client global.
    Chame esta função antes de usar log_event ou fetch_all_tickets_data.
    """
    global supabase_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("SUPABASE_URL ou SUPABASE_KEY não definidos nas env vars.")
        return
    supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Supabase client inicializado com sucesso.")

def log_event(level: str, message: str, details: dict | None = None) -> None:
    """
    Insere um registro na tabela `dashboard_logs`.
    """
    if supabase_client is None:
        logging.warning("Supabase client não inicializado. Ignorando log_event.")
        return
    payload = {
        "level": level,
        "message": message,
        "details": details or {},
        "timestamp": datetime.now().isoformat()
    }
    resp = supabase_client.table("dashboard_logs").insert(payload).execute()
    # no client v2, resp é APIResponse; status_code 201 == sucesso
    if getattr(resp, "status_code", None) != 201:
        logging.error(f"Falha ao logar evento: {getattr(resp, 'data', resp)}")
    else:
        logging.info("Evento logado com sucesso.")

def fetch_all_tickets_data() -> list[dict]:
    """
    Busca todos os registros da tabela `tickets`.
    Retorna lista de dicts, ou [] em caso de erro.
    """
    if supabase_client is None:
        logging.warning("Supabase client não inicializado. fetch_all_tickets_data retorna vazio.")
        return []
    resp = supabase_client.table("tickets").select("*").execute()
    if getattr(resp, "status_code", None) != 200:
        logging.warning(f"Falha ao buscar tickets: {getattr(resp, 'data', resp)}")
        return []
    return resp.data  # já é uma lista de dicts
