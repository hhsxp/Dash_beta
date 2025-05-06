import os
import logging
from supabase import create_client, Client
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client | None = None

logger = logging.getLogger(__name__)

def init_supabase_client() -> None:
    global supabase
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client inicializado.")
    else:
        logger.error("SUPABASE_URL ou SUPABASE_KEY não definidos nas env vars.")
        supabase = None

def fetch_all_tickets_data() -> list[dict]:
    """Retorna lista de dicionários com todos os tickets."""
    if not supabase:
        logger.warning("Supabase client não inicializado. fetch retorna vazio.")
        return []
    resp = supabase.table("tickets").select("*").execute()
    if resp.status_code != 200:
        logger.error("Erro ao buscar dados iniciais: %s", resp.error_message if hasattr(resp, "error_message") else resp.data)
        return []
    return resp.data or []

def log_event(level: str, message: str, details: dict | None = None) -> None:
    """Insere um log na tabela dashboard_logs."""
    if not supabase:
        logger.warning("Falha ao logar evento (%s): supabase não inicializado", level)
        return
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "message": message,
        "details": details or {},
    }
    resp = supabase.table("dashboard_logs").insert(payload).execute()
    # checagem genérica de HTTP 2xx
    if not (200 <= resp.status_code < 300):
        logger.error("Falha ao logar evento (status %s): %s", resp.status_code, resp.data)
