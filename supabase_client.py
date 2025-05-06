### supabase_client.py
```python
import os
import logging
from datetime import datetime
import pandas as pd
from supabase import create_client, Client

# Carrega URL e KEY do Supabase das env vars
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

    # Opcional: testar conexão
    try:
        supabase_client.table("dashboard_logs").select("id").limit(1).execute()
    except Exception as e:
        logging.error(f"Falha ao conectar no Supabase: {e}")


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
    status = getattr(resp, 'status_code', None)
    if status != 201:
        logging.error(f"Falha ao logar evento (status {status}): {getattr(resp, 'data', resp)}")
    else:
        logging.info("Evento logado com sucesso.")


def fetch_all_tickets_data() -> pd.DataFrame:
    """
    Busca todos os registros da tabela `tickets` e retorna um DataFrame.
    """
    if supabase_client is None:
        logging.warning("Supabase client não inicializado. fetch_all_tickets_data retorna vazio.")
        return pd.DataFrame()
    resp = supabase_client.table("tickets").select("*").execute()
    status = getattr(resp, 'status_code', None)
    if status != 200:
        logging.warning(f"Falha ao buscar tickets (status {status}): {getattr(resp, 'data', resp)}")
        return pd.DataFrame()
    # Converte lista de dicts para DataFrame
    return pd.DataFrame(resp.data)
```