# -*- coding: utf-8 -*-
import os
from datetime import datetime
import pandas as pd
from supabase import create_client, Client

# Tabelas usadas
LOGS_TABLE = "dashboard_logs"
DATA_TABLE = "tickets"

# Inicializa o cliente do Supabase usando variáveis de ambiente
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://seu-projeto.supabase.co"
)
SUPABASE_KEY = os.environ.get(
    "SUPABASE_SERVICE_KEY",
    "sua_chave_service_role_aqui"
)
_client: Client = None


def init_supabase_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


def log_event(level: str, message: str, details: dict = None) -> None:
    """
    Insere um registro na tabela de logs do Supabase.
    Se falhar, cai para o print local.
    """
    client = init_supabase_client()
    log_record = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": message,
        "details": details or {}
    }
    try:
        # Execute retorna um objeto APIResponse
        resp = client.table(LOGS_TABLE).insert(log_record).execute()
        # Se a operação não lançar exceção, considera-se sucesso
    except Exception as e:
        # Fallback para console
        print(f"[{level.upper()}] Falha ao gravar log no Supabase: {e}")
        print(f"   Conteúdo do log: {log_record}")


def fetch_tickets() -> pd.DataFrame:
    """
    Busca todos os registros da tabela 'tickets' no Supabase
    e retorna um DataFrame vazio caso haja erro.
    """
    client = init_supabase_client()
    try:
        resp = client.table(DATA_TABLE).select("*").execute()
        # A resposta costuma ter atributo .data
        data = resp.data if hasattr(resp, "data") else []
        return pd.DataFrame(data)
    except Exception as e:
        log_event("warning", "Falha ao buscar tickets do Supabase", {"error": str(e)})
        return pd.DataFrame()


# inicializa o cliente ao importar
init_supabase_client()
