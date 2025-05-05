# supabase_client.py
# -*- coding: utf-8 -*-

import os
from datetime import datetime
from supabase import create_client, Client

# Carrega URL e KEY do Supabase das variáveis de ambiente
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("É preciso definir SUPABASE_URL e SUPABASE_SERVICE_KEY no ambiente.")

# Cliente Supabase único
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def insert_tickets(records: list[dict]) -> list[dict]:
    """
    Insere uma lista de dicionários na tabela `tickets`.
    Retorna a lista de registros inseridos (com quaisquer defaults do banco).
    """
    if not records:
        return []
    resp = supabase.table("tickets").insert(records).execute()
    if resp.error:
        raise RuntimeError(f"Erro ao inserir tickets: {resp.error.message}")
    return resp.data  # lista de dicts inseridos


def fetch_all_tickets() -> list[dict]:
    """
    Busca todos os registros da tabela `tickets`.
    """
    resp = supabase.table("tickets").select("*").order("Criado", desc=False).execute()
    if resp.error:
        raise RuntimeError(f"Erro ao buscar tickets: {resp.error.message}")
    return resp.data or []


def log_event(level: str, message: str, details: dict | None = None) -> None:
    """
    Registra um log na tabela `dashboard_logs`.
    """
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "level":     level,
        "message":   message,
        "details":   details or {}
    }
    resp = supabase.table("dashboard_logs").insert(payload).execute()
    if resp.error:
        # só printa, não interrompe a aplicação
        print(f"Falha ao gravar log: {resp.error.message}")
