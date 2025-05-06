import io
import base64
import pandas as pd
from datetime import datetime

def process_sla_file(sla_content: str) -> pd.DataFrame:
    """
    Recebe o conteúdo base64 do SLA.xlsx (aba 'Tickets'),
    aplica transformações e devolve DataFrame pronto para upsert.
    """
    # 1) Decodifica base64
    _, b64 = sla_content.split(",", 1)
    data = base64.b64decode(b64)
    df = pd.read_excel(io.BytesIO(data), sheet_name="Tickets", engine="openpyxl")

    # 2) Converte datas
    df["Criado"]         = pd.to_datetime(df["Criado"], errors="coerce")
    df["Atualizado(a)"]  = pd.to_datetime(df["Atualizado(a)"], errors="coerce")

    # 3) Calcula tempos em horas
    df["HorasResolução"] = pd.to_timedelta(df["Tempo de resolução"]).dt.total_seconds() / 3600
    df["Horas1aResp"]    = pd.to_timedelta(df["Tempo até a primeira resposta"]).dt.total_seconds() / 3600

    # 4) SLA limite por prioridade
    prio_map = {
        "Highest": 4,
        "High":    6,
        "Medium": 12,
        "Low":    24,
        "Lowest":40
    }
    df["SLA_Horas"] = df["Prioridade"].map(prio_map)

    # 5) Flags de cumprimento
    df["CumpriuSLA_Res"] = df["HorasResolução"] <= df["SLA_Horas"]
    df["CumpriuSLA_1a"]  = df["Horas1aResp"]    <= (df["SLA_Horas"] / 2)

    # 6) Aging e LeadTime
    df["LeadTime_Horas"] = df["HorasResolução"]
    df["Aging_Horas"]    = (datetime.utcnow() - df["Atualizado(a)"]).dt.total_seconds() / 3600

    # 7) Campos de período para filtros
    df["Mes_Ano"]   = df["Criado"].dt.to_period("M").astype(str)
    df["Trimestre"] = df["Criado"].dt.to_period("Q").astype(str)
    df["Ano"]       = df["Criado"].dt.year

    return df
