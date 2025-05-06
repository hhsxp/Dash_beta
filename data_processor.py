import io
import base64
import pandas as pd
import numpy as np

def process_uploaded_files(piloto_content: str, sla_content: str) -> pd.DataFrame:
    # Decodifica base64 e lê Excel
    _, piloto_b64 = piloto_content.split(",", 1)
    piloto_bytes = base64.b64decode(piloto_b64)
    df_piloto = pd.read_excel(io.BytesIO(piloto_bytes), engine="openpyxl")

    _, sla_b64 = sla_content.split(",", 1)
    sla_bytes = base64.b64decode(sla_b64)
    df_sla = pd.read_excel(io.BytesIO(sla_bytes), engine="openpyxl")

    # Verifica coluna 'Chave'
    if "Chave" not in df_piloto.columns or "Chave" not in df_sla.columns:
        raise ValueError("Coluna 'Chave' ausente em um dos arquivos")

    # Merge pelos tickets
    df = pd.merge(df_piloto, df_sla, on="Chave", suffixes=("_Piloto", "_SLA"))
    if df.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique a coluna 'Chave'")

    # Converte datas
    df["Criado"] = pd.to_datetime(df["Criado"], errors="coerce")
    df["Resolvido_Piloto"] = pd.to_datetime(df["Resolvido"], errors="coerce")

    # Calcula cumprimento de SLA
    df["CumpriuSLA_Resolucao_Calculated"] = df["Resolvido_Piloto"] <= df["Prazo_Resolucao"]
    df["CumpriuSLA_PrimeiraResposta_Calculated"] = df["PrimeiraResposta"] <= df["Prazo_PrimeiraResp"]

    # Aging (horas até primeira resposta)
    df["Aging_Horas"] = (pd.to_datetime(df["PrimeiraResposta"], errors="coerce") - df["Criado"]).dt.total_seconds() / 3600
    df["Aging_Horas"] = df["Aging_Horas"].clip(lower=0)

    # Lead Time (horas até resolução para tickets fechados)
    closed = ["Fechado", "Resolvido"]
    df["Is_Open"] = ~df["Status"].isin(closed)
    df["LeadTime_Horas"] = np.where(
        ~df["Is_Open"],
        (df["Resolvido_Piloto"] - df["Criado"]).dt.total_seconds() / 3600,
        np.nan
    )
    df["LeadTime_Horas"] = df["LeadTime_Horas"].clip(lower=0)

    # Colunas de período para filtros
    df["Mes_Ano_Criacao"] = df["Criado"].dt.strftime("%Y-%m")
    df["Ano_Criacao"] = df["Criado"].dt.year
    df["Trimestre_Criacao"] = df["Criado"].dt.quarter

    return df
