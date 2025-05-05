# data_processor.py
# -*- coding: utf-8 -*-

import io
import pandas as pd
import numpy as np


def process_uploaded_files(piloto_bytes: bytes, sla_bytes: bytes) -> pd.DataFrame:
    """
    Recebe bytes de XLSX para piloto e SLA,
    retorna DataFrame preparado.
    """
    df_piloto = pd.read_excel(io.BytesIO(piloto_bytes))
    df_sla = pd.read_excel(io.BytesIO(sla_bytes))

    chave = "Chave"
    if chave not in df_piloto.columns or chave not in df_sla.columns:
        raise ValueError(f"Coluna '{chave}' ausente em um dos arquivos")

    df = df_piloto.merge(df_sla, on=chave, how="inner")
    if df.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique a coluna 'Chave'.")

    # Mapeamentos de SLA
    sla_res_map = {"Baixa": 72, "Média": 24, "Alta": 8, "Crítica": 4}
    sla_1resp_map = {"Baixa": 24, "Média": 8, "Alta": 2, "Crítica": 1}

    df["SLA_Horas_Resolucao"] = df["Prioridade"].map(sla_res_map)
    df["SLA_Horas_Primeira_Resposta"] = df["Prioridade"].map(sla_1resp_map)

    # Tempos calculados
    df["HorasResolucao_Calculated"] = (
        pd.to_datetime(df["Data_Fecha"]) - pd.to_datetime(df["Data_Cria"])
    ).dt.total_seconds() / 3600
    df["HorasPrimeiraResposta_Original"] = (
        pd.to_datetime(df["Data_Primeira_Resp"]) - pd.to_datetime(df["Data_Cria"])
    ).dt.total_seconds() / 3600

    agora = pd.Timestamp.now()
    df["Aging_Horas"] = np.where(
        df["Data_Fecha"].isna(),
        (agora - pd.to_datetime(df["Data_Cria"])).dt.total_seconds() / 3600,
        df["HorasResolucao_Calculated"]
    )

    def check_risk(row):
        if pd.isna(row.get("SLA_Horas_Resolucao")) or row["SLA_Horas_Resolucao"] <= 0:
            return "N/A"
        pct = row["Aging_Horas"] / row["SLA_Horas_Resolucao"]
        return "Atuação necessária" if pct > 0.8 else "OK"

    df["Status_Risco"] = df.apply(check_risk, axis=1)
    df["Periodo_Cria"] = pd.to_datetime(df["Data_Cria"]).dt.to_period("M")

    cols = [
        "Chave", "Projeto", "Unidade_de_Negocio", "Prioridade",
        "SLA_Horas_Resolucao", "SLA_Horas_Primeira_Resposta",
        "HorasResolucao_Calculated", "HorasPrimeiraResposta_Original",
        "Aging_Horas", "Status_Risco", "Periodo_Cria"
    ]
    return df[cols]
