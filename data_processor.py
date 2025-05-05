# data_processor.py
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import io
import base64
from datetime import datetime

def parse_excel_content(content_string: str) -> io.BytesIO:
    """Decodifica conteúdo base64 de Excel em BytesIO."""
    _, content_data = content_string.split(",", 1)
    decoded = base64.b64decode(content_data)
    return io.BytesIO(decoded)

def process_uploaded_files(piloto_content_str: str, sla_content_str: str) -> pd.DataFrame:
    """
    Processa arquivos Piloto e SLA (.xlsx), faz merge e calcula SLAs,
    aging, lead time e colunas de período.
    Retorna DataFrame pronto para inserção.
    """
    try:
        piloto_io = parse_excel_content(piloto_content_str)
        sla_io    = parse_excel_content(sla_content_str)

        # --- Carregar Piloto ---
        xls_piloto   = pd.ExcelFile(piloto_io)
        piloto_sheet = xls_piloto.sheet_names[0]
        df_piloto    = pd.read_excel(piloto_io, sheet_name=piloto_sheet)
        if any(col.startswith("Unnamed") for col in df_piloto.columns):
            df_piloto = pd.read_excel(piloto_io, sheet_name=piloto_sheet, header=1)

        # Auto-rename Key → Chave
        if "Chave" not in df_piloto.columns and "Key" in df_piloto.columns:
            df_piloto.rename(columns={"Key": "Chave"}, inplace=True)

        # --- Carregar SLA ---
        xls_sla   = pd.ExcelFile(sla_io)
        sla_sheet = "Tickets" if "Tickets" in xls_sla.sheet_names else xls_sla.sheet_names[0]
        df_sla    = pd.read_excel(sla_io, sheet_name=sla_sheet)

        df_sla = df_sla.rename(columns={
            "Key": "Chave",
            "Tempo até a primeira resposta": "HorasPrimeiraResposta_Original",
            "Tempo de resolução":             "HorasResolucao_Original",
            "SLA":                             "CumpriuSLA_Resolucao_Original",
            "Primeira Resposta":               "CumpriuSLA_PrimeiraResposta_Original"
        })
        df_sla_sel = df_sla[[
            "Chave",
            "HorasPrimeiraResposta_Original",
            "HorasResolucao_Original",
            "CumpriuSLA_Resolucao_Original",
            "CumpriuSLA_PrimeiraResposta_Original"
        ]]

        # Merge
        df = pd.merge(df_piloto, df_sla_sel, on="Chave", how="left")

        # Datas
        for col in ["Criado", "Resolvido", "Atualizado(a)"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Map SLAs por prioridade
        sla_map      = {"Highest":4, "High":6, "Medium":16, "Low":24, "Lowest":40}
        sla_resp_map = {k:v/2 for k,v in sla_map.items()}
        df["SLA_Horas_Resolucao"]         = df["Prioridade"].map(sla_map)
        df["SLA_Horas_Primeira_Resposta"] = df["Prioridade"].map(sla_resp_map)

        # Cálculo SLAs
        df["HorasResolucao_Calculated"] = (
            df["Resolvido"] - df["Criado"]
        ).dt.total_seconds() / 3600

        def check_sla_res(row):
            sla = row["SLA_Horas_Resolucao"]
            hr  = row["HorasResolucao_Calculated"]
            if pd.isna(sla): return "N/A"
            if pd.isna(hr):  return "Pendente"
            return "Sim" if hr <= sla else "Não"
        df["CumpriuSLA_Resolucao_Calculated"] = df.apply(check_sla_res, axis=1)

        def check_sla_resp(row):
            sla2 = row["SLA_Horas_Primeira_Resposta"]
            hpr  = row["HorasPrimeiraResposta_Original"]
            if pd.isna(sla2): return "N/A"
            if pd.isna(hpr):  return "Pendente"
            return "Sim" if hpr <= sla2 else "Não"
        df["CumpriuSLA_PrimeiraResposta_Calculated"] = df.apply(check_sla_resp, axis=1)

        # Aging & Lead Time
        closed_states = ["Concluído","Resolvido","Fechado","Cancelado"]
        df["Is_Open"] = ~df["Status"].isin(closed_states)
        now = datetime.now()

        df["Aging_Horas"] = np.where(
            df["Is_Open"],
            (now - df["Criado"]).dt.total_seconds() / 3600,
            np.nan
        )
        df["LeadTime_Horas"] = np.where(
            ~df["Is_Open"],
            (df["Resolvido"] - df["Criado"]).dt.total_seconds() / 3600,
            np.nan
        ).clip(lower=0)

        # Colunas de período
        df["Ano_Criacao"]       = df["Criado"].dt.year
        df["Trimestre_Criacao"] = df["Criado"].dt.quarter
        df["Mes_Ano_Criacao"]   = df["Criado"].dt.strftime("%Y-%m")

        return df

    except Exception as e:
        print(f"Error processing files: {e}")
        return pd.DataFrame()
