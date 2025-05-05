# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import io
import base64
from datetime import datetime, timedelta


def parse_excel_content(content_string: str) -> io.BytesIO:
    """Parses a base64-encoded Excel file content string into a BytesIO buffer."""
    content_type, content_data = content_string.split(",", 1)
    decoded = base64.b64decode(content_data)
    return io.BytesIO(decoded)


def process_uploaded_files(piloto_content_str: str, sla_content_str: str) -> pd.DataFrame:
    """
    Processes uploaded Excel files (piloto and SLA) and returns a merged DataFrame with
    SLA calculations, aging, lead time and categorization.
    """
    # Decode uploads
    piloto_io = parse_excel_content(piloto_content_str)
    sla_io    = parse_excel_content(sla_content_str)

    # --- Dynamic sheet loading for Piloto ---
    xls_piloto    = pd.ExcelFile(piloto_io)
    piloto_sheet  = xls_piloto.sheet_names[0]
    df_piloto     = pd.read_excel(piloto_io, sheet_name=piloto_sheet)
    # If header row is off (Unnamed columns), retry with header=1
    if any(col.startswith("Unnamed") for col in df_piloto.columns):
        df_piloto = pd.read_excel(piloto_io, sheet_name=piloto_sheet, header=1)
    print(f"Loaded Piloto: {df_piloto.shape[0]} rows from sheet '{piloto_sheet}'")

    # --- Dynamic sheet loading for SLA ---
    xls_sla    = pd.ExcelFile(sla_io)
    sla_sheet  = "Tickets" if "Tickets" in xls_sla.sheet_names else xls_sla.sheet_names[0]
    df_sla     = pd.read_excel(sla_io, sheet_name=sla_sheet)
    print(f"Loaded SLA: {df_sla.shape[0]} rows from sheet '{sla_sheet}'")

    # --- Clean and prepare Piloto ---
    df_piloto.rename(
        columns={"Unidade de Negócio.1": "Unidade de Negócio", "Resolvido": "Resolvido_Piloto"},
        inplace=True
    )

    # --- Clean and prepare SLA ---
    df_sla_sel = df_sla[[
        "Key", "Tempo até a primeira resposta", "Tempo de resolução", "SLA", "Primeira Resposta"
    ]].copy()
    df_sla_sel.rename(
        columns={
            "Key": "Chave",
            "Tempo até a primeira resposta": "HorasPrimeiraResposta_Original",
            "Tempo de resolução": "HorasResolucao_Original",
            "SLA": "CumpriuSLA_Resolucao_Original",
            "Primeira Resposta": "CumpriuSLA_PrimeiraResposta_Original"
        }, inplace=True
    )

    # --- Merge ---
    df = pd.merge(df_piloto, df_sla_sel, on="Chave", how="left")
    print(f"Merged data: {df.shape[0]} rows")

    # --- Parse dates ---
    for col in ["Criado", "Resolvido_Piloto", "Atualizado(a)"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # --- SLA maps ---
    sla_map_res  = {"Highest":4, "High":6, "Medium":16, "Low":24, "Lowest":40}
    sla_map_resp = {k: v/2 for k,v in sla_map_res.items()}
    df["SLA_Horas_Resolucao"]       = df["Prioridade"].map(sla_map_res)
    df["SLA_Horas_Primeira_Resposta"] = df["Prioridade"].map(sla_map_resp)

    # --- Calculations ---
    df["HorasResolucao_Calculated"] = (
        df["Resolvido_Piloto"] - df["Criado"]
    ).dt.total_seconds() / 3600

    def check_sla_res(row):
        sla = row.get("SLA_Horas_Resolucao")
        hr  = row.get("HorasResolucao_Calculated")
        if pd.isna(sla):
            return "N/A"
        if pd.isna(hr):
            # Open ticket or missing resolve
            aging = (datetime.now() - row["Criado"]).total_seconds()/3600 if pd.notna(row["Criado"]) else np.nan
            return "Não" if aging>sla else "Pendente"
        if hr <= sla and hr>=0:
            return "Sim"
        return "Não"
    df["CumpriuSLA_Resolucao_Calculated"] = df.apply(check_sla_res, axis=1)

    def check_sla_resp(row):
        sla  = row.get("SLA_Horas_Primeira_Resposta")
        hpr  = row.get("HorasPrimeiraResposta_Original")
        if pd.isna(sla): return "N/A"
        if pd.isna(hpr): return "Pendente"
        if hpr<0:      return "Não"
        return "Sim" if hpr<=sla else "Não"
    df["CumpriuSLA_PrimeiraResposta_Calculated"] = df.apply(check_sla_resp, axis=1)

    # Violations and status
    df["SLA_Violado_Calculated"] = np.where(
        df["CumpriuSLA_Resolucao_Calculated"]=="Não", "Sim",
        np.where(df["CumpriuSLA_Resolucao_Calculated"]=="Sim", "Não", "N/A")
    )
    closed = ["Concluído","Resolvido","Fechado","Cancelado"]
    df["Is_Open"] = ~df["Status"].isin(closed)

    # Aging for open
    now = datetime.now()
    df["Aging_Horas"] = np.nan
    mask_open = df["Is_Open"]
    df.loc[mask_open, "Aging_Horas"] = (
        now - df.loc[mask_open, "Criado"]
    ).dt.total_seconds()/3600

    # At risk
    def check_risk(row):
        if not row["Is_Open"]: return "N/A"
        sla = row.get("SLA_Horas_Resolucao")
        ag  = row.get("Aging_Horas")
        if pd.isna(sla) or pd.isna(ag): return "N/A"
        return "Sim" if ag > 0.8*sla and row["CumpriuSLA_Resolucao_Calculated"]!="Não" else "Não"
    df["Em_Risco"] = df.apply(check_risk, axis=1)

    # Status category
    df["Status_Categoria"] = df["Status"].apply(
        lambda s: "Fechado" if s in closed else ("Aguardando/Validação" if s in ["Aguardando Validação","Pendente"] else "Em Progresso")
    )

    # Lead time
    df["LeadTime_Horas"] = np.nan
    mask_closed = ~df["Is_Open"]
    df.loc[mask_closed, "LeadTime_Horas"] = (
        df.loc[mask_closed, "Resolvido_Piloto"] - df.loc[mask_closed, "Criado"]
    ).dt.total_seconds()/3600
    df.loc[df["LeadTime_Horas"]<0, "LeadTime_Horas"] = 0

    # Period columns
    df["Ano_Criacao"]       = df["Criado"].dt.year
    df["Trimestre_Criacao"] = df["Criado"].dt.quarter
    df["Mes_Ano_Criacao"]   = df["Criado"].dt.strftime("%Y-%m")

    print("Data processing complete.")
    return df

# Fallback returns empty DataFrame on error
    except Exception as e:
        print(f"Error processing files: {e}")
        return pd.DataFrame()
