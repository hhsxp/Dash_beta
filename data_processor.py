# data_processor.py
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import io
import base64
from datetime import datetime


def parse_excel_content(content_string: str) -> io.BytesIO:
    """Decodifica conteúdo base64 de Excel em um buffer BytesIO."""
    if not isinstance(content_string, str) or "," not in content_string:
        raise ValueError("Conteúdo inválido: deve ser string base64 iniciando com 'data:'")
    _, content_data = content_string.split(",", 1)
    decoded = base64.b64decode(content_data)
    return io.BytesIO(decoded)


def load_and_normalize_piloto(piloto_io: io.BytesIO) -> pd.DataFrame:
    """Lê a planilha Piloto de forma dinâmica e normaliza a coluna 'Chave'."""
    # Tenta cabeçalho na linha 0
    df = pd.read_excel(piloto_io, sheet_name=0, header=0)
    # Detecta e ajusta possíveis linhas de cabeçalho extras
    if "Chave" not in df.columns and "Key" in df.columns:
        df.rename(columns={"Key": "Chave"}, inplace=True)
    if "Chave" not in df.columns:
        # Tenta ler com header=1
        piloto_io.seek(0)
        df = pd.read_excel(piloto_io, sheet_name=0, header=1)
        if "Key" in df.columns:
            df.rename(columns={"Key": "Chave"}, inplace=True)
    if "Chave" not in df.columns:
        raise KeyError("Coluna 'Chave' não encontrada no arquivo Piloto.")
    # Strip e cast para string
    df["Chave"] = df["Chave"].astype(str).str.strip()
    return df


def load_and_normalize_sla(sla_io: io.BytesIO) -> pd.DataFrame:
    """Lê a planilha SLA de forma dinâmica, renomeia colunas e normaliza 'Chave'."""
    xls = pd.ExcelFile(sla_io)
    sheet = "Tickets" if "Tickets" in xls.sheet_names else xls.sheet_names[0]
    df = pd.read_excel(sla_io, sheet_name=sheet, header=0)
    mapping = {
        "Key": "Chave",
        "Tempo até a primeira resposta": "HorasPrimeiraResposta_Original",
        "Tempo de resolução": "HorasResolucao_Original",
        "SLA": "CumpriuSLA_Resolucao_Original",
        "Primeira Resposta": "CumpriuSLA_PrimeiraResposta_Original"
    }
    df.rename(columns=mapping, inplace=True)
    if "Chave" not in df.columns:
        # Tenta header=1
        sla_io.seek(0)
        df = pd.read_excel(sla_io, sheet_name=sheet, header=1)
        df.rename(columns=mapping, inplace=True)
    if "Chave" not in df.columns:
        raise KeyError("Coluna 'Chave' não encontrada no arquivo SLA.")
    df["Chave"] = df["Chave"].astype(str).str.strip()
    # Seleciona apenas colunas necessárias
    cols = [
        "Chave",
        "HorasPrimeiraResposta_Original",
        "HorasResolucao_Original",
        "CumpriuSLA_Resolucao_Original",
        "CumpriuSLA_PrimeiraResposta_Original"
    ]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Colunas faltando no SLA: {missing}")
    return df.loc[:, cols]


def process_uploaded_files(piloto_content_str: str, sla_content_str: str) -> pd.DataFrame:
    """
    Processa uploads Piloto e SLA, faz merge por 'Chave', calcula SLAs,
    aging, lead time e adiciona colunas de período.
    Gera erro se não encontrar nenhum ticket coincidente.
    """
    try:
        piloto_io = parse_excel_content(piloto_content_str)
        sla_io    = parse_excel_content(sla_content_str)

        df_piloto = load_and_normalize_piloto(piloto_io)
        df_sla    = load_and_normalize_sla(sla_io)

        # Merge inner: só tickets existentes em ambos
        df = pd.merge(df_piloto, df_sla, on="Chave", how="inner")
        if df.empty:
            raise ValueError(
                "Nenhum ticket encontrado após o merge. "
                "Verifique se a coluna 'Chave' existe e coincide em ambos arquivos."
            )

        # Converte colunas de data
        for col in ["Criado", "Resolvido", "Atualizado(a)"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Map SLAs por prioridade
        sla_map      = {"Highest":4, "High":6, "Medium":16, "Low":24, "Lowest":40}
        sla_resp_map = {k:v/2 for k,v in sla_map.items()}
        df["SLA_Horas_Resolucao"]         = df.get("Prioridade").map(sla_map)
        df["SLA_Horas_Primeira_Resposta"] = df.get("Prioridade").map(sla_resp_map)

        # Calcula tempos
        df["HorasResolucao_Calculated"] = (
            df["Resolvido"] - df["Criado"]
        ).dt.total_seconds() / 3600

        df["CumpriuSLA_Resolucao_Calculated"] = df.apply(
            lambda r: (
                "N/A" if pd.isna(r.SLA_Horas_Resolucao)
                else ("Pendente" if pd.isna(r.HorasResolucao_Calculated)
                      else ("Sim" if r.HorasResolucao_Calculated <= r.SLA_Horas_Resolucao else "Não"))
            ), axis=1
        )

        df["CumpriuSLA_PrimeiraResposta_Calculated"] = df.apply(
            lambda r: (
                "N/A" if pd.isna(r.SLA_Horas_Primeira_Resposta)
                else ("Pendente" if pd.isna(r.HorasPrimeiraResposta_Original)
                      else ("Sim" if r.HorasPrimeiraResposta_Original <= r.SLA_Horas_Primeira_Resposta else "Não"))
            ), axis=1
        )

        # Aging e Lead Time
        closed = ["Concluído","Resolvido","Fechado","Cancelado"]
        df["Is_Open"] = ~df.get("Status").isin(closed)
        now = datetime.now()
        df["Aging_Horas"] = np.where(
            df.Is_Open,
            (now - df.Criado).dt.total_seconds() / 3600,
            np.nan
        )
        df["LeadTime_Horas"] = np.where(
            ~df.Is_Open,
            (df.Resolvido - df.Criado).dt.total_seconds() / 3600,
            np.nan
        ).clip(lower=0)

        # Períodos
        df["Ano_Criacao"]       = df.Criado.dt.year
        df["Trimestre_Criacao"] = df.Criado.dt.quarter
        df["Mes_Ano_Criacao"]   = df.Criado.dt.strftime("%Y-%m")

        return df

    except Exception as e:
        print(f"Error processing files: {e}")
        return pd.DataFrame()
