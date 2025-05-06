# data_processor.py
# -*- coding: utf-8 -*-
import base64
import io
import pandas as pd
import numpy as np
from datetime import datetime

def process_uploaded_files(piloto_contents: str, sla_contents: str) -> pd.DataFrame:
    """
    Recebe os conteúdos base64 de Piloto e SLA vindos do dcc.Upload,
    decodifica, processa e retorna DataFrame final com todas as métricas.
    """
    def _decode(contents: str) -> bytes:
        try:
            header, b64 = contents.split(',', 1)
        except ValueError:
            raise ValueError("Formato inválido (esperado data:...;base64,...)")
        return base64.b64decode(b64)

    piloto_bytes = _decode(piloto_contents)
    sla_bytes    = _decode(sla_contents)

    # Leitura
    df_piloto = pd.read_excel(io.BytesIO(piloto_bytes))
    df_sla    = pd.read_excel(io.BytesIO(sla_bytes))

    # Verifica coluna Chave
    if 'Chave' not in df_piloto.columns or 'Chave' not in df_sla.columns:
        raise KeyError("Coluna 'Chave' ausente em um dos arquivos")

    # Merge
    df = pd.merge(df_piloto, df_sla, on='Chave', how='inner', suffixes=('_Piloto','_SLA'))
    if df.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique a coluna 'Chave'.")

    # Converter datas
    for col in ['Criado','Resolvido_Piloto','Prazo_Resposta_SLA','Prazo_Resolucao_SLA']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Cálculos de SLA
    # 1ª Resposta
    df['CumpriuSLA_PrimeiraResposta'] = np.where(
        df['Resposta_Piloto'] <= df['Prazo_Resposta_SLA'], 'Sim','Não'
    )
    # Resolução
    df['CumpriuSLA_Resolucao'] = np.where(
        df['Resolvido_Piloto'] <= df['Prazo_Resolucao_SLA'], 'Sim','Não'
    )

    # Flag de aberto
    closed = ["Concluído","Resolvido","Fechado","Cancelado"]
    df['Is_Open'] = ~df['Status'].isin(closed)

    # Aging (horas)
    now = datetime.now()
    df['Aging_Horas'] = df['Criado'].apply(
        lambda x: (now - x).total_seconds()/3600 if pd.notna(x) else np.nan
    )

    # Em risco
    df['Em_Risco'] = df.apply(lambda r: (
        'Sim' if r['Is_Open'] and
        pd.notna(r['Prazo_Resolucao_SLA']) and
        r['Aging_Horas'] > 0.8 * (r['Prazo_Resolucao_SLA'] - r['Criado']).total_seconds()/3600
        else 'Não'
    ), axis=1)

    # Categoria de Status
    def _cat_status(s):
        if s in closed: return "Fechado"
        if s in ["Aguardando Validação","Pendente"]: return "Aguardando/Validação"
        return "Em Progresso"
    df['Status_Categoria'] = df['Status'].map(_cat_status)

    # Lead Time (horas) – só tickets fechados
    df['LeadTime_Horas'] = df.apply(
        lambda r: (r['Resolvido_Piloto'] - r['Criado']).total_seconds()/3600
        if (r['Resolvedo_Piloto'] >= r['Criado']) else 0, axis=1
    )

    return df
