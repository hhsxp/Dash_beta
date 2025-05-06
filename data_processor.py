# data_processor.py
# -*- coding: utf-8 -*-

import pandas as pd
import io

def process_uploaded_files(piloto_contents: str, sla_contents: str) -> pd.DataFrame:
    """
    recebe as strings base64 de dois uploads (.contents do dcc.Upload),
    decodifica, lê como Excel, faz merge e retorna DataFrame tratado.
    """
    # piloto_contents é algo como "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,AAA..."
    def _decode_and_read(contents: str) -> pd.DataFrame:
        header, b64 = contents.split(",", 1)
        decoded = io.BytesIO(base64.b64decode(b64))
        return pd.read_excel(decoded)

    import base64

    # Leitura
    df_piloto = _decode_and_read(piloto_contents)
    df_sla = _decode_and_read(sla_contents)

    # Valida coluna 'Chave'
    if "Chave" not in df_piloto.columns or "Chave" not in df_sla.columns:
        raise ValueError("Coluna 'Chave' ausente em um dos arquivos")

    # Merge
    df = pd.merge(df_piloto, df_sla, on="Chave", how="inner", suffixes=("_piloto", "_sla"))
    if df.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique a coluna 'Chave'")

    # Exemplo de cálculos (ajuste conforme necessidade real)
    df["Tempo_Resposta"] = (df["Data_Resposta"] - df["Data_Criacao"]).dt.total_seconds() / 3600

    # Outros tratamentos: filtragem de colunas, renomear, converter tipos, etc.
    # ...
    return df
