import base64
import io
import pandas as pd
from typing import Tuple

def process_uploaded_files(piloto_b64: str, sla_b64: str) -> pd.DataFrame:
    """
    Recebe conteúdos Base64 dos dois arquivos,
    retorna DataFrame unificado e tratado.
    Lança ValueError em caso de colunas faltantes ou merge vazio.
    """
    def _decode_and_read(b64: str) -> pd.DataFrame:
        header, encoded = b64.split(",", 1)
        raw = base64.b64decode(encoded)
        return pd.read_excel(io.BytesIO(raw), engine="openpyxl")

    df_piloto = _decode_and_read(piloto_b64)
    df_sla = _decode_and_read(sla_b64)

    # checagem de coluna chave
    if "Chave" not in df_piloto.columns or "Chave" not in df_sla.columns:
        raise ValueError("Coluna 'Chave' ausente em um dos arquivos")

    # merge e validação
    df = df_piloto.merge(df_sla, on="Chave", how="inner", suffixes=("_piloto","_sla"))
    if df.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique coluna 'Chave'")

    # aqui você adiciona todos os tratamentos de data, numéricos, categorizações etc.
    # Exemplo:
    # df["LeadTimeHoras"] = (df["Data_Resposta"] - df["Data_Criacao"]).dt.total_seconds() / 3600

    return df
