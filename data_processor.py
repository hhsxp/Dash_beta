# data_processor.py
import io
import pandas as pd

def process_uploaded_files(piloto_bytes: bytes, sla_bytes: bytes) -> pd.DataFrame:
    """
    Lê os dois arquivos Excel (piloto e SLA), valida a coluna 'Chave',
    faz o merge e retorna o DataFrame resultante.
    """
    # lê cada planilha
    df_piloto = pd.read_excel(io.BytesIO(piloto_bytes))
    df_sla    = pd.read_excel(io.BytesIO(sla_bytes))

    # strip nos nomes de coluna
    df_piloto.columns = df_piloto.columns.str.strip()
    df_sla.columns    = df_sla.columns.str.strip()

    # checa presença da coluna Chave
    missing = []
    if "Chave" not in df_piloto.columns:
        missing.append("Piloto")
    if "Chave" not in df_sla.columns:
        missing.append("SLA")
    if missing:
        raise ValueError(f"Coluna 'Chave' ausente em: {', '.join(missing)}")

    # faz merge inner
    df_merged = pd.merge(df_piloto, df_sla, on="Chave", how="inner")
    if df_merged.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique se 'Chave' coincide em ambos.")

    # Aqui você pode adicionar as transformações/calculações que precisar:
    # ex: converter datas, calcular tempos, flags de SLA etc.

    return df_merged
