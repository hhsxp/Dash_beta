# data_processor.py
import io
import base64
import pandas as pd


def process_uploaded_files(piloto_bytes: bytes, sla_bytes: bytes) -> pd.DataFrame:
    """
    Recebe os bytes decodificados dos arquivos XLSX de Piloto e SLA,
    verifica a existência da coluna 'Chave', faz merge e retorna
    o DataFrame combinado.
    """
    # Leitura dos arquivos Excel
    df_piloto = pd.read_excel(io.BytesIO(piloto_bytes))
    df_sla = pd.read_excel(io.BytesIO(sla_bytes))

    # Verifica presença da coluna 'Chave'
    missing = []
    for name, df in [("Piloto", df_piloto), ("SLA", df_sla)]:
        if "Chave" not in df.columns:
            missing.append(name)
    if missing:
        raise ValueError(f"Coluna 'Chave' ausente em: {', '.join(missing)}")

    # Merge interno pela chave
    df_merged = pd.merge(df_piloto, df_sla, on="Chave", how="inner")
    if df_merged.empty:
        raise ValueError(
            "Nenhum ticket encontrado após o merge. "
            "Verifique se a coluna 'Chave' coincide em ambos os arquivos."
        )

    # (Opcional) Tratamentos adicionais podem ser aplicados aqui

    return df_merged
