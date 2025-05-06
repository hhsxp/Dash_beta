import pandas as pd
from io import BytesIO

def process_uploaded_files(piloto_contents: bytes | str, sla_contents: bytes | str) -> pd.DataFrame:
    """
    Recebe os conteúdos brutos (bytes ou string) dos dois arquivos .xlsx,
    faz a leitura, valida presença de coluna 'Chave', faz merge e devolve o DataFrame resultante.
    Lança ValueError com mensagem apropriada em caso de qualquer problema.
    """
    # Se recebeu string, converte para bytes (upload via Dash às vezes vem como str)
    piloto_bytes = piloto_contents.encode('latin1') if isinstance(piloto_contents, str) else piloto_contents
    sla_bytes    = sla_contents.encode('latin1')    if isinstance(sla_contents, str)    else sla_contents

    try:
        df_piloto = pd.read_excel(BytesIO(piloto_bytes))
        df_sla    = pd.read_excel(BytesIO(sla_bytes))
    except Exception as e:
        raise ValueError(f"Erro ao ler planilhas Excel: {e}")

    # Validação de coluna 'Chave'
    for df, nome in [(df_piloto, "Piloto"), (df_sla, "SLA")]:
        if "Chave" not in df.columns:
            raise ValueError("Coluna 'Chave' ausente em um dos arquivos")

    # Merge inner na coluna 'Chave'
    df_merged = pd.merge(df_piloto, df_sla, on="Chave", how="inner", suffixes=('_piloto', '_sla'))
    if df_merged.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique se há correspondências em 'Chave'.")

    # Aqui você pode adicionar normalizações, conversão de tipos, datas, etc.
    # Por exemplo:
    # df_merged["DataCriacao"] = pd.to_datetime(df_merged["DataCriacao"], dayfirst=True)

    return df_merged
