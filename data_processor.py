# data_processor.py

import pandas as pd
import base64
import io

def _parse_uploaded_excel(content: str) -> pd.DataFrame:
    """
    Recebe o content vindo do dcc.Upload (string 'data:...;base64,<dados>'),
    decodifica e retorna um DataFrame.
    """
    if not content:
        return pd.DataFrame()
    # separa header de dados
    try:
        header, b64 = content.split(",", 1)
    except ValueError:
        raise ValueError("Conteúdo inválido: não foi possível encontrar a parte base64.")
    # decodifica e lê
    decoded = base64.b64decode(b64)
    return pd.read_excel(io.BytesIO(decoded), engine="openpyxl")

def process_uploaded_files(
    piloto_content: str | None,
    sla_content: str | None
) -> pd.DataFrame:
    """
    Processa as duas planilhas (Piloto e SLA) e retorna o DataFrame final.
    Erra se faltar coluna 'Chave' ou se o merge resultar em DataFrame vazio.
    """
    # carrega cada planilha
    df_piloto = _parse_uploaded_excel(piloto_content or "")
    df_sla = _parse_uploaded_excel(sla_content or "")

    # verifica existência da coluna chave
    if "Chave" not in df_piloto.columns or "Chave" not in df_sla.columns:
        raise ValueError("Coluna 'Chave' ausente em um dos arquivos")

    # faz merge inner
    df = pd.merge(df_piloto, df_sla, on="Chave", how="inner")

    if df.empty:
        raise ValueError(
            "Nenhum ticket encontrado após o merge. "
            "Verifique se a coluna 'Chave' existe e coincide em ambos arquivos."
        )

    # aqui você pode aplicar as transformações adicionais
    # ex: renomear colunas, converter tipos, preencher valores default...

    return df
