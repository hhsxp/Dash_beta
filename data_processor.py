import pandas as pd
import io
import base64


def process_uploaded_files(piloto_content: str, sla_content: str) -> pd.DataFrame:
    """
    Receives the contents of the uploaded 'piloto' and 'sla' files (as base64 strings), decodes and reads them,
    validates presence of the 'Chave' column, merges them, and returns a processed DataFrame.
    """
    # Decodifica o conteúdo base64 e lê em DataFrame
    try:
        piloto_header, piloto_b64 = piloto_content.split(",", 1)
        sla_header, sla_b64 = sla_content.split(",", 1)
    except ValueError:
        raise ValueError("Conteúdo de upload inválido: não contém cabeçalho base64 esperado.")

    piloto_bytes = base64.b64decode(piloto_b64)
    sla_bytes = base64.b64decode(sla_b64)

    # Leitura dos arquivos Excel
    df_piloto = pd.read_excel(io.BytesIO(piloto_bytes))
    df_sla = pd.read_excel(io.BytesIO(sla_bytes))

    # Limpa espaços em branco nos nomes das colunas
    df_piloto.columns = df_piloto.columns.str.strip()
    df_sla.columns = df_sla.columns.str.strip()

    # Confirma se a coluna 'Chave' existe em ambos
    missing_p = 'Chave' not in df_piloto.columns
    missing_s = 'Chave' not in df_sla.columns
    if missing_p or missing_s:
        cols_p = list(df_piloto.columns)
        cols_s = list(df_sla.columns)
        msg_parts = []
        if missing_p:
            msg_parts.append(f"Arquivo Piloto colunas={cols_p}")
        if missing_s:
            msg_parts.append(f"Arquivo SLA colunas={cols_s}")
        raise ValueError("Coluna 'Chave' ausente: " + "; ".join(msg_parts))

    # Faz o merge
    df_merged = pd.merge(df_piloto, df_sla, on='Chave', how='inner')
    if df_merged.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique se a coluna 'Chave' existe e coincide em ambos arquivos.")

    # Aqui você pode adicionar transformações adicionais, por exemplo:
    # - Conversão de datas: df_merged['Data'] = pd.to_datetime(df_merged['Data'], dayfirst=True)
    # - Cálculo de métricas

    return df_merged


# Exemplo de teste local (remover antes do deploy):
# if __name__ == '__main__':
#     with open('Piloto.xlsx', 'rb') as f:
#         piloto = base64.b64encode(f.read()).decode()
#     with open('SLA.xlsx', 'rb') as f:
#         sla = base64.b64encode(f.read()).decode()
#     df = process_uploaded_files(f"data:;base64,{piloto}", f"data:;base64,{sla}")
#     print(df.head())