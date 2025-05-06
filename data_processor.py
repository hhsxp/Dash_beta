### data_processor.py
```python
import pandas as pd
import io


def process_uploaded_files(piloto_bytes: bytes, sla_bytes: bytes) -> pd.DataFrame:
    """
    Lê dois arquivos Excel em bytes, faz merge pela coluna 'Chave' e retorna DataFrame processado.
    Raise ValueError se faltar a coluna ou merge resultar vazio.
    """
    # lê planilhas
    df_piloto = pd.read_excel(io.BytesIO(piloto_bytes), engine='openpyxl')
    df_sla = pd.read_excel(io.BytesIO(sla_bytes), engine='openpyxl')

    # verifica coluna-chave
    if 'Chave' not in df_piloto.columns or 'Chave' not in df_sla.columns:
        raise ValueError("Coluna 'Chave' ausente em um dos arquivos")

    # merge
    merged = pd.merge(df_piloto, df_sla, on='Chave', how='inner')
    if merged.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique correspondência de 'Chave'.")

    # exemplo de processamento: renomear, filtrar, converter datas
    # merged['Criacao'] = pd.to_datetime(merged['Criacao'], dayfirst=True)
    # merged['SLA'] = merged['Tempo_SLA'].astype(float)

    # retorna apenas colunas necessárias
    # cols_needed = ['Chave', 'Projeto', 'Unidade de Negocio', 'Status', 'SLA', 'Criacao']
    # return merged[cols_needed]
    return merged
