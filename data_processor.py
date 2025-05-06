## data_processor.py
```python
import pandas as pd
import io
import base64

def process_uploaded_files(piloto_contents: str, sla_contents: str) -> pd.DataFrame:
    def _decode_and_read(contents: str) -> pd.DataFrame:
        header, b64 = contents.split(",", 1)
        decoded = io.BytesIO(base64.b64decode(b64))
        df = pd.read_excel(decoded)
        # Normaliza nomes de colunas
        df.columns = df.columns.str.strip()
        return df

    df_piloto = _decode_and_read(piloto_contents)
    df_sla = _decode_and_read(sla_contents)

    # Confere chave
    cols_piloto = set(df_piloto.columns)
    cols_sla = set(df_sla.columns)
    if "Chave" not in cols_piloto or "Chave" not in cols_sla:
        raise ValueError(
            f"Coluna 'Chave' ausente. Piloto: {sorted(cols_piloto)}, SLA: {sorted(cols_sla)}"
        )

    df = pd.merge(df_piloto, df_sla, on="Chave", how="inner", suffixes=("_piloto", "_sla"))
    if df.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique a coluna 'Chave'.")

    # Exemplo de cálculo
    if "Data_Criacao" in df.columns and "Data_Resposta" in df.columns:
        df["Tempo_Resposta_h"] = (
            df["Data_Resposta"] - df["Data_Criacao"]
        ).dt.total_seconds() / 3600
    return df
