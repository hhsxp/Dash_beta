# -*- coding: utf-8 -*-
import io
import pandas as pd
import numpy as np


def process_uploaded_files(piloto_bytes: bytes, sla_bytes: bytes) -> pd.DataFrame:
    """
    Recebe os dois arquivos XLSX em bytes:
      - piloto_bytes: dados originais (tickets)
      - sla_bytes: parâmetros de SLA (prioridades)
    Retorna DataFrame já preparado para inserção no Supabase.
    """
    # Lê ambos os arquivos
    df_piloto = pd.read_excel(io.BytesIO(piloto_bytes))
    df_sla = pd.read_excel(io.BytesIO(sla_bytes))

    # Verifica coluna-chave
    chave = "Chave"
    if chave not in df_piloto.columns or chave not in df_sla.columns:
        raise ValueError(f"Coluna '{chave}' ausente em um dos arquivos")

    # Merge pelos tickets
    df = df_piloto.merge(df_sla, on=chave, how="inner")
    if df.empty:
        raise ValueError("Nenhum ticket encontrado após o merge. Verifique se a coluna 'Chave' coincide.")

    # Exemplo de mapeamento de SLA (substitua pelos seus)
    sla_map_res = {
        "Baixa": 72,
        "Média": 24,
        "Alta": 8,
        "Crítica": 4,
    }
    sla_map_resp = {
        "Baixa": 24,
        "Média": 8,
        "Alta": 2,
        "Crítica": 1,
    }

    # Adiciona colunas de SLA em horas
    df["SLA_Horas_Resolucao"] = df["Prioridade"].map(sla_map_res)
    df["SLA_Horas_Primeira_Resposta"] = df["Prioridade"].map(sla_map_resp)

    # Calcula tempos em horas
    df["HorasResolucao_Calculated"] = (
        pd.to_datetime(df["Data_Fecha"]) - pd.to_datetime(df["Data_Cria"])
    ).dt.total_seconds() / 3600
    df["HorasPrimeiraResposta_Original"] = (
        pd.to_datetime(df["Data_Primeira_Resp"]) - pd.to_datetime(df["Data_Cria"])
    ).dt.total_seconds() / 3600

    # Aging: se ainda aberto, tempo até agora
    agora = pd.Timestamp.now()
    df["Aging_Horas"] = np.where(
        df["Data_Fecha"].isna(),
        (agora - pd.to_datetime(df["Data_Cria"])).dt.total_seconds() / 3600,
        df["HorasResolucao_Calculated"]
    )

    # Função auxiliar para status de risco
    def check_risk(row):
        # Se não tiver SLA definido ou já fechado
        if (
            (not row.get("Is_Open", True))
            or pd.isna(row["SLA_Horas_Resolucao"])
            or row["SLA_Horas_Resolucao"] <= 0
            or pd.isna(row["Aging_Horas"])
        ):
            return "N/A"
        # Em risco >80% do SLA de resolução
        if row["Aging_Horas"] > 0.8 * row["SLA_Horas_Resolucao"]:
            return "Atuação necessária"
        return "OK"

    df["Status_Risco"] = df.apply(check_risk, axis=1)

    # Outros cálculos e normalizações por coluna...
    # Exemplo: cria coluna de período
    df["Periodo_Cria"] = pd.to_datetime(df["Data_Cria"]).dt.to_period("M")

    # Seleciona e ordena colunas finais
    col_final = [
        "Chave", "Projeto", "Unidade_de_Negocio", "Prioridade",
        "SLA_Horas_Resolucao", "SLA_Horas_Primeira_Resposta",
        "HorasResolucao_Calculated", "HorasPrimeiraResposta_Original",
        "Aging_Horas", "Status_Risco", "Periodo_Cria"
    ]
    return df[col_final]


# Para testes independentes
if __name__ == "__main__":
    # exemplo de leitura local
    with open("Piloto.xlsx", "rb") as f1, open("SLA.xlsx", "rb") as f2:
        df_proc = process_uploaded_files(f1.read(), f2.read())
        print(df_proc.head())
