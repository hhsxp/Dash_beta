# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import base64

def parse_excel_content(content_string):
    """Parses the base64 encoded content string of an Excel file."""
    content_type, content_data = content_string.split(",")
    decoded = base64.b64decode(content_data)
    return io.BytesIO(decoded)

def process_uploaded_files(piloto_content_str, sla_content_str):
    """Processes uploaded Excel file contents (base64 strings) and returns a DataFrame."""
    try:
        piloto_io = parse_excel_content(piloto_content_str)
        sla_io = parse_excel_content(sla_content_str)

        # Load data from streams
        df_piloto = pd.read_excel(piloto_io, sheet_name=
"Worksheet"
, header=1) # Assuming header is on row 2 (index 1)
        df_sla = pd.read_excel(sla_io, sheet_name=
"Tickets"
) # Assuming default sheet name

        print(f"Loaded Piloto: {df_piloto.shape[0]} rows")
        print(f"Loaded SLA: {df_sla.shape[0]} rows")

        # --- Data Cleaning and Merging (adapted from previous process_data.py) ---
        # Rename columns for consistency
        df_piloto.rename(columns={
            "Unidade de Negócio.1": "Unidade de Negócio",
            "Resolvido": "Resolvido_Piloto"
        }, inplace=True)

        # Select and rename columns from df_sla
        df_sla_selected = df_sla[[
            "Key",
            "Tempo até a primeira resposta",
            "Tempo de resolução",
            "SLA",
            "Primeira Resposta"
        ]].copy()
        df_sla_selected.rename(columns={
            "Key": "Chave",
            "Tempo até a primeira resposta": "HorasPrimeiraResposta_Original",
            "Tempo de resolução": "HorasResolucao_Original",
            "SLA": "CumpriuSLA_Resolucao_Original",
            "Primeira Resposta": "CumpriuSLA_PrimeiraResposta_Original"
        }, inplace=True)

        # Merge dataframes
        df_merged = pd.merge(df_piloto, df_sla_selected, on="Chave", how="left")
        print(f"Merged data: {df_merged.shape[0]} rows")

        # Convert date columns
        date_cols = ["Criado", "Resolvido_Piloto", "Atualizado(a)"]
        for col in date_cols:
            df_merged[col] = pd.to_datetime(df_merged[col], errors=
"coerce"
)

        # --- Feature Engineering & SLA Calculation (adapted from previous process_data.py) ---
        # Define SLA hours
        sla_map_res = {
            "Highest": 4, "High": 6, "Medium": 16, "Low": 24, "Lowest": 40
        }
        sla_map_resp = {k: v / 2 for k, v in sla_map_res.items()}

        df_merged["SLA_Horas_Resolucao"] = df_merged["Prioridade"].map(sla_map_res)
        df_merged["SLA_Horas_Primeira_Resposta"] = df_merged["Prioridade"].map(sla_map_resp)

        # Calculate Resolution Time
        df_merged["HorasResolucao_Calculated"] = (df_merged["Resolvido_Piloto"] - df_merged["Criado"]).dt.total_seconds() / 3600

        # Calculate Resolution SLA Status
        def check_sla_res(row):
            if pd.isna(row["SLA_Horas_Resolucao"]):
                return "N/A"
            if pd.isna(row["HorasResolucao_Calculated"]):
                # Check if already past SLA based on current time if open
                if pd.isna(row["Resolvido_Piloto"]):
                    aging = (datetime.now(row["Criado"].tz) - row["Criado"]).total_seconds() / 3600 if row["Criado"].tz else (datetime.now() - row["Criado"]).total_seconds() / 3600
                    if aging > row["SLA_Horas_Resolucao"]:
                        return "Não" # Already violated even if open
                    else:
                        return "Pendente"
                else: # Resolved date missing, but SLA applies
                    return "Pendente"
            if row["HorasResolucao_Calculated"] < 0: # Resolvido before Criado
                 return "Não"
            if row["HorasResolucao_Calculated"] <= row["SLA_Horas_Resolucao"]:
                return "Sim"
            else:
                return "Não"

        df_merged["CumpriuSLA_Resolucao_Calculated"] = df_merged.apply(check_sla_res, axis=1)

        # Calculate First Response SLA Status (Handle negative original values)
        def check_sla_resp(row):
            if pd.isna(row["SLA_Horas_Primeira_Resposta"]):
                return "N/A"
            if pd.isna(row["HorasPrimeiraResposta_Original"]):
                return "Pendente"
            # Treat negative original values as violations
            if row["HorasPrimeiraResposta_Original"] < 0:
                return "Não"
            if row["HorasPrimeiraResposta_Original"] <= row["SLA_Horas_Primeira_Resposta"]:
                return "Sim"
            else:
                return "Não"

        df_merged["CumpriuSLA_PrimeiraResposta_Calculated"] = df_merged.apply(check_sla_resp, axis=1)

        # Violated Status
        def check_violated(row):
            if row["CumpriuSLA_Resolucao_Calculated"] == "Não":
                return "Sim"
            elif row["CumpriuSLA_Resolucao_Calculated"] == "Sim":
                return "Não"
            else: # Pendente or N/A
                return "N/A"
        df_merged["SLA_Violado_Calculated"] = df_merged.apply(check_violated, axis=1)

        # Is Open?
        closed_statuses = ["Concluído", "Resolvido", "Fechado", "Cancelado"]
        df_merged["Is_Open"] = ~df_merged["Status"].isin(closed_statuses)

        # Aging (for open tickets)
        now = datetime.now()
        df_merged["Aging_Horas"] = np.nan
        open_mask = df_merged["Is_Open"] == True
        # Ensure 'Criado' has timezone info consistent with 'now' or make 'now' naive
        # Assuming 'Criado' is naive for simplicity based on previous runs
        df_merged.loc[open_mask, "Aging_Horas"] = (now - df_merged.loc[open_mask, "Criado"]).dt.total_seconds() / 3600

        # Em Risco (Open, SLA Applicable, Aging > 80% of SLA)
        def check_risk(row):
            if not row["Is_Open"] or pd.isna(row["SLA_Horas_Resolucao"]) or row["SLA_Horas_Resolucao"] <= 0 or pd.isna(row["Aging_Horas"]):
                return "N/A"
            if row["Aging_Horas"] > (0.8 * row["SLA_Horas_Resolucao"]):
                # Also check if not already violated
                if row["CumpriuSLA_Resolucao_Calculated"] != "Não":
                     return "Sim"
                else:
                     return "Não" # Already violated, not just at risk
            else:
                return "Não"
        df_merged["Em_Risco"] = df_merged.apply(check_risk, axis=1)

        # Status Category
        def categorize_status(status):
            if status in closed_statuses:
                return "Fechado"
            elif status in ["Aguardando Validação", "Aguardando Aprovação", "Pendente"]: # Adjust as needed
                return "Aguardando/Validação"
            else:
                return "Em Progresso"
        df_merged["Status_Categoria"] = df_merged["Status"].apply(categorize_status)

        # Lead Time (only for closed tickets)
        df_merged["LeadTime_Horas"] = np.nan
        closed_mask = df_merged["Is_Open"] == False
        df_merged.loc[closed_mask, "LeadTime_Horas"] = (df_merged.loc[closed_mask, "Resolvido_Piloto"] - df_merged.loc[closed_mask, "Criado"]).dt.total_seconds() / 3600
        # Handle negative lead times if Resolvido < Criado
        df_merged.loc[df_merged["LeadTime_Horas"] < 0, "LeadTime_Horas"] = 0

        # Add Time Period Columns for Filtering
        df_merged["Mes_Criacao_Num"] = df_merged["Criado"].dt.month
        df_merged["Ano_Criacao"] = df_merged["Criado"].dt.year
        df_merged["Trimestre_Criacao"] = df_merged["Criado"].dt.quarter
        df_merged["Mes_Ano_Criacao"] = df_merged["Criado"].dt.strftime("%Y-%m")

        print("Data processing complete.")
        return df_merged

    except Exception as e:
        print(f"Error processing files: {e}")
        # Return an empty dataframe or raise the exception
        # For the app, returning empty df might be better to avoid crashing
        return pd.DataFrame() # Return empty DataFrame on error

# Example usage if run directly (for testing)
if __name__ == "__main__":
    # This part requires you to have the files locally to test
    # Replace with actual paths if needed for local testing
    piloto_path = "/home/ubuntu/upload/Piloto.xlsx"
    sla_path = "/home/ubuntu/upload/SLA.xlsx"

    try:
        with open(piloto_path, "rb") as pf, open(sla_path, "rb") as sf:
            piloto_content = base64.b64encode(pf.read()).decode("utf-8")
            sla_content = base64.b64encode(sf.read()).decode("utf-8")

            piloto_content_str = f"data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{piloto_content}"
            sla_content_str = f"data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{sla_content}"

            processed_df = process_uploaded_files(piloto_content_str, sla_content_str)

            if not processed_df.empty:
                print("\nProcessed DataFrame head:")
                print(processed_df.head())
                print("\nProcessed DataFrame info:")
                processed_df.info()
                # Save locally for inspection
                processed_df.to_csv("/home/ubuntu/test_processed_data.csv", index=False)
                print("\nTest processed data saved to /home/ubuntu/test_processed_data.csv")
            else:
                print("\nProcessing failed, returned empty DataFrame.")

    except FileNotFoundError:
        print("\nSkipping direct run example: Uploaded files not found at expected paths.")
    except Exception as e:
        print(f"\nError in direct run example: {e}")

