# app.py
# -*- coding: utf-8 -*-
import os
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import pandas as pd

import supabase_client
from data_processor import process_uploaded_files

# Initialize Supabase & fetch initial
supabase_client.init_supabase_client()
df_initial = supabase_client.fetch_all_tickets_data()
if df_initial is None or df_initial.empty:
    print("Supabase fetch failed or returned empty. Dashboard will start empty.")

# Theme
plotly_template = "plotly_dark"
light_text = "#FFFFFF"

# Helper
def create_kpi_card(title, value, suffix, color="dark"):
    return dbc.Card(
        [dbc.CardHeader(title, className="kpi-title"),
         dbc.CardBody(html.P(value, id=f"kpi-{suffix}", className="kpi-value"))],
        color=color, inverse=True, className="kpi-card"
    )

# App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
server = app.server
supabase_client.log_event("info", "Dashboard application starting.")

# Layout
app.layout = dbc.Container([...])  # (mantém seu layout existente)

# Upload callback
@app.callback(
    [Output("data-update-trigger", "data"), Output("upload-status", "children")],
    [Input("process-button", "n_clicks")],
    [State("upload-piloto", "contents"), State("upload-piloto", "filename"),
     State("upload-sla",    "contents"), State("upload-sla",    "filename")],
    prevent_initial_call=True
)
def process_and_upsert_data(nc, piloto_c, piloto_fn, sla_c, sla_fn):
    if piloto_c and sla_c:
        detail = {"piloto": piloto_fn, "sla": sla_fn}
        supabase_client.log_event("info", "Processing files start.", detail)
        try:
            df = process_uploaded_files(piloto_c, sla_c)

            if df.empty:
                raise ValueError(
                    "Nenhum ticket encontrado após o merge. "
                    "Verifique se ambos os arquivos possuem a coluna 'Chave' "
                    "e se os valores coincidem."
                )

            ok = supabase_client.upsert_tickets_data(df)
            if not ok:
                raise RuntimeError("Falha ao salvar no Supabase.")

            msg = f'Arquivos "{piloto_fn}" e "{sla_fn}" processados! ({len(df)} linhas)'
            supabase_client.log_event("info", "Process and upsert successful.", detail)
            return {"timestamp": datetime.now().isoformat()}, dbc.Alert(msg, color="success")

        except Exception as e:
            supabase_client.log_event("error", f"Error in processing: {e}", detail)
            return dash.no_update, dbc.Alert(f"Erro: {e}", color="danger")

    return dash.no_update, "Carregue ambos os arquivos e clique em processar."

# ... demais callbacks (mantidos conforme seu fluxo) ...

# Run
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8050)), debug=False)
