import os
import logging
import pandas as pd
import base64
import io

import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

from supabase_client import init_supabase_client, fetch_all_tickets_data, log_event, supabase
from data_processor import process_uploaded_files

# --- Setup logging
logging.basicConfig(level=logging.INFO)

# --- Initialize Supabase
init_supabase_client()

# --- Create Dash app
external_stylesheets = [dbc.themes.DARKLY]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

# --- Fetch initial data from Supabase
raw_tickets = fetch_all_tickets_data()
df_initial = pd.DataFrame(raw_tickets)
if df_initial.empty:
    logging.info("Nenhum ticket encontrado no Supabase ou tabela vazia.")

# --- App layout
app.layout = dbc.Container([
    html.H1("Dashboard Executivo – Monitoramento de Suporte", className="mt-4 text-light"),
    html.H5("Carregar Novos Dados", className="text-light"),
    dcc.Upload(
        id="upload-piloto",
        children=html.Div(["Arraste ou selecione o arquivo Piloto (.xlsx)"]),
        style={"width": "100%", "height": "60px", "lineHeight": "60px",
               "borderWidth": "1px", "borderStyle": "dashed", "borderRadius": "5px",
               "textAlign": "center", "marginBottom": "10px"},
        multiple=False
    ),
    dcc.Upload(
        id="upload-sla",
        children=html.Div(["Arraste ou selecione o arquivo SLA (.xlsx)"]),
        style={"width": "100%", "height": "60px", "lineHeight": "60px",
               "borderWidth": "1px", "borderStyle": "dashed", "borderRadius": "5px",
               "textAlign": "center", "marginBottom": "10px"},
        multiple=False
    ),
    dbc.Button("Processar e Salvar no Banco", id="process-button", color="primary", className="mb-2"),
    dbc.Alert(id="upload-alert", is_open=False, color="danger"),
    # Aqui você adiciona o restante do seu layout (dropdowns, cards, gráficos...)
], fluid=True, className="bg-dark vh-100")

# --- Callback: tratar upload e salvar em Supabase
@app.callback(
    Output("upload-alert", "children"),
    Output("upload-alert", "is_open"),
    Input("process-button", "n_clicks"),
    State("upload-piloto", "contents"),
    State("upload-sla", "contents"),
    State("upload-piloto", "filename"),
    State("upload-sla", "filename")
)
def handle_upload(n_clicks, piloto_content, sla_content, piloto_name, sla_name):
    if not n_clicks:
        return "", False

    details = {"piloto": piloto_name, "sla": sla_name}
    log_event("info", "Processing files start", details)

    try:
        df = process_uploaded_files(piloto_content, sla_content)
        # Converte DataFrame para lista de dicts e insere na tabela 'tickets'
        records = df.to_dict(orient="records")
        supabase.table("tickets").insert(records).execute()
        log_event("info", "Files processed and saved", {"records": len(records)})
        return "", False

    except Exception as e:
        msg = str(e)
        log_event("error", f"Error in processing: {msg}", details)
        return f"Erro: {msg}", True

# --- Main
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8050)))
