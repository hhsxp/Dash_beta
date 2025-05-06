### app.py
```python
import base64
import io
import logging
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State

from supabase_client import supabase, log_event
from data_processor import process_uploaded_files

# Initialize Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
server = app.server

# Layout
app.layout = dbc.Container([
    html.H1("Dashboard Executivo – Monitoramento de Suporte", className="mt-4"),
    html.H5("Carregar Novos Dados", className="mb-2"),
    dcc.Upload(
        id="upload-piloto",
        children=html.Div(["Arraste ou selecione o arquivo Piloto (.xlsx)"]),
        style={"border": "1px dashed #ccc", "padding": "20px", "margin-bottom": "10px"},
        multiple=False,
    ),
    dcc.Upload(
        id="upload-sla",
        children=html.Div(["Arraste ou selecione o arquivo SLA (.xlsx)"]),
        style={"border": "1px dashed #ccc", "padding": "20px", "margin-bottom": "10px"},
        multiple=False,
    ),
    dbc.Button("Processar e Salvar no Banco", id="btn-process", color="primary"),
    html.Div(id="alert-container", className="mt-3"),
    # ... aqui vão dropdowns, gráficos, etc.
], fluid=True)

# Callback para upload e processamento
@app.callback(
    Output("alert-container", "children"),
    Input("btn-process", "n_clicks"),
    State("upload-piloto", "contents"),
    State("upload-sla", "contents"),
)
def handle_upload(n_clicks, piloto_content, sla_content):
    if not n_clicks:
        return dash.no_update

    alert = None
    try:
        log_event("info", f"Iniciando processamento de arquivos")
        # converte base64 para bytes
        piloto_bytes = base64.b64decode(piloto_content.split(",", 1)[1])
        sla_bytes = base64.b64decode(sla_content.split(",", 1)[1])
        # processa
        df = process_uploaded_files(piloto_bytes, sla_bytes)
        # insere no Supabase
        records = df.to_dict(orient="records")
        resp = supabase.table("tickets").insert(records).execute()
        if resp.get("error"):
            raise Exception(resp["error"]["message"] if isinstance(resp.get("error"), dict) else resp.get("error"))
        log_event("info", f"Inseridos {len(records)} tickets no banco")
        alert = dbc.Alert(f"Sucesso: {len(records)} tickets salvos.", color="success")
    except ValueError as e:
        log_event("error", str(e))
        alert = dbc.Alert(f"Erro: {str(e)}", color="danger")
    except Exception as ex:
        log_event("error", str(ex))
        alert = dbc.Alert(f"Erro inesperado: {str(ex)}", color="danger")
    return alert

if __name__ == "__main__":
    app.run_server(debug=True, port=8050)
```
