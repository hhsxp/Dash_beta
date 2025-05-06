import os
import logging
from flask import Flask, send_from_directory
import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
from supabase_client import init_supabase_client, fetch_all_tickets_data, log_event
from data_processor import process_uploaded_files
from datetime import datetime

# inicialização do Flask + Dash
server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.DARKLY])
init_supabase_client()
logging.basicConfig(level=logging.INFO)

# busca inicial dos tickets
raw_tickets = fetch_all_tickets_data()
log_event("info", "Fetch inicial de tickets", {"count": len(raw_tickets)})

# layout
app.layout = html.Div([
    html.H1("Dashboard Executivo – Monitoramento de Suporte"),
    dcc.Upload(id="upload-piloto", children=html.Div("Arraste ou selecione o arquivo Piloto (.xlsx)")),
    dcc.Upload(id="upload-sla", children=html.Div("Arraste ou selecione o arquivo SLA (.xlsx)")),
    dbc.Button("Processar e Salvar no Banco", id="btn-process", color="primary"),
    html.Div(id="alert-container"),
    # restante do layout...
])

# callback de upload
@app.callback(
    Output("alert-container", "children"),
    Input("btn-process", "n_clicks"),
    State("upload-piloto", "contents"),
    State("upload-sla", "contents"),
)
def handle_upload(n, piloto_contents, sla_contents):
    if not n:
        return dash.no_update
    try:
        log_event("info", "Início de processamento", {"piloto": piloto_contents and piloto_contents[:30], "sla": sla_contents and sla_contents[:30]})
        df = process_uploaded_files(piloto_contents, sla_contents)
        # inserir no Supabase
        payload = df.to_dict(orient="records")
        resp = supabase.table("tickets").insert(payload).execute()
        if 200 <= resp.status_code < 300:
            log_event("info", "Tickets salvos", {"count": len(payload)})
            return dbc.Alert("Dados processados e salvos com sucesso!", color="success")
        else:
            log_event("error", "Falha ao inserir tickets", {"status": resp.status_code, "error": resp.data})
            return dbc.Alert(f"Erro ao salvar no banco: {resp.status_code}", color="danger")
    except ValueError as ve:
        log_event("warning", f"Erro de validação: {ve}", {})
        return dbc.Alert(f"Erro: {ve}", color="warning")
    except Exception as e:
        log_event("error", f"Erro em processamento: {e}", {})
        return dbc.Alert("Falha no processamento. Veja os logs.", color="danger")

if __name__ == "__main__":
    app.run_server(debug=True, port=int(os.getenv("PORT", 10000)))
