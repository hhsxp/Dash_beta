# app.py
# -*- coding: utf-8 -*-

import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import base64

import supabase_client
from data_processor import process_uploaded_files

# Inicializa o app Dash
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)
server = app.server  # para o Render ou Heroku

# --- Layout simplificado (ajuste IDs conforme seu código) ---
app.layout = dbc.Container(fluid=True, children=[
    html.H1("Dashboard Executivo – Monitoramento de Suporte", className="mt-4"),
    html.Hr(),
    dbc.Row([
        dbc.Col([
            dcc.Upload(
                id="upload-piloto",
                children=html.Div("Arraste ou selecione o Arquivo Piloto (.xlsx)"),
                style={
                    "width": "100%", "height": "60px",
                    "lineHeight": "60px", "borderWidth": "1px",
                    "borderStyle": "dashed", "borderRadius": "5px",
                    "textAlign": "center", "marginBottom": "10px"
                },
                multiple=False
            ),
            dcc.Upload(
                id="upload-sla",
                children=html.Div("Arraste ou selecione o Arquivo SLA (.xlsx)"),
                style={
                    "width": "100%", "height": "60px",
                    "lineHeight": "60px", "borderWidth": "1px",
                    "borderStyle": "dashed", "borderRadius": "5px",
                    "textAlign": "center"
                },
                multiple=False
            ),
            dbc.Button(
                "Processar e Salvar no Banco",
                id="process-button",
                color="primary",
                className="mt-3"
            ),
            html.Div(id="upload-status", className="mt-3")
        ], width=12)
    ]),
    # ... o restante do seu dashboard (filtros, gráficos, tabelas etc.) ...
])

# --- Callback de processamento e inserção ---
@app.callback(
    Output("upload-status", "children"),
    Input("process-button", "n_clicks"),
    State("upload-piloto", "contents"),
    State("upload-sla", "contents"),
    prevent_initial_call=True
)
def handle_upload(n_clicks, piloto_contents, sla_contents):
    # Verifica se os dois arquivos foram enviados
    if not piloto_contents or not sla_contents:
        return dbc.Alert("Por favor, envie ambos os arquivos.", color="warning")

    # Log de início
    supabase_client.log_event(
        "info",
        "Iniciando processamento de arquivos",
        {"piloto": piloto_contents[:30], "sla": sla_contents[:30]}
    )

    # Processa em DataFrame
    df = process_uploaded_files(piloto_contents, sla_contents)

    if df.empty:
        err = (
            "Erro: nenhum ticket encontrado após o merge. "
            "Verifique se a coluna 'Chave' existe e coincide em ambos os arquivos."
        )
        supabase_client.log_event("error", err)
        return dbc.Alert(err, color="danger")

    # Converte para lista de dicts e insere no Supabase
    records = df.to_dict(orient="records")
    try:
        inserted = supabase_client.insert_tickets(records)
        msg = f"{len(inserted)} tickets inseridos com sucesso!"
        supabase_client.log_event("info", msg)
        return dbc.Alert(msg, color="success")
    except Exception as e:
        err = f"Falha ao salvar no banco: {e}"
        supabase_client.log_event("error", err, {"trace": str(e)})
        return dbc.Alert(err, color="danger")


if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
