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
server = app.server  # para deploy

# Layout do app
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
    # ... restante do dashboard
])

# Callback para processar e inserir no Supabase
@app.callback(
    Output("upload-status", "children"),
    Input("process-button", "n_clicks"),
    State("upload-piloto", "contents"),
    State("upload-sla", "contents"),
    prevent_initial_call=True
)
def handle_upload(n_clicks, piloto_contents, sla_contents):
    # Verifica envio
    if not piloto_contents or not sla_contents:
        return dbc.Alert("Por favor, envie ambos os arquivos.", color="warning")

    # Decodifica base64
    try:
        _, piloto_b64 = piloto_contents.split(',', 1)
        piloto_bytes = base64.b64decode(piloto_b64)
        _, sla_b64 = sla_contents.split(',', 1)
        sla_bytes = base64.b64decode(sla_b64)
    except Exception:
        msg = "Falha ao decodificar arquivos. Verifique o formato enviado."
        supabase_client.log_event("error", msg)
        return dbc.Alert(msg, color="danger")

    supabase_client.log_event(
        "info",
        "Iniciando processamento de arquivos",
        {"piloto_len": len(piloto_bytes), "sla_len": len(sla_bytes)}
    )

    # Processa
    try:
        df = process_uploaded_files(piloto_bytes, sla_bytes)
    except Exception as e:
        err = str(e)
        supabase_client.log_event("error", f"Erro no processamento: {err}")
        return dbc.Alert(f"Erro: {err}", color="danger")

    if df.empty:
        msg = (
            "Nenhum ticket encontrado após o merge. "
            "Verifique se a coluna 'Chave' existe e coincide."
        )
        supabase_client.log_event("warning", msg)
        return dbc.Alert(msg, color="danger")

    # Insere no Supabase
    try:
        inserted = supabase_client._client.table(
            "tickets"
        ).insert(
            df.to_dict(orient="records")
        ).execute()
        count = len(inserted.data if hasattr(inserted, 'data') else [])
        msg = f"{count} tickets inseridos com sucesso!"
        supabase_client.log_event("info", msg)
        return dbc.Alert(msg, color="success")
    except Exception as e:
        err = str(e)
        supabase_client.log_event("error", f"Falha ao salvar no banco: {err}")
        return dbc.Alert(f"Falha ao salvar no banco: {err}", color="danger")


if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
