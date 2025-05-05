# app.py
import io
import base64

from dash import Dash, html, dcc, Input, Output, State, no_update
import dash_bootstrap_components as dbc

from supabase_client import init_supabase, supabase_client, log_event
from data_processor import process_uploaded_files

# Inicializa Supabase antes de tudo
init_supabase()

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    html.H1("Dashboard Executivo – Monitoramento de Suporte"),
    html.H5("Carregar Novos Dados"),
    dcc.Upload(id="upload-piloto", children=html.Div("Arraste ou selecione o Arquivo Piloto (.xlsx)"),
               style={"border": "1px dashed #ccc", "padding": "20px", "margin-bottom": "10px"}),
    dcc.Upload(id="upload-sla", children=html.Div("Arraste ou selecione o Arquivo SLA (.xlsx)"),
               style={"border": "1px dashed #ccc", "padding": "20px"}),
    dbc.Button("Processar e Salvar no Banco", id="btn-process", color="primary", className="mt-2"),
    html.Div(id="alert-container", className="mt-2"),
    # ... resto do layout (dropdowns, gráficos etc)
], fluid=True)


@app.callback(
    Output("alert-container", "children"),
    Input("btn-process", "n_clicks"),
    State("upload-piloto", "contents"),
    State("upload-piloto", "filename"),
    State("upload-sla", "contents"),
    State("upload-sla", "filename"),
    prevent_initial_call=True
)
def handle_upload(_, piloto_contents, piloto_filename, sla_contents, sla_filename):
    # precisa ter ambos arquivos
    if not piloto_contents or not sla_contents:
        return dbc.Alert("Selecione ambos os arquivos antes de processar.", color="warning")
    try:
        # decodifica base64 -> bytes
        _, piloto_b64 = piloto_contents.split(",", 1)
        piloto_bytes = base64.b64decode(piloto_b64)
        _, sla_b64 = sla_contents.split(",", 1)
        sla_bytes = base64.b64decode(sla_b64)

        log_event("info", "Iniciando processamento dos arquivos",
                  {"piloto": piloto_filename, "sla": sla_filename})

        df = process_uploaded_files(piloto_bytes, sla_bytes)

        # DataFrame vazio?
        if df.empty:
            msg = "Processed DataFrame está vazio após o merge."
            log_event("error", msg)
            return dbc.Alert(f"Erro: {msg}", color="danger")

        # insere no Supabase
        records = df.to_dict(orient="records")
        resp = supabase_client.table("tickets").insert(records).execute()

        # verifica resposta
        if resp.data is None:
            msg = f"Falha ao inserir {len(records)} registros."
            log_event("error", msg, {"response": resp})
            return dbc.Alert(f"Erro: {msg}", color="danger")

        msg = f"{len(records)} registros inseridos com sucesso!"
        log_event("info", msg)
        return dbc.Alert(msg, color="success")

    except Exception as e:
        log_event("error", "Erro no processamento", {"error": str(e)})
        return dbc.Alert(f"Erro: {e}", color="danger")


if __name__ == "__main__":
    app.run_server(debug=True)
