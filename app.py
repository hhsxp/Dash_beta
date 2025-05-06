import os
import io
import base64
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State, callback

import pandas as pd
from data_processor import process_sla_file
import supabase_client

# ------------------------------------------------------------------------------
# Inicialização do Supabase
# ------------------------------------------------------------------------------
supabase_client.init_supabase_client()

# ------------------------------------------------------------------------------
# Layout do Dash
# ------------------------------------------------------------------------------
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    assets_folder="."
)
server = app.server

app.layout = dbc.Container(fluid=True, children=[
    html.H1("Dashboard Executivo – Monitoramento de SLA", className="mt-3 mb-4"),

    # Upload único do SLA.xlsx
    html.H5("Carregar nova versão do SLA"),
    dcc.Upload(
        id='upload-sla',
        children=html.Div([
            "Arraste ou ",
            html.A("Selecione o arquivo SLA (.xlsx)")
        ]),
        style={
            'width': '100%', 'height': '60px', 'lineHeight': '60px',
            'borderWidth': '1px', 'borderStyle': 'dashed',
            'borderRadius': '5px', 'textAlign': 'center',
            'margin': '10px 0'
        },
        multiple=False
    ),
    dbc.Button('Processar e Salvar no Banco', id='process-button', color='primary'),
    html.Div(id='upload-status', className='mt-2'),

    html.Hr(),

    # Dropdown de versões geradas
    html.H5("Versões Disponíveis"),
    dcc.Dropdown(
        id='version-dropdown',
        options=[],
        placeholder="Selecione uma versão",
        clearable=False
    ),

    html.Hr(),
    html.Div(id='dashboard-content')
])

# ------------------------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------------------------

@callback(
    Output('upload-status', 'children'),
    Output('version-dropdown', 'options'),
    Input('process-button', 'n_clicks'),
    State('upload-sla', 'contents'),
    State('upload-sla', 'filename'),
    prevent_initial_call=True
)
def upload_and_list_versions(n_clicks, sla_contents, sla_filename):
    if not sla_contents:
        return "Por favor, carregue um arquivo SLA (.xlsx) primeiro.", dash.no_update

    try:
        # 1) Processa o arquivo e retorna DataFrame
        df = process_sla_file(sla_contents)

        # 2) Upserta no Supabase e obtém version_id
        version_id = supabase_client.upsert_sla(df)

        # 3) Lista versões de volta ao dropdown
        versions = supabase_client.fetch_sla_versions()
        options = [
            {"label": v["created_at"][:19].replace("T", " "), "value": v["id"]}
            for v in versions
        ]

        msg = dbc.Alert(
            f"Upload realizado! Versão criada: {version_id}",
            color="success"
        )
        return msg, options

    except Exception as e:
        err = dbc.Alert(f"Erro no upload: {e}", color="danger")
        return err, dash.no_update


@callback(
    Output('dashboard-content', 'children'),
    Input('version-dropdown', 'value')
)
def render_dashboard(version_id):
    if not version_id:
        return html.P("Selecione uma versão para visualizar o dashboard.", className="text-muted")

    # Busca dados da versão
    df = supabase_client.fetch_sla_data(version_id)
    if df.empty:
        return html.P("Nenhum dado encontrado para esta versão.", className="text-warning")

    # Cálculo rápido de KPIs
    total = len(df)
    atingidos = df["CumpriuSLA_Res"].sum()
    violados = total - atingidos
    em_risco = ((df["HorasResolução"] >= df["SLA_Horas"] * 0.8) & (df["CumpriuSLA_Res"]==False)).sum()
    aguard = (df["Status"] == "Aguardando").sum()

    cards = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("% SLA Atingido"),
            html.H2(f"{atingidos/total:.0%}", className="text-success")
        ])), md=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("% SLA Violado"),
            html.H2(f"{violados/total:.0%}", className="text-danger")
        ])), md=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("Em Risco"),
            html.H2(f"{em_risco}", className="text-warning")
        ])), md=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("Aguardando"),
            html.H2(f"{aguard}", className="text-primary")
        ])), md=3),
    ], className="mb-4")

    # Exemplo de tabela de preview (você pode trocar por gráficos Plotly)
    table = dbc.Table.from_dataframe(
        df[["Chave","Projeto","Prioridade","HorasResolução","SLA_Horas","CumpriuSLA_Res"]].head(20),
        striped=True, bordered=True, hover=True, responsive=True
    )

    return html.Div([cards, table])

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True, port=int(os.environ.get("PORT", 8050)))
