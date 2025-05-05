# -*- coding: utf-8 -*-
import os
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import numpy as np

import supabase_client
from data_processor import process_uploaded_files

# Initialize Supabase & fetch initial data
supabase_client.init_supabase_client()
df_initial = supabase_client.fetch_all_tickets_data()
if df_initial is None or df_initial.empty:
    print("Supabase fetch failed or returned empty. Dashboard will start empty.")

# Theme settings
plotly_template = "plotly_dark"

# KPI card helper
def create_kpi_card(title, value, suffix, color="dark"):
    return dbc.Card(
        [
            dbc.CardHeader(title, className="kpi-title"),
            dbc.CardBody(html.P(value, id=f"kpi-{suffix}", className="kpi-value"))
        ],
        color=color,
        inverse=True,
        className="m-2"
    )

# Dash app setup
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
server = app.server
supabase_client.log_event("info", "Dashboard application starting.")

# App layout
app.layout = dbc.Container([
    # Store for intermediate data
    dcc.Store(id="intermediate-data-store"),
    dcc.Store(id="data-update-trigger"),

    # Header
    dbc.Row(
        dbc.Col(html.H1("Dashboard Executivo - Monitoramento de Suporte"), width=12),
        className="mt-3"
    ),

    # Upload section
    dbc.Row(
        dbc.Col([
            html.H4("Carregar Novos Dados"),
            dcc.Upload(
                id="upload-piloto",
                children=html.Div(["Arraste ou ", html.A("Selecione o Arquivo Piloto (.xlsx)")]),
                style={"width": "100%", "height": "60px", "lineHeight": "60px",
                       "borderWidth": "1px", "borderStyle": "dashed",
                       "borderRadius": "5px", "textAlign": "center", "margin": "10px 0px"},
                multiple=False
            ),
            dcc.Upload(
                id="upload-sla",
                children=html.Div(["Arraste ou ", html.A("Selecione o Arquivo SLA (.xlsx)")]),
                style={"width": "100%", "height": "60px", "borderWidth": "1px",
                       "borderStyle": "dashed", "borderRadius": "5px",
                       "textAlign": "center", "margin": "10px 0px", "lineHeight": "60px"},
                multiple=False
            ),
            dbc.Button("Processar e Salvar no Banco", id="process-button", n_clicks=0, color="primary"),
            html.Div(id="upload-status", className="mt-2")
        ], width=12),
        className="mb-4"
    ),

    # Filters
    dbc.Row([
        dbc.Col([html.Label("Projeto:"), dcc.Dropdown(id="project-dropdown", clearable=False)], md=3),
        dbc.Col([html.Label("Tribo (Unidade de Negócio):"), dcc.Dropdown(id="tribo-dropdown", clearable=False)], md=3),
        dbc.Col([html.Label("Período (Criação):"),
                 dcc.Dropdown(id="period-dropdown",
                              options=[{'label':'Ano Inteiro','value':'year'},
                                       {'label':'Trimestre','value':'quarter'},
                                       {'label':'Mês','value':'month'}],
                              value='month', clearable=False)], md=2),
        dbc.Col([html.Label("Selecionar Período:"), dcc.Dropdown(id="period-value-dropdown", clearable=False)], md=4)
    ], className="mb-4"),

    # Tabs
    dbc.Tabs([
        dbc.Tab(label="Visão Geral", tab_id="tab-visao-geral"),
        dbc.Tab(label="Desempenho SLA", tab_id="tab-sla-perf"),
        dbc.Tab(label="Tempos e Status", tab_id="tab-tempos"),
        dbc.Tab(label="Dados Detalhados", tab_id="tab-dados"),
    ], id="tabs-main", active_tab="tab-visao-geral"),

    html.Div(id="tabs-content-main", className="mt-3")
], fluid=True)

# Callbacks

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
                    "Nenhum ticket encontrado após o merge. Verifique se a coluna 'Chave' existe e coincide em ambos arquivos."
                )
            ok = supabase_client.upsert_tickets_data(df)
            if not ok:
                raise RuntimeError("Falha ao salvar no Supabase.")
            msg = f'Arquivos "{piloto_fn}" e "{sla_fn}" processados ({len(df)} linhas).'
            supabase_client.log_event("info", "Process and upsert successful.", detail)
            return {"timestamp": datetime.now().isoformat()}, dbc.Alert(msg, color="success")
        except Exception as e:
            supabase_client.log_event("error", f"Error in processing: {e}", detail)
            return dash.no_update, dbc.Alert(f"Erro: {e}", color="danger")
    return dash.no_update, "Carregue ambos os arquivos e clique em processar."

@app.callback(
    Output("intermediate-data-store", "data"),
    Input("data-update-trigger", "data")
)
def load_data(trigger):
    supabase_client.log_event("info", "Fetching data.")
    df = supabase_client.fetch_all_tickets_data()
    if df is None or df.empty:
        supabase_client.log_event("warning", "No data fetched.")
        return []
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]):
        df[col] = df[col].astype(str)
    return df.replace({pd.NaT: None}).to_dict("records")

@app.callback(
    [Output("project-dropdown", "options"), Output("project-dropdown", "value"),
     Output("tribo-dropdown", "options"), Output("tribo-dropdown", "value"),
     Output("period-value-dropdown", "options"), Output("period-value-dropdown", "value")],
    Input("intermediate-data-store", "data")
)
def update_filters(data):
    if not data:
        empty_opt = [{"label": "N/A", "value": "all"}]
        return empty_opt, "all", empty_opt, "all", [], None
    df = pd.DataFrame(data)
    if "Criado" in df.columns:
        df["Criado"] = pd.to_datetime(df["Criado"], errors="coerce")
    projects = [{"label":"Todos","value":"all"}] + [
        {"label": p, "value": p} for p in sorted(df["Projeto"].dropna().unique())
    ]
    tribes = [{"label":"Todas","value":"all"}] + [
        {"label": t, "value": t} for t in sorted(df["Unidade de Negócio"].dropna().unique())
    ]
    return projects, "all", tribes, "all", [], None

@app.callback(
    [Output("period-value-dropdown", "options", allow_duplicate=True),
     Output("period-value-dropdown", "value", allow_duplicate=True)],
    [Input("period-dropdown", "value"), Input("intermediate-data-store", "data")],
    prevent_initial_call=True
)
def update_period_values(period, data):
    if not data:
        return [], None
    df = pd.DataFrame(data)
    df["Criado"] = pd.to_datetime(df["Criado"], errors="coerce")
    if period == "year":
        vals = sorted(df["Criado"].dt.year.dropna().unique())
    elif period == "quarter":
        vals = sorted(df["Criado"].dt.to_period('Q').astype(str).unique(), reverse=True)
    else:
        vals = sorted(df["Criado"].dt.to_period('M').astype(str).unique(), reverse=True)
    opts = [{"label": v, "value": v} for v in vals]
    return opts, opts[0]["value"] if opts else ([], None)

@app.callback(
    Output("tabs-content-main", "children"),
    Input("tabs-main", "active_tab")
)
def render_tab_content(active_tab):
    if active_tab == "tab-visao-geral":
        return dbc.Row([
            dbc.Col(create_kpi_card("% SLA Res. Atingido", "-", "sla-res-atingido", "success"), md=3),
            dbc.Col(create_kpi_card("% SLA Res. Violado", "-", "sla-res-violado", "danger"), md=3),
            dbc.Col(create_kpi_card("% SLA 1ª Resp. Atingido", "-", "sla-resp-atingido", "success"), md=3),
            dbc.Col(create_kpi_card("% SLA 1ª Resp. Violado", "-", "sla-resp-violado", "danger"), md=3),
            dbc.Col(dcc.Graph(id="graph-sla-res-projeto"), md=6),
            dbc.Col(dcc.Graph(id="graph-tickets-tipo"), md=6)
        ])
    return html.P("Selecione uma aba")

@app.callback(
    [
        Output("kpi-sla-res-atingido", "children"), Output("kpi-sla-res-violado", "children"),
        Output("kpi-sla-resp-atingido", "children"), Output("kpi-sla-resp-violado", "children"),
        Output("graph-sla-res-projeto", "figure"), Output("graph-tickets-tipo", "figure"),
        Output("graph-tickets-prioridade", "figure"), Output("graph-tickets-status-cat", "figure"),
        Output("graph-top-5-violacoes-res", "figure"), Output("graph-timeline-violacoes-res", "figure"),
        Output("graph-sla-resp-projeto", "figure"), Output("graph-timeline-violacoes-resp", "figure"),
        Output("kpi-lead-time", "children"), Output("kpi-aging", "children"),
        Output("kpi-em-risco", "children"), Output("kpi-aguardando", "children"),
        Output("graph-lead-time-projeto", "figure"), Output("graph-aging-tribo", "figure"),
        Output("graph-tempo-medio-status", "figure"),
        Output("data-table", "data"), Output("data-table", "columns")
    ],
    [Input("intermediate-data-store", "data"), Input("project-dropdown","value"),
     Input("tribo-dropdown","value"), Input("period-dropdown","value"),
     Input("period-value-dropdown","value")]
)
def update_dashboard(data, proj, tribo, period, val):
    if not data:
        empty_fig = go.Figure(layout=dict(template=plotly_template))
        empty_fig.add_annotation(text="Sem dados.", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return (["N/A"]*4 + [empty_fig]*8 + [], [])
    df = pd.DataFrame(data)
    # Placeholder values/figures
    kpis = ["0%","0%","0%","0%","0","0","0","0"]
    figs = [go.Figure(layout=dict(template=plotly_template)) for _ in range(11)]
    table_data, table_cols = [], []
    return (*kpis[:4], *figs[:4], *kpis[4:], *figs[4:], table_data, table_cols)

# Run server
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
