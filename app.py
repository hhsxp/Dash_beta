# app.py
# -*- coding: utf-8 -*-
import os
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State
import pandas as pd
import numpy as np
from datetime import datetime
from data_processor import process_uploaded_files
import supabase_client

# --- Inicialização Supabase e dados iniciais ---
supabase_client.init_supabase_client()

df_initial = supabase_client.fetch_all_tickets_data()
if df_initial.empty:
    print("Supabase fetch falhou ou retornou vazio. Dashboard iniciará vazio.")

# Cores e template
dark_bg = "#121212"; light_text = "#FFFFFF"
sla_atingido_color = "#28a745"; sla_violado_color = "#dc3545"
em_risco_color = "#ffc107"; aguardando_color = "#0d6efd"; pendente_color = "#6c757d"
plotly_template = "plotly_dark"

# Helper para KPI cards
def create_kpi_card(title, value, id_suffix, color_class=""):
    return dbc.Card([
        dbc.CardHeader(title),
        dbc.CardBody(html.P(value, id=f"kpi-{id_suffix}"))
    ], color=color_class or "dark", inverse=True, className="m-2")

# --- App & Layout ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
server = app.server

supabase_client.log_event("info", "Dashboard application starting.")

app.layout = dbc.Container(fluid=True, children=[
    dcc.Store(id="intermediate-data-store", storage_type="memory"),
    dcc.Store(id="data-update-trigger", storage_type="memory"),

    dbc.Row(dbc.Col(html.H1("Dashboard Executivo – Monitoramento de Suporte"), width=12)),
    dbc.Row([
        dbc.Col([
            html.H4("Carregar Novos Dados"),
            dcc.Upload(
                id="upload-piloto",
                children=html.Div(["Arraste ou ", html.A("selecione o arquivo Piloto (.xlsx)")]),
                style={"width":"100%","height":"60px","lineHeight":"60px",
                       "borderWidth":"1px","borderStyle":"dashed","borderRadius":"5px",
                       "textAlign":"center","margin":"10px 0"},
                multiple=False
            ),
            dcc.Upload(
                id="upload-sla",
                children=html.Div(["Arraste ou ", html.A("selecione o arquivo SLA (.xlsx)")]),
                style={"width":"100%","height":"60px","lineHeight":"60px",
                       "borderWidth":"1px","borderStyle":"dashed","borderRadius":"5px",
                       "textAlign":"center","margin":"10px 0"},
                multiple=False
            ),
            dbc.Button("Processar e Salvar no Banco", id="process-button", color="primary", n_clicks=0),
            html.Div(id="upload-status", className="mt-2")
        ], width=12)
    ], className="mb-4"),

    # Filtros
    dbc.Row([
        dbc.Col([html.Label("Projeto:"), dcc.Dropdown(id="project-dropdown")], md=3),
        dbc.Col([html.Label("Tribo:"), dcc.Dropdown(id="tribo-dropdown")], md=3),
        dbc.Col([html.Label("Período:"), dcc.Dropdown(
            id="period-dropdown",
            options=[
                {"label":"Ano Inteiro","value":"year"},
                {"label":"Trimestre","value":"quarter"},
                {"label":"Mês","value":"month"}
            ], value="month"
        )], md=2),
        dbc.Col([html.Label("Selecionar Período:"), dcc.Dropdown(id="period-value-dropdown")], md=4),
    ], className="mb-4"),

    # Tabs
    dbc.Tabs([
        dbc.Tab(label="Visão Geral", tab_id="tab-visao-geral"),
        dbc.Tab(label="Desempenho SLA", tab_id="tab-sla-perf"),
        dbc.Tab(label="Tempos e Status", tab_id="tab-tempos"),
        dbc.Tab(label="Dados Detalhados", tab_id="tab-dados")
    ], id="tabs-main", active_tab="tab-visao-geral"),

    html.Div(id="tabs-content-main", className="mt-3")
])

# --- Callbacks ---

# 1. Processar upload e fazer upsert
@app.callback(
    [Output("data-update-trigger", "data"), Output("upload-status", "children")],
    Input("process-button", "n_clicks"),
    State("upload-piloto", "contents"), State("upload-piloto", "filename"),
    State("upload-sla", "contents"), State("upload-sla", "filename"),
    prevent_initial_call=True
)
def process_and_upsert(n_clicks, piloto_ct, piloto_fn, sla_ct, sla_fn):
    if piloto_ct and sla_ct:
        detail = {"piloto": piloto_fn, "sla": sla_fn}
        supabase_client.log_event("info", "Iniciando processamento de arquivos.", detail)
        try:
            df = process_uploaded_files(piloto_ct, sla_ct)
            if df.empty:
                raise ValueError("DataFrame resultante está vazio após processamento.")
            success = supabase_client.upsert_tickets_data(df)
            if success:
                msg = dbc.Alert(f'Arquivos "{piloto_fn}" e "{sla_fn}" importados ({len(df)} linhas)', color="success")
                supabase_client.log_event("info", "Upsert realizado com sucesso.", detail)
                return {"ts": datetime.now().isoformat()}, msg
            else:
                msg = dbc.Alert("Falha ao salvar no banco. Veja logs.", color="warning")
                supabase_client.log_event("error", "Upsert falhou.", detail)
                return dash.no_update, msg
        except Exception as e:
            supabase_client.log_event("error", f"Erro no processamento: {e}", detail)
            return dash.no_update, dbc.Alert(f"Erro: {e}", color="danger")
    return dash.no_update, dash.no_update

# 2. Recarregar dados após upsert ou na carga inicial
@app.callback(
    Output("intermediate-data-store", "data"),
    Input("data-update-trigger", "data")
)
def load_data(trigger):
    supabase_client.log_event("info", "Buscando dados do Supabase.")
    df = supabase_client.fetch_all_tickets_data()
    if df.empty:
        supabase_client.log_event("warning", "Tabela vazia ou fetch falhou.")
        return []
    # serializar datas para JSON
    for c in df.select_dtypes(["datetime64[ns]"]):
        df[c] = df[c].astype(str)
    return df.to_dict("records")

# 3. Popular filtros
@app.callback(
    [Output("project-dropdown","options"), Output("project-dropdown","value"),
     Output("tribo-dropdown","options"), Output("tribo-dropdown","value"),
     Output("period-value-dropdown","options"), Output("period-value-dropdown","value")],
    Input("intermediate-data-store","data")
)
def update_filters(data):
    if not data:
        opts = [{"label":"N/A","value":"all"}]
        return opts, "all", opts, "all", [], None
    df = pd.DataFrame(data)
    # converter "Criado" se existir
    if "Criado" in df:
        df["Criado"] = pd.to_datetime(df["Criado"], errors="coerce")
    # Projetos
    projs = sorted(df["Projeto"].dropna().unique())
    proj_opts = [{"label":"Todos","value":"all"}] + [{"label":p,"value":p} for p in projs]
    # Tribos
    tribs = sorted(df["Unidade de Negócio"].dropna().unique())
    trib_opts = [{"label":"Todas","value":"all"}] + [{"label":t,"value":t} for t in tribs]
    return proj_opts, "all", trib_opts, "all", [], None

# demais callbacks de tabs, gráficos etc...

if __name__ == "__main__":
    app.run_server(debug=True)
