# -*- coding: utf-8 -*-
import os
import io
import base64
import logging
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State, callback
import pandas as pd

from data_processor import process_uploaded_files
import supabase_client

# --- Inicialização Supabase ---
supabase_client.init_supabase_client()

# Busca inicial de dados
df_initial = supabase_client.fetch_all_tickets_data()
if df_initial is None or (hasattr(df_initial, "empty") and df_initial.empty):
    logging.info("Supabase fetch falhou ou retornou vazio. Dashboard iniciando vazio.")

# --- Cores e tema ---
plotly_template = "plotly_dark"

# --- Inicialização do Dash ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
server = app.server

supabase_client.log_event("info", "Dashboard application starting.")

# --- Layout ---
app.layout = dbc.Container(fluid=True, children=[

    # Stores para dados e trigger
    dcc.Store(id="data-update-trigger", storage_type="memory"),

    # Cabeçalho
    dbc.Row(dbc.Col(html.H1("Dashboard Executivo - Monitoramento de Suporte"), width=12)),

    # Seção de upload
    dbc.Row([
        dbc.Col([
            html.H4("Carregar Novos Dados"),
            dcc.Upload(
                id="upload-piloto",
                children=html.Div(["Arraste ou ", html.A("Selecione o arquivo Piloto (.xlsx)")]),
                style={
                    "width": "100%", "height": "60px", "lineHeight": "60px",
                    "borderWidth": "1px", "borderStyle": "dashed",
                    "borderRadius": "5px", "textAlign": "center", "margin": "10px 0"
                },
                multiple=False
            ),
            dcc.Upload(
                id="upload-sla",
                children=html.Div(["Arraste ou ", html.A("Selecione o arquivo SLA (.xlsx)")]),
                style={
                    "width": "100%", "height": "60px", "lineHeight": "60px",
                    "borderWidth": "1px", "borderStyle": "dashed",
                    "borderRadius": "5px", "textAlign": "center", "margin": "10px 0"
                },
                multiple=False
            ),
            dbc.Button("Processar e Salvar no Banco", id="process-button", color="primary", className="mt-2"),
            html.Div(id="upload-status", className="mt-2")
        ], width=12)
    ], className="mb-4"),

    # Filtros de Projeto / Tribo / Período
    dbc.Row([
        dbc.Col([html.Label("Projeto:"), dcc.Dropdown(id="project-dropdown", clearable=False)], md=3),
        dbc.Col([html.Label("Tribo:"), dcc.Dropdown(id="tribo-dropdown", clearable=False)], md=3),
        dbc.Col([
            html.Label("Período:"),
            dcc.Dropdown(
                id="period-dropdown",
                options=[
                    {"label": "Ano Inteiro", "value": "year"},
                    {"label": "Trimestre", "value": "quarter"},
                    {"label": "Mês", "value": "month"},
                ],
                value="month",
                clearable=False
            )
        ], md=2),
        dbc.Col([html.Label("Selecionar Valor:"), dcc.Dropdown(id="period-value-dropdown")], md=4)
    ], className="mb-4"),

    # Abas
    dbc.Tabs(id="tabs-main", active_tab="tab-visao-geral", children=[
        dbc.Tab(label="Visão Geral", tab_id="tab-visao-geral"),
        dbc.Tab(label="Desempenho SLA", tab_id="tab-sla-perf"),
        dbc.Tab(label="Tempos e Status", tab_id="tab-tempos"),
        dbc.Tab(label="Dados Detalhados", tab_id="tab-dados"),
    ]),
    html.Div(id="tabs-content-main", className="mt-3")
])

# --- Callback de processamento e upsert ---
@callback(
    [Output("data-update-trigger", "data"), Output("upload-status", "c]()
