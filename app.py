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

# Inicializa Supabase
supabase_client.init_supabase_client()

# Busca inicial de tickets
df_initial = supabase_client.fetch_all_tickets_data()
if df_initial is None or (hasattr(df_initial, 'empty') and df_initial.empty):
    logging.info('Nenhum ticket encontrado no Supabase ou tabela vazia.')

# Cria app Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
server = app.server
supabase_client.log_event('info', 'Dashboard application starting.')

# Layout
app.layout = dbc.Container(fluid=True, children=[
    dcc.Store(id='data-update-trigger', storage_type='memory'),
    dbc.Row(dbc.Col(html.H1('Dashboard Executivo - Monitoramento de Suporte'), width=12)),
    dbc.Row([
        dbc.Col([
            html.H4('Carregar Novos Dados'),
            dcc.Upload(
                id='upload-piloto',
                children=html.Div(['Arraste ou ', html.A('Selecione o arquivo Piloto (.xlsx)')]),
                style={
                    'width': '100%', 'height': '60px', 'lineHeight': '60px',
                    'borderWidth': '1px', 'borderStyle': 'dashed',
                    'borderRadius': '5px', 'textAlign': 'center', 'margin': '10px 0'
                },
                multiple=False
            ),
            dcc.Upload(
                id='upload-sla',
                children=html.Div(['Arraste ou ', html.A('Selecione o arquivo SLA (.xlsx)')]),
                style={
                    'width': '100%', 'height': '60px', 'lineHeight': '60px',
                    'borderWidth': '1px', 'borderStyle': 'dashed',
                    'borderRadius': '5px', 'textAlign': 'center', 'margin': '10px 0'
                },
                multiple=False
            ),
            dbc.Button('Processar e Salvar no Banco', id='process-button', color='primary', className='mt-2'),
            html.Div(id='upload-status', className='mt-2')
        ], width=12)
    ], className='mb-4'),
    # Filtros
    dbc.Row([
        dbc.Col([html.Label('Projeto:'), dcc.Dropdown(id='project-dropdown', clearable=False)], md=3),
        dbc.Col([html.Label('Tribo:'), dcc.Dropdown(id='tribo-dropdown', clearable=False)], md=3),
        dbc.Col([
            html.Label('Período:'),
            dcc.Dropdown(
                id='period-dropdown',
                options=[
                    {'label': 'Ano Inteiro', 'value': 'year'},
                    {'label': 'Trimestre', 'value': 'quarter'},
                    {'label': 'Mês', 'value': 'month'}
                ],
                value='month', clearable=False
            )
        ], md=2),
        dbc.Col([html.Label('Selecionar Valor:'), dcc.Dropdown(id='period-value-dropdown')], md=4)
    ], className='mb-4'),
    # Abas
    dbc.Tabs(id='tabs-main', active_tab='tab-visao-geral', children=[
        dbc.Tab(label='Visão Geral', tab_id='tab-visao-geral'),
        dbc.Tab(label='Desempenho SLA', tab_id='tab-sla-perf'),
        dbc.Tab(label='Tempos e Status', tab_id='tab-tempos'),
        dbc.Tab(label='Dados Detalhados', tab_id='tab-dados')
    ]),
    html.Div(id='tabs-content-main', className='mt-3')
])

# Callback de processamento e upsert
@callback(
    [Output('data-update-trigger', 'data'), Output('upload-status', 'children')],
    Input('process-button', 'n_clicks'),
    State('upload-piloto', 'contents'), State('upload-piloto', 'filename'),
    State('upload-sla', 'contents'), State('upload-sla', 'filename'),
    prevent_initial_call=True
)
def process_and_upsert(n_clicks, piloto_contents, piloto_filename, sla_contents, sla_filename):
    if n_clicks and piloto_contents and sla_contents:
        detail = {'piloto': piloto_filename, 'sla': sla_filename}
        supabase_client.log_event('info', 'Iniciando processamento de arquivos', detail)
        try:
            df = process_uploaded_files(piloto_contents, sla_contents)
            if df.empty:
                raise ValueError('DataFrame processado está vazio')
            ok = supabase_client.upsert_tickets_data(df)
            if not ok:
                raise RuntimeError('Falha ao inserir dados no Supabase')
            msg = dbc.Alert(
                f'Arquivos "{piloto_filename}" e "{sla_filename}" processados! Linhas: {df.shape[0]}',
                color='success'
            )
            supabase_client.log_event('info', 'Processamento concluído', detail)
            return {'triggered': datetime.utcnow().isoformat()}, msg
        except Exception as e:
            supabase_client.log_event('error', f'Erro no processamento: {e}', detail)
            return dash.no_update, dbc.Alert(f'Erro: {e}', color='danger')
    return dash.no_update, dash.no_update

# Execução
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8050))
    app.run_server(host='0.0.0.0', port=port)
