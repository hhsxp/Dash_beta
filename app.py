### app.py
```python
import logging
import pandas as pd
from flask import Flask
import dash
from dash import dcc, html, Input, Output, State

import supabase_client
from data_processor import process_uploaded_files

# Inicializa logs e Supabase
type logging.basicConfig(level=logging.INFO)
supabase_client.init_supabase_client()

# Busca inicial de dados (DataFrame)
df_initial = supabase_client.fetch_all_tickets_data()
if df_initial.empty:
    logging.info("Nenhum ticket encontrado no Supabase ou tabela vazia.")

# Cria app Flask + Dash
server = Flask(__name__)
app = dash.Dash(__name__, server=server)

# Layout mínimo de exemplo
app.layout = html.Div([
    dcc.Upload(id='upload-piloto', children=html.Div('Arraste ou selecione o Arquivo Piloto (.xlsx)')),
    dcc.Upload(id='upload-sla', children=html.Div('Arraste ou selecione o Arquivo SLA (.xlsx)')),
    html.Button('Processar e Salvar no Banco', id='process-btn'),
    html.Div(id='output-msg')
])

@dash.callback(
    Output('output-msg', 'children'),
    Input('process-btn', 'n_clicks'),
    State('upload-piloto', 'contents'),
    State('upload-sla', 'contents')
)
def handle_upload(n_clicks, piloto_contents, sla_contents):
    if not n_clicks:
        return ''
    supabase_client.log_event('info', 'Processamento iniciado', {'piloto': piloto_contents and 'ok', 'sla': sla_contents and 'ok'})
    try:
        df = process_uploaded_files(piloto_contents, sla_contents)
        # Aqui: salvar df no Supabase
        records = df.to_dict(orient='records')
        resp = supabase_client.supabase_client.table('tickets').insert(records).execute()
        if getattr(resp, 'status_code', None) != 201:
            raise ValueError(f"Falha ao inserir no Supabase: {getattr(resp, 'data', resp)}")
        supabase_client.log_event('info', 'Dados inseridos', {'count': len(records)})
        return f"Sucesso: {len(records)} tickets inseridos."
    except Exception as e:
        supabase_client.log_event('error', f'Erro no processamento: {e}')
        return f"Erro: {e}"

if __name__ == '__main__':
    app.run(debug=True)
