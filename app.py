import io
import os
import base64
import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
from supabase_client import init_supabase_client, supabase_client, log_event
from data_processor import process_uploaded_files

# Inicializa Supabase
init_supabase_client()

# Carrega dados iniciais (pode retornar DataFrame vazio)
try:
    df_initial = supabase_client.fetch_all_tickets_data()
except Exception as e:
    log_event("error", f"Erro ao buscar dados iniciais: {e}")
    df_initial = pd.DataFrame()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

app.layout = dbc.Container([
    html.H1("Dashboard Executivo - Monitoramento de Suporte", className="mt-4 mb-4 text-light"),
    dcc.Upload(
        id='upload-piloto',
        children=html.Div(['Arraste ou selecione o Arquivo Piloto (.xlsx)']),
        className='upload-box'
    ),
    dcc.Upload(
        id='upload-sla',
        children=html.Div(['Arraste ou selecione o Arquivo SLA (.xlsx)']),
        className='upload-box'
    ),
    dbc.Button("Processar e Salvar no Banco", id='process-btn', color='primary', className='mt-2 mb-2'),
    dbc.Alert(id='alert-error', is_open=False, color='danger'),
    # Demais componentes (dropdowns, gráficos, etc.)
], fluid=True, className="bg-dark text-white")

@app.callback(
    Output('alert-error', 'children'),
    Output('alert-error', 'is_open'),
    Input('process-btn', 'n_clicks'),
    State('upload-piloto', 'contents'),
    State('upload-sla', 'contents'),
)
def handle_upload(n_clicks, piloto_contents, sla_contents):
    if not n_clicks:
        return '', False
    try:
        # Extrai bytes dos uploads
        piloto_bytes = base64.b64decode(piloto_contents.split(',')[1])
        sla_bytes    = base64.b64decode(sla_contents.split(',')[1])

        log_event("info", "Processing files start.", details={"piloto": piloto_contents[:50], "sla": sla_contents[:50]})

        # Processa
        df = process_uploaded_files(piloto_bytes, sla_bytes)

        # Salva no banco
        response = supabase_client.table('tickets').insert(df.to_dict('records')).execute()
        if response.status_code != 201:
            raise RuntimeError(f"Falha ao inserir no Supabase: {response.data}")

        log_event("info", "Files processed and saved.")
        return '', False

    except ValueError as ve:
        log_event("error", f"Validation error: {ve}")
        return f"Erro: {ve}", True
    except Exception as e:
        log_event("error", f"Error in processing: {e}")
        return "Erro no processamento. Verifique os logs ou o formato dos arquivos.", True

if __name__ == '__main__':
    app.run_server(debug=True, port=int(os.environ.get('PORT', 8050)))
