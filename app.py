# -*- coding: utf-8 -*-
import os
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State, dash_table, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime
import io

# Import Supabase client and data processing function
from data_processor import process_uploaded_files
import supabase_client

# --- Configuration & Initial Data Loading ---
# Initialize Supabase client
supabase_client.init_supabase_client()

# Try fetching initial data from Supabase
df_initial = supabase_client.fetch_all_tickets_data()
if df_initial.empty:
    print("Supabase fetch failed or returned empty. Dashboard will start empty.")
    # Optionally, could fall back to local CSV, but aiming for Supabase first
    # try:
    #     df_initial = pd.read_csv("/home/ubuntu/processed_ticket_data.csv", ...)
    # except:
    #     df_initial = pd.DataFrame()

# Theme Colors (same as before)
dark_bg = "#121212"
light_text = "#FFFFFF"
sla_atingido_color = "#28a745"
sla_violado_color = "#dc3545"
em_risco_color = "#ffc107"
aguardando_color = "#0d6efd"
pendente_color = "#6c757d"

plotly_template = "plotly_dark"

# --- Helper Functions (same as before) ---
def create_kpi_card(title, value, id_suffix, color_class=""):
    return dbc.Card([
        dbc.CardHeader(title, className="kpi-title"),
        dbc.CardBody(html.P(value, id=f"kpi-{id_suffix}", className="kpi-value"))
    ], className=f"kpi-card {color_class}", color=color_class.split("-")[-1] if color_class else "dark", inverse=True)

# --- App Initialization ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
server = app.server # Expose server for deployment
supabase_client.log_event("info", "Dashboard application starting.")

# --- App Layout (Mostly the same, using dcc.Store for triggers) ---
app.layout = dbc.Container([
    # Store for data fetched from Supabase or processed from upload
    # Use timestamp to trigger updates after upsert
    dcc.Store(id=	"data-update-trigger	", storage_type=	"memory	"),
    dbc.Row(dbc.Col(html.H1("Dashboard Executivo - Monitoramento de Suporte"), width=12)),

    # Upload Section (same as before)
    dbc.Row([
        dbc.Col([
            html.H4("Carregar Novos Dados"),
            dcc.Upload(
                id=	"upload-piloto	",
                children=html.Div(["Arraste ou ", html.A("Selecione o Arquivo Piloto (.xlsx)")]),
                style={
                    "width": "100%", "height": "60px", "lineHeight": "60px",
                    "borderWidth": "1px", "borderStyle": "dashed",
                    "borderRadius": "5px", "textAlign": "center", "margin": "10px 0px"
                },
                multiple=False
            ),
            dcc.Upload(
                id=	"upload-sla	",
                children=html.Div(["Arraste ou ", html.A("Selecione o Arquivo SLA (.xlsx)")]),
                style={
                    "width": "100%", "height": "60px", "lineHeight": "60px",
                    "borderWidth": "1px", "borderStyle": "dashed",
                    "borderRadius": "5px", "textAlign": "center", "margin": "10px 0px"
                },
                multiple=False
            ),
            dbc.Button("Processar e Salvar no Banco", id="process-button", n_clicks=0, color="primary", className="mt-2"),
            html.Div(id=	"upload-status	", className="mt-2")
        ], width=12)
    ], className="mb-4"),

    # Filters (same as before)
    dbc.Row([
        dbc.Col([
            html.Label("Projeto:"),
            dcc.Dropdown(id=	"project-dropdown	", multi=False, clearable=False)
        ], md=3),
        dbc.Col([
            html.Label("Tribo (Unidade de Negócio):"),
            dcc.Dropdown(id=	"tribo-dropdown	", multi=False, clearable=False)
        ], md=3),
        dbc.Col([
            html.Label("Período (Criação):"),
            dcc.Dropdown(
                id=	"period-dropdown	",
                options=[
                    {"label": "Ano Inteiro", "value": "year"},
                    {"label": "Trimestre", "value": "quarter"},
                    {"label": "Mês", "value": "month"}
                ],
                value="month", # Default to monthly view
                clearable=False
            )
        ], md=2),
        dbc.Col([
            html.Label("Selecionar Período:"),
            dcc.Dropdown(id=	"period-value-dropdown	", clearable=False) # Options populated by callback
        ], md=4),
    ], className="mb-4"),

    # Tabs (same as before)
    dbc.Tabs([
        dbc.Tab(label="Visão Geral", tab_id="tab-visao-geral"),
        dbc.Tab(label="Desempenho SLA", tab_id="tab-sla-perf"),
        dbc.Tab(label="Tempos e Status", tab_id="tab-tempos"),
        dbc.Tab(label="Dados Detalhados", tab_id="tab-dados"),
    ], id="tabs-main", active_tab="tab-visao-geral"),
    html.Div(id="tabs-content-main", className="mt-3")

], fluid=True, className="dbc")

# --- Callbacks ---

# Process Uploaded Files and Upsert to Supabase
@callback(
    [Output(	"data-update-trigger	", "data"), # Trigger update by changing timestamp
     Output(	"upload-status	", "children")],
    [Input(	"process-button	", "n_clicks")],
    [State(	"upload-piloto	", "contents"),
     State(	"upload-piloto	", "filename"),
     State(	"upload-sla	", "contents"),
     State(	"upload-sla	", "filename")],
    prevent_initial_call=True
)
def process_and_upsert_data(n_clicks, piloto_contents, piloto_filename, sla_contents, sla_filename):
    if n_clicks > 0 and piloto_contents and sla_contents:
        log_detail = {"piloto_file": piloto_filename, "sla_file": sla_filename}
        supabase_client.log_event("info", "Processing uploaded files started.", log_detail)
        print(f"Processing uploaded files: {piloto_filename}, {sla_filename}")
        try:
            df_processed = process_uploaded_files(piloto_contents, sla_contents)
            if not df_processed.empty:
                print(f"Processed {df_processed.shape[0]} rows. Attempting Supabase upsert...")
                upsert_success = supabase_client.upsert_tickets_data(df_processed)
                if upsert_success:
                    status_message = dbc.Alert(f"Arquivos 	"{piloto_filename}	" e 	"{sla_filename}	" processados e salvos no banco com sucesso! ({df_processed.shape[0]} linhas)", color="success")
                    supabase_client.log_event("info", "File processing and Supabase upsert successful.", log_detail)
                    # Return current timestamp to trigger dashboard update
                    return {"timestamp": datetime.now().isoformat()}, status_message
                else:
                    status_message = dbc.Alert("Arquivos processados, mas falha ao salvar no banco de dados. Verifique os logs.", color="warning")
                    supabase_client.log_event("error", "File processing successful, but Supabase upsert failed.", log_detail)
                    return dash.no_update, status_message # Don	 trigger update if save failed
            else:
                status_message = dbc.Alert("Falha no processamento dos arquivos. Verifique os logs ou o formato dos arquivos.", color="danger")
                supabase_client.log_event("error", "File processing failed.", log_detail)
                return dash.no_update, status_message
        except Exception as e:
            print(f"Error during file processing/upsert callback: {e}")
            supabase_client.log_event("error", f"Exception during file processing/upsert: {e}", log_detail)
            status_message = dbc.Alert(f"Erro ao processar/salvar arquivos: {e}", color="danger")
            return dash.no_update, status_message
    else:
        return dash.no_update, "Por favor, carregue ambos os arquivos Piloto e SLA e clique em processar."

# Fetch data from Supabase when trigger changes or on initial load
@callback(
    Output(	"intermediate-data-store	", "data"), # Store fetched data here
    [Input(	"data-update-trigger	", "data")] # Triggered by successful upload/upsert
)
def load_data_from_supabase(trigger_data):
    ctx = dash.callback_context
    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else "initial_load"

    print(f"Fetching data from Supabase... Triggered by: {trigger_id}")
    supabase_client.log_event("info", f"Fetching data from Supabase. Trigger: {trigger_id}")
    df = supabase_client.fetch_all_tickets_data()

    if not df.empty:
        # Convert datetime columns to string for JSON serialization in dcc.Store
        for col in df.select_dtypes(include=[	"datetime64[ns, UTC]	", 	"datetime64[ns]	"]).columns:
            df[col] = df[col].astype(str)
        # Replace NaT/NaN/inf with None for JSON compatibility
        df = df.replace({pd.NaT: None})
        df = df.replace([np.inf, -np.inf], np.nan).fillna(value=None)
        print(f"Data fetched and prepared for store ({df.shape[0]} rows).")
        return df.to_dict(	"records	")
    else:
        print("Failed to fetch data from Supabase or table is empty.")
        supabase_client.log_event("warning", "Failed to fetch data from Supabase or table is empty.")
        return [] # Return empty list if fetch fails

# Add intermediate store
app.layout.children.insert(1, dcc.Store(id=	"intermediate-data-store	", storage_type=	"memory	"))

# Populate Filter Options based on Fetched Data
@callback(
    [Output(	"project-dropdown	", "options"),
     Output(	"project-dropdown	", "value"),
     Output(	"tribo-dropdown	", "options"),
     Output(	"tribo-dropdown	", "value"),
     Output(	"period-value-dropdown	", "options"),
     Output(	"period-value-dropdown	", "value")],
    [Input(	"intermediate-data-store	", "data")] # Use data from intermediate store
)
def update_filter_options(data):
    if not data:
        print("Filter options: No data available.")
        return ([{"label": "N/A", "value": "all"}], "all",
                [{"label": "N/A", "value": "all"}], "all",
                [], None)

    df = pd.DataFrame(data)
    print(f"Updating filter options based on {df.shape[0]} rows.")
    # Convert date strings back to datetime if needed for filtering options
    if 	"Criado	" in df.columns:
        df[	"Criado	"] = pd.to_datetime(df[	"Criado	"], errors="coerce")
        # Ensure time period columns exist (might be missing if initial fetch failed)
        if 	"Ano_Criacao	" not in df.columns and pd.api.types.is_datetime64_any_dtype(df[	"Criado	"]):
            df[	"Ano_Criacao	"] = df[	"Criado	"].dt.year
        if 	"Trimestre_Criacao	" not in df.columns and pd.api.types.is_datetime64_any_dtype(df[	"Criado	"]):
            df[	"Trimestre_Criacao	"] = df[	"Criado	"].dt.quarter
        if 	"Mes_Ano_Criacao	" not in df.columns and pd.api.types.is_datetime64_any_dtype(df[	"Criado	"]):
            df[	"Mes_Ano_Criacao	"] = df[	"Criado	"].dt.strftime("%Y-%m")
    else:
        print("Filter options: 	"Criado	" column missing.")
        return ([{"label": "N/A", "value": "all"}], "all",
                [{"label": "N/A", "value": "all"}], "all",
                [], None)

    # Project options
    projects = sorted(df[	"Projeto	"].astype(str).unique()) if 	"Projeto	" in df.columns else []
    project_options = ([{"label": "Todos", "value": "all"}] +
                       [{"label": proj, "value": proj} for proj in projects if proj != "None"])
    default_project = "all"

    # Tribo options
    tribos = sorted(df[	"Unidade de Negócio	"].astype(str).unique()) if 	"Unidade de Negócio	" in df.columns else []
    tribo_options = ([{"label": "Todas", "value": "all"}] +
                     [{"label": tribo, "value": tribo} for tribo in tribos if tribo != "None"])
    default_tribo = "all"

    # Period value options (populated by next callback)
    period_value_options = []
    default_period_value = None

    print("Filter options updated.")
    return project_options, default_project, tribo_options, default_tribo, period_value_options, default_period_value

# Populate Period Value Dropdown based on Period Type and Fetched Data
@callback(
    [Output(	"period-value-dropdown	", "options", allow_duplicate=True),
     Output(	"period-value-dropdown	", "value", allow_duplicate=True)],
    [Input(	"period-dropdown	", "value"),
     Input(	"intermediate-data-store	", "data")], # Use data from intermediate store
    prevent_initial_call=True
)
def update_period_value_options(period_type, data):
    if not data:
        return [], None

    df = pd.DataFrame(data)
    # Ensure date/period columns exist and dates are parsed
    if 	"Criado	" in df.columns:
        df[	"Criado	"] = pd.to_datetime(df[	"Criado	"], errors="coerce")
        if 	"Ano_Criacao	" not in df.columns and pd.api.types.is_datetime64_any_dtype(df[	"Criado	"]):
            df[	"Ano_Criacao	"] = df[	"Criado	"].dt.year
        if 	"Trimestre_Criacao	" not in df.columns and pd.api.types.is_datetime64_any_dtype(df[	"Criado	"]):
            df[	"Trimestre_Criacao	"] = df[	"Criado	"].dt.quarter
        if 	"Mes_Ano_Criacao	" not in df.columns and pd.api.types.is_datetime64_any_dtype(df[	"Criado	"]):
            df[	"Mes_Ano_Criacao	"] = df[	"Criado	"].dt.strftime("%Y-%m")
    else:
        return [], None

    options = []
    default_value = None

    try:
        if period_type == "year" and 	"Ano_Criacao	" in df.columns:
            years = sorted(df[	"Ano_Criacao	"].dropna().astype(int).unique(), reverse=True)
            options = [{"label": str(y), "value": y} for y in years]
        elif period_type == "quarter" and 	"Ano_Criacao	" in df.columns and 	"Trimestre_Criacao	" in df.columns:
            df[	"Ano_Trimestre	"] = df[	"Ano_Criacao	"].astype(str) + "-Q" + df[	"Trimestre_Criacao	"].astype(str)
            quarters = sorted(df[	"Ano_Trimestre	"].dropna().unique(), reverse=True)
            options = [{"label": q, "value": q} for q in quarters]
        elif period_type == "month" and 	"Mes_Ano_Criacao	" in df.columns:
            months = sorted(df[	"Mes_Ano_Criacao	"].dropna().unique(), reverse=True)
            options = [{"label": m, "value": m} for m in months]
    except Exception as e:
        print(f"Error generating period value options: {e}")
        supabase_client.log_event("error", f"Error generating period value options: {e}")
        return [], None

    if options:
        default_value = options[0]["value"] # Default to the latest period

    return options, default_value

# Render Tab Content (same as before)
@callback(Output(	"tabs-content-main	", "children"),
          Input(	"tabs-main	", "active_tab"))
def render_tab_content(active_tab):
    # ... (Tab rendering logic remains the same as previous version) ...
    if active_tab == "tab-visao-geral":
        return dbc.Row([
            dbc.Col(create_kpi_card("% SLA Res. Atingido", "-", "sla-res-atingido", "kpi-atingido"), md=3),
            dbc.Col(create_kpi_card("% SLA Res. Violado", "-", "sla-res-violado", "kpi-violado"), md=3),
            dbc.Col(create_kpi_card("% SLA 1ª Resp. Atingido", "-", "sla-resp-atingido", "kpi-atingido"), md=3),
            dbc.Col(create_kpi_card("% SLA 1ª Resp. Violado", "-", "sla-resp-violado", "kpi-violado"), md=3),
            dbc.Col(dcc.Graph(id=	"graph-sla-res-projeto	"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id=	"graph-tickets-tipo	"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id=	"graph-tickets-prioridade	"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id=	"graph-tickets-status-cat	"), md=6, className="mt-3"),
        ])
    elif active_tab == "tab-sla-perf":
         return dbc.Row([
            dbc.Col(dcc.Graph(id=	"graph-top-5-violacoes-res	"), md=6),
            dbc.Col(dcc.Graph(id=	"graph-timeline-violacoes-res	"), md=6),
            dbc.Col(dcc.Graph(id=	"graph-sla-resp-projeto	"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id=	"graph-timeline-violacoes-resp	"), md=6, className="mt-3"),
        ])
    elif active_tab == "tab-tempos":
        return dbc.Row([
            dbc.Col(create_kpi_card("Lead Time Médio (Horas)", "-", "lead-time", "kpi-aguardando"), md=3),
            dbc.Col(create_kpi_card("Aging Médio Aberto (Horas)", "-", "aging", "kpi-risco"), md=3),
            dbc.Col(create_kpi_card("# Em Risco SLA Res.", "-", "em-risco", "kpi-risco"), md=3),
            dbc.Col(create_kpi_card("# Aguardando/Validação", "-", "aguardando", "kpi-aguardando"), md=3),
            dbc.Col(dcc.Graph(id=	"graph-lead-time-projeto	"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id=	"graph-aging-tribo	"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id=	"graph-tempo-medio-status	"), md=12, className="mt-3"), # Placeholder
        ])
    elif active_tab == "tab-dados":
        return dbc.Row([
            dbc.Col([
                html.H4("Dados Detalhados dos Tickets"),
                dash_table.DataTable(
                    id=	"data-table	",
                    columns=[], # Populated by callback
                    page_size=15,
                    style_table={	"overflowX	": 	"auto	"},
                    style_cell={
                        "backgroundColor": "#2a2a2a",
                        "color": light_text,
                        "border": f"1px solid {light_text}",
                        "textAlign": "left",
                        "padding": "5px",
                        "fontFamily": "Poppins, sans-serif",
                        "minWidth": "100px", "width": "150px", "maxWidth": "300px",
                        "whiteSpace": "normal",
                        "height": "auto",
                    },
                    style_header={
                        "backgroundColor": "#0d6efd",
                        "fontWeight": "bold",
                        "color": light_text,
                        "fontFamily": "Poppins, sans-serif",
                    },
                    style_data_conditional=[
                        {
                            "if": {	"row_index	": 	"odd	"},
                            "backgroundColor": "#333333"
                        }
                    ],
                    filter_action="native",
                    sort_action="native",
                    sort_mode="multi",
                )
            ], width=12)
        ])
    return html.P("Selecione uma aba")

# Update Graphs and KPIs based on filters and fetched data
@callback(
    [
     # Visão Geral KPIs
     Output(	"kpi-sla-res-atingido	", "children"),
     Output(	"kpi-sla-res-violado	", "children"),
     Output(	"kpi-sla-resp-atingido	", "children"),
     Output(	"kpi-sla-resp-violado	", "children"),
     # Visão Geral Graphs
     Output(	"graph-sla-res-projeto	", "figure"),
     Output(	"graph-tickets-tipo	", "figure"),
     Output(	"graph-tickets-prioridade	", "figure"),
     Output(	"graph-tickets-status-cat	", "figure"),
     # SLA Perf Graphs
     Output(	"graph-top-5-violacoes-res	", "figure"),
     Output(	"graph-timeline-violacoes-res	", "figure"),
     Output(	"graph-sla-resp-projeto	", "figure"),
     Output(	"graph-timeline-violacoes-resp	", "figure"),
     # Tempos KPIs
     Output(	"kpi-lead-time	", "children"),
     Output(	"kpi-aging	", "children"),
     Output(	"kpi-em-risco	", "children"),
     Output(	"kpi-aguardando	", "children"),
     # Tempos Graphs
     Output(	"graph-lead-time-projeto	", "figure"),
     Output(	"graph-aging-tribo	", "figure"),
     Output(	"graph-tempo-medio-status	", "figure"), # Placeholder
     # Dados Table
     Output(	"data-table	", "data"),
     Output(	"data-table	", "columns")
    ],
    [
     Input(	"intermediate-data-store	", "data"), # Use data fetched from Supabase
     Input(	"project-dropdown	", "value"),
     Input(	"tribo-dropdown	", "value"),
     Input(	"period-dropdown	", "value"),
     Input(	"period-value-dropdown	", "value")
    ]
)
def update_dashboard(data, selected_project, selected_tribo, period_type, selected_period_value):
    if not data:
        print("Update dashboard: No data available.")
        empty_fig = go.Figure(layout=go.Layout(template=plotly_template, font_family="Poppins, sans-serif"))
        no_data_text = "N/A"
        # Ensure the number of outputs matches the callback definition
        return ([no_data_text]*4 + [empty_fig]*8 + [no_data_text]*4 + [empty_fig]*3 + [[], []])

    df = pd.DataFrame(data)
    print(f"Updating dashboard with {df.shape[0]} rows.")
    # Convert relevant columns back to numeric/datetime after JSON conversion
    num_cols = ["SLA_Horas_Resolucao", "HorasResolucao_Calculated", "SLA_Horas_Primeira_Resposta",
                "HorasPrimeiraResposta_Original", "Aging_Horas", "LeadTime_Horas",
                "Ano_Criacao", "Trimestre_Criacao"]
    date_cols = ["Criado", "Resolvido_Piloto", "Atualizado(a)"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in date_cols:
         if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Ensure time period columns exist (might be missing if initial fetch failed)
    if 	"Criado	" in df.columns and pd.api.types.is_datetime64_any_dtype(df[	"Criado	"]):
        if 	"Ano_Criacao	" not in df.columns:
            df[	"Ano_Criacao	"] = df[	"Criado	"].dt.year
        if 	"Trimestre_Criacao	" not in df.columns:
            df[	"Trimestre_Criacao	"] = df[	"Criado	"].dt.quarter
        if 	"Mes_Ano_Criacao	" not in df.columns:
            df[	"Mes_Ano_Criacao	"] = df[	"Criado	"].dt.strftime("%Y-%m")

    # --- Apply Filters ---
    filtered_df = df.copy()
    if selected_project and selected_project != "all":
        filtered_df = filtered_df[filtered_df[	"Projeto	"] == selected_project]
    if selected_tribo and selected_tribo != "all":
        filtered_df = filtered_df[filtered_df[	"Unidade de Negócio	"] == selected_tribo]
    if selected_period_value:
        try:
            if period_type == "year" and 	"Ano_Criacao	" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df[	"Ano_Criacao	"] == int(selected_period_value)]
            elif period_type == "quarter" and 	"Ano_Criacao	" in filtered_df.columns and 	"Trimestre_Criacao	" in filtered_df.columns:
                year, quarter = selected_period_value.split("-Q")
                filtered_df = filtered_df[(filtered_df[	"Ano_Criacao	"] == int(year)) & (filtered_df[	"Trimestre_Criacao	"] == int(quarter))]
            elif period_type == "month" and 	"Mes_Ano_Criacao	" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df[	"Mes_Ano_Criacao	"] == selected_period_value]
        except Exception as filter_err:
             print(f"Error applying period filter ({period_type}={selected_period_value}): {filter_err}")
             supabase_client.log_event("error", f"Error applying period filter: {filter_err}")
             # Optionally return empty figures or skip filtering

    print(f"Filtered data: {filtered_df.shape[0]} rows.")

    # --- Recalculate KPIs & Figures (Logic remains largely the same as previous version) ---
    # ... (KPI calculation logic) ...
    # Resolution SLA
    sla_res_applicable = filtered_df[filtered_df[	"CumpriuSLA_Resolucao_Calculated	"].isin(["Sim", "Não"])]
    total_res_sla = len(sla_res_applicable)
    perc_res_atingido = (sla_res_applicable[	"CumpriuSLA_Resolucao_Calculated	"] == "Sim").sum() / total_res_sla * 100 if total_res_sla > 0 else 0
    perc_res_violado = (sla_res_applicable[	"CumpriuSLA_Resolucao_Calculated	"] == "Não").sum() / total_res_sla * 100 if total_res_sla > 0 else 0

    # First Response SLA
    sla_resp_applicable = filtered_df[filtered_df[	"CumpriuSLA_PrimeiraResposta_Calculated	"].isin(["Sim", "Não"])]
    total_resp_sla = len(sla_resp_applicable)
    perc_resp_atingido = (sla_resp_applicable[	"CumpriuSLA_PrimeiraResposta_Calculated	"] == "Sim").sum() / total_resp_sla * 100 if total_resp_sla > 0 else 0
    perc_resp_violado = (sla_resp_applicable[	"CumpriuSLA_PrimeiraResposta_Calculated	"] == "Não").sum() / total_resp_sla * 100 if total_resp_sla > 0 else 0

    # Other KPIs
    num_em_risco = filtered_df[filtered_df[	"Em_Risco	"] == "Sim"].shape[0]
    num_aguardando = filtered_df[filtered_df[	"Status_Categoria	"] == "Aguardando/Validação"].shape[0]
    avg_lead_time = filtered_df[	"LeadTime_Horas	"].mean()
    avg_aging = filtered_df.loc[filtered_df[	"Is_Open	"] == True, 	"Aging_Horas	"].mean()

    # Format KPI values
    kpi_res_atingido_val = f"{perc_res_atingido:.1f}%"
    kpi_res_violado_val = f"{perc_res_violado:.1f}%"
    kpi_resp_atingido_val = f"{perc_resp_atingido:.1f}%"
    kpi_resp_violado_val = f"{perc_resp_violado:.1f}%"
    kpi_lead_time_val = f"{avg_lead_time:.1f}" if pd.notna(avg_lead_time) else "N/A"
    kpi_aging_val = f"{avg_aging:.1f}" if pd.notna(avg_aging) else "N/A"
    kpi_risco_val = num_em_risco
    kpi_aguardando_val = num_aguardando

    # ... (Figure generation logic - same as before) ...
    common_layout_args = dict(template=plotly_template, font_family="Poppins, sans-serif")
    empty_fig = go.Figure(layout=common_layout_args)
    empty_fig.add_annotation(text="Sem dados para exibir com os filtros selecionados.",
                           xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)

    # Fig: SLA Res por Projeto
    sla_res_proj_fig = go.Figure(layout=common_layout_args)
    if not sla_res_applicable.empty and 	"Projeto	" in sla_res_applicable.columns:
        try:
            sla_by_project = sla_res_applicable.groupby(	"Projeto	")[	"CumpriuSLA_Resolucao_Calculated	"].value_counts().unstack(fill_value=0)
            if "Sim" not in sla_by_project.columns: sla_by_project["Sim"] = 0
            if "Não" not in sla_by_project.columns: sla_by_project["Não"] = 0
            sla_by_project = sla_by_project.sort_values("Não", ascending=False)
            sla_res_proj_fig.add_trace(go.Bar(name="Atingido", x=sla_by_project.index, y=sla_by_project["Sim"], marker_color=sla_atingido_color))
            sla_res_proj_fig.add_trace(go.Bar(name="Violado", x=sla_by_project.index, y=sla_by_project["Não"], marker_color=sla_violado_color))
        except Exception as e: print(f"Error graph sla_res_proj: {e}")
    else: sla_res_proj_fig = empty_fig
    sla_res_proj_fig.update_layout(title="SLA de Resolução por Projeto", barmode="group")

    # Fig: Tickets por Tipo
    if not filtered_df.empty and 	"Tipo de item	" in filtered_df.columns:
        type_counts = filtered_df[	"Tipo de item	"].value_counts()
        if not type_counts.empty:
            tickets_tipo_fig = px.pie(values=type_counts.values, names=type_counts.index,
                                      title="Distribuição por Tipo de Item", **common_layout_args, hole=0.3)
            tickets_tipo_fig.update_traces(textinfo="percent+label", marker=dict(line=dict(color="#000000", width=1)))
        else: tickets_tipo_fig = empty_fig
    else: tickets_tipo_fig = empty_fig

    # Fig: Tickets por Prioridade
    if not filtered_df.empty and 	"Prioridade	" in filtered_df.columns:
        priority_counts = filtered_df[	"Prioridade	"].value_counts()
        if not priority_counts.empty:
            tickets_prio_fig = px.bar(x=priority_counts.index, y=priority_counts.values,
                                      title="Distribuição por Prioridade", labels={"x": "Prioridade", "y": "Contagem"},
                                      **common_layout_args, color_discrete_sequence=px.colors.qualitative.Pastel)
        else: tickets_prio_fig = empty_fig
    else: tickets_prio_fig = empty_fig

    # Fig: Tickets por Status Categoria
    if not filtered_df.empty and 	"Status_Categoria	" in filtered_df.columns:
        status_cat_counts = filtered_df[	"Status_Categoria	"].value_counts()
        if not status_cat_counts.empty:
            status_colors = { # Map categories to consistent colors
                "Fechado": pendente_color,
                "Em Progresso": em_risco_color,
                "Aguardando/Validação": aguardando_color,
                "Desconhecido": "#FFFFFF" # Add color for unknown
            }
            tickets_status_fig = px.bar(x=status_cat_counts.index, y=status_cat_counts.values,
                                        title="Distribuição por Categoria de Status",
                                        labels={"x": "Categoria", "y": "Contagem"}, **common_layout_args,
                                        color=status_cat_counts.index, color_discrete_map=status_colors)
        else: tickets_status_fig = empty_fig
    else: tickets_status_fig = empty_fig

    # Fig: Top 5 Violações Res
    if not filtered_df.empty and 	"SLA_Violado_Calculated	" in filtered_df.columns and 	"Projeto	" in filtered_df.columns:
        violation_res_counts = filtered_df[filtered_df[	"SLA_Violado_Calculated	"] == "Sim"][	"Projeto	"].value_counts().head(5)
        if not violation_res_counts.empty:
            top_5_viol_res_fig = px.bar(x=violation_res_counts.index, y=violation_res_counts.values,
                                        title="Top 5 Projetos com Violações SLA Resolução",
                                        labels={"x": "Projeto", "y": "Violações"}, **common_layout_args,
                                        color_discrete_sequence=[sla_violado_color])
        else: top_5_viol_res_fig = empty_fig
    else: top_5_viol_res_fig = empty_fig

    # Fig: Timeline Violações Res
    if not filtered_df.empty and 	"SLA_Violado_Calculated	" in filtered_df.columns and 	"Mes_Ano_Criacao	" in filtered_df.columns:
        timeline_res = filtered_df[filtered_df[	"SLA_Violado_Calculated	"] == "Sim"].groupby(	"Mes_Ano_Criacao	").size().reset_index(name="Count")
        if not timeline_res.empty:
            timeline_viol_res_fig = px.line(timeline_res, x=	"Mes_Ano_Criacao	", y="Count",
                                            title="Timeline Violações SLA Resolução", markers=True,
                                            labels={	"Mes_Ano_Criacao	": "Mês Criação", "Count": "Violações"}, **common_layout_args)
            timeline_viol_res_fig.update_traces(line_color=sla_violado_color)
            timeline_viol_res_fig.update_layout(xaxis_type="category")
        else: timeline_viol_res_fig = empty_fig
    else: timeline_viol_res_fig = empty_fig

    # Fig: SLA Resp por Projeto
    sla_resp_proj_fig = go.Figure(layout=common_layout_args)
    if not sla_resp_applicable.empty and 	"Projeto	" in sla_resp_applicable.columns:
        try:
            sla_resp_by_project = sla_resp_applicable.groupby(	"Projeto	")[	"CumpriuSLA_PrimeiraResposta_Calculated	"].value_counts().unstack(fill_value=0)
            if "Sim" not in sla_resp_by_project.columns: sla_resp_by_project["Sim"] = 0
            if "Não" not in sla_resp_by_project.columns: sla_resp_by_project["Não"] = 0
            sla_resp_by_project = sla_resp_by_project.sort_values("Não", ascending=False)
            sla_resp_proj_fig.add_trace(go.Bar(name="Atingido", x=sla_resp_by_project.index, y=sla_resp_by_project["Sim"], marker_color=sla_atingido_color))
            sla_resp_proj_fig.add_trace(go.Bar(name="Violado", x=sla_resp_by_project.index, y=sla_resp_by_project["Não"], marker_color=sla_violado_color))
        except Exception as e: print(f"Error graph sla_resp_proj: {e}")
    else: sla_resp_proj_fig = empty_fig
    sla_resp_proj_fig.update_layout(title="SLA de Primeira Resposta por Projeto", barmode="group")

    # Fig: Timeline Violações Resp
    if not filtered_df.empty and 	"CumpriuSLA_PrimeiraResposta_Calculated	" in filtered_df.columns and 	"Mes_Ano_Criacao	" in filtered_df.columns:
        timeline_resp = filtered_df[filtered_df[	"CumpriuSLA_PrimeiraResposta_Calculated	"] == "Não"].groupby(	"Mes_Ano_Criacao	").size().reset_index(name="Count")
        if not timeline_resp.empty:
            timeline_viol_resp_fig = px.line(timeline_resp, x=	"Mes_Ano_Criacao	", y="Count",
                                             title="Timeline Violações SLA Primeira Resposta", markers=True,
                                             labels={	"Mes_Ano_Criacao	": "Mês Criação", "Count": "Violações"}, **common_layout_args)
            timeline_viol_resp_fig.update_traces(line_color=sla_violado_color)
            timeline_viol_resp_fig.update_layout(xaxis_type="category")
        else: timeline_viol_resp_fig = empty_fig
    else: timeline_viol_resp_fig = empty_fig

    # Fig: Lead Time Médio por Projeto
    if not filtered_df.empty and 	"LeadTime_Horas	" in filtered_df.columns and 	"Projeto	" in filtered_df.columns:
        lead_time_proj = filtered_df.groupby(	"Projeto	")[	"LeadTime_Horas	"].mean().reset_index().dropna()
        if not lead_time_proj.empty:
            lead_time_proj_fig = px.bar(lead_time_proj.sort_values(	"LeadTime_Horas	", ascending=False),
                                        x=	"Projeto	", y=	"LeadTime_Horas	",
                                        title="Lead Time Médio por Projeto (Tickets Fechados)",
                                        labels={	"LeadTime_Horas	": "Lead Time Médio (Horas)"}, **common_layout_args,
                                        color=	"LeadTime_Horas	", color_continuous_scale=px.colors.sequential.Blues)
        else: lead_time_proj_fig = empty_fig
    else: lead_time_proj_fig = empty_fig

    # Fig: Aging Médio por Tribo
    if not filtered_df.empty and 	"Is_Open	" in filtered_df.columns and 	"Unidade de Negócio	" in filtered_df.columns and 	"Aging_Horas	" in filtered_df.columns:
        aging_tribo = filtered_df[filtered_df[	"Is_Open	"] == True].groupby(	"Unidade de Negócio	")[	"Aging_Horas	"].mean().reset_index().dropna()
        if not aging_tribo.empty:
            aging_tribo_fig = px.bar(aging_tribo.sort_values(	"Aging_Horas	", ascending=False),
                                     x=	"Unidade de Negócio	", y=	"Aging_Horas	",
                                     title="Aging Médio por Tribo (Tickets Abertos)",
                                     labels={	"Unidade de Negócio	": "Tribo", 	"Aging_Horas	": "Aging Médio (Horas)"}, **common_layout_args,
                                     color=	"Aging_Horas	", color_continuous_scale=px.colors.sequential.YlOrRd)
        else: aging_tribo_fig = empty_fig
    else: aging_tribo_fig = empty_fig

    # Fig: Tempo Médio por Status (Placeholder)
    tempo_medio_status_fig = go.Figure(layout=common_layout_args)
    tempo_medio_status_fig.update_layout(title="Tempo Médio por Status (Placeholder)")
    tempo_medio_status_fig.add_annotation(text="Gráfico de tempo médio por status requer dados de transição.",
                                          xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)

    # --- Prepare Data Table --- #
    table_data = []
    table_columns = []
    if not filtered_df.empty:
        display_columns = [
            "Chave", "Resumo", "Projeto", "Unidade de Negócio", "Status", "Prioridade", "Tipo de item",
            "Criado", "Resolvido_Piloto", "Aging_Horas", "LeadTime_Horas",
            "CumpriuSLA_Resolucao_Calculated", "CumpriuSLA_PrimeiraResposta_Calculated", "Em_Risco"
        ]
        table_df = filtered_df[[col for col in display_columns if col in filtered_df.columns]].copy()
        for col in ["Aging_Horas", "LeadTime_Horas"]:
            if col in table_df.columns:
                table_df[col] = table_df[col].round(1)
        # Convert dates back to string for display if they are datetime objects
        for col in ["Criado", "Resolvido_Piloto"]:
             if col in table_df.columns and pd.api.types.is_datetime64_any_dtype(table_df[col]):
                  table_df[col] = table_df[col].dt.strftime("%Y-%m-%d %H:%M") # Format for display

        table_df.rename(columns={
            "Unidade de Negócio": "Tribo",
            "CumpriuSLA_Resolucao_Calculated": "SLA Resolução",
            "CumpriuSLA_PrimeiraResposta_Calculated": "SLA 1ª Resp.",
            "Resolvido_Piloto": "Resolvido"
        }, inplace=True)
        # Replace None/NaN with empty string for display
        table_df = table_df.fillna("")
        table_data = table_df.to_dict("records")
        table_columns = [{"name": i, "id": i} for i in table_df.columns]

    print("Dashboard update complete.")
    return (
        kpi_res_atingido_val, kpi_res_violado_val, kpi_resp_atingido_val, kpi_resp_violado_val,
        sla_res_proj_fig, tickets_tipo_fig, tickets_prio_fig, tickets_status_fig,
        top_5_viol_res_fig, timeline_viol_res_fig, sla_resp_proj_fig, timeline_viol_resp_fig,
        kpi_lead_time_val, kpi_aging_val, kpi_risco_val, kpi_aguardando_val,
        lead_time_proj_fig, aging_tribo_fig, tempo_medio_status_fig,
        table_data, table_columns
    )

# --- Run App ---
if __name__ == "__main__":
    # Use port from environment variable PORT for Render compatibility, default to 8050
    port = int(os.environ.get("PORT", 8050))
    # Use 0.0.0.0 to be accessible externally
    app.run(debug=False, host="0.0.0.0", port=port)

