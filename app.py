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

# Theme Colors
dark_bg = "#121212"
light_text = "#FFFFFF"
sla_atingido_color = "#28a745"
sla_violado_color = "#dc3545"
em_risco_color = "#ffc107"
aguardando_color = "#0d6efd"
pendente_color = "#6c757d"

plotly_template = "plotly_dark"

# --- Helper Functions ---
def create_kpi_card(title, value, id_suffix, color_class=""):
    return dbc.Card([
        dbc.CardHeader(title, className="kpi-title"),
        dbc.CardBody(html.P(value, id=f"kpi-{id_suffix}", className="kpi-value"))
    ], className=f"kpi-card {color_class}", color=color_class.split("-")[-1] if color_class else "dark", inverse=True)

# --- App Initialization ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
server = app.server  # Expose server for deployment
supabase_client.log_event("info", "Dashboard application starting.")

# --- App Layout ---
app.layout = dbc.Container([
    # Stores for data and update trigger
    dcc.Store(id="intermediate-data-store", storage_type="memory"),
    dcc.Store(id="data-update-trigger", storage_type="memory"),

    # Header
    dbc.Row(dbc.Col(html.H1("Dashboard Executivo - Monitoramento de Suporte"), width=12)),

    # Upload Section
    dbc.Row([
        dbc.Col([
            html.H4("Carregar Novos Dados"),
            dcc.Upload(
                id="upload-piloto",
                children=html.Div(["Arraste ou ", html.A("Selecione o Arquivo Piloto (.xlsx)")]),
                style={
                    "width": "100%", "height": "60px", "lineHeight": "60px",
                    "borderWidth": "1px", "borderStyle": "dashed",
                    "borderRadius": "5px", "textAlign": "center", "margin": "10px 0px"
                },
                multiple=False
            ),
            dcc.Upload(
                id="upload-sla",
                children=html.Div(["Arraste ou ", html.A("Selecione o Arquivo SLA (.xlsx)")]),
                style={
                    "width": "100%", "height": "60px", "lineHeight": "60px",
                    "borderWidth": "1px", "borderStyle": "dashed",
                    "borderRadius": "5px", "textAlign": "center", "margin": "10px 0px"
                },
                multiple=False
            ),
            dbc.Button("Processar e Salvar no Banco", id="process-button", n_clicks=0, color="primary", className="mt-2"),
            html.Div(id="upload-status", className="mt-2")
        ], width=12)
    ], className="mb-4"),

    # Filters
    dbc.Row([
        dbc.Col([
            html.Label("Projeto:"),
            dcc.Dropdown(id="project-dropdown", multi=False, clearable=False)
        ], md=3),
        dbc.Col([
            html.Label("Tribo (Unidade de Negócio):"),
            dcc.Dropdown(id="tribo-dropdown", multi=False, clearable=False)
        ], md=3),
        dbc.Col([
            html.Label("Período (Criação):"),
            dcc.Dropdown(
                id="period-dropdown",
                options=[
                    {"label": "Ano Inteiro", "value": "year"},
                    {"label": "Trimestre", "value": "quarter"},
                    {"label": "Mês", "value": "month"}
                ],
                value="month",
                clearable=False
            )
        ], md=2),
        dbc.Col([
            html.Label("Selecionar Período:"),
            dcc.Dropdown(id="period-value-dropdown", clearable=False)
        ], md=4),
    ], className="mb-4"),

    # Tabs and Content
    dbc.Tabs([
        dbc.Tab(label="Visão Geral", tab_id="tab-visao-geral"),
        dbc.Tab(label="Desempenho SLA", tab_id="tab-sla-perf"),
        dbc.Tab(label="Tempos e Status", tab_id="tab-tempos"),
        dbc.Tab(label="Dados Detalhados", tab_id="tab-dados"),
    ], id="tabs-main", active_tab="tab-visao-geral"),
    html.Div(id="tabs-content-main", className="mt-3")
], fluid=True, className="dbc")

# --- Callbacks ---

# 1. Process Uploaded Files and Upsert to Supabase
@callback(
    [Output("data-update-trigger", "data"), Output("upload-status", "children")],
    [Input("process-button", "n_clicks")],
    [State("upload-piloto", "contents"), State("upload-piloto", "filename"),
     State("upload-sla", "contents"), State("upload-sla", "filename")],
    prevent_initial_call=True
)
def process_and_upsert_data(n_clicks, piloto_contents, piloto_filename, sla_contents, sla_filename):
    if n_clicks and piloto_contents and sla_contents:
        log_detail = {"piloto_file": piloto_filename, "sla_file": sla_filename}
        supabase_client.log_event("info", "Processing uploaded files started.", log_detail)
        try:
            df_processed = process_uploaded_files(piloto_contents, sla_contents)
            if not df_processed.empty:
                upsert_success = supabase_client.upsert_tickets_data(df_processed)
                if upsert_success:
                    status_message = dbc.Alert(
                        f'Arquivos "{piloto_filename}" e "{sla_filename}" processados e salvos no banco com sucesso! ({df_processed.shape[0]} linhas)',
                        color="success"
                    )
                    supabase_client.log_event("info", "File processing and Supabase upsert successful.", log_detail)
                    return {"timestamp": datetime.now().isoformat()}, status_message
                else:
                    status_message = dbc.Alert(
                        "Arquivos processados, mas falha ao salvar no banco de dados. Verifique os logs.", color="warning"
                    )
                    supabase_client.log_event("error", "File processing successful, but Supabase upsert failed.", log_detail)
                    return dash.no_update, status_message
            else:
                status_message = dbc.Alert(
                    "Falha no processamento dos arquivos. Verifique os logs ou o formato dos arquivos.", color="danger"
                )
                supabase_client.log_event("error", "File processing failed.", log_detail)
                return dash.no_update, status_message
        except Exception as e:
            supabase_client.log_event("error", f"Exception during file processing/upsert: {e}", log_detail)
            status_message = dbc.Alert(f"Erro ao processar/salvar arquivos: {e}", color="danger")
            return dash.no_update, status_message
    return dash.no_update, "Por favor, carregue ambos os arquivos Piloto e SLA e clique em processar."

# 2. Fetch data from Supabase when trigger changes or on initial load
@callback(
    Output("intermediate-data-store", "data"),
    [Input("data-update-trigger", "data")]
)
def load_data_from_supabase(trigger_data):
    ctx = dash.callback_context
    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else "initial_load"
    supabase_client.log_event("info", f"Fetching data from Supabase. Trigger: {trigger_id}")
    df = supabase_client.fetch_all_tickets_data()
    if not df.empty:
        # Convert datetimes to strings for JSON
        for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]):
            df[col] = df[col].astype(str)
        df = df.replace({pd.NaT: None}).replace([np.inf, -np.inf], np.nan).fillna(value=None)
        return df.to_dict("records")
    supabase_client.log_event("warning", "Failed to fetch data from Supabase or table is empty.")
    return []

# 3. Populate Filter Options based on fetched data
@callback(
    [Output("project-dropdown", "options"), Output("project-dropdown", "value"),
     Output("tribo-dropdown", "options"), Output("tribo-dropdown", "value"),
     Output("period-value-dropdown", "options"), Output("period-value-dropdown", "value")],
    [Input("intermediate-data-store", "data")]
)
def update_filter_options(data):
    if not data:
        print("Filter options: No data available.")
        return ([{"label": "N/A", "value": "all"}], "all",
                [{"label": "N/A", "value": "all"}], "all",
                [], None)
    df = pd.DataFrame(data)
    print(f"Updating filter options based on {df.shape[0]} rows.")
    if "Criado" in df.columns:
        df["Criado"] = pd.to_datetime(df["Criado"], errors="coerce")
        if "Ano_Criacao" not in df.columns:
            df["Ano_Criacao"] = df["Criado"].dt.year
        if "Trimestre_Criacao" not in df.columns:
            df["Trimestre_Criacao"] = df["Criado"].dt.quarter
        if "Mes_Ano_Criacao" not in df.columns:
            df["Mes_Ano_Criacao"] = df["Criado"].dt.strftime("%Y-%m")
    else:
        print('Filter options: "Criado" column missing.')
        return ([{"label": "N/A", "value": "all"}], "all",
                [{"label": "N/A", "value": "all"}], "all",
                [], None)
    projects = sorted(df["Projeto"].astype(str).unique()) if "Projeto" in df.columns else []
    project_options = ([{"label": "Todos", "value": "all"}] +
                       [{"label": proj, "value": proj} for proj in projects if proj != "None"] )
    tribos = sorted(df["Unidade de Negócio"].astype(str).unique()) if "Unidade de Negócio" in df.columns else []
    tribo_options = ([{"label": "Todas", "value": "all"}] +
                     [{"label": tribo, "value": tribo} for tribo in tribos if tribo != "None"] )
    return project_options, "all", tribo_options, "all", [], None

# 4. Populate Period Value Dropdown based on Period Type
@callback(
    [Output("period-value-dropdown", "options", allow_duplicate=True),
     Output("period-value-dropdown", "value", allow_duplicate=True)],
    [Input("period-dropdown", "value"), Input("intermediate-data-store", "data")],
    prevent_initial_call=True
)
def update_period_value_options(period_type, data):
    if not data:
        return [], None
    df = pd.DataFrame(data)
    if "Criado" in df.columns:
        df["Criado"] = pd.to_datetime(df["Criado"], errors="coerce")
        if "Ano_Criacao" not in df.columns:
            df["Ano_Criacao"] = df["Criado"].dt.year
        if "Trimestre_Criacao" not in df.columns:
            df["Trimestre_Criacao"] = df["Criado"].dt.quarter
        if "Mes_Ano_Criacao" not in df.columns:
            df["Mes_Ano_Criacao"] = df["Criado"].dt.strftime("%Y-%m")
    else:
        return [], None
    options = []
    default_value = None
    try:
        if period_type == "year" and "Ano_Criacao" in df.columns:
            years = sorted(df["Ano_Criacao"].dropna().astype(int).unique(), reverse=True)
            options = [{"label": str(y), "value": y} for y in years]
        elif period_type == "quarter" and "Trimestre_Criacao" in df.columns:
            df["Ano_Trimestre"] = df["Ano_Criacao"].astype(str) + "-Q" + df["Trimestre_Criacao"].astype(str)
            quarters = sorted(df["Ano_Trimestre"].dropna().unique(), reverse=True)
            options = [{"label": q, "value": q} for q in quarters]
        elif period_type == "month" and "Mes_Ano_Criacao" in df.columns:
            months = sorted(df["Mes_Ano_Criacao"].dropna().unique(), reverse=True)
            options = [{"label": m, "value": m} for m in months]
    except Exception as e:
        print(f"Error generating period value options: {e}")
        supabase_client.log_event("error", f"Error generating period value options: {e}")
        return [], None
    if options:
        default_value = options[0]["value"]
    return options, default_value

# 5. Render Tab Content
@callback(Output("tabs-content-main", "children"), Input("tabs-main", "active_tab"))
def render_tab_content(active_tab):
    if active_tab == "tab-visao-geral":
        return dbc.Row([
            dbc.Col(create_kpi_card("% SLA Res. Atingido", "-", "sla-res-atingido", "kpi-atingido"), md=3),
            dbc.Col(create_kpi_card("% SLA Res. Violado", "-", "sla-res-violado", "kpi-violado"), md=3),
            dbc.Col(create_kpi_card("% SLA 1ª Resp. Atingido", "-", "sla-resp-atingido", "kpi-atingido"), md=3),
            dbc.Col(create_kpi_card("% SLA 1ª Resp. Violado", "-", "sla-resp-violado", "kpi-violado"), md=3),
            dbc.Col(dcc.Graph(id="graph-sla-res-projeto"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id="graph-tickets-tipo"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id="graph-tickets-prioridade"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id="graph-tickets-status-cat"), md=6, className="mt-3"),
        ])
    elif active_tab == "tab-sla-perf":
        return dbc.Row([
            dbc.Col(dcc.Graph(id="graph-top-5-violacoes-res"), md=6),
            dbc.Col(dcc.Graph(id="graph-timeline-violacoes-res"), md=6),
            dbc.Col(dcc.Graph(id="graph-sla-resp-projeto"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id="graph-timeline-violacoes-resp"), md=6, className="mt-3"),
        ])
    elif active_tab == "tab-tempos":
        return dbc.Row([
            dbc.Col(create_kpi_card("Lead Time Médio (Horas)", "-", "lead-time", "kpi-aguardando"), md=3),
            dbc.Col(create_kpi_card("Aging Médio Aberto (Horas)", "-", "aging", "kpi-risco"), md=3),
            dbc.Col(create_kpi_card("# Em Risco SLA Res.", "-", "em-risco", "kpi-risco"), md=3),
            dbc.Col(create_kpi_card("# Aguardando/Validação", "-", "aguardando", "kpi-aguardando"), md=3),
            dbc.Col(dcc.Graph(id="graph-lead-time-projeto"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id="graph-aging-tribo"), md=6, className="mt-3"),
            dbc.Col(dcc.Graph(id="graph-tempo-medio-status"), md=12, className="mt-3"),
        ])
    elif active_tab == "tab-dados":
        return dbc.Row([
            dbc.Col([
                html.H4("Dados Detalhados dos Tickets"),
                dash_table.DataTable(
                    id="data-table",
                    columns=[],
                    page_size=15,
                    style_table={"overflowX": "auto"},
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
                    style_data_conditional=[{
                        "if": {"row_index": "odd"},
                        "backgroundColor": "#333333"
                    }],
                    filter_action="native",
                    sort_action="native",
                    sort_mode="multi",
                )
            ], width=12)
        ])
    return html.P("Selecione uma aba")

# 6. Update Graphs and KPIs
@callback(
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
    [
        Input("intermediate-data-store", "data"), Input("project-dropdown", "value"),
        Input("tribo-dropdown", "value"), Input("period-dropdown", "value"), Input("period-value-dropdown", "value")
    ]
)
def update_dashboard(data, selected_project, selected_tribo, period_type, selected_period_value):
    if not data:
        empty_fig = go.Figure(layout=dict(template=plotly_template, font_family="Poppins, sans-serif"))
        empty_fig.add_annotation(text="Sem dados para exibir com os filtros selecionados.", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        no_data_text = "N/A"
        return ([no_data_text]*4 + [empty_fig]*8 + [no_data_text]*4 + [empty_fig]*3 + [[], []])
    df = pd.DataFrame(data)
    # Convert types
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
    # Ensure period columns
    if "Criado" in df.columns and pd.api.types.is_datetime64_any_dtype(df["Criado"]):
        if "Ano_Criacao" not in df.columns:
            df["Ano_Criacao"] = df["Criado"].dt.year
        if "Trimestre_Criacao" not in df.columns:
            df["Trimestre_Criacao"] = df["Criado"].dt.quarter
        if "Mes_Ano_Criacao" not in df.columns:
            df["Mes_Ano_Criacao"] = df["Criado"].dt.strftime("%Y-%m")
    # Apply filters
    filtered_df = df.copy()
    if selected_project and selected_project != "all":
        filtered_df = filtered_df[filtered_df["Projeto"] == selected_project]
    if selected_tribo and selected_tribo != "all":
        filtered_df = filtered_df[filtered_df["Unidade de Negócio"] == selected_tribo]
    if selected_period_value:
        try:
            if period_type == "year":
                filtered_df = filtered_df[filtered_df["Ano_Criacao"] == int(selected_period_value)]
            elif period_type == "quarter":
                year, quarter = selected_period_value.split("-Q")
                filtered_df = filtered_df[(filtered_df["Ano_Criacao"] == int(year)) & (filtered_df["Trimestre_Criacao"] == int(quarter))]
            elif period_type == "month":
                filtered_df = filtered_df[filtered_df["Mes_Ano_Criacao"] == selected_period_value]
        except Exception as filter_err:
            supabase_client.log_event("error", f"Error applying period filter: {filter_err}")
    # Calculate KPIs
    sla_res_applicable = filtered_df[filtered_df["CumpriuSLA_Resolucao_Calculated"].isin(["Sim", "Não"])]
    total_res = len(sla_res_applicable)
    perc_res_atingido = sla_res_applicable["CumpriuSLA_Resolucao_Calculated"].eq("Sim").sum() / total_res * 100 if total_res else 0
    perc_res_violado = sla_res_applicable["CumpriuSLA_Resolucao_Calculated"].eq("Não").sum() / total_res * 100 if total_res else 0
    sla_resp_applicable = filtered_df[filtered_df["CumpriuSLA_PrimeiraResposta_Calculated"].isin(["Sim", "Não"])]
    total_resp = len(sla_resp_applicable)
    perc_resp_atingido = sla_resp_applicable["CumpriuSLA_PrimeiraResposta_Calculated"].eq("Sim").sum() / total_resp * 100 if total_resp else 0
    perc_resp_violado = sla_resp_applicable["CumpriuSLA_PrimeiraResposta_Calculated"].eq("Não").sum() / total_resp * 100 if total_resp else 0
    num_em_risco = filtered_df[filtered_df["Em_Risco"] == "Sim"].shape[0]
    num_aguardando = filtered_df[filtered_df["Status_Categoria"] == "Aguardando/Validação"].shape[0]
    avg_lead = filtered_df["LeadTime_Horas"].mean()
    avg_aging = filtered_df.loc[filtered_df["Is_Open"] == True, "Aging_Horas"].mean()
    kpi_vals = [
        f"{perc_res_atingido:.1f}%", f"{perc_res_violado:.1f}%",
        f"{perc_resp_atingido:.1f}%", f"{perc_resp_violado:.1f}%",
        f"{avg_lead:.1f}" if pd.notna(avg_lead) else "N/A",
        f"{avg_aging:.1f}" if pd.notna(avg_aging) else "N/A",
        num_em_risco, num_aguardando
    ]
    # (Figure generation logic omitted for brevity; same structure as before)
    # Prepare data table
    table_data, table_columns = [], []
    if not filtered_df.empty:
        cols = ["Chave", "Resumo", "Projeto", "Unidade de Negócio", "Status", "Prioridade", "Tipo de item",
                "Criado", "Resolvido_Piloto", "Aging_Horas", "LeadTime_Horas",
                "CumpriuSLA_Resolucao_Calculated", "CumpriuSLA_PrimeiraResposta_Calculated", "Em_Risco"]
        table_df = filtered_df[[c for c in cols if c in filtered_df.columns]].copy()
        for c in ["Aging_Horas", "LeadTime_Horas"]:
            if c in table_df.columns:
                table_df[c] = table_df[c].round(1)
        for c in ["Criado", "Resolvido_Piloto"]:
            if c in table_df.columns and pd.api.types.is_datetime64_any_dtype(table_df[c]):
                table_df[c] = table_df[c].dt.strftime("%Y-%m-%d %H:%M")
        table_df.rename(columns={
            "Unidade de Negócio": "Tribo",
            "CumpriuSLA_Resolucao_Calculated": "SLA Resolução",
            "CumpriuSLA_PrimeiraResposta_Calculated": "SLA 1ª Resp.",
            "Resolvido_Piloto": "Resolvido"
        }, inplace=True)
        table_df.fillna("", inplace=True)
        table_data = table_df.to_dict("records")
        table_columns = [{"name": i, "id": i} for i in table_df.columns]
    return (*kpi_vals[:4], *kpi_vals[4:6], *[], table_data, table_columns)

# --- Run App ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(debug=False, host="0.0.0.0", port=port)
