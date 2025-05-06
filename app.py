from supabase_client import init_supabase_client, fetch_all_tickets_data, log_event
from data_processor import process_uploaded_files
# ... demais imports dash ...

# inicializa supabase
init_supabase_client()

# pega dados iniciais
raw_tickets = fetch_all_tickets_data()
# raw_tickets será lista de dicts. Converta para df se precisar.

# dentro do seu callback de upload:
@app.callback(
    [
      Output("alert-upload", "children"),
      # ...
    ],
    [Input("upload-piloto", "contents"),
     Input("upload-sla", "contents"),
     Input("upload-piloto", "filename"),
     Input("upload-sla", "filename")],
)
def handle_upload(piloto_content, sla_content, piloto_name, sla_name):
    log_event("info", "Processing files start", {"piloto": piloto_name, "sla": sla_name})
    try:
        df = process_uploaded_files(piloto_content, sla_content)
        # salvar no supabase: supabase_client.supabase.table("tickets")...
        log_event("info", "Files processed", {"records": len(df)})
        return "", # limpa alert de erro
    except Exception as e:
        log_event("error", f"Error in processing: {e}", {"piloto": piloto_name, "sla": sla_name})
        return f"Erro: {e}"
