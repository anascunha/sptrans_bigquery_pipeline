# ingest/create_table_posicoes.py
import sys
import os
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

# === CORREÇÃO DE PATH E IMPORTS ===
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from core.config_loader import load_config
from core.bigquery_client import BigQueryClient  # Opcional, se usar cliente customizado

# === CARREGAR CONFIG ===
config_path = os.path.join(project_root, "core", "config.json")
config = load_config(config_path)
if config is None:
    raise FileNotFoundError(f"config.json não encontrado em {config_path}")

# === CLIENTE BIGQUERY ===
client = bigquery.Client.from_service_account_json(config['bigquery']['credentials_file'])
project_id = config['bigquery']['project_id']
dataset_id = config['bigquery']['dataset_id']
table_id = config['bigquery']['table_id']  # sptrans_posicoes
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# === SCHEMA ===
schema = [
    SchemaField("fetch_time", "TIMESTAMP", mode="NULLABLE"),
    SchemaField("hr", "STRING", mode="NULLABLE"),
    SchemaField("line_c", "STRING", mode="NULLABLE"),
    SchemaField("line_cl", "INTEGER", mode="NULLABLE"),
    SchemaField("line_sl", "INTEGER", mode="NULLABLE"),
    SchemaField("line_lt0", "STRING", mode="NULLABLE"),
    SchemaField("line_lt1", "STRING", mode="NULLABLE"),
    SchemaField("vehicle_p", "STRING", mode="NULLABLE"),
    SchemaField("vehicle_a", "BOOL", mode="NULLABLE"),
    SchemaField("vehicle_ta", "STRING", mode="NULLABLE"),
    SchemaField("vehicle_py", "FLOAT", mode="NULLABLE"),
    SchemaField("vehicle_px", "FLOAT", mode="NULLABLE")
]

# === CRIAR TABELA ===
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

try:
    client.get_table(table_ref)
    print(f"Tabela {full_table_id} já existe.")
except Exception:
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"Tabela {full_table_id} criada com sucesso.")