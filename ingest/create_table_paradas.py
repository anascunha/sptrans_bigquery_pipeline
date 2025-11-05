# ingest/create_table_paradas.py
import sys
import os
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from core.config_loader import load_config

config_path = os.path.join(project_root, "core", "config.json")
config = load_config(config_path)
if config is None:
    raise FileNotFoundError(f"config.json não encontrado em {config_path}")

client = bigquery.Client.from_service_account_json(config['bigquery']['credentials_file'])
project_id = config['bigquery']['project_id']
dataset_id = config['bigquery']['dataset_id']
table_id = "sptrans_paradas"
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

schema = [
    SchemaField("fetch_time", "TIMESTAMP", mode="NULLABLE"),
    SchemaField("line_c", "STRING", mode="NULLABLE"),
    SchemaField("cl", "INTEGER", mode="NULLABLE"),
    SchemaField("cp", "INTEGER", mode="NULLABLE"),
    SchemaField("np", "STRING", mode="NULLABLE"),
    SchemaField("py", "FLOAT", mode="NULLABLE"),
    SchemaField("px", "FLOAT", mode="NULLABLE")
]

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

try:
    client.get_table(table_ref)
    print(f"Tabela {full_table_id} já existe.")
except NotFound:
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"Tabela {full_table_id} criada com sucesso.")