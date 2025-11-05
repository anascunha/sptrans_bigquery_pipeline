# ingest/ingest_gtfs.py
import sys
import os
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery  # ADICIONADO: Import para LoadJobConfig

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from core.config_loader import load_config
from core.bigquery_client import BigQueryClient

config = load_config(os.path.join(project_root, "core", "config.json"))
bigquery_client = BigQueryClient(
    credentials_file=config['bigquery']['credentials_file'],
    project_id=config['bigquery']['project_id']
)

GTFS_PATH = os.path.join(project_root, "data", "gtfs")
LOAD_DATE = datetime.now().date().isoformat()

GTFS_FILES = {
    "agency": ("agency.txt", "gtfs_agency"),
    "calendar": ("calendar.txt", "gtfs_calendar"),
    "fare_attributes": ("fare_attributes.txt", "gtfs_fare_attributes"),
    "fare_rules": ("fare_rules.txt", "gtfs_fare_rules"),
    "frequencies": ("frequencies.txt", "gtfs_frequencies"),
    "routes": ("routes.txt", "gtfs_routes"),
    "shapes": ("shapes.txt", "gtfs_shapes"),
    "trips": ("trips.txt", "gtfs_trips"),
    "stops": ("stops.txt", "gtfs_stops"),
    "stop_times": ("stop_times.txt", "gtfs_stop_times")
}

def load_csv_to_bigquery(csv_path, table_id):
    """LOAD JOB direto de CSV (free tier)"""
    if not os.path.exists(csv_path):
        logging.warning(f"Arquivo não encontrado: {csv_path}")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Pula header
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,  # Usa schema da tabela (já criada)
        allow_quoted_newlines=True,  # Suporta aspas em campos
        field_delimiter=','
    )

    try:
        with open(csv_path, 'rb') as f:
            job = bigquery_client.client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()  # Espera
        row_count = job.output_rows
        logging.info(f"LOAD JOB CSV: {row_count} linhas em {table_id} (truncate)")
    except Exception as e:
        logging.error(f"Erro LOAD JOB CSV em {csv_path}: {e}")

def parse_and_load():
    logging.info("Carregando GTFS offline de CSV...")
    for file_name, table_name in GTFS_FILES.values():
        csv_path = os.path.join(GTFS_PATH, file_name)
        table_id = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.{table_name}"
        
        if not os.path.exists(csv_path):
            logging.warning(f"Arquivo {file_name} não encontrado em {GTFS_PATH}. Pulando...")
            continue
        
        # Adiciona load_date manualmente
        df = pd.read_csv(csv_path, dtype=str, low_memory=False)  # low_memory=False para arquivos grandes
        df['load_date'] = LOAD_DATE
        temp_csv = csv_path.replace('.txt', '_temp.csv')
        df.to_csv(temp_csv, index=False)
        
        load_csv_to_bigquery(temp_csv, table_id)
        if os.path.exists(temp_csv):
            os.remove(temp_csv)  # Limpa temp

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    parse_and_load()