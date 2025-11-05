# core/load_job.py
import json
import tempfile
import os
from google.cloud import bigquery

def load_json_to_bigquery(client, table_id, rows, mode='append'):
    """LOAD JOB via JSON temporário (FREE TIER)"""
    if not rows:
        print("Nenhum dado para load.")
        return

    # JSON temporário (NDJSON)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
        temp_path = f.name

    # Config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition='WRITE_APPEND' if mode == 'append' else 'WRITE_TRUNCATE',
        autodetect=False  # Usa schema da tabela
    )

    # Executa
    try:
        with open(temp_path, 'rb') as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()  # Espera
        print(f"LOAD JOB: {len(rows)} linhas em {table_id} ({mode})")
    except Exception as e:
        print(f"Erro LOAD JOB: {e}")
    finally:
        os.unlink(temp_path)  # Delete temp