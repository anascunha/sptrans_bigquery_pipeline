# bigquery_client.py
import pandas as pd
from google.cloud import bigquery
import tempfile
import os

class BigQueryClient:
    def __init__(self, credentials_file, project_id):
        self.client = bigquery.Client.from_service_account_json(credentials_file, project=project_id)

    def insert_rows(self, table_id, rows_to_insert):
        if not rows_to_insert:
            print("Nenhum dado para inserir.")
            return []

        # Converter para DataFrame
        df = pd.DataFrame(rows_to_insert)

        # Criar arquivo temporário CSV
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as temp_file:
            df.to_csv(temp_file.name, index=False)
            temp_path = temp_file.name

        # Configuração do job de load
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Pula cabeçalho
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False  # Usaremos schema da tabela
        )

        try:
            # Fazer upload
            with open(temp_path, "rb") as source_file:
                job = self.client.load_table_from_file(source_file, table_id, job_config=job_config)
            
            job.result()  # Aguarda conclusão
            print(f"Load job concluído: {job.output_rows} linhas inseridas.")
            return []
        except Exception as e:
            print(f"Erro no load job: {e}")
            return [str(e)]
        finally:
            # Apagar arquivo temporário
            if os.path.exists(temp_path):
                os.unlink(temp_path)