# pipelines/posicoes/main_posicoes.py
import time
import logging
from datetime import datetime, timezone
import sys
import os

# === CORREÇÃO DE PATH E IMPORTS ===
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from core.config_loader import load_config
from core.bigquery_client import BigQueryClient
from core.sptrans_client import SPTransClient
from core.load_job import load_json_to_bigquery  # NOVO: LOAD JOB FREE TIER

# === CONFIGURAÇÃO DE LOG (centralizado em /logs) ===
log_path = os.path.join(project_root, "logs", "etl_posicoes.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# === CARREGAR CONFIG ===
config_path = os.path.join(project_root, "core", "config.json")
config = load_config(config_path)
if config is None:
    raise FileNotFoundError(f"config.json não encontrado em {config_path}")

# === CLIENTES ===
sptrans_client = SPTransClient(
    base_url=config['sptrans']['base_url'],
    token=config['sptrans']['token'],
    proxies=config.get('proxy')
)

bigquery_client = BigQueryClient(
    credentials_file=config['bigquery']['credentials_file'],
    project_id=config['bigquery']['project_id']
)

# === TABELAS ===
posicoes_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.{config['bigquery']['table_id']}"


def etl_cycle():
    """Um ciclo completo de ETL para posições (FREE TIER: APPEND via LOAD JOB)"""
    start_time = time.time()
    logging.info("="*70)
    logging.info(f"ETL POSIÇÕES - {datetime.now(timezone.utc)}")
    logging.info("="*70)

    try:
        # 1. Autenticar (se necessário)
        if not sptrans_client.session.cookies:
            sptrans_client.authenticate()

        # 2. Extrair dados da API /Posicao
        logging.info("Extraindo dados da API /Posicao...")
        data = sptrans_client.get_posicao()
        if not data:
            logging.warning("Nenhum dado retornado pela API. Pulando ciclo.")
            return

        hr = data.get('hr', 'N/A')
        rows_to_insert = []

        for line in data.get('l', []):
            line_c = line.get("c")
            line_cl = line.get("cl")
            line_sl = line.get("sl")
            line_lt0 = line.get("lt0")
            line_lt1 = line.get("lt1")

            for vehicle in line.get('vs', []):
                row = {
                    "fetch_time": datetime.now(timezone.utc).isoformat(),
                    "hr": hr,
                    "line_c": line_c,
                    "line_cl": line_cl,
                    "line_sl": line_sl,
                    "line_lt0": line_lt0,
                    "line_lt1": line_lt1,
                    "vehicle_p": vehicle.get("p"),
                    "vehicle_a": vehicle.get("a"),
                    "vehicle_ta": vehicle.get("ta"),
                    "vehicle_py": vehicle.get("py"),
                    "vehicle_px": vehicle.get("px")
                }
                rows_to_insert.append(row)

        logging.info(f"{len(rows_to_insert)} veículos extraídos.")

        # 3. LOAD JOB APPEND (FREE TIER - ZERO DML)
        logging.info("Iniciando LOAD JOB (append) via JSON temporário...")
        load_json_to_bigquery(
            client=bigquery_client.client,
            table_id=posicoes_table,
            rows=rows_to_insert,
            mode='append'  # Acumula em tempo real
        )
        logging.info("LOAD JOB CONCLUÍDO! Dados appendados com sucesso.")

    except Exception as e:
        logging.error(f"Erro crítico no ciclo ETL: {e}")
        try:
            logging.info("Tentando reautenticar...")
            sptrans_client.authenticate()
        except Exception as auth_e:
            logging.error(f"Falha na reautenticação: {auth_e}")

    # === GARANTIR 60 SEGUNDOS ===
    elapsed = time.time() - start_time
    sleep_time = max(0, 60 - elapsed)
    logging.info(f"Ciclo concluído em {elapsed:.1f}s. Aguardando {sleep_time:.1f}s até próximo...")
    time.sleep(sleep_time)


if __name__ == '__main__':
    logging.info("PIPELINE DE POSIÇÕES INICIADO (a cada 60s - FREE TIER)")
    try:
        while True:
            etl_cycle()
    except KeyboardInterrupt:
        logging.info("Pipeline interrompido pelo usuário (Ctrl+C). Encerrando.")