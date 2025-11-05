# pipelines/linhas/enrich_linhas.py
import time
import logging
from datetime import datetime, timezone
import urllib.parse
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
log_path = os.path.join(project_root, "logs", "enrich_linhas.log")
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
linhas_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.sptrans_linhas"


def get_linhas_unicas():
    """Busca line_c únicos da tabela de posições"""
    query = f"""
    SELECT DISTINCT line_c
    FROM `{posicoes_table}`
    WHERE line_c IS NOT NULL
      AND DATE(fetch_time) = CURRENT_DATE()  -- Free tier: só hoje
    """
    logging.info("Buscando linhas únicas em sptrans_posicoes (hoje)...")
    try:
        job = bigquery_client.client.query(query)
        results = job.result()
        linhas = [row.line_c for row in results]
        logging.info(f"{len(linhas)} linhas únicas encontradas.")
        return linhas
    except Exception as e:
        logging.error(f"Erro ao buscar linhas únicas: {e}")
        return []


def buscar_dados_linha(line_c):
    """Chama API /Linha/Buscar?termosBusca=XXX"""
    termo = urllib.parse.quote(line_c)
    url = f"{config['sptrans']['base_url']}/Linha/Buscar?termosBusca={termo}"
    try:
        response = sptrans_client.session.get(url, proxies=sptrans_client.proxies, timeout=15)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            logging.warning(f"Token expirado para {line_c}. Reautenticando...")
            sptrans_client.authenticate()
            return buscar_dados_linha(line_c)
        else:
            logging.error(f"Erro {response.status_code} para {line_c}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Exceção ao buscar {line_c}: {e}")
        return None


def enrich_cycle():
    """Um ciclo completo de enriquecimento (FREE TIER: REPLACE via LOAD JOB)"""
    start_time = time.time()
    logging.info("="*70)
    logging.info(f"ENRIQUECIMENTO DE LINHAS - {datetime.now(timezone.utc)}")
    logging.info("="*70)

    try:
        sptrans_client.authenticate()
        linhas_unicas = get_linhas_unicas()
        if not linhas_unicas:
            logging.warning("Nenhuma linha única encontrada. Pulando ciclo.")
            return

        rows_to_insert = []
        for i, line_c in enumerate(linhas_unicas):
            logging.info(f"[{i+1}/{len(linhas_unicas)}] Buscando: {line_c}")
            dados = buscar_dados_linha(line_c)
            if dados and isinstance(dados, list):
                for linha in dados:
                    row = {
                        "fetch_time": datetime.now(timezone.utc).isoformat(),
                        "line_c": line_c,
                        "cl": linha.get("cl"),
                        "lc": linha.get("lc"),
                        "lt": linha.get("lt"),
                        "tl": linha.get("tl"),
                        "sl": linha.get("sl"),
                        "tp": linha.get("tp"),
                        "ts": linha.get("ts")
                    }
                    rows_to_insert.append(row)
            time.sleep(0.3)  # Respeita API

        # LOAD JOB REPLACE (sobrescreve partição do dia)
        logging.info("Iniciando LOAD JOB (truncate) via JSON temporário...")
        load_json_to_bigquery(
            client=bigquery_client.client,
            table_id=linhas_table,
            rows=rows_to_insert,
            mode='truncate'  # REPLACE diário
        )
        logging.info("LOAD JOB CONCLUÍDO! Linhas atualizadas (hoje).")

    except Exception as e:
        logging.error(f"Erro crítico no ciclo: {e}")

    elapsed = time.time() - start_time
    sleep_time = max(0, 60 - elapsed)
    logging.info(f"Ciclo concluído em {elapsed:.1f}s. Aguardando {sleep_time:.1f}s até próximo...")
    time.sleep(sleep_time)


if __name__ == '__main__':
    logging.info("PIPELINE DE LINHAS INICIADO (a cada 60s - FREE TIER)")
    try:
        while True:
            enrich_cycle()
    except KeyboardInterrupt:
        logging.info("Pipeline interrompido pelo usuário (Ctrl+C). Encerrando.")