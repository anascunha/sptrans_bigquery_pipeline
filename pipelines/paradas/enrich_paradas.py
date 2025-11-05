# pipelines/paradas/enrich_paradas.py
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
log_path = os.path.join(project_root, "logs", "enrich_paradas.log")
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
linhas_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.sptrans_linhas"
paradas_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.sptrans_paradas"

# === INTERVALO: 24 HORAS ===
INTERVALO_24H = 86400  # 24 horas em segundos


def get_linhas_com_cl():
    """Pega line_c e cl da tabela sptrans_linhas (hoje)"""
    query = f"""
    SELECT DISTINCT line_c, cl
    FROM `{linhas_table}`
    WHERE cl IS NOT NULL
      AND DATE(fetch_time) = CURRENT_DATE()  -- Free tier: só hoje
    """
    logging.info("Buscando linhas com código interno (cl) em sptrans_linhas (hoje)...")
    try:
        job = bigquery_client.client.query(query)
        results = job.result()
        linhas = [(row.line_c, row.cl) for row in results]
        logging.info(f"{len(linhas)} linhas com cl encontradas.")
        return linhas
    except Exception as e:
        logging.error(f"Erro ao buscar linhas com cl: {e}")
        return []


def buscar_paradas_por_linha(cl):
    """Chama API /Parada/BuscarParadasPorLinha?codigoLinha=12345"""
    url = f"{config['sptrans']['base_url']}/Parada/BuscarParadasPorLinha?codigoLinha={cl}"
    try:
        response = sptrans_client.session.get(url, proxies=sptrans_client.proxies, timeout=15)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            logging.warning(f"Token expirado para cl={cl}. Reautenticando...")
            sptrans_client.authenticate()
            return buscar_paradas_por_linha(cl)
        else:
            logging.error(f"Erro {response.status_code} para cl={cl}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Exceção para cl={cl}: {e}")
        return None


def enrich_cycle():
    """Um ciclo completo de enriquecimento (FREE TIER: REPLACE via LOAD JOB - 24h)"""
    start_time = time.time()
    logging.info("="*70)
    logging.info(f"ENRIQUECIMENTO DE PARADAS - {datetime.now(timezone.utc)}")
    logging.info("PRÓXIMA EXECUÇÃO EM 24 HORAS")
    logging.info("="*70)

    try:
        sptrans_client.authenticate()
        linhas = get_linhas_com_cl()
        if not linhas:
            logging.warning("Nenhuma linha com cl. Pulando ciclo.")
            return

        rows_to_insert = []
        for i, (line_c, cl) in enumerate(linhas):
            logging.info(f"[{i+1}/{len(linhas)}] Buscando paradas para {line_c} (cl={cl})")
            dados = buscar_paradas_por_linha(cl)
            if dados and isinstance(dados, list):
                for p in dados:
                    row = {
                        "fetch_time": datetime.now(timezone.utc).isoformat(),
                        "line_c": line_c,
                        "cl": cl,
                        "cp": p.get("cp"),
                        "np": p.get("np"),
                        "py": p.get("py"),
                        "px": p.get("px")
                    }
                    rows_to_insert.append(row)
            time.sleep(0.3)  # Respeita API

        # LOAD JOB REPLACE (sobrescreve partição do dia)
        logging.info("Iniciando LOAD JOB (truncate) via JSON temporário...")
        load_json_to_bigquery(
            client=bigquery_client.client,
            table_id=paradas_table,
            rows=rows_to_insert,
            mode='truncate'  # REPLACE diário
        )
        logging.info("LOAD JOB CONCLUÍDO! Paradas atualizadas (hoje).")

    except Exception as e:
        logging.error(f"Erro crítico no ciclo: {e}")

    elapsed = time.time() - start_time
    logging.info(f"Ciclo concluído em {elapsed:.1f}s. Aguardando 24h...")
    time.sleep(INTERVALO_24H)  # 24 HORAS


if __name__ == '__main__':
    logging.info("PIPELINE DE PARADAS INICIADO (a cada 24h - FREE TIER)")
    try:
        while True:
            enrich_cycle()
    except KeyboardInterrupt:
        logging.info("Interrompido pelo usuário.")