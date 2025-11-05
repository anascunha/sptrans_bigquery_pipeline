# run_all.py
import subprocess
import os
from datetime import datetime
import logging

# Config logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

# Caminhos absolutos
project_root = os.path.dirname(os.path.abspath(__file__))
ingest_gtfs_path = os.path.join(project_root, 'ingest', 'ingest_gtfs.py')
flag_file = os.path.join(project_root, '.gtfs_ingested_today')

# Pipelines OLHO VIVO (sempre em paralelo)
pipelines = [
    f"python {os.path.join(project_root, 'pipelines', 'posicoes', 'main_posicoes.py')}",
    f"python {os.path.join(project_root, 'pipelines', 'linhas', 'enrich_linhas.py')}",
    f"python {os.path.join(project_root, 'pipelines', 'paradas', 'enrich_paradas.py')}"
]

def should_run_gtfs():
    """Roda GTFS apenas 1x por dia (verifica flag + data)"""
    today = datetime.now().date().isoformat()
    if not os.path.exists(flag_file):
        return True
    try:
        with open(flag_file, 'r') as f:
            last_date = f.read().strip()
        return last_date != today
    except:
        return True

def mark_gtfs_done():
    """Cria/atualiza flag com data de hoje"""
    today = datetime.now().date().isoformat()
    with open(flag_file, 'w') as f:
        f.write(today)
    logging.info(f"GTFS ingest marcado como executado em {today}")

# === EXECUÇÃO ===
logging.info("Iniciando pipelines SPTrans + GTFS (se necessário)...")

# 1. GTFS: Apenas 1x por dia
if should_run_gtfs():
    logging.info("Executando ingest GTFS (1x/dia)...")
    gtfs_cmd = f"python {ingest_gtfs_path}"
    result = subprocess.run(gtfs_cmd, shell=True, cwd=project_root, capture_output=True, text=True)
    if result.returncode == 0:
        logging.info("GTFS ingest concluído com sucesso.")
        mark_gtfs_done()
    else:
        logging.error(f"Erro no GTFS ingest: {result.stderr}")
else:
    logging.info("GTFS já executado hoje. Pulando.")

# 2. OLHO VIVO: Sempre em paralelo
logging.info("Iniciando pipelines OLHO VIVO em paralelo...")
processes = []
for cmd in pipelines:
    proc = subprocess.Popen(cmd, shell=True, cwd=project_root)
    processes.append(proc)
    logging.info(f"Iniciado: {cmd.split('/')[-1]}")

logging.info("Todos pipelines rodando! Ctrl+C para parar.")

# Mantém script vivo
try:
    for proc in processes:
        proc.wait()
except KeyboardInterrupt:
    logging.info("Parando pipelines...")
    for proc in processes:
        proc.terminate()
    logging.info("Pipelines parados.")