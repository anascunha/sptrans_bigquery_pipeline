# SPTrans BigQuery Pipeline (Free Tier + GTFS)
## Pipeline ETL near real-time para dados da SPTrans (Olho Vivo) + GTFS estático → BigQuery.

Posições: Append a cada 60s.

* Linhas/Paradas: Replace diário.
* GTFS: Routes, trips, stops, shapes, stop_times (estático, replace semanal).
* Zero custo: LOAD JOB CSV + partitioned tables (Sandbox OK).
* Zero downtime: Append em posições, truncate/replace em metadados.
* Enriquecimento: JOINs com GTFS para nomes longos, cores, trajetos, ETA vs. programado.

## Estrutura Atualizada
```
sptrans_bigquery_pipeline/
├── core/                  # Módulos compartilhados
│   ├── config.json
│   ├── config_loader.py
│   ├── bigquery_client.py
│   ├── sptrans_client.py
│   └── load_job.py
├── pipelines/
│   ├── posicoes/main_posicoes.py
│   ├── linhas/enrich_linhas.py
│   └── paradas/enrich_paradas.py
├── ingest/
│   ├── ingest_gtfs.py     # LOAD CSV GTFS (offline)
│   └── create_tables.sql  # Schemas bronze
├── data/
│   └── gtfs/              # Arquivos .txt (baixe ZIP completo)
├── logs/
├── run_all.py             # Rode OLHO VIVO + GTFS
└── README.md
```
## Pré-requisitos

* Python 3.10+
* Instalação de dependências:
```
pip install google-cloud-bigquery requests pandas
```
* `config.json` em `/core`:
```
{
  "sptrans": {"base_url": "http://api.olhovivo.sptrans.com.br/v2.1", "token": "SEU_TOKEN"},
  "bigquery": {
    "credentials_file": "caminho/para/service-account.json",
    "project_id": ""SEU_DATASET_GCP"",
    "dataset_id": "sptrans"
  }
}
```
* GTFS: Baixe o ZIP completo e extraia em `/data/gtfs/` (agency.txt, routes.txt, etc.)


## Passo a Passo (Atualizado)

**1. Crie Tabelas Bronze (BigQuery Console - uma vez)**

Rode `ingest/create_tables.sql` ou cole:
```
-- Posições, Linhas, Paradas (OLHO VIVO)
CREATE OR REPLACE TABLE `"SEU_DATASET_GCP".sptrans.sptrans_posicoes` (...) PARTITION BY DATE(fetch_time);
CREATE OR REPLACE TABLE `"SEU_DATASET_GCP".sptrans.sptrans_linhas` (...) PARTITION BY DATE(fetch_time);
CREATE OR REPLACE TABLE `"SEU_DATASET_GCP".sptrans.sptrans_paradas` (...) PARTITION BY DATE(fetch_time);

-- GTFS Bronze (load_date flexível)
CREATE OR REPLACE TABLE `"SEU_DATASET_GCP".sptrans.gtfs_routes` (
  route_id STRING, agency_id STRING, route_short_name STRING, route_long_name STRING,
  route_type INTEGER, route_color STRING, route_text_color STRING, load_date DATE
) PARTITION BY load_date;

-- Repita para: `gtfs_trips`, `gtfs_stops`, `gtfs_stop_times`, `gtfs_shapes`, etc.
-- Veja `ingest_gtfs.py` para schemas completos.
```

**2. Ingira GTFS (Offline - Rode semanalmente)**
```
python ingest/ingest_gtfs.py
```
* Lê `/data/gtfs/*.txt` → adiciona `load_date` → LOAD JOB CSV (free tier).
* Logs: ~1.1M shapes, ~22k stops.

**3. Rode Pipelines OLHO VIVO + GTFS**
```
python run_all.py
```
- Posições: Append real-time.
- Linhas/Paradas: Replace diário.
- GTFS: Opcional (adicione em `run_all.py` se automático).

**4. Verifique Dados**
```
SELECT COUNT(*) FROM `sptrans.sptrans_posicoes` WHERE DATE(fetch_time) = CURRENT_DATE();  -- ~10k
SELECT MAX(load_date) FROM `sptrans.gtfs_routes`;  -- Hoje
```
* Gold Layer: VIEWs Enriquecidas (Real-Time + GTFS)
* Crie no Console (qualificação completa: "SEU_DATASET_GCP".sptrans.*).

## Gold Layer: VIEWs Enriquecidas
### VIEW 1: Posições + Nome Longo + Cor

```
CREATE OR REPLACE VIEW `"SEU_DATASET_GCP".sptrans.vw_posicoes_enriquecidas` AS
SELECT 
  p.fetch_time,
  p.line_c AS letreiro,
  p.vehicle_p AS prefixo_onibus,
  p.vehicle_py AS lat_onibus,
  p.vehicle_px AS lon_onibus,
  r.route_long_name AS nome_longo_linha,
  r.route_color AS cor_linha_hex,
  l.ts AS terminal_secundario,
  l.sl AS sentido
FROM `"SEU_DATASET_GCP".sptrans.sptrans_posicoes` p
JOIN `"SEU_DATASET_GCP".sptrans.sptrans_linhas` l ON p.line_c = l.line_c
JOIN `"SEU_DATASET_GCP".sptrans.gtfs_routes` r ON p.line_c = r.route_short_name
WHERE DATE(p.fetch_time) = CURRENT_DATE()
  AND DATE(l.fetch_time) = CURRENT_DATE();
```
## VIEW 2: Parada Mais Próxima + ETA (<1km)
```
CREATE OR REPLACE VIEW `"SEU_DATASET_GCP".sptrans.vw_eta_paradas` AS
WITH gtfs_latest AS (
  SELECT * FROM `"SEU_DATASET_GCP".sptrans.gtfs_stops`
  WHERE load_date = (SELECT MAX(load_date) FROM `"SEU_DATASET_GCP".sptrans.gtfs_stops`)
),
nearest AS (
  SELECT 
    p.vehicle_p,
    p.line_c,
    p.vehicle_py AS bus_lat,
    p.vehicle_px AS bus_lon,
    s.stop_name AS parada_proxima,
    s.stop_lat,
    s.stop_lon,
    ST_DISTANCE(
      ST_GEOGPOINT(p.vehicle_px, p.vehicle_py),
      ST_GEOGPOINT(s.stop_lon, s.stop_lat)
    ) AS distancia_metros,
    ROW_NUMBER() OVER (PARTITION BY p.vehicle_p ORDER BY ST_DISTANCE(
      ST_GEOGPOINT(p.vehicle_px, p.vehicle_py),
      ST_GEOGPOINT(s.stop_lon, s.stop_lat)
    )) AS rn
  FROM `"SEU_DATASET_GCP".sptrans.sptrans_posicoes` p
  JOIN `"SEU_DATASET_GCP".sptrans.sptrans_linhas` l ON p.line_c = l.line_c
  JOIN `"SEU_DATASET_GCP".sptrans.gtfs_routes` r ON p.line_c = r.route_short_name
  JOIN `"SEU_DATASET_GCP".sptrans.gtfs_trips` t ON r.route_id = t.route_id
  JOIN `"SEU_DATASET_GCP".sptrans.gtfs_stop_times` st ON t.trip_id = st.trip_id
  JOIN gtfs_latest s ON CAST(st.stop_id AS INTEGER) = s.stop_id
  WHERE DATE(p.fetch_time) = CURRENT_DATE()
    AND ST_DISTANCE(
      ST_GEOGPOINT(p.vehicle_px, p.vehicle_py),
      ST_GEOGPOINT(s.stop_lon, s.stop_lat)
    ) < 1000
)
SELECT 
  vehicle_p AS prefixo_onibus,
  line_c AS letreiro,
  parada_proxima,
  ROUND(distancia_metros) AS distancia_metros,
  ROUND(distancia_metros / (20 * 1000 / 60), 1) AS eta_minutos
FROM nearest
WHERE rn = 1;
```
## VIEW 3: Trajeto Completo (Shapes) por Linha
```
CREATE OR REPLACE VIEW `"SEU_DATASET_GCP".sptrans.vw_trajetos_linhas` AS
SELECT 
  r.route_short_name AS line_c,
  r.route_long_name,
  s.shape_pt_lat AS lat_ponto,
  s.shape_pt_lon AS lon_ponto,
  s.shape_pt_sequence AS sequencia
FROM `"SEU_DATASET_GCP".sptrans.gtfs_routes` r
JOIN `"SEU_DATASET_GCP".sptrans.gtfs_trips` t ON r.route_id = t.route_id
JOIN `"SEU_DATASET_GCP".sptrans.gtfs_shapes` s ON t.shape_id = s.shape_id
WHERE s.load_date = (SELECT MAX(load_date) FROM `"SEU_DATASET_GCP".sptrans.gtfs_shapes`);
```
## VIEW 4: KPIs Diários (Qualificação Completa + MAX load_date)
```
CREATE OR REPLACE VIEW `"SEU_DATASET_GCP".sptrans.vw_kpis_diarios` AS
SELECT 
  CURRENT_DATE() AS data_referencia,
  (SELECT COUNT(DISTINCT vehicle_p) 
   FROM `"SEU_DATASET_GCP".sptrans.sptrans_posicoes` 
   WHERE DATE(fetch_time) = CURRENT_DATE()) AS onibus_ativos,
  (SELECT COUNT(DISTINCT line_c) 
   FROM `"SEU_DATASET_GCP".sptrans.sptrans_linhas` 
   WHERE DATE(fetch_time) = CURRENT_DATE()) AS linhas_ativas,
  (SELECT COUNT(DISTINCT stop_id) 
   FROM `"SEU_DATASET_GCP".sptrans.gtfs_stops` 
   WHERE load_date = (SELECT MAX(load_date) FROM `"SEU_DATASET_GCP".sptrans.gtfs_stops`)) AS paradas_totais,
  (SELECT SAFE_CAST(AVG(eta_minutos) AS FLOAT64) 
   FROM `"SEU_DATASET_GCP".sptrans.vw_eta_paradas`) AS eta_media_min;
```
- Dashboard (Looker Studio)
- Fonte: Dataset sptrans.
- Mapa: vw_posicoes_enriquecidas (lat/lon, cor por cor_hex).
- Polilinhas: vw_trajetos_linhas (layer por line_c).
- Tabela: vw_eta_paradas.
- Gauges: vw_kpis_diarios.

### Observações:
- Atualize GTFS: Substitua arquivos em /data/gtfs/ → rode `ingest_gtfs.py`.
- Limpeza: `DELETE FROM sptrans_posicoes WHERE DATE(fetch_time) < CURRENT_DATE() - 7;`
- Scheduler: Cloud Scheduler → Cloud Run / Functions (disponível no Free Trial).
