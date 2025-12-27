
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Configuración de Rutas (Internas de Docker)
PROJECT_ROOT = "/opt/airflow"
PYTHON_BIN = "python3" # Usamos el python del contenedor

default_args = {
    'owner': 'Antigravity',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spdp_data_platform_main',
    default_args=default_args,
    description='Pipeline principal de Salvando Patitas (Local Sandbox)',
    schedule_interval='30 23 * * 0', # Todos los domingos a las 23:30 UTC
    catchup=False,
    max_active_tasks=4, # Limitamos para evitar sobrecarga de memoria
    max_active_runs=1,
    tags=['spdp', 'pyspark', 'gold', 'silver'],
) as dag:

    # ---------------------------------------------------------
    # NIVEL 0: EXTRACCIÓN (RAW)
    # ---------------------------------------------------------
    def extract_task(table_name):
        return BashOperator(
            task_id=f'extract_{table_name}',
            bash_command=f"export ENV=local && {PYTHON_BIN} {PROJECT_ROOT}/scripts/quick_mock_data.py {table_name}",
        )

    e_donantes = extract_task('donantes')
    e_casos = extract_task('casos')
    e_donaciones = extract_task('donaciones')
    e_gastos = extract_task('gastos')
    e_proveedores = extract_task('proveedores')
    e_hogar = extract_task('hogar_de_paso')

    # ---------------------------------------------------------
    # NIVEL 1: CAPA SILVER (LIMPIEZA)
    # ---------------------------------------------------------
    def silver_task(table_name):
        return BashOperator(
            task_id=f'silver_{table_name}',
            bash_command=f"export ENV=local && {PYTHON_BIN} {PROJECT_ROOT}/jobs/silver/{table_name}.py",
        )

    s_donantes = silver_task('donantes')
    s_casos = silver_task('casos')
    s_donaciones = silver_task('donaciones')
    s_gastos = silver_task('gastos')
    s_proveedores = silver_task('proveedores')
    s_hogar = silver_task('hogar_de_paso')

    # ---------------------------------------------------------
    # NIVEL 2: CAPA GOLD BASE (DIMENSIONES Y HECHOS)
    # ---------------------------------------------------------
    def gold_task(job_name):
        return BashOperator(
            task_id=f'gold_{job_name}',
            bash_command=f"export ENV=local && {PYTHON_BIN} {PROJECT_ROOT}/jobs/gold/{job_name}.py",
        )

    g_dim_cal = gold_task('dim_calendario')
    g_dim_don = gold_task('dim_donantes')
    g_dim_casos = gold_task('dim_casos')
    g_fact_don = gold_task('fact_donaciones')
    g_fact_gastos = gold_task('fact_gastos')

    # ---------------------------------------------------------
    # NIVEL 3: CAPA GOLD METRICS (FEATURES)
    # ---------------------------------------------------------
    g_feat_don = gold_task('feat_donantes')
    g_feat_casos = gold_task('feat_casos')
    g_feat_prov = gold_task('feat_proveedores')

    # ---------------------------------------------------------
    # NIVEL 4: CAPA GOLD PRESENTACIÓN (DASHBOARDS)
    # ---------------------------------------------------------
    g_dash_don = gold_task('dashboard_donaciones')
    g_dash_gastos = gold_task('dashboard_gastos')
    g_dash_fin = gold_task('dashboard_financiero')

    # =========================================================
    # DEFINICIÓN DE DEPENDENCIAS (FLUJO)
    # =========================================================
    
    # Dependencias de Extracción a Silver
    e_donantes >> s_donantes
    e_casos >> s_casos
    e_donaciones >> s_donaciones
    e_gastos >> s_gastos
    e_proveedores >> s_proveedores
    e_hogar >> s_hogar
    
    # Silver a Gold Base
    [s_donantes, g_dim_cal] >> g_dim_don
    s_casos >> g_dim_casos
    s_donaciones >> g_fact_don
    s_gastos >> g_fact_gastos
    
    # Gold Base a Features
    g_fact_don >> g_feat_don
    [g_dim_casos, g_fact_gastos] >> g_feat_casos
    g_fact_gastos >> g_feat_prov
    
    # Features/Base a Dashboards
    [g_dim_don, g_feat_don] >> g_dash_don
    [g_dim_casos, g_fact_gastos] >> g_dash_gastos
    [g_fact_don, g_fact_gastos] >> g_dash_fin
