
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
    max_active_tasks=2,  # Reducido para local (evitar competencia RAM de Spark JVMs)
    max_active_runs=1,
    tags=['spdp', 'pyspark', 'gold', 'silver'],
) as dag:

    # ---------------------------------------------------------
    # NIVEL 0: CAPA RAW (EXTRACCIÓN DE DATOS)
    # ---------------------------------------------------------
    def raw_task(table_name):
        return BashOperator(
            task_id=f'raw_{table_name}',
            bash_command=f"export ENV=local && {PYTHON_BIN} {PROJECT_ROOT}/scripts/quick_mock_data.py {table_name}",
        )

    r_donantes = raw_task('donantes')
    r_casos = raw_task('casos')
    r_donaciones = raw_task('donaciones')
    r_gastos = raw_task('gastos')
    r_proveedores = raw_task('proveedores')
    r_hogar = raw_task('hogar_de_paso')

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
    g_dim_prov = gold_task('dim_proveedores')
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
    # DEFINICIÓN DE DEPENDENCIAS (MODELADO DIMENSIONAL KIMBALL)
    # =========================================================
    
    # CAPA 1: RAW (Todas en paralelo)
    # ================================
    raws = [r_donantes, r_casos, r_donaciones, r_gastos, r_proveedores, r_hogar]
    
    # CAPA 2: SILVER (Esperan su raw correspondiente)
    # ================================================
    r_donantes >> s_donantes
    r_casos >> s_casos  
    r_donaciones >> s_donaciones
    r_gastos >> s_gastos
    r_proveedores >> s_proveedores
    r_hogar >> s_hogar
    
    silvers = [s_donantes, s_casos, s_donaciones, s_gastos, s_proveedores, s_hogar]
    
    # CAPA 3: DIMENSION CALENDARIO (Base de todo Gold)
    # ==================================================
    # dim_calendario es PRIMERA tabla Gold (independiente)
    # Todas las tablas Gold dependen de ella
    
    g_dim_cal  # Primera dimensión (no depende de nadie)
    
    # CAPA 4: DIMENSIONS + FACTS (Dependen de calendario + Silver)
    # ==============================================================
    # Todas las dimensions y facts se conectan a calendario
    
    [s_donantes, g_dim_cal] >> g_dim_don
    [s_casos, g_dim_cal] >> g_dim_casos
    s_proveedores >> g_dim_prov
    [s_donaciones, g_dim_cal] >> g_fact_don
    [s_gastos, g_dim_cal] >> g_fact_gastos
    
    gold_dims = [g_dim_cal, g_dim_don, g_dim_casos, g_dim_prov]
    gold_facts = [g_fact_don, g_fact_gastos]
    
    # CAPA 5: FEATURES (Vienen de DIMS, enriquecidas con FACTS)
    # ===========================================================
    # Features parten de una dimension y se enriquecen con facts
    
    # feat_casos viene de dim_casos + enriquecida con facts
    [g_dim_casos, g_fact_gastos, g_fact_don] >> g_feat_casos
    
    # feat_donantes viene de dim_donantes + enriquecida con fact_donaciones
    [g_dim_don, g_fact_don] >> g_feat_don
    
    # feat_proveedores viene de fact_gastos + dim_proveedores
    [g_fact_gastos, g_dim_prov] >> g_feat_prov
    
    gold_features = [g_feat_don, g_feat_casos, g_feat_prov]
    
    # CAPA 6: DASHBOARDS (Vienen de FACTS principal + FEATURES)
    # ===========================================================
    # Dashboards parten de fact principal, enriquecidos con features
    # (calendario ya heredado de facts/features - no necesita dependencia explícita)
    
    # dashboard_donaciones: fact_donaciones + feat_donantes + feat_casos
    [g_fact_don, g_feat_don, g_feat_casos] >> g_dash_don
    
    # dashboard_gastos: fact_gastos + dim_proveedores (+ feat_casos + feat_prov)
    [g_fact_gastos, g_dim_prov, g_feat_casos, g_feat_prov] >> g_dash_gastos
    
    # dashboard_financiero: CONSOLIDA los otros dos dashboards
    # (hereda calendario de ellos - join por fecha/día)
    [g_dash_don, g_dash_gastos] >> g_dash_fin





