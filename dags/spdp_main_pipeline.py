
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys

# Importar alertas (Asegurando que el path sea visible)
sys.path.insert(0, '/opt/airflow')
from jobs.utils.alerts import slack_failure_callback

# Configuración de Rutas (Internas de Docker)
PROJECT_ROOT = "/opt/airflow"
PYTHON_BIN = "python3"  # Usamos el python del contenedor

default_args = {
    'owner': 'Antigravity',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_failure_callback, # 🔔 Alerta automática
}

with DAG(
    'spdp_data_platform_main',
    default_args=default_args,
    description='Pipeline principal de Salvando Patitas - Producción',
    schedule_interval='30 23 * * 0',  # Todos los domingos a las 23:30 UTC
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    tags=['spdp', 'pyspark', 'gold', 'silver', 'production'],
) as dag:

    # ---------------------------------------------------------
    # NIVEL 0: EXTRACCIÓN DESDE SUPABASE (BRONZE/RAW)
    # ---------------------------------------------------------
    extract_supabase = BashOperator(
        task_id='extract_from_supabase',
        bash_command=f"export ENV=cloud && {PYTHON_BIN} {PROJECT_ROOT}/scripts/extract_from_supabase.py",
    )

    # Reparticionamiento por fechas de negocio
    repartition_raw = BashOperator(
        task_id='repartition_raw',
        bash_command=f"export ENV=cloud && {PYTHON_BIN} {PROJECT_ROOT}/scripts/repartition_raw.py",
    )

    # ---------------------------------------------------------
    # NIVEL 1: CAPA SILVER (LIMPIEZA Y NORMALIZACIÓN)
    # ---------------------------------------------------------
    def silver_task(table_name):
        return BashOperator(
            task_id=f'silver_{table_name}',
            bash_command=f"export ENV=cloud && {PYTHON_BIN} {PROJECT_ROOT}/jobs/silver/{table_name}.py",
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
            bash_command=f"export ENV=cloud && {PYTHON_BIN} {PROJECT_ROOT}/jobs/gold/{job_name}.py",
        )

    g_dim_cal = gold_task('dim_calendario')
    g_dim_don = gold_task('dim_donantes')
    g_dim_casos = gold_task('dim_casos')
    g_dim_prov = gold_task('dim_proveedores')
    g_dim_hogar = gold_task('dim_hogar_de_paso')
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

    # ---------------------------------------------------------
    # NIVEL 5: CARGA A BIGQUERY
    # ---------------------------------------------------------
    load_to_bigquery = BashOperator(
        task_id='load_to_bigquery',
        bash_command=f"export ENV=cloud && {PYTHON_BIN} {PROJECT_ROOT}/scripts/load_to_bigquery.py",
    )

    # ---------------------------------------------------------
    # NIVEL 6: ACTUALIZACIÓN DE WATERMARK
    # ---------------------------------------------------------
    update_watermark = BashOperator(
        task_id='update_watermark',
        bash_command=f"""
        export ENV=cloud && {PYTHON_BIN} -c "
import sys
import json
import os
from pathlib import Path
sys.path.insert(0, '{PROJECT_ROOT}')

from jobs.utils.watermark import update_watermark
from jobs.utils.spark_session import get_spark_session
from datetime import datetime

spark = get_spark_session('UpdateWatermark')
try:
    # Intentar leer estado del pipeline (Smart Update)
    state_file = '/tmp/pipeline_state.json'
    new_watermark = None
    
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                new_watermark = state.get('next_watermark')
                print(f'📄 Leído next_watermark desde archivo: {{new_watermark}}')
        except Exception as e:
            print(f'⚠️ Error leyendo state file: {{e}}')
    
    # Fallback a NOW() si no hay archivo
    if not new_watermark:
        new_watermark = datetime.utcnow().isoformat()
        print(f'⚠️ Usando Fallback (NOW): {{new_watermark}}')

    update_watermark(spark, new_watermark, 'spdp_data_platform_main')
    print(f'✅ Watermark FINAL actualizado a: {{new_watermark}}')
finally:
    spark.stop()
"
        """,
    )

    # =========================================================
    # DEFINICIÓN DE DEPENDENCIAS (MODELADO DIMENSIONAL KIMBALL)
    # =========================================================
    
    # CAPA 0: EXTRACCIÓN Y REPARTICIONAMIENTO
    # ========================================
    extract_supabase >> repartition_raw
    
    # CAPA 1: SILVER (Esperan reparticionamiento)
    # ============================================
    repartition_raw >> [s_donantes, s_casos, s_donaciones, s_gastos, s_proveedores, s_hogar]
    
    # CAPA 2: DIMENSION CALENDARIO (Base de todo Gold)
    # =================================================
    # dim_calendario es PRIMERA tabla Gold (independiente)
    g_dim_cal
    
    # CAPA 3: DIMENSIONS + FACTS (Dependen de calendario + Silver)
    # =============================================================
    [s_donantes, g_dim_cal] >> g_dim_don
    [s_casos, g_dim_cal] >> g_dim_casos
    s_proveedores >> g_dim_prov
    s_hogar >> g_dim_hogar
    [s_donaciones, g_dim_cal] >> g_fact_don
    [s_gastos, g_dim_cal] >> g_fact_gastos
    
    # CAPA 4: FEATURES (Vienen de DIMS, enriquecidas con FACTS)
    # ==========================================================
    [g_dim_casos, g_fact_gastos, g_fact_don] >> g_feat_casos
    [g_dim_don, g_fact_don] >> g_feat_don
    [g_fact_gastos, g_dim_prov] >> g_feat_prov
    
    # CAPA 5: DASHBOARDS (Vienen de FACTS principal + FEATURES)
    # ==========================================================
    [g_fact_don, g_feat_don, g_feat_casos] >> g_dash_don
    [g_fact_gastos, g_dim_prov, g_feat_casos, g_feat_prov] >> g_dash_gastos
    [g_dash_don, g_dash_gastos] >> g_dash_fin
    
    # CAPA 6: CARGA A BIGQUERY Y WATERMARK
    # =====================================
    g_dash_fin >> load_to_bigquery >> update_watermark

    # ---------------------------------------------------------
    # NIVEL 7: AUTO-APAGADO (COST SAVING)
    # ---------------------------------------------------------
    # CRÍTICO: La VM se apaga SIEMPRE, incluso si el pipeline falla
    if os.getenv('ENV') == 'cloud':
        stop_instance = BashOperator(
            task_id='stop_instance',
            bash_command="gcloud compute instances stop airflow-server-prod --zone=us-central1-a --quiet",
            trigger_rule='all_done'  # Se ejecuta SIEMPRE (éxito o fallo)
        )
        # Todas las tareas finales apuntan al shutdown
        [
            update_watermark,
            g_dash_don,
            g_dash_gastos, 
            g_feat_casos,
            g_feat_don,
            g_feat_prov,
            g_dim_don,
            g_dim_casos,
            g_dim_prov,
            g_dim_hogar,
            g_fact_don,
            g_fact_gastos
        ] >> stop_instance

