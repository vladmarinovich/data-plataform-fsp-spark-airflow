"""
DAG de Airflow: Pipeline CRM Donaciones
========================================

Este DAG orquesta el pipeline completo de datos:
1. Extracción de Supabase
2. Transformación Bronze -> Silver
3. Transformación Silver -> Gold

Schedule: Diario a las 2 AM
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Configuración por defecto del DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Definición del DAG
dag = DAG(
    'crm_donations_pipeline',
    default_args=default_args,
    description='Pipeline ETL para donaciones CRM (Portfolio)',
    # OPCIONES DE SCHEDULE (descomentar la que quieras):
    # schedule_interval='0 2 * * 0',      # Semanal: Domingos a las 2 AM
    schedule_interval='0 2 1,15 * *',   # Quincenal: Días 1 y 15 a las 2 AM (ACTIVO)
    # schedule_interval='0 2 * * *',      # Diario: Todos los días a las 2 AM (costoso)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crm', 'donations', 'etl'],
)

# ============================================
# TASK 1: Verificar dependencias
# ============================================
check_dependencies = BashOperator(
    task_id='check_dependencies',
    bash_command="""
        echo "Verificando Spark..."
        spark-submit --version
        echo "Verificando Python..."
        python3 --version
    """,
    dag=dag,
)

# ============================================
# TASK 2: Extracción de datos (Python)
# ============================================
# En producción, esto ejecutaría un script de extracción
extract_data = BashOperator(
    task_id='extract_crm_data',
    bash_command="""
        cd {{ params.project_root }} && \
        source venv/bin/activate && \
        python scripts/extract_from_supabase.py
    """,
    params={'project_root': '/path/to/project'},
    dag=dag,
)

# ============================================
# TASK 3: Transformación Bronze -> Silver (PySpark)
# ============================================
transform_to_silver = SparkSubmitOperator(
    task_id='transform_to_silver',
    application='{{ params.project_root }}/jobs/transform_donations.py',
    name='crm-silver-transform',
    conn_id='spark_default',
    conf={
        'spark.master': 'local[*]',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
    },
    params={'project_root': '/path/to/project'},
    dag=dag,
)

# ============================================
# TASK 4: Validación de calidad de datos
# ============================================
validate_silver_data = PythonOperator(
    task_id='validate_silver_data',
    python_callable=lambda: print("Validando datos Silver..."),
    dag=dag,
)

# ============================================
# TASK 5: Transformación Silver -> Gold (PySpark)
# ============================================
transform_to_gold = SparkSubmitOperator(
    task_id='transform_to_gold',
    application='{{ params.project_root }}/jobs/aggregate_donations.py',
    name='crm-gold-transform',
    conn_id='spark_default',
    conf={
        'spark.master': 'local[*]',
        'spark.driver.memory': '2g',
    },
    params={'project_root': '/path/to/project'},
    dag=dag,
)

# ============================================
# TASK 6: Notificación de éxito
# ============================================
notify_success = BashOperator(
    task_id='notify_success',
    bash_command='echo "Pipeline completado exitosamente"',
    dag=dag,
)

# ============================================
# Definir dependencias (orden de ejecución)
# ============================================
check_dependencies >> extract_data >> transform_to_silver >> validate_silver_data >> transform_to_gold >> notify_success

"""
Flujo del DAG:

check_dependencies
        ↓
  extract_data
        ↓
transform_to_silver
        ↓
validate_silver_data
        ↓
 transform_to_gold
        ↓
  notify_success
"""
