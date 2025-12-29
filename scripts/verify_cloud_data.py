"""
Script de Auditoría: Comparativo de Conteos RAW vs SILVER.
"""
import sys
from pathlib import Path
from pyspark.sql import functions as F

# Agregar raíz al path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
# Configurar credenciales ADC explícitamente para Spark local
adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = adc_path

from jobs.utils.spark_session import get_spark_session
import config

TABLES = ['donaciones', 'gastos', 'donantes', 'casos', 'proveedores', 'hogar_de_paso']

def audit_counts():
    spark = get_spark_session("AuditCounts")
    
    results = []
    
    print("\n📊 INICIANDO AUDITORÍA DE CONTEOS (CLOUD GCS: gs://salvando-patitas-spark)\n")
    
    # Bucket Base
    BUCKET = "gs://salvando-patitas-spark"
    
    for table in TABLES:
        # Paths GCS explícitos
        raw_path = f"{BUCKET}/raw/{table}"
        silver_path = f"{BUCKET}/silver/{table}"
        
        # Count RAW
        try:
            count_raw = spark.read.parquet(raw_path).count()
        except:
            count_raw = 0
            
        # Count SILVER
        try:
            count_silver = spark.read.parquet(silver_path).count()
        except:
            count_silver = 0
            
        delta = count_raw - count_silver
        retention = (count_silver / count_raw * 100) if count_raw > 0 else 0
        
        results.append({
            "Tabla": table,
            "Raw": count_raw,
            "Silver": count_silver,
            "Filtrados (DQ/Dedup)": delta,
            "Retención %": f"{retention:.1f}%"
        })

    # Imprimir Tabla Markdown
    print("\n" + "="*85)
    print(f"{'TABLA':<20} | {'RAW (Rows)':<12} | {'SILVER (Rows)':<15} | {'FILTRADOS':<10} | {'RETENCIÓN':<10}")
    print("-" * 85)
    
    for r in results:
        print(f"{r['Tabla']:<20} | {r['Raw']:<12} | {r['Silver']:<15} | {r['Filtrados (DQ/Dedup)']:<10} | {r['Retención %']:<10}")
    
    print("-" * 85)
    print("\n✅ Auditoría finalizada.\n")
    
    spark.stop()

if __name__ == "__main__":
    audit_counts()
