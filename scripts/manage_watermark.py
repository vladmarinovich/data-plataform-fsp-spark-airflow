
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Agregar root del proyecto al path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from jobs.utils.spark_session import get_spark_session
from jobs.utils.watermark import update_watermark, get_watermark

def reset_watermark(new_date):
    spark = get_spark_session("WatermarkManager")
    try:
        print(f"🔍 Leeyendo watermark actual...")
        current = get_watermark(spark, "spdp_data_platform_main")
        print(f"   Valor actual: {current}")
        
        print(f"🛠  Reseteando watermark a: {new_date}")
        update_watermark(spark, new_date, "spdp_data_platform_main")
        
        print(f"✅ Éxito! El próximo DAG run comenzará desde: {new_date}")
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python scripts/manage_watermark.py <YYYY-MM-DD>")
        sys.exit(1)
    
    target_date = sys.argv[1]
    # Validar formato ISO simple
    try:
        datetime.fromisoformat(target_date)
    except ValueError:
        print("❌ Error: Formato de fecha inválido. Usa YYYY-MM-DD o ISO format.")
        sys.exit(1)

    reset_watermark(target_date)
