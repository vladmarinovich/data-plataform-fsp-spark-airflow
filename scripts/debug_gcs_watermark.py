
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.utils.spark_session import get_spark_session
from jobs.utils.watermark import get_watermark

def test_watermark_read():
    spark = get_spark_session("DebugWatermark")
    try:
        print("🔍 Testing GCS Watermark Read...")
        wm = get_watermark(spark, "CualquierCosa")
        print(f"✅ Watermark LEÍDO desde GCS: {wm}")
        
        if wm and wm.year == 2023:
            print("🎉 ÉXITO: El reseteo a 2023-01-01 funciona.")
        else:
            print(f"⚠️ ALERTA: Valor inesperado: {wm}")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    test_watermark_read()
