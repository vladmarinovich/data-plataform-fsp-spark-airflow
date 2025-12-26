
"""Job Silver: Proveedores."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session

def run_silver_proveedores():
    spark = get_spark_session("SilverProveedores")
    try:
        input_path = f"{config.RAW_PATH}/raw_proveedores"
        output_path = f"{config.SILVER_PATH}/proveedores"
        
        df_raw = spark.read.parquet(input_path + "/*.parquet")
        
        df_clean = df_raw.withColumn("id_proveedor", F.col("id_proveedor").cast("long")) \
                         .withColumn("created_at", F.col("created_at").cast("timestamp")) \
                         .withColumn("last_modified_at", F.col("last_modified_at").cast("timestamp"))
        
        # Snapshot puro, no requiere deduplicación compleja si Raw ya es snapshot (upload overwrite)
        # Pero por seguridad si Raw tuviera duplicados:
        df_clean = df_clean.dropDuplicates(["id_proveedor"])

        df_clean.write.mode("overwrite").parquet(output_path)
        print("✅ Proveedores procesados.")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_proveedores()
