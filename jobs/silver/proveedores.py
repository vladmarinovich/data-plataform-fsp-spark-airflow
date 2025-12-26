
"""
Job Silver: Transformación Proveedores.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_silver_proveedores():
    spark = get_spark_session("SilverProveedores")
    try:
        print("🚀 JOB SILVER: PROVEEDORES")
        input_path = f"{config.RAW_PATH}/raw_proveedores"
        output_path = f"{config.SILVER_PATH}/proveedores"
        
        df_raw = spark.read.parquet(input_path)
        if df_raw.rdd.isEmpty(): return

        # Casting Robusto
        def cast_to_timestamp(col_name):
            col = F.col(col_name)
            return F.when(
                col.cast("string").rlike(r'^\d+$'), 
                F.from_unixtime(col.cast("long")/1000000).cast("timestamp")
            ).otherwise(F.to_timestamp(col))

        df_clean = df_raw.withColumn("id_proveedor", F.col("id_proveedor").cast("long")) \
                         .withColumn("created_at", cast_to_timestamp("created_at")) \
                         .withColumn("last_modified_at", cast_to_timestamp("last_modified_at"))
        
        # Normalización
        df_clean = df_clean.withColumn("nombre_proveedor", F.upper(F.trim(F.col("nombre_proveedor")))) \
                           .withColumn("correo", F.lower(F.trim(F.col("correo"))))

        # Deduplicación
        window_spec = Window.partitionBy("id_proveedor").orderBy(F.col("last_modified_at").desc())
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

        # Escritura (Full Overwrite sin partición)
        df_dedup.write.mode("overwrite").parquet(output_path)
        
        rename_spark_output("silver", "proveedores", output_path)
        print("✅ Proveedores procesados.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_proveedores()
