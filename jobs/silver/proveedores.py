
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
        
        # Transformaciones de Negocio (silver_proveedores.sqlx)
        # -----------------------------------------------------
        
        df_clean = df_raw.withColumn("id_proveedor", F.col("id_proveedor").cast("long")) \
                         .withColumn("created_at", F.col("created_at").cast("timestamp")) \
                         .withColumn("last_modified_at", F.col("last_modified_at").cast("timestamp"))

        # Recategorización de Tipo de Proveedor
        tipo_clean = F.lower(F.trim(F.col("tipo_proveedor")))
        df_clean = df_clean.withColumn("tipo_proveedor", 
            F.when(tipo_clean.isin("empaques", "veterinaria"), "servicios médicos")
             .when(tipo_clean == "exámenes", "laboratorio")
             .when(tipo_clean == "insumos", "transporte")
             .when(tipo_clean == "tecnología", "marketing")
             .otherwise(tipo_clean)
        )

        # Defaults y Normalización
        df_clean = df_clean.withColumn("nombre_proveedor", F.trim(F.col("nombre_proveedor"))) \
                           .withColumn("nit", F.trim(F.col("nit"))) \
                           .withColumn("nombre_contacto", F.trim(F.col("nombre_contacto"))) \
                           .withColumn("correo", F.coalesce(F.lower(F.trim(F.col("correo"))), F.lit("desconocido"))) \
                           .withColumn("telefono", F.coalesce(F.trim(F.col("telefono")), F.lit("desconocido"))) \
                           .withColumn("ciudad", F.coalesce(F.lower(F.trim(F.col("ciudad"))), F.lit("bogota")))

        # Filtros Duros (Data Quality)
        df_clean = df_clean.filter(F.col("nombre_proveedor").isNotNull()) \
                           .filter(F.col("nit").isNotNull()) \
                           .filter(F.col("nombre_contacto").isNotNull()) \
                           .filter(F.col("tipo_proveedor").isNotNull())

        # Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_proveedores"))

        df_clean.write.mode("overwrite").parquet(output_path)
        print("✅ Proveedores procesados.")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_proveedores()
