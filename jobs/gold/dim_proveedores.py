
"""
Job Gold: Dimensión Proveedores.
Lógica: SQLX translation (categorización, validación NIT).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session

def run_gold_dim_proveedores():
    spark = get_spark_session("GoldDimProveedores")
    try:
        print("🚀 JOB GOLD: DIM PROVEEDORES")
        silver_proveedores = f"{config.SILVER_PATH}/proveedores"
        output_path = f"{config.GOLD_PATH}/dim_proveedores"
        
        df = spark.read.parquet(silver_proveedores)
        
        # Lógica SQLX
        df_enriched = df.withColumn("ciudad_normalizada", F.lower(F.trim(F.col("ciudad")))) \
                        .withColumn("tiene_id_valido", 
                                    F.when((F.col("nit").isNotNull()) & (F.col("nit") != ""), True).otherwise(False))
        
        # Categorización Proveedor
        tipo = F.lower(F.col("tipo_proveedor"))
        df_enriched = df_enriched.withColumn("categoria_proveedor",
            F.when(tipo.like("%veterinario%"), "Servicios Veterinarios")
             .when(tipo.like("%insumo%"), "Insumos")
             .when(tipo.like("%transporte%"), "Logística")
             .otherwise("General")
        )

        df_enriched.write.mode("overwrite").parquet(output_path)
        print("✅ Gold Dim Proveedores procesada.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_dim_proveedores()
