
"""
Job Gold: Dimensión Casos.
Lógica: SQLX translation (estado, duración, métricas derivadas).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import config
from jobs.utils.spark_session import get_spark_session

def run_gold_dim_casos():
    spark = get_spark_session("GoldDimCasos")
    try:
        print("🚀 JOB GOLD: DIM CASOS")
        silver_casos = f"{config.SILVER_PATH}/casos"
        output_path = f"{config.GOLD_PATH}/dim_casos"
        
        df = spark.read.parquet(silver_casos)
        
        # Lógica SQLX
        # Fecha salida efectiva (hoy si es null)
        fecha_salida_efectiva = F.coalesce(F.col("fecha_salida"), F.current_timestamp())
        
        df_enriched = df.withColumn("fecha_ingreso_date", F.to_date("fecha_ingreso")) \
                        .withColumn("fecha_salida_date", F.to_date("fecha_salida")) \
                        .withColumn("dias_duracion", F.datediff(fecha_salida_efectiva, F.col("fecha_ingreso"))) \
                        .withColumn("indicador_caso", F.when(F.col("fecha_salida").isNull(), True).otherwise(False)) \
                        .withColumn("antiguedad_caso", F.datediff(F.current_date(), F.col("fecha_ingreso"))) \
                        .withColumn("estado_normalizado", 
                                    F.when(F.col("fecha_salida").isNull(), "Abierto").otherwise("Cerrado"))
        
        # Clasificación Duración
        df_enriched = df_enriched.withColumn("clasificacion_duracion", 
            F.when(F.col("dias_duracion") < 30, "Corta")
             .when(F.col("dias_duracion").between(30, 90), "Media")
             .otherwise("Larga")
        )

        # Clasificación Presupuesto
        df_enriched = df_enriched.withColumn("clasificacion_presupuesto", 
            F.when(F.col("presupuesto_estimado") < 200000, "Bajo")
             .when(F.col("presupuesto_estimado").between(200000, 600000), "Medio")
             .otherwise("Alto")
        )

        df_enriched.write.mode("overwrite").parquet(output_path)
        print("✅ Gold Dim Casos procesada.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_dim_casos()
