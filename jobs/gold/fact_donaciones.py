
"""
Job Gold: Fact Donaciones.
Fact table simplificada de donaciones.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F, Window
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_fact_donaciones():
    spark = get_spark_session("GoldFactDonaciones")
    try:
        print("🚀 JOB GOLD: FACT DONACIONES")
        
        # Paths
        silver_donaciones = f"{config.SILVER_PATH}/donaciones"
        output_path = f"{config.GOLD_PATH}/fact_donaciones"
        
        df_silver = spark.read.option("basePath", silver_donaciones).parquet(silver_donaciones + "/*")
        
        # Fact Table: Todas las donaciones sin filtro de estado
        # Lógica de Negocio
        df_enriched = df_silver.withColumn(
            "es_donacion_critica", 
            F.when(F.col("monto") > 500000, True).otherwise(False)
        ).withColumn("monto_log", F.log1p(F.col("monto"))) \
         .withColumn("recencia_donacion_dias", F.datediff(F.current_date(), F.col("fecha_donacion"))) \
         .withColumn("anio_mes_donacion", F.date_format("fecha_donacion", "yyyy-MM"))

        # Window para Recencia del Donante
        window_donante = Window.partitionBy("id_donante")
        df_enriched = df_enriched.withColumn("ultima_fecha_donante", F.max("fecha_donacion").over(window_donante)) \
                                 .withColumn("recencia_donaciones_dias", F.datediff(F.current_date(), F.col("ultima_fecha_donante"))) \
                                 .drop("ultima_fecha_donante")

        # Categorías de monto
        df_fact = df_enriched.withColumn(
            "categoria_monto",
            F.when(F.col("monto") < 50000, "Bajo")
             .when(F.col("monto") < 200000, "Medio")
             .otherwise("Alto")
        )

        # Select final columns
        df_final = df_fact.select(
            "id_donacion",
            "id_donante",
            "monto",
            "fecha_donacion",
            "medio_pago",  # Fixed: was metodo_pago
            "es_donacion_critica",
            "monto_log",
            "recencia_donacion_dias",
            "recencia_donaciones_dias",
            "anio_mes_donacion",
            "categoria_monto",
            "y", "m", "d"
        )


        (df_final.write.mode("overwrite")
         .partitionBy("y", "m", "d")
         .option("partitionOverwriteMode", "dynamic")
         .parquet(output_path))
         
        rename_spark_output("gold", "fact_donaciones", output_path)
         
        print("✅ Gold Fact Donaciones procesada.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_fact_donaciones()
