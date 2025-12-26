
"""
Job Gold: Dimensión Hogar de Paso (con KPIs).
Lógica: Join aggregation con Casos para calcular ocupación y rotación.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session

def run_gold_dim_hogar():
    spark = get_spark_session("GoldDimHogar")
    try:
        print("🚀 JOB GOLD: DIM HOGAR DE PASO")
        silver_hogar = f"{config.SILVER_PATH}/hogar_de_paso"
        silver_casos = f"{config.SILVER_PATH}/casos"
        output_path = f"{config.GOLD_PATH}/dim_hogar_de_paso"
        
        df_hogar = spark.read.parquet(silver_hogar)
        df_casos = spark.read.parquet(silver_casos)
        
        # Agregación de Casos por Hogar
        kpis_hogar = df_casos.groupBy("id_hogar_de_paso").agg(
            F.count("id_caso").alias("numero_casos_total"),
            F.count(F.when(F.col("fecha_salida").isNull(), 1)).alias("numero_casos_activos"),
            F.count(F.when(F.col("fecha_salida").isNotNull(), 1)).alias("numero_casos_finalizado")
        )
        
        # Join con dimensión base
        df_final = df_hogar.join(kpis_hogar, "id_hogar_de_paso", "left").fillna(0, subset=["numero_casos_total", "numero_casos_activos", "numero_casos_finalizado"])
        
        # Cálculos de ratios (SAFE_DIVIDE logic)
        df_final = df_final.withColumn("tasa_ocupacion", 
                                       F.when(F.col("cupo_maximo") > 0, F.col("numero_casos_activos") / F.col("cupo_maximo")).otherwise(0.0)) \
                           .withColumn("rotacion_casos",
                                       F.when(F.col("numero_casos_total") > 0, F.col("numero_casos_finalizado") / F.col("numero_casos_total")).otherwise(0.0))

        # Clasificaciones
        df_final = df_final.withColumn("clasificacion_ocupacion", 
            F.when(F.col("tasa_ocupacion") > 0.85, "Alta")
             .when(F.col("tasa_ocupacion").between(0.40, 0.85), "Media")
             .otherwise("Baja")
        ).withColumn("clasificacion_rotacion",
            F.when(F.col("rotacion_casos") > 0.7, "Alta rotación")
             .when(F.col("rotacion_casos").between(0.3, 0.7), "Media")
             .otherwise("Baja rotación")
        )

        df_final.write.mode("overwrite").parquet(output_path)
        print("✅ Gold Dim Hogar procesada.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_dim_hogar()
