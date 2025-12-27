
"""
Job Gold: Feat Casos.
Métricas agregadas por caso (Costos, Tiempos de Resolución, Eficiencia).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_feat_casos():
    spark = get_spark_session("GoldFeatCasos")
    try:
        print("🚀 JOB GOLD: FEAT CASOS")
        
        # Paths
        dim_casos_path = f"{config.GOLD_PATH}/dim_casos"
        fact_gastos_path = f"{config.GOLD_PATH}/fact_gastos"
        output_path = f"{config.GOLD_PATH}/feat_casos"
        
        # Lectura
        df_dim_casos = spark.read.parquet(dim_casos_path)
        df_fact_gastos = spark.read.parquet(fact_gastos_path)
        
        # Agregaciones de Gastos por Caso
        df_agg_gastos = df_fact_gastos.groupBy("id_caso").agg(
            F.sum("monto").alias("costo_total_caso"),
            F.count("id_gasto").alias("num_gastos_caso"),
            F.avg("monto").alias("promedio_gasto_caso"),
            F.max("monto").alias("gasto_maximo_caso")
        )
        
        # Join con Dimensión
        df_feat = df_dim_casos.join(df_agg_gastos, "id_caso", "left")
        
        # Lógica de Features
        df_final = df_feat.withColumn(
            "tiempo_resolucion_dias",
            F.datediff(F.col("fecha_salida"), F.col("fecha_ingreso"))
        ).withColumn(
            "es_caso_alto_costo",
            F.when(F.col("costo_total_caso") > 1000000, True).otherwise(False)
        ).withColumn(
            "categoria_costo",
            F.when(F.col("costo_total_caso") > 1000000, "Critico")
             .when(F.col("costo_total_caso") > 500000, "Alto")
             .when(F.col("costo_total_caso") > 100000, "Medio")
             .otherwise("Bajo")
        )
        
        # Escritura
        (df_final.write.mode("overwrite").parquet(output_path))
        
        # Renombrar
        rename_spark_output("gold", "feat_casos", output_path)
        
        print("✅ Gold Feat Casos procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_feat_casos()
