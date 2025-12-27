
"""
Job Gold: Feat Donantes.
Tabla de características y métricas agregadas por donante (LTV, Recencia, Frecuencia).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_feat_donantes():
    spark = get_spark_session("GoldFeatDonantes")
    try:
        print("🚀 JOB GOLD: FEAT DONANTES")
        
        # Paths
        dim_donantes_path = f"{config.GOLD_PATH}/dim_donantes"
        fact_donaciones_path = f"{config.GOLD_PATH}/fact_donaciones"
        output_path = f"{config.GOLD_PATH}/feat_donantes"
        
        # Lectura de dependencias Gold
        df_dim_donantes = spark.read.parquet(dim_donantes_path)
        df_fact_donaciones = spark.read.parquet(fact_donaciones_path)
        
        # Agregaciones de Donaciones
        df_agg = df_fact_donaciones.groupBy("id_donante").agg(
            F.sum("monto").alias("total_donado"),
            F.count("id_donacion").alias("num_donaciones"),
            F.avg("monto").alias("promedio_donacion"),
            F.min("fecha_donacion").alias("primera_donacion"),
            F.max("fecha_donacion").alias("ultima_donacion"),
            F.max("monto").alias("monto_maximo"),
            F.min("monto").alias("monto_minimo")
        )
        
        # Join con Dimensión
        df_feat = df_dim_donantes.join(df_agg, "id_donante", "left")
        
        # Lógica de Features (ML / Dashboard)
        df_final = df_feat.withColumn(
            "recencia_dias", 
            F.datediff(F.current_date(), F.col("ultima_donacion"))
        ).withColumn(
            "categoria_financiera",
            F.when(F.col("total_donado") > 2000000, "oro")
             .when(F.col("total_donado") > 700000, "plata")
             .when(F.col("total_donado") > 200000, "bronce")
             .otherwise("basico")
        ).withColumn(
            "donante_recurrente_ml",
            F.when(F.col("num_donaciones") >= 3, True).otherwise(False)
        ).withColumn(
            "ltv", F.col("total_donado")
        ).withColumn(
            "velocidad_crecimiento",
            F.col("total_donado") / F.col("antiguedad_donante_meses")
        )
        
        # Escritura (Snapshot Table - Full Refresh)
        (df_final.write.mode("overwrite")
         .parquet(output_path))
         
        # Renombrar archivos
        rename_spark_output("gold", "feat_donantes", output_path)
        
        print("✅ Gold Feat Donantes procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_feat_donantes()
