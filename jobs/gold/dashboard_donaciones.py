
"""
Job Gold: Dashboard Donaciones.
Tabla optimizada para dashboards de donaciones, uniendo hechos con dimensiones y calculando métricas RFM.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_dashboard_donaciones():
    spark = get_spark_session("GoldDashboardDonaciones")
    try:
        print("🚀 JOB GOLD: DASHBOARD DONACIONES")
        
        # Paths
        fact_donaciones_path = f"{config.GOLD_PATH}/fact_donaciones"
        dim_donantes_path = f"{config.GOLD_PATH}/dim_donantes"
        output_path = f"{config.GOLD_PATH}/dashboard_donaciones"
        
        # Lectura de dependencias Gold
        df_fact = spark.read.parquet(fact_donaciones_path)
        df_dim = spark.read.parquet(dim_donantes_path)
        
        # Join Base
        df_base = df_fact.alias("d").join(
            df_dim.alias("dd"),
            F.col("d.id_donante") == F.col("dd.id_donante"),
            "left"
        ).select(
            "d.*",
            "dd.donante", "dd.tipo_id", "dd.identificacion", "dd.correo",
            "dd.ciudad", "dd.tipo_donante", "dd.pais", "dd.canal_origen", "dd.consentimiento"
        )
        
        # Window specs para RFM
        window_donante = Window.partitionBy("id_donante")
        
        # Agregaciones y RFM
        df_agg = df_base.withColumn(
            "anio", F.year("fecha_donacion")
        ).withColumn(
            "mes", F.month("fecha_donacion")
        ).withColumn(
            "anio_mes", F.date_format("fecha_donacion", "yyyy-MM")
        ).withColumn(
            "frecuencia_donaciones", F.count("id_donacion").over(window_donante)
        ).withColumn(
            "monto_total_donado", F.sum("monto").over(window_donante)
        ).withColumn(
            "promedio_donacion_donante", F.avg("monto").over(window_donante)
        ).withColumn(
            "monto_maximo_donante", F.max("monto").over(window_donante)
        ).withColumn(
            "monto_minimo_donante", F.min("monto").over(window_donante)
        ).withColumn(
            "recencia_dias", F.datediff(F.current_date(), F.max("fecha_donacion").over(window_donante))
        ).withColumn(
            "segmento_rfm",
            F.when(F.col("monto_total_donado") > 2000000, "oro")
             .when(F.col("monto_total_donado") > 700000, "plata")
             .when(F.col("monto_total_donado") > 200000, "bronce")
             .otherwise("basico")
        ).withColumn(
            "tamano_donacion",
            F.when(F.col("monto") >= 500000, "critica")
             .when(F.col("monto") >= 200000, "media")
             .otherwise("pequena")
        )
        
        # Escritura (Full Refresh para Dashboard)
        (df_agg.write.mode("overwrite").parquet(output_path))
         
        # Renombrar archivos
        rename_spark_output("gold", "dashboard_donaciones", output_path)
        
        print("✅ Gold Dashboard Donaciones procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_dashboard_donaciones()
