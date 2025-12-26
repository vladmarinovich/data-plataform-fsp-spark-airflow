
"""
Job Gold: Fact Donaciones.
Tabla de hechos transaccional con métricas enriquecidas.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
        
        # Filtro Global de Fact Table (Solo exitosas pasan a Fact de Ingresos Reales)
        # SQLX: LOWER(d.estado) IN ('aprobada', 'completada', 'exitoso')
        df_filtered = df_silver.filter(F.lower(F.col("estado")).isin("aprobada", "completada", "exitoso"))

        # Lógica de Negocio (Idéntica a Dataform)
        df_enriched = df_filtered.withColumn(
            "es_donacion_valida", 
            F.when(F.lower(F.col("estado")) == "aprobada", True).otherwise(False)
        ).withColumn(
            "es_donacion_critica", 
            F.when(F.col("monto") > 500000, True).otherwise(False)
        ).withColumn("monto_log", F.log1p(F.col("monto"))) \
         .withColumn("recencia_donacion_dias", F.datediff(F.current_date(), F.col("fecha_donacion"))) \
         .withColumn("anio_mes_donacion", F.date_format("fecha_donacion", "yyyy-MM"))

        # Window para Recencia del Donante (Última donación de ESTE donante)
        window_donante = Window.partitionBy("id_donante")
        df_enriched = df_enriched.withColumn("ultima_fecha_donante", F.max("fecha_donacion").over(window_donante)) \
                                 .withColumn("recencia_donaciones_dias", F.datediff(F.current_date(), F.col("ultima_fecha_donante"))) \
                                 .drop("ultima_fecha_donante")

        # Columnas de partición física para almacenamiento
        df_final = df_enriched.withColumn("anio", F.year("fecha_donacion")) \
                              .withColumn("mes", F.lpad(F.month("fecha_donacion"), 2, "0")) \
                              .withColumn("dia", F.lpad(F.dayofmonth("fecha_donacion"), 2, "0"))

        # Escritura Particionada
        (df_final.write.mode("overwrite")
         .partitionBy("anio", "mes", "dia")
         .option("partitionOverwriteMode", "dynamic")
         .parquet(output_path))
         
        # Renombrar archivos
        rename_spark_output("gold", "fact_donaciones", output_path)
         
        print("✅ Gold Fact Donaciones procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_fact_donaciones()
