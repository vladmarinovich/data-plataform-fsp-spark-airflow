
"""
Job Gold: Generación de Dimensión Calendario.
Genera una tabla de fechas desde 2020 hasta 2030 con atributos derivados (mes, trimestre, semana, etc).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql import types as T
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_dim_calendario():
    spark = get_spark_session("GoldDimCalendario")
    try:
        output_path = f"{config.GOLD_PATH}/dim_calendario"
        print("📅 JOB GOLD: Generando DIM CALENDARIO")
        
        # Generar rango de fechas (2020-01-01 a 2030-12-31)
        start_date = "2020-01-01"
        end_date = "2030-12-31"
        
        df_dates = spark.sql(f"""
            SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as fecha
        """)
        
        # Enriquecer con atributos
        df_cal = df_dates.withColumn("y", F.year("fecha")) \
                         .withColumn("m", F.month("fecha")) \
                         .withColumn("d", F.dayofmonth("fecha")) \
                         .withColumn("trimestre", F.quarter("fecha")) \
                         .withColumn("semestre", F.when(F.col("mes") <= 6, 1).otherwise(2)) \
                         .withColumn("nombre_dia", F.date_format("fecha", "EEEE")) \
                         .withColumn("nombre_mes", F.date_format("fecha", "MMMM")) \
                         .withColumn("anio_mes", F.format_string("%d-%02d", F.col("anio"), F.col("mes"))) \
                         .withColumn("anio_trimestre", F.concat(F.col("anio"), F.lit("-T"), F.col("trimestre"))) \
                         .withColumn("anio_semestre", F.concat(F.col("anio"), F.lit("-S"), F.col("semestre"))) \
                         .withColumn("semana_iso", F.weekofyear("fecha")) \
                         .withColumn("es_fin_de_semana", F.when(F.dayofweek("fecha").isin(1, 7), True).otherwise(False))

        # Escritura
        (df_cal.write.mode("overwrite").parquet(output_path))
        
        # Renombrar archivos
        rename_spark_output("gold", "dim_calendario", output_path)
        
        print(f"✅ Dim Calendario generada: {df_cal.count()} días.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_dim_calendario()
