
"""
Job Gold: Dimensión Donantes.
Lógica: SQLX translation (antigüedad, internacionalidad, validación contacto).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session

def run_gold_dim_donantes():
    spark = get_spark_session("GoldDimDonantes")
    try:
        print("🚀 JOB GOLD: DIM DONANTES")
        
        # Paths
        silver_donantes = f"{config.SILVER_PATH}/donantes"
        gold_calendario = f"{config.GOLD_PATH}/dim_calendario"
        output_path = f"{config.GOLD_PATH}/dim_donantes"
        
        # Leer inputs
        df_don = spark.read.parquet(silver_donantes)
        df_cal = spark.read.parquet(gold_calendario)
        
        # Lógica de Negocio (Derived Columns)
        df_enriched = df_don.withColumn("es_donante_internacional", 
                                        F.when(F.lower(F.col("pais")) != "colombia", True).otherwise(False)) \
                            .withColumn("es_donante_local", 
                                        F.when(F.lower(F.col("pais")) == "colombia", True).otherwise(False)) \
                            .withColumn("antiguedad_donante_meses", 
                                        F.months_between(F.current_date(), F.col("created_at"))) \
                            .withColumn("contacto_valido",
                                        F.when(F.col("correo").isNotNull() & F.col("correo").contains("@"), True).otherwise(False))
        
        # Join con Calendario (Left Join por created_at)
        # Nota: created_at es timestamp, dim_calendario es date. Debemos castear.
        
        # Seleccionamos cols de calendario para no traer basura
        cal_cols = df_cal.select(
            F.col("fecha"),
            F.col("anio").alias("anio_creacion"),
            F.col("mes").alias("mes_creacion"),
            F.col("anio_mes").alias("anio_mes_creacion"),
            F.col("trimestre").alias("trimestre_creacion"),
            F.col("anio_trimestre").alias("anio_trimestre_creacion"),
            F.col("semestre").alias("semestre_creacion"),
            F.col("anio_semestre").alias("anio_semestre_creacion"),
            F.col("semana_iso").alias("semana_creacion")
        )
        
        df_final = df_enriched.join(cal_cols, F.to_date(df_enriched.created_at) == cal_cols.fecha, "left") \
                              .drop("fecha") # Drop key redundancy

        # Particionado para el Lake (Mandatory)
        df_final = df_final.withColumn("anio", F.year("created_at").cast("string")) \
                           .withColumn("mes", F.lpad(F.month("created_at"), 2, "0")) \
                           .withColumn("dia", F.lpad(F.dayofmonth("created_at"), 2, "0"))

        # Escritura
        (df_final.write.mode("overwrite")
         .partitionBy("anio", "mes", "dia")
         .option("partitionOverwriteMode", "dynamic")
         .parquet(output_path))
        
        # Renombrar archivos
        from jobs.utils.file_utils import rename_spark_output
        rename_spark_output("gold", "dim_donantes", output_path)
        
        print("✅ Gold Dim Donantes procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_dim_donantes()
