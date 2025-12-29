
"""
Job Gold: Fact Gastos.
Fact table simplificada de gastos.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_fact_gastos():
    spark = get_spark_session("GoldFactGastos")
    try:
        print("🚀 JOB GOLD: FACT GASTOS")
        silver_gastos = f"{config.SILVER_PATH}/gastos"
        output_path = f"{config.GOLD_PATH}/fact_gastos"
        
        df = spark.read.parquet(silver_gastos)
        
        # Lógica de Negocio: Solo gastos efectivamente pagados
        df = df.filter(F.col("estado") == "pagado")
        
        # Fact Table: Métricas de gastos
        df_fact = df.select(
            "id_gasto",
            "id_caso",
            "id_proveedor",
            "monto",
            "fecha_pago",
            "medio_pago",
            "estado",
            F.col("nombre_gasto").alias("descripcion"),
            "created_at",
            "y", "m", "d"
        )

        # Métricas calculadas
        df_final = df_fact.withColumn("monto_log", F.log1p(F.col("monto"))) \
                          .withColumn("recencia_dias", F.datediff(F.current_date(), F.col("fecha_pago")))
        
        (df_final.write.mode("overwrite")
         .partitionBy("y", "m", "d")
         .option("partitionOverwriteMode", "dynamic")
         .parquet(output_path))
         
        # Renombrar archivos
        rename_spark_output("gold", "fact_gastos", output_path)
         
        print("✅ Gold Fact Gastos procesada.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_fact_gastos()
