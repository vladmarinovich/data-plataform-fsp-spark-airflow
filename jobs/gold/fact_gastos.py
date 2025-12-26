
"""
Job Gold: Fact Gastos.
Lógica: Categorización de business rules sobre strings y métricas.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session

def run_gold_fact_gastos():
    spark = get_spark_session("GoldFactGastos")
    try:
        print("🚀 JOB GOLD: FACT GASTOS")
        silver_gastos = f"{config.SILVER_PATH}/gastos"
        output_path = f"{config.GOLD_PATH}/fact_gastos"
        
        df = spark.read.option("basePath", silver_gastos).parquet(silver_gastos + "/*")
        
        # Filtro: Solo pagados
        df = df.filter(F.lower(F.col("estado")) == "pagado")

        # Categorización por nombre
        nombre = F.lower(F.col("nombre_gasto"))
        df_enriched = df.withColumn("tipo_gasto_estandarizado", 
            F.when(nombre.like("%veterin%"), "Veterinaria")
             .when(nombre.like("%insumo%"), "Insumos")
             .when(nombre.like("%transpor%"), "Transporte")
             .when(nombre.like("%uci%"), "UCI")
             .otherwise("General")
        )

        # Flag Crítico
        df_enriched = df_enriched.withColumn("es_gasto_critico", 
            F.when(nombre.like("%veterin%"), True)
             .when(nombre.like("%uci%"), True)
             .otherwise(False)
        )

        # Métricas
        df_enriched = df_enriched.withColumn("monto_total_log", F.log1p(F.col("monto"))) \
                                 .withColumn("recencia_gasto_dias", F.datediff(F.current_date(), F.col("fecha_pago")))
        
        # Particionamiento
        df_final = df_enriched.withColumn("anio_part", F.year("fecha_pago")) \
                              .withColumn("mes_part", F.month("fecha_pago"))
        
        (df_final.write.mode("overwrite")
         .partitionBy("anio_part", "mes_part")
         .option("partitionOverwriteMode", "dynamic")
         .parquet(output_path))
         
        print("✅ Gold Fact Gastos procesada.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_fact_gastos()
