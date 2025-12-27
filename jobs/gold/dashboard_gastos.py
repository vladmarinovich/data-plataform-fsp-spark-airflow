
"""
Job Gold: Dashboard Gastos.
Tabla optimizada para dashboards de gastos, uniendo hechos con dimensiones.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_dashboard_gastos():
    spark = get_spark_session("GoldDashboardGastos")
    try:
        print("🚀 JOB GOLD: DASHBOARD GASTOS")
        
        # Paths
        fact_gastos_path = f"{config.GOLD_PATH}/fact_gastos"
        silver_proveedores_path = f"{config.SILVER_PATH}/proveedores"
        silver_casos_path = f"{config.SILVER_PATH}/casos"
        output_path = f"{config.GOLD_PATH}/dashboard_gastos"
        
        # Lectura
        df_fact = spark.read.parquet(fact_gastos_path)
        df_dim_prov = spark.read.parquet(silver_proveedores_path)
        df_dim_casos = spark.read.parquet(silver_casos_path)
        
        # Join
        df_base = df_fact.alias("g").join(
            df_dim_prov.alias("p"), "id_proveedor", "left"
        ).join(
            df_dim_casos.alias("c"), "id_caso", "left"
        ).select(
            "g.*",
            "p.nombre_proveedor", "p.categoria_proveedor", 
            F.col("p.ciudad_normalizada").alias("proveedor_ciudad"),
            F.col("p.tiene_id_valido").alias("proveedor_id_valido"),
            "c.nombre_caso", F.col("c.estado").alias("estado_caso"),
            "c.presupuesto_estimado", "c.fecha_salida"
        )
        
        window_prov = Window.partitionBy("id_proveedor")
        
        # Métricas
        df_final = df_base.withColumn(
            "es_caso_activo", F.when(F.col("fecha_salida").isNull(), True).otherwise(False)
        ).withColumn(
            "anio", F.year("fecha_pago")
        ).withColumn(
            "mes", F.month("fecha_pago")
        ).withColumn(
            "anio_mes", F.date_format("fecha_pago", "yyyy-MM")
        ).withColumn(
            "frecuencia_gastos", F.count("id_gasto").over(window_prov)
        ).withColumn(
            "monto_total_gastado", F.sum("monto").over(window_prov)
        ).withColumn(
            "monto_maximo_gasto", F.max("monto").over(window_prov)
        ).withColumn(
            "monto_minimo_gasto", F.min("monto").over(window_prov)
        ).withColumn(
            "recencia_gasto_dias", F.datediff(F.current_date(), F.col("fecha_pago"))
        ).withColumn(
            "es_gasto_critico", 
            F.when(F.col("tipo_gasto_estandarizado").isin('Veterinaria', 'UCI'), True).otherwise(False)
        ).withColumn(
            "monto_log", F.log1p(F.col("monto"))
        ).withColumn(
            "categoria_monto",
            F.when(F.col("monto") < 200000, "Pequeño")
             .when(F.col("monto").between(200000, 400000), "Mediano")
             .otherwise("Grande")
        ).withColumn(
            "sobrepresupuesto",
            F.when(F.col("presupuesto_estimado").isNull(), None)
             .when(F.col("monto") > F.col("presupuesto_estimado"), True)
             .otherwise(False)
        )
        
        # Escritura
        (df_final.write.mode("overwrite").parquet(output_path))
        
        # Renombrar
        rename_spark_output("gold", "dashboard_gastos", output_path)
        
        print("✅ Gold Dashboard Gastos procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_dashboard_gastos()
