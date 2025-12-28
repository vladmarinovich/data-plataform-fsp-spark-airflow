
"""
Job Gold: Dashboard Financiero.
Unión de Dashboard Donaciones y Dashboard Gastos para métricas financieras de alto nivel.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_dashboard_financiero():
    spark = get_spark_session("GoldDashboardFinanciero")
    try:
        print("🚀 JOB GOLD: DASHBOARD FINANCIERO")
        
        # Paths
        dash_donaciones_path = f"{config.GOLD_PATH}/dashboard_donaciones"
        dash_gastos_path = f"{config.GOLD_PATH}/dashboard_gastos"
        output_path = f"{config.GOLD_PATH}/dashboard_financiero"
        
        # Lectura
        df_don = spark.read.parquet(dash_donaciones_path)
        df_gas = spark.read.parquet(dash_gastos_path)
        
        # Preparar unión (unificando a nivel de mes/año)
        # Notas: id_proveedor e id_caso son NULL en donaciones
        # id_donante es NULL en gastos
        df_don_union = df_don.select(
            F.to_date(F.col("anio_mes"), "yyyy-MM").alias("anio_mes"),
            "id_donante",
            F.lit(None).alias("id_proveedor"),
            F.lit(None).alias("id_caso"),
            "monto",
            F.lit(False).alias("es_gasto"),
            F.lit(False).alias("sobrepresupuesto"),
            "categoria_monto"
        )
        
        df_gas_union = df_gas.select(
            F.to_date(F.col("anio_mes"), "yyyy-MM").alias("anio_mes"),
            F.lit(None).alias("id_donante"),
            "id_proveedor",
            "id_caso",
            "monto",
            F.lit(True).alias("es_gasto"),
            "sobrepresupuesto",
            "categoria_monto"
        )
        
        df_movimientos = df_don_union.unionByName(df_gas_union)
        
        # Agregaciones por Mes
        df_agg = df_movimientos.groupBy("anio_mes").agg(
            F.sum(F.when(F.col("es_gasto") == False, F.col("monto")).otherwise(0)).alias("total_donado"),
            F.sum(F.when(F.col("es_gasto") == True, F.col("monto")).otherwise(0)).alias("total_gastado"),
            F.sum(F.when(F.col("sobrepresupuesto") == True, F.col("monto")).otherwise(0)).alias("gastos_sobrepresupuesto"),
            F.countDistinct("id_caso").alias("casos_activos_mes"),
            F.countDistinct("id_donante").alias("donantes_activos_mes"),
            F.countDistinct("id_proveedor").alias("proveedores_activos_mes")
        )
        
        # Window para acumulados
        window_hist = Window.orderBy("anio_mes").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        window_rolling = Window.orderBy("anio_mes").rowsBetween(-2, Window.currentRow)
        
        # Métricas Finales
        df_final = df_agg.withColumn(
            "balance_neto", F.col("total_donado") - F.col("total_gastado")
        ).withColumn(
            "balance_acumulado", F.sum(F.col("total_donado") - F.col("total_gastado")).over(window_hist)
        ).withColumn(
            "rolling_3m_ingresos", F.sum("total_donado").over(window_rolling)
        ).withColumn(
            "rolling_3m_gastos", F.sum("total_gastado").over(window_rolling)
        ).withColumn(
            "porcentaje_sobrepresupuesto", F.col("gastos_sobrepresupuesto") / F.col("total_gastado")
        ).withColumn(
            "runway_financiero", 
            F.sum(F.col("total_donado") - F.col("total_gastado")).over(window_hist) / F.col("total_gastado")
        ).withColumn(
            "estado_financiero_mes",
            F.when(F.col("balance_neto") > 0, "Superávit")
             .when(F.col("balance_neto") < 0, "Déficit")
             .otherwise("Equilibrado")
        )
        
        # Escritura
        (df_final.write.mode("overwrite").parquet(output_path))
        
        # Renombrar
        rename_spark_output("gold", "dashboard_financiero", output_path)
        
        print("✅ Gold Dashboard Financiero procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_dashboard_financiero()
