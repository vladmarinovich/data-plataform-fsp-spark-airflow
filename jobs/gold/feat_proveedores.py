
"""
Job Gold: Feat Proveedores.
Métricas de rendimiento y gasto por proveedor.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_feat_proveedores():
    spark = get_spark_session("GoldFeatProveedores")
    try:
        print("🚀 JOB GOLD: FEAT PROVEEDORES")
        
        # Paths
        dim_proveedores_path = f"{config.GOLD_PATH}/dim_proveedores"
        fact_gastos_path = f"{config.GOLD_PATH}/fact_gastos"
        output_path = f"{config.GOLD_PATH}/feat_proveedores"
        
        # Lectura
        df_dim_prov = spark.read.parquet(dim_proveedores_path)
        df_fact_gastos = spark.read.parquet(fact_gastos_path)
        
        # Agregaciones
        df_agg = df_fact_gastos.groupBy("id_proveedor").agg(
            F.sum("monto").alias("total_gastado_proveedor"),
            F.count("id_gasto").alias("num_transacciones"),
            F.avg("monto").alias("promedio_transaccion"),
            F.max("fecha_pago").alias("ultimo_pago_proveedor")
        )
        
        # Join
        df_feat = df_dim_prov.join(df_agg, "id_proveedor", "left")
        
        # Features
        df_final = df_feat.withColumn(
            "importancia_proveedor",
            F.when(F.col("total_gastado_proveedor") > 5000000, "Socio Estratégico")
             .when(F.col("total_gastado_proveedor") > 1000000, "Proveedor Frecuente")
             .otherwise("Transaccional")
        )
        
        # Escritura
        (df_final.write.mode("overwrite").parquet(output_path))
        
        # Renombrar
        rename_spark_output("gold", "feat_proveedores", output_path)
        
        print("✅ Gold Feat Proveedores procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_feat_proveedores()
