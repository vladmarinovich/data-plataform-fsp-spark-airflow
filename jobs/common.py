"""
Lógica compartida y agnóstica para jobs de PySpark.
Define contratos de lectura/escritura y funciones estándar.
"""
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Importar config desde directorio superior
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

def get_spark_session(app_name: str):
    """Crea sesión de Spark configurada para el Data Platform."""
    
    # Ruta al jar del conector GCS
    jar_path = str(Path(__file__).parent.parent / "lib" / "gcs-connector-hadoop3-latest.jar")
    
    return (SparkSession.builder
        .appName(app_name)
        .master(config.SPARK_MASTER)
        # Configurar JARs externos
        .config("spark.jars", jar_path)
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        # Configurar FileSystem GCS
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        # Autenticación (usa Application Default Credentials por defecto)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        # Importante: Permite sobrescribir solo las particiones afectadas
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate())

def read_raw(spark, table_name):
    """
    Lee datos desde la capa RAW (agnóstico del cloud provider).
    """
    path = f"{config.RAW_PATH}/raw_{table_name}"
    print(f"📖 LEYENDO RAW: {path}")
    
    try:
        return spark.read.parquet(path)
    except Exception as e:
        print(f"⚠️ Error leyendo {path}: {e}")
        # Retornar DataFrame vacío si falla (para no romper el job completo si es opcional)
        # O re-lanzar la excepción si es crítico. Aquí re-lanzamos.
        raise e

def write_silver(df, table_name, partition_cols=["anio", "mes"]):
    """
    Escribe datos en la capa SILVER (Formato Parquet).
    """
    path = f"{config.SILVER_PATH}/{table_name}"
    print(f"💾 ESCRIBIENDO SILVER: {path}")
    
    if "anio" not in df.columns and "anio" in partition_cols:
         print("⚠️ Advertencia: No se encontraron columnas de partición, se escribirá sin particionar.")
         partition_cols = []

    (df.write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(path))
    print(f"✅ Escritura completada: {path}")

def standard_dedup(df, id_col, date_col="last_modified_at"):
    """
    Aplica deduplicación estándar CDC (Change Data Capture).
    Se queda con el registro con fecha más reciente.
    """
    print(f"🔄 DEDUPLICANDO por {id_col} usando {date_col}")
    
    # Validar que exista la columna de ordenamiento
    sort_col = date_col
    if date_col not in df.columns:
        if "created_at" in df.columns:
            print(f"⚠️ Columna {date_col} no encontrada, usando 'created_at' como fallback.")
            sort_col = "created_at"
        else:
            print(f"⚠️ Columna CDC no encontrada. No se puede deduplicar confiablemente.")
            return df

    window = Window.partitionBy(id_col).orderBy(F.col(sort_col).desc())
    
    df_dedup = (df.withColumn("rn", F.row_number().over(window))
                  .filter(F.col("rn") == 1)
                  .drop("rn"))
    
    count_before = df.count()
    count_after = df_dedup.count()
    print(f"   📉 Reducción: {count_before} -> {count_after} registros")
    
    return df_dedup
