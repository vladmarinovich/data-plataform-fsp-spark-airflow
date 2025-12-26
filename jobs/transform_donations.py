"""
Job PySpark: Transformación de Donaciones CRM
==============================================

Este job lee datos de donaciones desde archivos Parquet (simulando extracción del CRM),
aplica transformaciones de negocio y escribe el resultado en formato Parquet particionado.

Uso:
    spark-submit --master local[*] jobs/transform_donations.py

Arquitectura:
    Bronze (Raw) -> Silver (Cleaned) -> Gold (Aggregated)
"""
import sys
from pathlib import Path
from datetime import datetime

# Agregar proyecto al path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, 
    sum as spark_sum, count, avg,
    when, lit, coalesce, to_date
)

import config
from jobs.utils.spark_session import get_spark_session, stop_spark_session
from jobs.utils.data_quality import (
    validate_required_columns,
    validate_date_range,
    validate_positive_amounts,
    log_dataframe_summary
)


def read_bronze_data(spark) -> DataFrame:
    """
    Lee datos crudos de donaciones desde la capa Bronze.
    
    En producción, esto leería desde:
    - S3/GCS/Azure Blob Storage
    - Delta Lake
    - Iceberg Tables
    
    Para desarrollo local, lee desde data/raw/
    
    Returns:
        DataFrame con datos crudos de donaciones
    """
    print("📥 Leyendo datos Bronze (raw)...")
    
    # Path al archivo de entrada
    input_path = config.DATA_RAW_DIR / "donaciones_mock.parquet"
    
    if not input_path.exists():
        raise FileNotFoundError(
            f"❌ No se encontró archivo de entrada: {input_path}\n"
            f"   Ejecuta primero: python scripts/generate_mock_data.py"
        )
    
    df = spark.read.parquet(str(input_path))
    log_dataframe_summary(df, "Bronze - Donaciones")
    
    return df


def transform_to_silver(df: DataFrame) -> DataFrame:
    """
    Transforma datos Bronze a Silver (limpieza y estandarización).
    
    Transformaciones aplicadas:
    1. Validar columnas requeridas
    2. Convertir tipos de datos
    3. Manejar valores nulos
    4. Agregar columnas derivadas (año, mes, día)
    5. Filtrar registros inválidos
    
    Args:
        df: DataFrame Bronze (raw)
        
    Returns:
        DataFrame Silver (cleaned)
    """
    print("\n🔄 Transformando a capa Silver (cleaned)...")
    
    # 1. Validar esquema
    validate_required_columns(
        df, 
        config.REQUIRED_COLUMNS["donaciones"],
        "donaciones"
    )
    
    # 2. Limpiar y estandarizar
    df_silver = (
        df
        # Convertir fecha a tipo Date si viene como string
        .withColumn("fecha_donacion", to_date(col("fecha_donacion")))
        
        # Manejar montos nulos (reemplazar con 0 o filtrar según negocio)
        .withColumn("monto", coalesce(col("monto"), lit(0.0)))
        
        # Agregar columnas de particionamiento
        .withColumn("anio", year(col("fecha_donacion")))
        .withColumn("mes", month(col("fecha_donacion")))
        .withColumn("dia", dayofmonth(col("fecha_donacion")))
        
        # Agregar metadatos de procesamiento
        .withColumn("processed_at", lit(datetime.now()))
        
        # Filtrar registros inválidos (opcional según reglas de negocio)
        .filter(col("monto") > 0)
        .filter(col("fecha_donacion").isNotNull())
    )
    
    # 3. Validaciones de calidad
    validate_date_range(df_silver, "fecha_donacion", min_year=2020)
    validate_positive_amounts(df_silver, "monto")
    
    log_dataframe_summary(df_silver, "Silver - Donaciones")
    
    return df_silver


def transform_to_gold(df_silver: DataFrame) -> DataFrame:
    """
    Transforma datos Silver a Gold (agregaciones de negocio).
    
    Crea métricas agregadas por mes:
    - Total donaciones
    - Cantidad de donaciones
    - Promedio de donación
    - Donación máxima/mínima
    
    Args:
        df_silver: DataFrame Silver (cleaned)
        
    Returns:
        DataFrame Gold (aggregated)
    """
    print("\n📊 Transformando a capa Gold (aggregated)...")
    
    df_gold = (
        df_silver
        .groupBy("anio", "mes")
        .agg(
            spark_sum("monto").alias("total_donaciones"),
            count("id").alias("cantidad_donaciones"),
            avg("monto").alias("promedio_donacion"),
            # Agregar más métricas según necesidad
        )
        .orderBy("anio", "mes")
    )
    
    log_dataframe_summary(df_gold, "Gold - Donaciones Agregadas")
    
    return df_gold


def write_silver_data(df: DataFrame) -> None:
    """
    Escribe datos Silver particionados por fecha.
    
    Estrategia de escritura:
    - Formato: Parquet (compresión Snappy)
    - Particionamiento: Por año/mes para queries eficientes
    - Modo: Overwrite (en producción sería append con deduplicación)
    
    Args:
        df: DataFrame Silver a escribir
    """
    print("\n💾 Escribiendo datos Silver...")
    
    output_path = config.DATA_PROCESSED_DIR / "silver" / "donaciones"
    
    (
        df
        .write
        .mode("overwrite")  # En producción: "append" con merge
        .partitionBy("anio", "mes")
        .parquet(str(output_path))
    )
    
    print(f"✅ Datos Silver escritos en: {output_path}")


def write_gold_data(df: DataFrame) -> None:
    """
    Escribe datos Gold (agregados).
    
    Args:
        df: DataFrame Gold a escribir
    """
    print("\n💾 Escribiendo datos Gold...")
    
    output_path = config.DATA_OUTPUT_DIR / "gold" / "donaciones_monthly"
    
    (
        df
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )
    
    print(f"✅ Datos Gold escritos en: {output_path}")


def main():
    """
    Función principal del job.
    Orquesta el flujo Bronze -> Silver -> Gold.
    """
    print("=" * 60)
    print("🚀 Iniciando Job: Transform Donations")
    print("=" * 60)
    
    spark = None
    
    try:
        # 1. Inicializar Spark
        spark = get_spark_session("Transform-Donations")
        
        # 2. Bronze -> Silver
        df_bronze = read_bronze_data(spark)
        df_silver = transform_to_silver(df_bronze)
        write_silver_data(df_silver)
        
        # 3. Silver -> Gold
        df_gold = transform_to_gold(df_silver)
        write_gold_data(df_gold)
        
        print("\n" + "=" * 60)
        print("✅ Job completado exitosamente")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error en el job: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Siempre detener Spark
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
