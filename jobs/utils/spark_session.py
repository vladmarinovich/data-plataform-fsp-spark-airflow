"""
Factory para crear SparkSession configurada correctamente.
Centraliza la configuración de Spark para todos los jobs.
"""
from pyspark.sql import SparkSession
import config


def get_spark_session(app_name: str = None) -> SparkSession:
    """
    Crea y retorna una SparkSession configurada.
    
    Args:
        app_name: Nombre de la aplicación Spark. Si es None, usa el default del config.
        
    Returns:
        SparkSession configurada y lista para usar.
        
    Example:
        >>> spark = get_spark_session("MiJob")
        >>> df = spark.read.parquet("data/input.parquet")
        >>> spark.stop()
    """
    app_name = app_name or config.SPARK_APP_NAME
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY)
        # Configuraciones adicionales para optimización
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Configuración para evitar warnings de Hive
        .config("spark.sql.warehouse.dir", str(config.PROJECT_ROOT / "spark-warehouse"))
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )
    
    # Configurar nivel de log (menos verbose)
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Detiene la SparkSession de forma segura.
    
    Args:
        spark: SparkSession a detener.
    """
    if spark:
        spark.stop()
