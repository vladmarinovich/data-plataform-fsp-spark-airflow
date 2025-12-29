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
    
    # Configuración base
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY)
        # Configuración para GCS (Google Cloud Storage)
        .config("spark.jars", str(config.PROJECT_ROOT / "lib/gcs-connector-hadoop3-latest.jar"))
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        
        # Configuraciones de optimización para BAJO CONSUMO DE MEMORIA
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        .config("spark.sql.parquet.compression.codec", "snappy")
        
        # PARTITION PRUNING - Clave para manejar muchas particiones
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.hive.metastorePartitionPruning", "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        
        # AUMENTAR LÍMITES para manejar muchas particiones pequeñas
        .config("spark.sql.files.maxPartitionBytes", "268435456")  # 256MB (aumentado)
        .config("spark.sql.files.openCostInBytes", "8388608")      # 8MB (reducido para muchos archivos)
        .config("spark.sql.files.maxRecordsPerFile", "0")          # Sin límite
        
        # Reducir shuffles y paralelismo
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        
        # Reducir memoria de broadcast
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        
        # Serialización eficiente
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")
        
        # IMPORTANTE: Aumentar límite de archivos que Spark puede listar
        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "64")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")
        
        # Configuración Warehouse
        .config("spark.sql.warehouse.dir", str(config.PROJECT_ROOT / "spark-warehouse"))
        .config("spark.sql.catalogImplementation", "in-memory")
    )

    # Configuración específica para entorno LOCAL
    if config.ENV == "local":
        builder = builder.config("spark.ui.enabled", "false") \
                         .config("spark.driver.bindAddress", "127.0.0.1") \
                         .config("spark.driver.host", "127.0.0.1")

    spark = builder.getOrCreate()
    
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
