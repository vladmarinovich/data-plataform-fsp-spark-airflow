"""
Validaciones de calidad de datos para PySpark DataFrames.
Implementa checks defensivos antes de escribir datos.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


def validate_required_columns(df: DataFrame, required_cols: List[str], table_name: str) -> bool:
    """
    Valida que todas las columnas requeridas existan en el DataFrame.
    
    Args:
        df: DataFrame a validar
        required_cols: Lista de nombres de columnas requeridas
        table_name: Nombre de la tabla (para logging)
        
    Returns:
        True si todas las columnas existen, False en caso contrario
        
    Raises:
        ValueError: Si faltan columnas requeridas
    """
    missing_cols = set(required_cols) - set(df.columns)
    
    if missing_cols:
        error_msg = f"❌ Tabla '{table_name}': Faltan columnas requeridas: {missing_cols}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"✅ Tabla '{table_name}': Todas las columnas requeridas presentes")
    return True


def check_null_values(df: DataFrame, columns: List[str]) -> Dict[str, int]:
    """
    Cuenta valores nulos en columnas específicas.
    
    Args:
        df: DataFrame a analizar
        columns: Lista de columnas a verificar
        
    Returns:
        Diccionario {columna: cantidad_nulos}
    """
    null_counts = {}
    
    for col_name in columns:
        if col_name in df.columns:
            null_count = df.filter(
                col(col_name).isNull() | isnan(col(col_name))
            ).count()
            null_counts[col_name] = null_count
            
            if null_count > 0:
                logger.warning(f"⚠️  Columna '{col_name}': {null_count} valores nulos")
    
    return null_counts


def validate_date_range(df: DataFrame, date_col: str, min_year: int = 2020) -> bool:
    """
    Valida que las fechas estén en un rango razonable.
    
    Args:
        df: DataFrame a validar
        date_col: Nombre de la columna de fecha
        min_year: Año mínimo aceptable
        
    Returns:
        True si todas las fechas son válidas
        
    Raises:
        ValueError: Si hay fechas fuera del rango
    """
    from pyspark.sql.functions import year
    
    invalid_dates = df.filter(
        (year(col(date_col)) < min_year) | 
        (year(col(date_col)) > 2030)
    ).count()
    
    if invalid_dates > 0:
        error_msg = f"❌ Columna '{date_col}': {invalid_dates} fechas fuera de rango"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"✅ Columna '{date_col}': Todas las fechas válidas")
    return True


def validate_positive_amounts(df: DataFrame, amount_col: str) -> bool:
    """
    Valida que los montos sean positivos.
    
    Args:
        df: DataFrame a validar
        amount_col: Nombre de la columna de monto
        
    Returns:
        True si todos los montos son positivos
        
    Raises:
        ValueError: Si hay montos negativos o cero
    """
    invalid_amounts = df.filter(col(amount_col) <= 0).count()
    
    if invalid_amounts > 0:
        error_msg = f"❌ Columna '{amount_col}': {invalid_amounts} montos <= 0"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"✅ Columna '{amount_col}': Todos los montos positivos")
    return True


def log_dataframe_summary(df: DataFrame, name: str) -> None:
    """
    Imprime un resumen del DataFrame para debugging.
    
    Args:
        df: DataFrame a resumir
        name: Nombre descriptivo del DataFrame
    """
    row_count = df.count()
    col_count = len(df.columns)
    
    logger.info(f"📊 DataFrame '{name}':")
    logger.info(f"   - Filas: {row_count:,}")
    logger.info(f"   - Columnas: {col_count}")
    logger.info(f"   - Schema: {df.columns}")
    
    # Mostrar primeras filas en modo debug
    if logger.level == logging.DEBUG:
        df.show(5, truncate=False)
