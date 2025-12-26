"""
Tests unitarios para transformaciones PySpark.

Ejecutar con:
    pytest tests/test_transformations.py -v
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType


@pytest.fixture(scope="session")
def spark():
    """Fixture para crear SparkSession para tests."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_donations_data(spark):
    """Fixture con datos de ejemplo para tests."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("fecha_donacion", DateType(), False),
        StructField("monto", DoubleType(), False),
        StructField("id_donante", IntegerType(), False),
    ])
    
    data = [
        (1, datetime(2024, 1, 15).date(), 100.0, 1),
        (2, datetime(2024, 1, 20).date(), 250.0, 2),
        (3, datetime(2024, 2, 10).date(), 500.0, 1),
        (4, datetime(2024, 2, 15).date(), 75.0, 3),
    ]
    
    return spark.createDataFrame(data, schema)


def test_dataframe_not_empty(sample_donations_data):
    """Test que el DataFrame no esté vacío."""
    assert sample_donations_data.count() > 0


def test_dataframe_has_correct_columns(sample_donations_data):
    """Test que el DataFrame tenga las columnas esperadas."""
    expected_columns = {"id", "fecha_donacion", "monto", "id_donante"}
    actual_columns = set(sample_donations_data.columns)
    assert expected_columns == actual_columns


def test_all_amounts_positive(sample_donations_data):
    """Test que todos los montos sean positivos."""
    negative_amounts = sample_donations_data.filter("monto <= 0").count()
    assert negative_amounts == 0


def test_no_null_dates(sample_donations_data):
    """Test que no haya fechas nulas."""
    null_dates = sample_donations_data.filter("fecha_donacion IS NULL").count()
    assert null_dates == 0


def test_aggregation_by_month(sample_donations_data):
    """Test de agregación por mes."""
    from pyspark.sql.functions import year, month, sum as spark_sum, count
    
    result = (
        sample_donations_data
        .withColumn("anio", year("fecha_donacion"))
        .withColumn("mes", month("fecha_donacion"))
        .groupBy("anio", "mes")
        .agg(
            spark_sum("monto").alias("total"),
            count("id").alias("cantidad")
        )
    )
    
    # Verificar que hay 2 meses (enero y febrero)
    assert result.count() == 2
    
    # Verificar totales de enero
    enero = result.filter("mes = 1").first()
    assert enero["total"] == 350.0  # 100 + 250
    assert enero["cantidad"] == 2


def test_date_range_validation(sample_donations_data):
    """Test que las fechas estén en un rango válido."""
    from pyspark.sql.functions import year
    
    invalid_dates = sample_donations_data.filter(
        (year("fecha_donacion") < 2020) | 
        (year("fecha_donacion") > 2030)
    ).count()
    
    assert invalid_dates == 0


def test_partitioning_columns_added(sample_donations_data):
    """Test que se agreguen columnas de particionamiento."""
    from pyspark.sql.functions import year, month, dayofmonth
    
    df_with_partitions = (
        sample_donations_data
        .withColumn("anio", year("fecha_donacion"))
        .withColumn("mes", month("fecha_donacion"))
        .withColumn("dia", dayofmonth("fecha_donacion"))
    )
    
    assert "anio" in df_with_partitions.columns
    assert "mes" in df_with_partitions.columns
    assert "dia" in df_with_partitions.columns


def test_filter_invalid_amounts(sample_donations_data):
    """Test de filtrado de montos inválidos."""
    # Agregar un registro con monto 0
    from pyspark.sql import Row
    
    invalid_row = Row(id=5, fecha_donacion=datetime(2024, 3, 1).date(), monto=0.0, id_donante=4)
    df_with_invalid = sample_donations_data.union(
        sample_donations_data.sparkSession.createDataFrame([invalid_row])
    )
    
    # Filtrar montos válidos
    df_filtered = df_with_invalid.filter("monto > 0")
    
    # Verificar que se eliminó el registro inválido
    assert df_filtered.count() == sample_donations_data.count()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
