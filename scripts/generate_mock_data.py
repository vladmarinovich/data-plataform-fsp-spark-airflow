
"""
Script para generar datos de prueba (Mock Data) para el Sandbox Local.
Genera 50 registros por tabla con mezcla de formatos (Micros vs Strings).
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import config

def generate_mock_data():
    # Leer argumento de tabla si existe
    target_table = sys.argv[1] if len(sys.argv) > 1 else "all"

    spark = SparkSession.builder.appName(f"MockDataGen_{target_table}").getOrCreate()
    
    print(f"🏗️ Generando Sandbox Local para: {target_table}...")

    # ... (Datos base y esquemas se mantienen igual)
    base_time_micros = 1704067200000000 
    base_time_str = "2024-06-15T12:00:00"

    from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

    # Esquemas (Se definen siempre)
    schema_donantes = StructType([
        StructField("id_donante", LongType()), StructField("nombre", StringType()),
        StructField("correo", StringType()), StructField("telefono", StringType()),
        StructField("pais", StringType()), StructField("ciudad", StringType()),
        StructField("id_organizacion", LongType()), StructField("created_at", LongType()),
        StructField("last_modified_at", LongType())
    ])

    schema_casos = StructType([
        StructField("id_caso", LongType()), StructField("nombre_caso", StringType()),
        StructField("veterinaria", StringType()), StructField("diagnostico", StringType()),
        StructField("ciudad", StringType()), StructField("id_hogar_de_paso", LongType()),
        StructField("presupuesto_estimado", DoubleType()), StructField("fecha_ingreso", StringType()),
        StructField("fecha_salida", StringType()), StructField("estado", StringType()),
        StructField("created_at", LongType()), StructField("last_modified_at", LongType())
    ])

    schema_donaciones = StructType([
        StructField("id_donacion", LongType()), StructField("id_donante", LongType()),
        StructField("id_caso", LongType()), StructField("fecha_donacion", LongType()),
        StructField("monto", DoubleType()), StructField("medio_pago", StringType()),
        StructField("estado", StringType()), StructField("comprobante", StringType()),
        StructField("created_at", StringType()), StructField("last_modified_at", LongType())
    ])

    schema_gastos = StructType([
        StructField("id_gasto", LongType()), StructField("id_caso", LongType()),
        StructField("id_proveedor", LongType()), StructField("monto", DoubleType()),
        StructField("fecha_pago", LongType()), StructField("medio_pago", StringType()),
        StructField("descripcion", StringType()), StructField("created_at", LongType()),
        StructField("last_modified_at", LongType())
    ])

    # 1. RAW DONANTES
    if target_table in ["all", "donantes"]:
        donantes_data = []
        for i in range(1, 51):
            donantes_data.append((
                i, f"Donante {i}", f"donante{i}@ejemplo.com", "3001234567",
                "Colombia" if i % 2 == 0 else "España", "Bogota", 1,
                base_time_micros + (i * 10000000), base_time_micros + (i * 10000000)
            ))
        donantes_data.append((541, "Donante Anonimo", None, None, "Colombia", "Bogota", 1, base_time_micros, base_time_micros))
        df_donantes = spark.createDataFrame(donantes_data, schema_donantes)
        df_donantes.write.mode("overwrite").parquet(f"{config.RAW_PATH}/raw_donantes")
        print("✅ RAW Donantes generado.")

    # 2. RAW CASOS
    if target_table in ["all", "casos"]:
        casos_data = []
        for i in range(1, 51):
            casos_data.append((
                i, f"Mascota {i}", "CBA" if i % 3 == 0 else None, "Recuperacion", "Bogota",
                i % 5 + 1, 3500000.0, str(base_time_str) if i % 2 == 0 else str(base_time_micros + (i * 10000000)),
                None, "urgente", base_time_micros, base_time_micros
            ))
        casos_data.append((541, "Caso Genérico", "CBA", "Revision", "Bogota", 9, 0.0, base_time_str, None, "no urgente", base_time_micros, base_time_micros))
        df_casos = spark.createDataFrame(casos_data, schema_casos)
        df_casos.write.mode("overwrite").parquet(f"{config.RAW_PATH}/raw_casos")
        print("✅ RAW Casos generado.")

    # 3. RAW DONACIONES
    if target_table in ["all", "donaciones"]:
        donaciones_data = []
        for i in range(1, 51):
            donaciones_data.append((
                i, i if i % 10 != 0 else None, i if i % 15 != 0 else None,
                base_time_micros + (i * 86400000000), 100000.0 + (i * 1000),
                "Transferencia Bancaria" if i % 2 == 0 else "Efectivo",
                "completada", "voucher123", str(base_time_str) if i % 2 == 0 else None,
                base_time_micros
            ))
        # Registro MALO para probar CUARENTENA:
        donaciones_data.append((999, 1, 1, base_time_micros, -50.0, "Efectivo", "error", "v1", str(base_time_str), base_time_micros))
        df_donaciones = spark.createDataFrame(donaciones_data, schema_donaciones)
        df_donaciones.write.mode("overwrite").parquet(f"{config.RAW_PATH}/raw_donaciones")
        print("✅ RAW Donaciones generado.")

    # 4. RAW GASTOS
    if target_table in ["all", "gastos"]:
        gastos_data = []
        for i in range(1, 51):
            gastos_data.append((
                i, i % 10 + 1, i % 5 + 1, 50000.0 * i, 
                base_time_micros + (i * 10000000), "Tarjeta" if i % 2 == 0 else "Transferencia", 
                f"Gasto medico {i}", base_time_micros, base_time_micros
            ))
        df_gastos = spark.createDataFrame(gastos_data, schema_gastos)
        df_gastos.write.mode("overwrite").parquet(f"{config.RAW_PATH}/raw_gastos")
        print("✅ RAW Gastos generado.")

    # 5. RAW PROVEEDORES
    if target_table in ["all", "proveedores"]:
        schema_prov_local = StructType([
            StructField("id_proveedor", LongType()), StructField("nombre", StringType()),
            StructField("tipo_servicio", StringType()), StructField("nit", StringType()),
            StructField("contacto", StringType()), StructField("email", StringType()),
            StructField("telefono", StringType()), StructField("ciudad", StringType()),
            StructField("created_at", LongType()), StructField("last_modified_at", LongType())
        ])
        proveedores_data = []
        for i in range(1, 51):
            proveedores_data.append((
                i, f"Vet Proveedor {i}", "Veterinaria", "800123-1", f"Contacto {i}", 
                f"proveedor{i}@vets.com", "300444555", "Bogota", base_time_micros, base_time_micros
            ))
        df_prov = spark.createDataFrame(proveedores_data, schema_prov_local)
        df_prov.write.mode("overwrite").parquet(f"{config.RAW_PATH}/raw_proveedores")
        print("✅ RAW Proveedores generado.")

    # 6. RAW HOGAR DE PASO
    if target_table in ["all", "hogar_de_paso"]:
        schema_hogar_local = StructType([
            StructField("id_hogar_de_paso", LongType()), StructField("nombre", StringType()),
            StructField("tipo_hogar", StringType()), StructField("encargado", StringType()),
            StructField("email", StringType()), StructField("telefono", StringType()),
            StructField("ciudad", StringType()), StructField("pais", StringType()),
            StructField("capacidad_maxima", LongType()), StructField("costo_mensual", DoubleType()),
            StructField("cupos_disponibles", LongType()), StructField("created_at", LongType()),
            StructField("last_modified_at", LongType())
        ])
        hogares_data = []
        for i in range(1, 11):
            hogares_data.append((
                i, f"Hogar Feliz {i}", "Temporario", f"Madre/Padre {i}", f"hogar{i}@apoyo.com", 
                "311555222", "Bogota", "COLOMBIA", 5, 25000.0, 5, base_time_micros, base_time_micros
            ))
        df_hogar = spark.createDataFrame(hogares_data, schema_hogar_local)
        df_hogar.write.mode("overwrite").parquet(f"{config.RAW_PATH}/raw_hogar_de_paso")
        print("✅ RAW Hogar de Paso generado.")

    spark.stop()

if __name__ == "__main__":
    generate_mock_data()
