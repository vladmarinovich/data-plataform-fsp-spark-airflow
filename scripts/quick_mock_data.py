#!/usr/bin/env python3
"""
Generador rápido de datos mock SIN Spark para testing local.
Crea archivos Parquet directamente usando Pandas + PyArrow.
"""
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import random

# Configuración
PROJECT_ROOT = Path(__file__).parent.parent
RAW_PATH = PROJECT_ROOT / "data/lake/raw"
RAW_PATH.mkdir(parents=True, exist_ok=True)

def generate_donantes():
    """Generar tabla donantes"""
    print("📦 Generando donantes...")
    data = []
    base_time = int(datetime(2024, 1, 1).timestamp() * 1000000)
    
    for i in range(1, 101):
        data.append({
            'id_donante': i,
            'id_organizacion': 1,
            'nombre': f'Donante {i}',
            'correo': f'donante{i}@example.com',
            'telefono': f'300{i:07d}',
            'ciudad': random.choice(['bogota', 'medellin', 'cali']),
            'pais': 'COLOMBIA',
            'created_at': base_time + (i * 1000000),
            'last_modified_at': base_time + (i * 1000000)
        })
    
    df = pd.DataFrame(data)
    output = RAW_PATH / "raw_donantes.parquet"
    df.to_parquet(output, index=False)
    print(f"✅ Generados {len(data)} donantes")

def generate_casos():
    """Generar tabla casos"""
    print("📦 Generando casos...")
    data = []
    base_time = int(datetime(2024, 1, 1).timestamp() * 1000000)
    
    for i in range(1, 51):
        data.append({
            'id_caso': i,
            'nombre_caso': f'perrito_{i}',
            'estado': 'no urgente',
            'fecha_ingreso': base_time + (i * 1000000),
            'fecha_salida': None,
            'veterinaria': 'cba',
            'diagnostico': f'diagnostico_{i}',
            'id_hogar_de_paso': (i % 5) + 1,
            'presupuesto_estimado': 3500000.0,
            'ciudad': 'bogota',
            'created_at': base_time,
            'last_modified_at': base_time + (i * 1000000)
        })
    
    df = pd.DataFrame(data)
    output = RAW_PATH / "raw_casos.parquet"
    df.to_parquet(output, index=False)
    print(f"✅ Generados {len(data)} casos")

def generate_donaciones():
    """Generar tabla donaciones"""
    print("📦 Generando donaciones...")
    data = []
    base_time = int(datetime(2024, 1, 1).timestamp() * 1000000)
    
    for i in range(1, 201):
        # Añadir 1 registro "malo" intencionalmente
        if i == 100:
            monto = -50000.0  # MONTO NEGATIVO (para cuarentena)
        else:
            monto = random.uniform(50000, 500000)
            
        data.append({
            'id_donacion': i,
            'id_donante': (i % 100) + 1,
            'id_caso': (i % 50) + 1,
            'fecha_donacion': base_time + (i * 1000000),
            'monto': monto,
            'medio_pago': random.choice(['Tarjeta', 'Transferencia', 'Efectivo']),
            'estado': 'completado',
            'created_at': base_time,
            'last_modified_at': base_time + (i * 1000000)
        })
    
    df = pd.DataFrame(data)
    output = RAW_PATH / "raw_donaciones.parquet"
    df.to_parquet(output, index=False)
    print(f"✅ Generados {len(data)} donaciones (1 con error intencional)")

def generate_gastos():
    """Generar tabla gastos"""
    print("📦 Generando gastos...")
    data = []
    base_time = int(datetime(2024, 1, 1).timestamp() * 1000000)
    
    for i in range(1, 51):
        data.append({
            'id_gasto': i,
            'id_caso': (i % 50) + 1,
            'id_proveedor': (i % 5) + 1,
            'monto': 50000.0 * i,
            'fecha_pago': base_time + (i * 10000000),
            'medio_pago': 'Tarjeta' if i % 2 == 0 else 'Transferencia',
            'descripcion': f'Gasto medico {i}',
            'created_at': base_time,
            'last_modified_at': base_time
        })
    
    df = pd.DataFrame(data)
    output = RAW_PATH / "raw_gastos.parquet"
    df.to_parquet(output, index=False)
    print(f"✅ Generados {len(data)} gastos")

def generate_proveedores():
    """Generar tabla proveedores"""
    print("📦 Generando proveedores...")
    data = []
    base_time = int(datetime(2024, 1, 1).timestamp() * 1000000)
    
    for i in range(1, 11):
        data.append({
            'id_proveedor': i,
            'nombre_proveedor': f'Proveedor {i}',
            'tipo_proveedor': 'Veterinaria',
            'nit': f'900{i:06d}',
            'nombre_contacto': f'Contacto {i}',
            'correo': f'proveedor{i}@example.com',
            'telefono': f'601{i:07d}',
            'ciudad': 'bogota',
            'created_at': base_time,
            'last_modified_at': base_time
        })
    
    df = pd.DataFrame(data)
    output = RAW_PATH / "raw_proveedores.parquet"
    df.to_parquet(output, index=False)
    print(f"✅ Generados {len(data)} proveedores")

def generate_hogar_de_paso():
    """Generar tabla hogar de paso"""
    print("📦 Generando hogar_de_paso...")
    data = []
    base_time = int(datetime(2024, 1, 1).timestamp() * 1000000)
    
    for i in range(1, 6):
        data.append({
            'id_hogar_de_paso': i,
            'nombre_hogar': f'HOGAR {i}',
            'tipo_hogar': 'temporario',
            'nombre_contacto': f'Contacto Hogar {i}',
            'correo': f'hogar{i}@example.com',
            'telefono': f'310{i:07d}',
            'ciudad': 'bogota',
            'pais': 'COLOMBIA',
            'capacidad_maxima': 10,
            'costo_mensual': 300000.0,
            'desempeno': 4.5,
            'ultimo_contacto': base_time
        })
    
    df = pd.DataFrame(data)
    output = RAW_PATH / "raw_hogar_de_paso.parquet"
    df.to_parquet(output, index=False)
    print(f"✅ Generados {len(data)} hogares")

GENERATORS = {
    'donantes': generate_donantes,
    'casos': generate_casos,
    'donaciones': generate_donaciones,
    'gastos': generate_gastos,
    'proveedores': generate_proveedores,
    'hogar_de_paso': generate_hogar_de_paso,
}

if __name__ == "__main__":
    if len(sys.argv) > 1:
        table_name = sys.argv[1]
        if table_name in GENERATORS:
            GENERATORS[table_name]()
        else:
            print(f"❌ Tabla desconocida: {table_name}")
            sys.exit(1)
    else:
        # Generar todas
        print("🚀 Generando TODAS las tablas mock...")
        for name, func in GENERATORS.items():
            func()
        print("\n✅ Generación completa!")
