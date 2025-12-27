#!/usr/bin/env python3
"""
Script para inspeccionar datos en cuarentena.
Muestra registros que fallaron las validaciones de Data Quality.
"""
import sys
from pathlib import Path

# Agregar el proyecto al path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from jobs.utils.spark_session import get_spark_session
import config

def inspect_quarantine(table_name=None):
    """Inspeccionar datos en cuarentena"""
    spark = get_spark_session("QuarantineInspector")
    
    quarantine_base = Path(config.QUARANTINE_PATH)
    
    if table_name:
        tables = [table_name]
    else:
        # Listar todas las tablas con cuarentena
        if quarantine_base.exists():
            tables = [d.name for d in quarantine_base.iterdir() if d.is_dir()]
        else:
            print("❌ No existe directorio de cuarentena")
            return
    
    for table in tables:
        quarantine_path = str(quarantine_base / table)
        
        try:
            df = spark.read.parquet(quarantine_path)
            count = df.count()
            
            if count == 0:
                print(f"\n✅ {table}: Sin registros en cuarentena")
                continue
                
            print(f"\n☣️  {table}: {count} registros rechazados")
            print("="*80)
            
            # Mostrar primeros registros con sus errores
            if "dq_errors" in df.columns:
                df.select("*").show(20, truncate=False)
            else:
                df.show(20, truncate=False)
                
        except Exception as e:
            if "Path does not exist" in str(e):
                print(f"\n✅ {table}: Sin cuarentena (ningún registro rechazado)")
            else:
                print(f"\n❌ {table}: Error al leer - {str(e)}")
    
    spark.stop()

if __name__ == "__main__":
    table = sys.argv[1] if len(sys.argv) > 1 else None
    
    if table:
        print(f"🔍 Inspeccionando cuarentena de: {table}")
    else:
        print("🔍 Inspeccionando TODA la cuarentena...")
    
    inspect_quarantine(table)
