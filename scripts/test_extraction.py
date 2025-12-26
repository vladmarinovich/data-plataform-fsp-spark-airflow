"""
Script de prueba para extraer 20 registros de cada tabla y verificar schemas.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from supabase import create_client
import config

# Conectar a Supabase
client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)

# Tablas a probar
TABLES = ["donaciones", "gastos", "donantes", "casos", "proveedores"]

print("="*70)
print("🔍 PRUEBA DE EXTRACCIÓN - 20 REGISTROS POR TABLA")
print("="*70)

for table_name in TABLES:
    print(f"\n{'='*70}")
    print(f"📊 Tabla: {table_name}")
    print(f"{'='*70}")
    
    try:
        # Extraer 20 registros
        response = client.table(table_name).select("*").limit(20).execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            
            print(f"\n✅ Registros extraídos: {len(df)}")
            print(f"\n📋 Schema (columnas y tipos):")
            print("-" * 70)
            for col in df.columns:
                dtype = df[col].dtype
                non_null = df[col].notna().sum()
                sample = df[col].iloc[0] if len(df) > 0 else None
                print(f"  {col:25s} | {str(dtype):15s} | Non-null: {non_null:2d}/20 | Sample: {sample}")
            
            print(f"\n📄 Primeros 3 registros:")
            print("-" * 70)
            print(df.head(3).to_string())
            
            # Guardar en data/raw/
            output_path = config.DATA_RAW_DIR / f"{table_name}_sample.parquet"
            df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
            print(f"\n💾 Guardado en: {output_path}")
            
        else:
            print(f"\n⚠️  Tabla vacía (0 registros)")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")

print(f"\n{'='*70}")
print("✅ Prueba completada")
print(f"{'='*70}")
