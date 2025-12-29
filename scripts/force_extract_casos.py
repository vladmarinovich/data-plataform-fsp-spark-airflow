"""
Script para forzar la Extracción Completa de CASOS desde Supabase.
Ignora watermarks y trae todo el historial.
"""
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.extract_from_supabase import get_supabase_client, write_to_bronze
import config

def force_extract_casos():
    print("🚀 FORZANDO FULL LOAD: CASOS")
    
    try:
        # 1. Conectar
        client = get_supabase_client()
        
        # 2. Extraer TODO (sin filtro de fecha)
        print("   📥 Querying Supabase (SELECT * FROM casos)...")
        response = client.table("casos").select("*").execute()
        
        if not response.data:
            print("   ⚠️  Supabase devolvió 0 registros!")
            return

        df = pd.DataFrame(response.data)
        count = len(df)
        print(f"   ✅ Registros descargados: {count}")
        
        if count < 200:
             print("   ⚠️  WARNING: Aún son menos de 200. ¿La base de datos tiene realmente 256?")
             
        # 3. Escribir a Bronze
        write_to_bronze(df, "casos", mode="overwrite")
        
        print("\n✅ Proceso completado.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    force_extract_casos()
