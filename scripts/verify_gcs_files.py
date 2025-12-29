import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import subprocess
import config

BUCKET = "gs://salvando-patitas-spark"
TABLES = ['donaciones', 'gastos', 'donantes', 'casos', 'proveedores', 'hogar_de_paso']

def count_files_gcs(path):
    try:
        # Listar recursivamente y filtrar por .parquet
        cmd = ["gsutil", "ls", "-r", f"{path}/**.parquet"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            return 0, 0 # Probablemente path no existe o vacío
            
        files = [line for line in result.stdout.splitlines() if line.strip()]
        return len(files), files
    except:
        return 0, []

def main():
    print("\n📊 AUDITORÍA DE ARCHIVOS EN CLOUD STORAGE (gsutil)\n")
    print(f"{'TABLA':<20} | {'CAPA':<10} | {'ARCHIVOS PARQUET':<15} | {'ESTADO':<15}")
    print("-" * 75)
    
    for table in TABLES:
        for layer in ["raw", "silver"]:
            path = f"{BUCKET}/{layer}/{table}"
            count, files = count_files_gcs(path)
            
            status = "✅ OK" if count > 0 else "❌ VACÍO"
            if layer == "silver" and table in ["hogar_de_paso", "proveedores"] and count > 0:
                 status = "✅ OK (Snapshot)"
            
            print(f"{table:<20} | {layer:<10} | {count:<15} | {status:<15}")
            
    print("-" * 75)
    print("\n Nota: Este reporte cuenta ARCHIVOS físicos generados, no filas internas.")

if __name__ == "__main__":
    main()
