"""
Script para generar datos mock de donaciones CRM.
Simula datos extraídos de Supabase para desarrollo local.

Uso:
    python scripts/generate_mock_data.py
"""
import sys
from pathlib import Path

# Agregar proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import config


def generate_mock_donations(num_records: int = 1000) -> pd.DataFrame:
    """
    Genera datos mock de donaciones para testing.
    
    Args:
        num_records: Cantidad de registros a generar
        
    Returns:
        DataFrame de pandas con donaciones mock
    """
    print(f"🎲 Generando {num_records} registros mock de donaciones...")
    
    # Seed para reproducibilidad
    np.random.seed(42)
    
    # Generar fechas aleatorias en los últimos 2 años
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    date_range = (end_date - start_date).days
    
    dates = [
        start_date + timedelta(days=np.random.randint(0, date_range))
        for _ in range(num_records)
    ]
    
    # Generar IDs de donantes (simulando 100 donantes únicos)
    donante_ids = np.random.randint(1, 101, size=num_records)
    
    # Generar montos (distribución realista)
    # 70% donaciones pequeñas (10-100), 25% medianas (100-1000), 5% grandes (1000-10000)
    montos = []
    for _ in range(num_records):
        rand = np.random.random()
        if rand < 0.70:
            monto = np.random.uniform(10, 100)
        elif rand < 0.95:
            monto = np.random.uniform(100, 1000)
        else:
            monto = np.random.uniform(1000, 10000)
        montos.append(round(monto, 2))
    
    # Generar tipos de donación
    tipos = np.random.choice(
        ["Efectivo", "Transferencia", "Tarjeta", "Especie"],
        size=num_records,
        p=[0.3, 0.4, 0.25, 0.05]
    )
    
    # Crear DataFrame
    df = pd.DataFrame({
        "id": range(1, num_records + 1),
        "fecha_donacion": dates,
        "monto": montos,
        "id_donante": donante_ids,
        "tipo_donacion": tipos,
        "descripcion": [f"Donación #{i}" for i in range(1, num_records + 1)],
        "created_at": datetime.now(),
    })
    
    # Ordenar por fecha
    df = df.sort_values("fecha_donacion").reset_index(drop=True)
    
    print(f"✅ Generados {len(df)} registros")
    print(f"   Rango de fechas: {df['fecha_donacion'].min()} a {df['fecha_donacion'].max()}")
    print(f"   Total donado: ${df['monto'].sum():,.2f}")
    print(f"   Promedio: ${df['monto'].mean():,.2f}")
    
    return df


def save_to_parquet(df: pd.DataFrame, filename: str = "donaciones_mock.parquet") -> None:
    """
    Guarda el DataFrame en formato Parquet.
    
    Args:
        df: DataFrame a guardar
        filename: Nombre del archivo de salida
    """
    output_path = config.DATA_RAW_DIR / filename
    
    print(f"\n💾 Guardando en: {output_path}")
    
    # Asegurar que el directorio existe
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Guardar en Parquet
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    
    print(f"✅ Archivo guardado exitosamente")
    print(f"   Tamaño: {output_path.stat().st_size / 1024:.2f} KB")


def main():
    """Función principal."""
    print("=" * 60)
    print("🎲 Generador de Datos Mock - CRM Donaciones")
    print("=" * 60)
    
    try:
        # Generar datos
        df = generate_mock_donations(num_records=1000)
        
        # Guardar en Parquet
        save_to_parquet(df)
        
        print("\n" + "=" * 60)
        print("✅ Datos mock generados exitosamente")
        print("=" * 60)
        print("\n💡 Siguiente paso:")
        print("   spark-submit --master local[*] jobs/transform_donations.py")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
