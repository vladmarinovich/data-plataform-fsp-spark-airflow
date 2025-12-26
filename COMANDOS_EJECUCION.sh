#!/bin/bash
# ============================================
# COMANDOS EXACTOS PARA EJECUTAR EL PROYECTO
# ============================================
# Copia y pega estos comandos en tu terminal

echo "🚀 Iniciando setup del proyecto PySpark + Airflow..."

# ============================================
# PASO 1: Navegar al proyecto
# ============================================
cd "/Users/vladislavmarinovich/Library/CloudStorage/GoogleDrive-consultor@vladmarinovich.com/Shared drives/Vladislav/Salvando Patitas (SPDP) S-A/pyspark-airflow-data-platform"

# ============================================
# PASO 2: Verificar requisitos del sistema
# ============================================
echo ""
echo "📋 Verificando requisitos del sistema..."
echo "Python:"
python3 --version

echo ""
echo "Spark:"
spark-submit --version 2>&1 | head -n 1

echo ""
echo "Java:"
java -version 2>&1 | head -n 1

# ============================================
# PASO 3: Ejecutar setup automatizado
# ============================================
echo ""
echo "🔧 Ejecutando setup automatizado..."
./scripts/setup.sh

# ============================================
# PASO 4: Activar entorno virtual
# ============================================
echo ""
echo "🐍 Activando entorno virtual..."
source venv/bin/activate

# ============================================
# PASO 5: Verificar instalación
# ============================================
echo ""
echo "✅ Verificando instalación..."
which python
pip list | grep -E "pandas|pyarrow|supabase"

# ============================================
# PASO 6: Ejecutar job de prueba
# ============================================
echo ""
echo "🚀 Ejecutando job PySpark de prueba..."
spark-submit --master local[*] jobs/transform_donations.py

# ============================================
# PASO 7: Verificar resultados
# ============================================
echo ""
echo "📊 Verificando resultados..."
echo "Datos Silver:"
ls -lh data/processed/silver/donaciones/ 2>/dev/null || echo "Directorio aún no creado"

echo ""
echo "Datos Gold:"
ls -lh data/output/gold/donaciones_monthly/ 2>/dev/null || echo "Directorio aún no creado"

echo ""
echo "✅ Setup completado!"
echo ""
echo "📝 Próximos pasos:"
echo "1. Edita .env con tus credenciales de Supabase"
echo "2. Ejecuta: spark-submit --master local[*] jobs/transform_donations.py"
echo "3. Revisa la documentación en docs/"
