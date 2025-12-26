#!/bin/bash
# Script para inicializar repositorio Git y hacer primer commit

echo "🔧 Inicializando repositorio Git..."

# Inicializar Git
git init

# Agregar todos los archivos
git add .

# Primer commit
git commit -m "Initial commit: PySpark + Airflow Data Platform

- Estructura de proyecto profesional
- Jobs PySpark con Medallion Architecture (Bronze/Silver/Gold)
- DAG de Airflow para orquestación
- Configuración cloud-agnostic
- Documentación completa
- Scripts de setup automatizado
- Validaciones de calidad de datos
- Generador de datos mock para testing

Stack: PySpark, Airflow, Python, Parquet
Arquitectura: Medallion (Bronze/Silver/Gold)
"

echo "✅ Repositorio Git inicializado"
echo ""
echo "📝 Próximos pasos para subir a GitHub:"
echo "1. Crea un repositorio en GitHub (vacío, sin README)"
echo "2. Ejecuta estos comandos:"
echo ""
echo "   git remote add origin https://github.com/tu-usuario/tu-repo.git"
echo "   git branch -M main"
echo "   git push -u origin main"
