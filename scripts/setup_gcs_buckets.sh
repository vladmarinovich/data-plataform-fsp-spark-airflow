#!/bin/bash
# Script para crear los buckets de GCS necesarios para el proyecto
# Ejecutar: bash scripts/setup_gcs_buckets.sh

PROJECT_ID="salvando-patitas-de-spark"
REGION="us-central1"  # Cambia si prefieres otra región

echo "🚀 Creando buckets de GCS para proyecto: $PROJECT_ID"
echo "📍 Región: $REGION"
echo ""

# Lista de buckets a crear
BUCKETS=(
    "salvando-patitas-spark-raw"
    "salvando-patitas-spark-silver"
    "salvando-patitas-spark-gold"
)

for BUCKET in "${BUCKETS[@]}"; do
    echo "📦 Creando bucket: gs://$BUCKET"
    
    # Verificar si ya existe
    if gcloud storage buckets describe gs://$BUCKET --project=$PROJECT_ID &>/dev/null; then
        echo "   ✅ Bucket ya existe"
    else
        # Crear bucket
        gcloud storage buckets create gs://$BUCKET \
            --project=$PROJECT_ID \
            --location=$REGION \
            --uniform-bucket-level-access
        
        if [ $? -eq 0 ]; then
            echo "   ✅ Bucket creado exitosamente"
        else
            echo "   ❌ Error creando bucket"
        fi
    fi
    echo ""
done

echo "="
echo "✅ Setup de buckets completado"
echo ""
echo "Puedes verificar con:"
echo "  gcloud storage buckets list --project=$PROJECT_ID"
