#!/bin/bash

# -------------------------------
# Variabili d'ambiente per la pipeline
# -------------------------------

# Bucket S3 dove sono contenuti script, sorgenti dati e output
export S3_BUCKET="big-data-gm"

# Cartella dove si trovano i file sorgente su S3
export DATA_DIR="art-data-sources"

# Prefisso all'interno del bucket dove Spark scrive le triplette
export S3_LLM_TRIPLETS_PREFIX="triplets"

# Batch size per l'inserimento in Neo4j
export BATCH_SIZE=500

export SPARK_MASTER="yarn"

# LLM configuration
export LLM_MODEL_NAME="llama-3.1-8b-instant"

# Misurazioni e statistiche dei JOB
export METRICS_PREFIX="job-metrics"


echo "Bootstrap EMR completato: variabili d'ambiente impostate"
echo "S3_BUCKET=$S3_BUCKET"
echo "S3_LLM_TRIPLETS_PREFIX=$S3_LLM_TRIPLETS_PREFIX"
echo "BATCH_SIZE=$BATCH_SIZE"
echo "LLM_MODEL_NAME=$LLM_MODEL_NAME"
echo "SPARK_MASTER=$SPARK_MASTER"
