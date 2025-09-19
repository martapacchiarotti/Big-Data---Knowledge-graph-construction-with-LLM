from pyspark.sql import SparkSession
from monitor import monitor_job
import json
import asyncio
import os
import boto3

from LLM.pipeline import (
    extract_triplets_batch,
    normalize_triplets_batch,
    standardize_relations_batch,
)

# ---------- Funzione per processare una partizione ----------
def process_partition(records):
    """Applica la pipeline LLM su ogni partizione di Spark."""
    texts = [json.dumps(r, ensure_ascii=False) for r in records]
    if not texts:
        return []

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def process():
        raw = await extract_triplets_batch(texts, batch_size=5)
        norm = await normalize_triplets_batch(raw, batch_size=5)
        std = await standardize_relations_batch(norm, batch_size=5)
        return std

    result = loop.run_until_complete(process())
    return result

# ---------- Main ----------
@monitor_job("extraction_llm")
def main():
    # ---------- Spark Session ----------
spark = (
    SparkSession.builder.appName("KG-Pipeline-LLM")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .master(os.getenv("SPARK_MASTER", "local[*]"))
    .getOrCreate()
)

    # ---------- Config da variabili d'ambiente ----------
    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_JSON_DATA_PREFIX = os.getenv("S3_JSON_DATA_PREFIX")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH")
    
    if not (S3_BUCKET and S3_JSON_DATA_PREFIX and OUTPUT_PATH):
        raise ValueError("Le variabili S3_BUCKET, S3_JSON_DATA_PREFIX e OUTPUT_PATH devono essere impostate!")

    INPUT_PATH = f"s3a://{S3_BUCKET}/{S3_JSON_DATA_PREFIX}/*/*.json"
    print(f"Lettura dati da {INPUT_PATH}")

    # ---------- Lettura dati ----------
    df = spark.read.json(INPUT_PATH)

    # ---------- Pipeline distribuita ----------
    rdd = df.rdd.mapPartitions(process_partition)

    # ---------- Scrittura finale su S3 ----------
    rdd = rdd.map(lambda x: json.dumps(x, ensure_ascii=False))
    rdd.saveAsTextFile(OUTPUT_PATH)
    print(f"Triplette salvate in {OUTPUT_PATH}")

    spark.stop()

if __name__ == "__main__":
    main()

