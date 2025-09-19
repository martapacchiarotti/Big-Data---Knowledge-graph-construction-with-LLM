import os
from pyspark.sql import SparkSession
from DataIngestionBatch import DataIngestionBatch
from monitor import monitor_job

@monitor_job("ingestion_parsing")
def main():
    # ---------- Config da variabili d'ambiente ----------
    DATA = os.getenv("DATA_DIR", "data")  # cartella data-sources su bucket S3
    BUCKET = os.getenv("S3_BUCKET")
    PREFIX = os.getenv("S3_LLM_TRIPLETS_PREFIX", "output")

    if not BUCKET:
        raise ValueError("La variabile S3_BUCKET non è impostata nel cluster EMR.")

    # Path S3 per Spark
    DATA_DIR = f"s3a://{BUCKET}/{DATA}"
    OUTPUT_PATH = f"s3a://{BUCKET}/{PREFIX}"

    print(f"[INFO] Parsing file da {DATA_DIR} → salvataggio in {OUTPUT_PATH}")

    # ---------- Spark Session ----------
    spark = (
        SparkSession.builder.appName("KG-Pipeline-Parsing")
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

    # ---------- Parsing e scrittura diretta su S3 ----------
    ingestion = DataIngestionBatch(DATA_DIR, spark)

    # CSV, TSV, JSON → Spark nativo
    # XML/RDF → Pandas + rdflib e successiva conversione a Spark DF
    ingestion.parse_and_write(OUTPUT_PATH)

    print(f"[INFO] Parsing completato. Dati salvati su {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()

