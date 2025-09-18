import os
import json
import boto3
from Graph.graph_builder import GraphBuilder
from monitor import monitor_job

# -------------------------------
# Funzione per leggere secrets da AWS Secrets Manager
# -------------------------------
def get_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    return json.loads(secret)

# -------------------------------
# Main
# -------------------------------
@monitor_job("neo4j_load")
def main():
    # ---------- Recupera bucket da variabile d'ambiente ----------
    # ---------- Config da variabili di ambiente ----------
    BUCKET = os.getenv("S3_BUCKET")
    PREFIX = os.getenv("S3_LLM_TRIPLETS_PREFIX", "output")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))

    if not BUCKET:
        raise ValueError("La variabile S3_BUCKET non è impostata nel cluster EMR.")

    # ---------- Recupera secrets ----------
    secrets = get_secrets("KGPipelineSecrets")  # Nome del secret in AWS Secrets Manager
    NEO4J_URI = secrets["NEO4J_URI"]
    NEO4J_USER = secrets["NEO4J_USER"]
    NEO4J_PASSWORD = secrets["NEO4J_PASSWORD"]


    # ---------- Connessione Neo4j ----------
    graph = GraphBuilder(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    # ---------- Connessione S3 ----------
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    if "Contents" not in response:
        print(f"Nessun file trovato in s3://{BUCKET}/{PREFIX}")
        return

    # ---------- Itera sui file ----------
    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".json") and not key.endswith(".jsonl") and "part-" not in key:
            continue

        print(f"Lettura file {key} da S3...")
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]

        buffer = []
        total_inserted = 0

        # Lettura riga per riga (streaming)
        for line_bytes in body.iter_lines():
            line = line_bytes.decode("utf-8").strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                buffer.append(record)
            except Exception as e:
                print(f"Errore parsing linea in {key}: {e}")
                continue

            # Inserisci batch in Neo4j
            if len(buffer) >= BATCH_SIZE:
                graph.insert_triplets(buffer)
                total_inserted += len(buffer)
                buffer.clear()

        # Inserisci eventuali rimanenti
        if buffer:
            graph.insert_triplets(buffer)
            total_inserted += len(buffer)

        print(f"Inserite {total_inserted} triplette da {key}")

    # Chiudi connessione Neo4j
    graph.close()
    print("Caricamento completato in Neo4j.")


if __name__ == "__main__":
    main()

