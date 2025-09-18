import asyncio
import json
import time
from ingestion.data_ingestion_batch import DataIngestionBatch
from LLM.pipeline import (
    extract_triplets_batch,
    normalize_triplets_batch
)
from config import BATCH_SIZE, DATA_PATH
from Graph.graph_builder import GraphBuilder

LOG_FILE = "pipeline_times.log"

def log_time(message):
    """Scrive i messaggi di tempo su file in append."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

async def run_pipeline():
    # 1. Caricamento dati
    t_start = time.time()
    ingestion = DataIngestionBatch(data_dir=DATA_PATH)
    batches = ingestion.get_batches(batch_size=BATCH_SIZE)
    t_load = time.time() - t_start
    log_time(f"Caricamento dati: {t_load:.2f}s")

    if not batches:
        log_time("Nessun dato trovato.")
        return

    all_standardized = []
    log_time(f"Primo batch: {batches[0]}")

    for batch_idx, batch in enumerate(batches, start=1):
        log_time(f"=== Elaborazione batch {batch_idx}/{len(batches)} ===")

        # Decodifica batch e JSON compatto
        sample_data = json.loads(batch)
        texts = [json.dumps(record, ensure_ascii=False, separators=(',', ':')) for record in sample_data]

        # 2. Estrazione triplette
        t_extract_start = time.time()
        raw_triplets = await extract_triplets_batch(texts, batch_size=20)
        t_extract = time.time() - t_extract_start
        log_time(f"Triplette grezze: {len(raw_triplets)} (tempo: {t_extract:.2f}s)")

        # Debug (prime 20 triplette grezze)
        log_time(f"Prime 20 triplette grezze: {json.dumps(raw_triplets[:20], ensure_ascii=False)}")

        # 3. Normalizzazione entità
        t_norm_start = time.time()
        normalized_triplets = await normalize_triplets_batch(raw_triplets, batch_size=20)
        t_norm = time.time() - t_norm_start
        log_time(f"Triplette normalizzate: {len(normalized_triplets)} (tempo: {t_norm:.2f}s)")

        # 4. Accumulo risultati
        all_standardized.extend(normalized_triplets)

    # 5. Conversione in formato Neo4j (tuple)
    neo4j_triplets = [
        {
            "subject": t["subject"],
            "subject_type": t.get("subject_type", "Entity"),
            "relation": t["relation"],
            "object": t["object"],
            "object_type": t.get("object_type", "Entity")
        }
        for t in all_standardized
        if "subject" in t and "relation" in t and "object" in t
    ]

    log_time(f"Triplette pronte per Neo4j: {len(all_standardized)}")
    log_time(f"Esempio prime 5: {json.dumps(all_standardized[:5], ensure_ascii=False)}")

    # 6. Esportazione in Neo4j
    t_neo4j_start = time.time()
    graph = GraphBuilder()
    graph.insert_triplets(neo4j_triplets, batch_size=50)
    graph.close()
    t_neo4j = time.time() - t_neo4j_start
    log_time(f"Triplette importate in Neo4j! (tempo: {t_neo4j:.2f}s)")

    # Tempo totale
    t_total = time.time() - t_start
    log_time(f"[TEMPO TOTALE PIPELINE] {t_total:.2f}s")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
