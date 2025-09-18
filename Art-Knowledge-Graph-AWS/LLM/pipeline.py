import asyncio
import json
import re
from LLM.chains import extract_chain, normalize_chain, standardize_rel_chain

def extract_json(text):
    """
    Estrae un array JSON o un singolo oggetto JSON dal testo.
    Tollerante a eventuali note o testo extra nell'output.
    """
    text = text.strip()
    if not text:
        return None

    # Prova a caricare come JSON completo
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass

    # Se fallisce, cerca oggetti JSON singoli con regex
    matches = re.findall(r"\{.*?\}", text, re.DOTALL)
    parsed = []
    for m in matches:
        try:
            parsed.append(json.loads(m))
        except json.JSONDecodeError:
            continue
    return parsed if parsed else None

async def extract_triplets_batch(texts, batch_size=5):
    """Estrae triplette grezze da un batch di testi."""
    triplets = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        tasks = [extract_chain.ainvoke({"input_text": text}) for text in batch]
        results = await asyncio.gather(*tasks)

        for r in results:
            output = r if isinstance(r, str) else getattr(r, "content", None)
            if not output:
                print("Nessun output dall'LLM:", r)
                continue

            parsed = extract_json(output)
            if not parsed:
                print("Invalid JSON in extract:", output)
                continue

            if isinstance(parsed, dict):
                triplets.append(parsed)
            elif isinstance(parsed, list):
                triplets.extend(parsed)
            else:
                print("Formato inatteso:", type(parsed), parsed)

    return triplets


async def normalize_triplets_batch(triplets, batch_size=5):
    """Normalizza entità nelle triplette."""
    normalized = []
    for i in range(0, len(triplets), batch_size):
        batch = triplets[i:i + batch_size]
        tasks = [normalize_chain.ainvoke({"triplets": json.dumps(batch, ensure_ascii=False)})]
        results = await asyncio.gather(*tasks)

        for r in results:
            output = r if isinstance(r, str) else getattr(r, "content", None)
            if not output:
                continue
            parsed = extract_json(output)
            if not parsed:
                print("Invalid JSON in normalize:", output)
                continue
            normalized.extend(parsed)

    return normalized


async def standardize_relations_batch(triplets, batch_size=5):
    """Standardizza relazioni nelle triplette."""
    standardized = []
    for i in range(0, len(triplets), batch_size):
        batch = triplets[i:i + batch_size]
        tasks = [standardize_rel_chain.ainvoke({"triplets": json.dumps(batch, ensure_ascii=False)})]
        results = await asyncio.gather(*tasks)

        for r in results:
            output = r if isinstance(r, str) else getattr(r, "content", None)
            if not output:
                continue
            parsed = extract_json(output)
            if not parsed:
                print("Invalid JSON in standardize:", output)
                continue
            standardized.extend(parsed)

    return standardized

