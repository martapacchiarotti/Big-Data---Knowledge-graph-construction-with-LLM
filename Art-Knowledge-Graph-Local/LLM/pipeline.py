import asyncio
import json
import re
import random
from groq import RateLimitError
from LLM.chains import extract_chain, normalize_chain

# -------------------
# Funzioni di supporto
# -------------------

def extract_json(text):
    """Estrae array o singolo oggetto JSON dal testo, tollerante a note extra."""
    text = text.strip()
    if not text:
        return None

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass

    matches = re.findall(r"\{.*?\}", text, re.DOTALL)
    parsed = []
    for m in matches:
        try:
            parsed.append(json.loads(m))
        except json.JSONDecodeError:
            continue
    return parsed if parsed else None

async def call_with_retry(func, *args, max_retries=5, initial_delay=1, **kwargs):
    """
    Chiama una funzione asincrona gestendo il RateLimitError con retry e backoff.
    Se l'errore contiene un tempo di attesa (es. "Please try again in 1m22.71s"),
    il codice aspetta esattamente quel tempo.
    """
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)

        except RateLimitError as e:
            msg = str(e)
            # Prova ad estrarre un tempo dal messaggio (es. "1m22.7144s")
            match = re.search(r"(\d+)m([\d\.]+)s", msg)
            if match:
                minutes = int(match.group(1))
                seconds = float(match.group(2))
                wait_time = minutes * 60 + seconds
                print(f" Rate limit raggiunto, attendo {wait_time:.2f}s "
                      f"(tentativo {attempt+1}/{max_retries})")
                await asyncio.sleep(wait_time)
            else:
                print(f" Rate limit raggiunto, retry in {delay:.2f}s "
                      f"(tentativo {attempt+1}/{max_retries})")
                await asyncio.sleep(delay)
                delay *= random.uniform(5.5, 6.0)

        except Exception as e:
            print(f" Errore generico nella chiamata LLM: {e}")
            raise

    raise RuntimeError("Rate limit persistente, impossibile completare la chiamata")
# -------------------
# Pipeline LLM senza concorrenza
# -------------------

async def extract_triplets_batch(texts, batch_size=5):
    triplets = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        for text in batch:
            result = await call_with_retry(extract_chain.ainvoke, {"input_text": text})
            output = result if isinstance(result, str) else getattr(result, "content", None)
            if not output:
                print("Nessun output dall'LLM:", result)
                continue

            parsed = extract_json(output)
            if not parsed:
                print("Invalid JSON in extract:", output)
                continue

            if isinstance(parsed, dict):
                triplets.append(parsed)
            elif isinstance(parsed, list):
                triplets.extend(parsed)

    return triplets

async def normalize_triplets_batch(triplets, batch_size=5):
    normalized = []
    for i in range(0, len(triplets), batch_size):
        batch = triplets[i:i + batch_size]
        batch_json = json.dumps(batch, ensure_ascii=False, separators=(',', ':'))
        result = await call_with_retry(normalize_chain.ainvoke, {"triplets": batch_json})
        output = result if isinstance(result, str) else getattr(result, "content", None)
        if not output:
            continue
        parsed = extract_json(output)
        if not parsed:
            print("Invalid JSON in normalize:", output)
            continue
        normalized.extend(parsed)

    return normalized

