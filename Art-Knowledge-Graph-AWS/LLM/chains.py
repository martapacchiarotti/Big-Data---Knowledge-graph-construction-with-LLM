import os
import json
import boto3
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate

# -------------------------------
# Funzione per leggere secrets
# -------------------------------
def get_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    return json.loads(secret)

# -------------------------------
# Recupera API key dai secrets
# -------------------------------
secrets = get_secrets("KGPipelineSecrets")
GROQ_API_KEY = secrets["GROQ_API_KEY"]

# -------------------------------
# Recupera model name dal bootstrap
# -------------------------------
MODEL_NAME = os.getenv("LLM_MODEL_NAME", "llama3-70b")  # default di sicurezza

# -------------------------------
# Istanzia LLM
# -------------------------------
llm = ChatGroq(
    api_key=GROQ_API_KEY,
    model=MODEL_NAME,
    temperature=0
)

# Dizionario relazioni canoniche
CANONICAL_RELATIONS = {
    # Creazione artistica
    "painted": ["painted by", "created by", "made by", "realized by","painted_by", "created_by", "made_by", "realized_by"],
    "sculpted": ["sculpted by", "carved by", "modeled by", "fashioned by"],
    "drawn": ["drawn by", "sketched by", "illustrated by", "drafted by"],
    "engraved": ["engraved by", "etched by", "incised by"],
    "photographed": ["photographed by", "captured by", "shot by"],
    "composed_music": ["composed by", "music by", "score by", "written music by"],
    "written_text": ["written by", "authored by", "penned by", "scripted by"],
    "crafted": ["crafted by", "handmade by", "fashioned by"],

    # Pubblicazione e diffusione
    "published": ["published by", "issued by", "released by"],
    "exhibited_at": ["exhibited at", "displayed at", "shown at", "presented at"],
    "cataloged_by": ["cataloged by", "listed by", "documented by"],
    "curated_by": ["curated by", "organized by", "managed by"],

    # Proprietà e collezioni
    "owned_by": ["owned by", "property of", "collection of"],
    "in_collection_of": ["in collection of", "part of collection", "held by"],
    "donated_by": ["donated by", "gift of", "bequeathed by"],
    "acquired_by": ["acquired by", "purchased by", "obtained by"],
    "loaned_by": ["loaned by", "on loan from"],

    # Conservazione e restauro
    "restored_by": ["restored by", "conserved by", "repaired by"],
    "conserved_by": ["conserved by", "maintained by"],

    # Commissione e patrocinio
    "commissioned_by": ["commissioned by", "ordered by", "requested by"],
    "patron_of": ["patron of", "sponsor of", "benefactor of"],

    # Relazioni tra artisti
    "collaborated_with": ["collaborated with", "worked with", "in partnership with"],
    "student_of": ["student of", "pupil of", "disciple of"],
    "teacher_of": ["teacher of", "mentor of", "master of"],
    "influenced_by": ["influenced by", "inspired by", "affected by"],
    "influenced": ["influenced", "shaped", "affected"],

    # Contenuto e rappresentazione
    "depicts": ["depicts", "represents", "portrays", "illustrates"],
    "illustrates": ["illustrates", "visualizes", "depicts concept"],
    "dedicated_to": ["dedicated to", "in honor of", "commemorating"],
    "replica_of": ["replica of", "copy of", "reproduction of"],
    "inspired": ["inspired", "motivated", "sparked by"],

    # Informazioni generali sull'opera
    "style": ["in style of", "artistic style", "movement"],
    "medium": ["medium", "material", "technique"],
    "dimensions": ["dimensions", "size", "measurements"],
    "signed_by": ["signed by", "autographed by"],
    "dated": ["dated", "inscribed with date", "year of creation"],
    "awarded": ["awarded", "prize received", "honored with"],

    # Posizione e contesto museale
    "located_in": ["located in", "housed in", "kept at", "stored in"],
    "displayed_in_room": ["displayed in room", "exhibited in gallery", "shown in hall"],
    "part_of_exhibition": ["part of exhibition", "included in show"],

    # Movimenti e periodi
    "belongs_to_period": ["belongs to period", "from era", "dating to", "associated with period"],
    "belongs_to_movement": ["belongs to movement", "art movement", "associated with style"],

    # Altro
    "restored_in_year": ["restored in year", "conserved in year"],
    "donated_in_year": ["donated in year", "gifted in year"],
    "acquired_in_year": ["acquired in year", "purchased in year"]
}

# Prompt: estrazione triplette
extract_prompt = ChatPromptTemplate.from_template("""
Sei un linguista professionista che estrae triplette da sorgenti di dati in formato JSON.
Le sorgenti dati sono in ambito artistico-museale.
Devi descrivere ogni tripletta nel seguente modo:
- subject, subject_type
- relation,
- object, object_type

Restituisci solo le triplette in formato JSON puro, senza spiegazioni.

Esempio di output desiderato:
[
  {{"subject": "Pablo Picasso", "subject_type": "artist", "relation": "painted","object": "Guernica", "object_type": "painting"}}
]
Testo di input:
{input_text}
""")
extract_chain = extract_prompt | llm

# Prompt: normalizzazione entità
normalize_prompt = ChatPromptTemplate.from_template("""
Sei un assistente che normalizza le entità nelle triplette in formato json.
Restituisci solo le triplette in formato JSON puro, senza spiegazioni.                                                    
Regole:
1. Artisti: usa il nome canonico se noto, altrimenti lascia il nome presente + "CANONICAL UNKNOWN".
2. Musei: Usa sempre i nomi completi per i musei; se c'è un acronimo mantienilo tra parentesi.
3. Date:
   - Se è una data completa, usa il formato dd/mm/yyyy.
   - Se è solo un anno o contiene un anno (es. "1975", "1975-01-01", "anno 1975"), usa il formato "yyyy" e assegna sempre object_type = "Year".
4. Non modificare i tipi assegnati.
5. Non inventare nulla. 

Triplette da normalizzare:
{triplets}
""")
normalize_chain = normalize_prompt | llm

# Prompt: standardizzazione relazioni
relations_context = "\n".join(
    f"- {canonical}: {', '.join(variants)}"
    for canonical, variants in CANONICAL_RELATIONS.items()
)
standardize_rel_prompt = ChatPromptTemplate.from_template(f"""
Sei un assistente che uniforma le relazioni nelle triplette in formato json.
Restituisci solo le triplette in formato JSON puro, senza spiegazioni.
Regole:
1. Le relazioni devono essere rese minuscole e senza underscore (usa spazi).
   - Esempio: "PAINTED_BY" → "painted by"
   - Esempio: "BORN_IN" → "born in"
2. Usa questo dizionario di relazioni canoniche:

{relations_context}

3. Se una relazione non c'è nel dizionario, mantienila invariata ma sempre in minuscolo senza underscore.
4. Restituisci le triplette nello stesso formato JSON.

Triplette da standardizzare:
{{triplets}}
""")
standardize_rel_chain = standardize_rel_prompt | llm

