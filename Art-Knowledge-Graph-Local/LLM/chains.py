import os
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate
from config import GROQ_API_KEY, MODEL_NAME


llm = ChatGroq(
    api_key=GROQ_API_KEY,
    model=MODEL_NAME,
    temperature=0
)


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

