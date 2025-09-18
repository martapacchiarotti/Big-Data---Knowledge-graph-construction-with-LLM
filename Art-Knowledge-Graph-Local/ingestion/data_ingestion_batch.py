import pandas as pd
import glob
import os
import json
import xmltodict
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import SKOS, RDFS, DC, DCTERMS

class DataIngestionBatch:
    def __init__(self, data_dir):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(base_dir, "..", data_dir)

    # ---------- Loader di base ----------
    def load_csv(self, path, sep=","):
        return pd.read_csv(path, sep=sep, dtype=str)

    def load_json(self, path):
        return pd.read_json(path, lines=False, dtype=str)

    def load_xml(self, path, row_tag="record"):
        """Carica file XML o RDF/XML con filtraggio predicati e gestione hexBinary."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            # RDF/XML
            if "<rdf:RDF" in content:
                g = Graph()
                try:
                    g.parse(path, format="xml")
                except Exception as e:
                    print(f"Errore parsing RDF/XML: {e}")
                    return pd.DataFrame()

                LABEL_PREDICATES = [
                    SKOS.prefLabel,
                    SKOS.altLabel,
                    RDFS.label,
                    DC.title,
                    DCTERMS.title,
                ]
                KEEP_PREDICATES = {
                    "prefLabel",
                    "altLabel",
                    "note",
                    "isRelatedTo",
                    "sameAs",
                    "creator",
                    "depicts",
                }

                def get_label(resource):
                    if isinstance(resource, Literal):
                        return str(resource)
                    for pred_uri in LABEL_PREDICATES:
                        label = g.value(resource, pred_uri)
                        if label:
                            return str(label)
                    if isinstance(resource, URIRef):
                        return resource.split("/")[-1].split("#")[-1]
                    return str(resource)

                triples = []
                for subj, pred, obj in g:
                    # Ignora literal hexBinary
                    if isinstance(obj, Literal) and obj.datatype and 'hexBinary' in obj.datatype:
                        continue

                    subject_label = get_label(subj)
                    predicate_label = pred.split("#")[-1] if "#" in pred else pred.split("/")[-1]
                    object_label = get_label(obj) if not isinstance(obj, Literal) else str(obj)
                    lang = obj.language if isinstance(obj, Literal) else None

                    if predicate_label not in KEEP_PREDICATES:
                        continue

                    if subject_label.strip() and object_label.strip():
                        triples.append({
                            "subject": subject_label,
                            "xml_label": predicate_label,
                            "object": object_label,
                            "lang": lang,
                        })

                df_triples = pd.DataFrame(triples).drop_duplicates()
                print(f"Triplette RDF/XML caricate da {path}: {len(df_triples)}")
                return df_triples

            # XML normale
            doc = xmltodict.parse(content)
            records = doc.get(row_tag)
            if not records:
                for v in doc.values():
                    if isinstance(v, dict) and row_tag in v:
                        records = v[row_tag]
                        break
            if not records:
                print(f"Nessun nodo '{row_tag}' trovato in {path}")
                return pd.DataFrame()
            if isinstance(records, dict):
                records = [records]

            df = pd.DataFrame(records)
            print(f"Record XML caricate da {path}: {len(df)}")
            return df

        except Exception as e:
            print(f"Errore caricamento XML: {e}")
            return pd.DataFrame()

    # ---------- Caricamento di tutti i file ----------
    def load_all(self):
        df_list = []

        # CSV
        for file in glob.glob(os.path.join(self.data_dir, "*.csv")):
            try:
                df = self.load_csv(file, sep=",")
                if not df.empty:
                    df_list.append(df)
                    print(f"Caricati {len(df)} record da {file}")
            except Exception as e:
                print("Errore caricamento CSV:", e)

        # TSV
        for file in glob.glob(os.path.join(self.data_dir, "*.tsv")):
            try:
                df = self.load_csv(file, sep="\t")
                if not df.empty:
                    df_list.append(df)
                    print(f"Caricati {len(df)} record da {file}")
            except Exception as e:
                print("Errore caricamento TSV:", e)

        # JSON
        for file in glob.glob(os.path.join(self.data_dir, "*.json")):
            try:
                df = self.load_json(file)
                if not df.empty:
                    df_list.append(df)
                    print(f"Caricati {len(df)} record da {file}")
            except Exception as e:
                print("Errore caricamento JSON:", e)

        # XML (incluso RDF/XML)
        for file in glob.glob(os.path.join(self.data_dir, "*.xml")):
            try:
                df = self.load_xml(file, row_tag="record")
                if not df.empty:
                    df_list.append(df)
                    print(f"Caricati {len(df)} record da {file}")
            except Exception as e:
                print("Errore caricamento XML:", e)

        if not df_list:
            print("Nessun file trovato")
            return None

        df_final = pd.concat(df_list, ignore_index=True).fillna("")
        return df_final

    # ---------- Batching ----------
    def get_batches(self, batch_size=100):
        df = self.load_all()
        if df is None:
            return []

        data = df.to_dict(orient="records")
        batches = [
            json.dumps(data[i:i + batch_size], indent=2, ensure_ascii=False)
            for i in range(0, len(data), batch_size)
        ]
        return batches
