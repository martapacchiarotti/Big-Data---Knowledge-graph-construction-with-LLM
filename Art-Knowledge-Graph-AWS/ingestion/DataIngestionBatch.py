import os
import glob
import pandas as pd
import xmltodict
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import SKOS, RDFS, DC, DCTERMS
from pyspark.sql import SparkSession


class DataIngestionBatch:
    def __init__(self, data_dir, spark: SparkSession):
        """
        :param data_dir: path locale o montato (può essere S3 se montato con s3fs/hadoop)
        :param spark: SparkSession attiva
        """
        self.data_dir = data_dir.rstrip("/")
        self.spark = spark

    # ---------- Loader di base ----------
    def load_csv(self, path, sep=","):
        return pd.read_csv(path, sep=sep, dtype=str)

    def load_json(self, path):
        return pd.read_json(path, lines=True, dtype=str)

    def load_xml(self, path, row_tag="record"):
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

                LABEL_PREDICATES = [SKOS.prefLabel, SKOS.altLabel, RDFS.label, DC.title, DCTERMS.title]
                KEEP_PREDICATES = {"prefLabel", "altLabel", "note", "isRelatedTo", "sameAs", "creator", "depicts"}

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
                    if isinstance(obj, Literal) and obj.datatype and "hexBinary" in obj.datatype:
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

                return pd.DataFrame(triples).drop_duplicates()

            # XML "normale"
            doc = xmltodict.parse(content)
            records = doc.get(row_tag)
            if not records:
                for v in doc.values():
                    if isinstance(v, dict) and row_tag in v:
                        records = v[row_tag]
                        break
            if not records:
                return pd.DataFrame()
            if isinstance(records, dict):
                records = [records]

            return pd.DataFrame(records)

        except Exception as e:
            print(f"Errore caricamento XML: {e}")
            return pd.DataFrame()

    # ---------- Parsing e scrittura diretta su S3 ----------
    def parse_and_write(self, output_prefix: str):
        """
        Carica i file dalla sorgente, li converte in Spark DF e li scrive su S3 (JSON).
        :param output_prefix: path S3 o locale dove salvare i JSON
        """
        # CSV
        for file in glob.glob(os.path.join(self.data_dir, "*.csv")):
            df = self.load_csv(file)
            if not df.empty:
                sdf = self.spark.createDataFrame(df.astype(str).to_dict(orient="records"))
                sdf.write.mode("append").json(os.path.join(output_prefix, "csv"))
                print(f"Salvati {len(df)} record da {file} → {output_prefix}/csv")

        # TSV
        for file in glob.glob(os.path.join(self.data_dir, "*.tsv")):
            df = self.load_csv(file, sep="\t")
            if not df.empty:
                sdf = self.spark.createDataFrame(df.astype(str).to_dict(orient="records"))
                sdf.write.mode("append").json(os.path.join(output_prefix, "tsv"))
                print(f"Salvati {len(df)} record da {file} → {output_prefix}/tsv")

        # JSON
        for file in glob.glob(os.path.join(self.data_dir, "*.json")):
            df = self.load_json(file)
            if not df.empty:
                sdf = self.spark.createDataFrame(df.astype(str).to_dict(orient="records"))
                sdf.write.mode("append").json(os.path.join(output_prefix, "json"))
                print(f"Salvati {len(df)} record da {file} → {output_prefix}/json")

        # XML
        for file in glob.glob(os.path.join(self.data_dir, "*.xml")):
            df = self.load_xml(file)
            if not df.empty:
                sdf = self.spark.createDataFrame(df.astype(str).to_dict(orient="records"))
                sdf.write.mode("append").json(os.path.join(output_prefix, "xml"))
                print(f"Salvati {len(df)} record da {file} → {output_prefix}/xml")

