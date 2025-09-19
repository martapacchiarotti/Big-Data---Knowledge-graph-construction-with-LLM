import os
import xmltodict
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import SKOS, RDFS, DC, DCTERMS
from pyspark.sql import SparkSession
import pandas as pd


class DataIngestionBatch:
    def __init__(self, data_dir, spark: SparkSession):
        """
        :param data_dir: path locale o S3 (s3a://...)
        :param spark: SparkSession attiva
        """
        self.data_dir = data_dir.rstrip("/")
        self.spark = spark

    # ---------- Loader RDF/XML con Pandas ----------
    def load_rdfxml(self, path):
        """Parsing RDF/XML → DataFrame Pandas con triple filtrate"""
        try:
            g = Graph()
            g.parse(path, format="xml")

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

        except Exception as e:
            print(f"Errore parsing RDF/XML: {e}")
            return pd.DataFrame()

    # ---------- Parsing e scrittura su S3 ----------
    def parse_and_write(self, output_prefix: str):
        """
        Carica i file dalla sorgente, li converte in Spark DF e li scrive su S3 (JSON).
        :param output_prefix: path S3 o locale dove salvare i JSON
        """
        # CSV (Spark)
        csv_path = os.path.join(self.data_dir, "*.csv")
        sdf_csv = self.spark.read.option("header", "true").csv(csv_path)
        if sdf_csv.count() > 0:
            sdf_csv.write.mode("append").json(os.path.join(output_prefix, "csv"))
            print(f"Salvati {sdf_csv.count()} record da {csv_path} → {output_prefix}/csv")

        # TSV (Spark)
        tsv_path = os.path.join(self.data_dir, "*.tsv")
        sdf_tsv = self.spark.read.option("header", "true").option("sep", "\t").csv(tsv_path)
        if sdf_tsv.count() > 0:
            sdf_tsv.write.mode("append").json(os.path.join(output_prefix, "tsv"))
            print(f"Salvati {sdf_tsv.count()} record da {tsv_path} → {output_prefix}/tsv")

        # JSON (Spark)
        json_path = os.path.join(self.data_dir, "*.json")
        sdf_json = self.spark.read.json(json_path)
        if sdf_json.count() > 0:
            sdf_json.write.mode("append").json(os.path.join(output_prefix, "json"))
            print(f"Salvati {sdf_json.count()} record da {json_path} → {output_prefix}/json")

        # XML → RDF/XML con Pandas + conversione a Spark
        xml_path = os.path.join(self.data_dir, "*.xml")
        # ATTENZIONE: qui non c'è lettura distribuita, ma va bene per RDF/XML complessi
        import glob
        for file in glob.glob(xml_path):
            if not os.path.isfile(file):
                continue
            df = self.load_rdfxml(file)
            if not df.empty:
                sdf = self.spark.createDataFrame(df.astype(str).to_dict(orient="records"))
                sdf.write.mode("append").json(os.path.join(output_prefix, "xml"))
                print(f"Salvati {len(df)} record da {file} → {output_prefix}/xml")
