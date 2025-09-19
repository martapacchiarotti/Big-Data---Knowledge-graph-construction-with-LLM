import json
import boto3
from neo4j import GraphDatabase

class GraphBuilder:
    def __init__(self, uri, user, password):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            with self.driver.session() as session:
                result = session.run("RETURN 1").single()
                if result and result[0] == 1:
                    print("Connessione a Neo4j stabilita con successo.")
                else:
                    print("Connessione a Neo4j attiva ma non verificata.")
        except Exception as e:
            print(f"Errore di connessione a Neo4j: {e}")
            self.driver = None

    def close(self):
        if self.driver:
            self.driver.close()
            print("Connessione a Neo4j chiusa.")

    # -------------------------------
    # Inserimento da lista di triplette con batching
    # -------------------------------
    def insert_triplets(self, triplets, batch_size=500):
        if not triplets:
            print("Nessuna tripletta da inserire.")
            return
        if not self.driver:
            print("Driver Neo4j non inizializzato.")
            return

        query = """
        UNWIND $batch AS t
        MERGE (s {name: t.subject})
        MERGE (o {name: t.object})
        FOREACH (_ IN CASE WHEN t.relation IS NOT NULL AND t.object IS NOT NULL THEN [1] ELSE [] END |
            MERGE (s)-[r:RELATED]->(o)
            SET r.type = t.relation
        )
        CALL apoc.create.addLabels(s, [t.subject_type]) YIELD node
        CALL apoc.create.addLabels(o, [t.object_type]) YIELD node
        RETURN count(*)
        """

        with self.driver.session() as session:
            for i in range(0, len(triplets), batch_size):
                batch = triplets[i:i + batch_size]
                session.run(query, {"batch": batch})
                print(f"Inserite {i + len(batch)} / {len(triplets)} triplette")

        print(f"Tutte le {len(triplets)} triplette inserite!")

    # -------------------------------
    # Inserimento da file S3
    # -------------------------------
    def insert_triplets_from_s3(self, bucket, prefix, batch_size=500):
        if not self.driver:
            print("Driver Neo4j non inizializzato.")
            return

        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in response:
            print("Nessun file trovato su S3.")
            return

        for obj in response["Contents"]:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            try:
                triplets = json.loads(body.decode("utf-8"))
            except Exception as e:
                print(f"Errore parsing JSON in {key}: {e}")
                continue

            print(f"Carico {len(triplets)} triplette da {key}")
            self.insert_triplets(triplets, batch_size=batch_size)

        print("Triplette da S3 importate in Neo4j!")

