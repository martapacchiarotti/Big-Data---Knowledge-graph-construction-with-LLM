import json
from neo4j import GraphDatabase
from config import AURADB_URI, AURADB_USER, AURADB_PASSWORD


class GraphBuilder:
    def __init__(self):
        try:
            self.driver = GraphDatabase.driver(
                AURADB_URI, auth=(AURADB_USER, AURADB_PASSWORD)
            )
            # Test connessione
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

    # --- funzione di normalizzazione etichette ---
    def normalize_label(self, label: str) -> str:
        if not label:
            return "Entity"
        return "".join(word.capitalize() for word in label.replace("_", " ").split())
    
    def normalize_relation(self, relation: str) -> str:
        if not relation:
            return "RELATED_TO"
        return relation.strip().replace("-", "_").replace(" ", "_").upper()


    # ---  inserimento triplette ---
    def insert_triplets(self, triplets, batch_size=100):
        if not triplets:
            print(" Nessuna tripletta da inserire.")
            return
        if not self.driver:
            print(" Driver Neo4j non inizializzato.")
            return

        print(" Inizio la costruzione del grafo...")

        with self.driver.session() as session:
            for i in range(0, len(triplets), batch_size):
                batch = triplets[i:i + batch_size]

                for triple in batch:
                    subject = triple.get("subject")
                    subject_type = self.normalize_label(triple.get("subject_type"))
                    relation = self.normalize_relation(triple.get("relation")) 
                    obj = triple.get("object")
                    object_type = self.normalize_label(triple.get("object_type"))

                    if not subject and not obj:
                        print(f" Tripla senza nodi saltata → {triple}")
                        continue

                    if subject and not obj:
                        session.run(
                            f"MERGE (h:{subject_type} {{name: $head}})",
                            {"head": subject}
                        )
                        continue

                    if obj and not subject:
                        session.run(
                            f"MERGE (t:{object_type} {{name: $tail}})",
                            {"tail": obj}
                        )
                        continue

                    # Relazione normalizzata
                    relation_clean = self.normalize_relation(relation)

                    session.run(
                        f"""
                        MERGE (h:{subject_type} {{name: $head}})
                        MERGE (t:{object_type} {{name: $tail}})
                        MERGE (h)-[r:{relation_clean}]->(t)
                        """,
                        {"head": subject, "tail": obj},
                    )

                print(f" Inserite {i + len(batch)} / {len(triplets)} triplette")

        print(" Triplette importate in Neo4j!")