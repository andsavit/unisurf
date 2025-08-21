from pymongo import MongoClient
import csv

#Script per il filtraggio delle institutions italiane presenti in OpenAlex ai fini di matching con quelle del MIUR

# Connessione a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["unisurf"]
collection = db["institutions"]

pipeline = [
    {"$match": {
        "type": {"$in": ["funder", "education"]},
        "works_count": {"$gt": 1000}
    }},
    {"$project": {
        "_id": 0,
        "id": 1,
        "ror": 1,
        "display_name": 1,
        "type": 1
    }}
]

results = collection.aggregate(pipeline)

with open("institutions_export.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["id", "ror", "display_name", "type"])
    writer.writeheader()
    for doc in results:
        writer.writerow(doc)

print("✅ Esportazione completata: institutions_export.csv")

