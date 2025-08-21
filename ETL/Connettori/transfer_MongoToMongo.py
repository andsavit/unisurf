from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
client2 = MongoClient("mongodb+srv://utente:utente@unisurf.bgfqeh0.mongodb.net/")

# Collezione sorgente
src_db = client["unisurf"]
src_coll = src_db["institutions"]

# Collezione di destinazione
dst_db = client2["unisurf"]
dst_coll = dst_db["institutions_it_sample"]

# Estrai 10 documenti casuali
sample_docs = list(src_coll.aggregate([{"$sample": {"size": 10}}]))

# Inserisci nella collezione di destinazione
dst_coll.insert_many(sample_docs)

print("10 documenti copiati con successo!")