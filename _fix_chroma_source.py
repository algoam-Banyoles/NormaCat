"""Fix: afegir camp 'source' a les metadades de ChromaDB."""
import sys, sqlite3
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, ".")
import chromadb

print("Actualitzant metadades ChromaDB amb source...", flush=True)
client = chromadb.PersistentClient(path="db/chroma_db")
col = client.get_collection("normativa")

# Mapa doc_id -> source des de SQLite
conn = sqlite3.connect("db/normativa.db")
doc_source = {}
for row in conn.execute("SELECT id, source FROM documents WHERE source IS NOT NULL AND source != ''"):
    doc_source[row[0]] = row[1]
conn.close()
print(f"  {len(doc_source)} documents amb source a SQLite", flush=True)

total = col.count()
print(f"  {total:,} vectors a ChromaDB", flush=True)

batch_size = 500
offset = 0
updated = 0

while offset < total:
    batch = col.get(limit=batch_size, offset=offset, include=["metadatas"])
    ids = batch["ids"]
    metas = batch["metadatas"]

    update_ids = []
    update_metas = []

    for cid, meta in zip(ids, metas):
        if meta.get("source"):
            continue
        doc_id = meta.get("doc_id", 0)
        source = doc_source.get(doc_id, "")
        if source:
            meta["source"] = source
            update_ids.append(cid)
            update_metas.append(meta)

    if update_ids:
        col.update(ids=update_ids, metadatas=update_metas)
        updated += len(update_ids)

    offset += batch_size
    if offset % 10000 == 0:
        print(f"  {offset:>9,} / {total:,} processats, {updated:,} actualitzats...", flush=True)

print(f"\n  Completat: {updated:,} metadades actualitzades de {total:,} vectors", flush=True)
