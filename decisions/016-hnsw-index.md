# 016: HNSW with cosine distance for the wiki vectors

Stage: S6 · Date: 2026-11-22 · Author: Prince0906 · Drafted by agent: yes (the choice is the author's; rewrite the reasoning in your own words, you will be asked to defend it)

## Choice

`rag.chunks.embedding` is `vector(768)` with `create index ... using hnsw (embedding vector_cosine_ops)`, and the
app orders by `embedding <=> query` (cosine distance, pgvector 0.8.1 on RDS).

## Rejected alternative

**IVFFlat.** It builds faster and uses less memory, but it needs rows in the table before it is built (it picks its
cluster centres from them), and its recall drops as the data changes because the centres are not updated. The
wiki changes daily, and the table starts empty, so an IVFFlat index would have to be rebuilt after the first load
and again after big changes. An index built on an empty table is useless to it.

## Why

- HNSW can be created on an empty table and keeps answering well as chunks are added and removed, so the daily
  ingest does not need a rebuild step.
- Today the wiki is 16 chunks, and an exact scan would be instant: neither index matters for speed now. I chose
  for what the pipeline does each day, and for a bigger wiki.
- Cosine distance because nomic-embed-text is trained for it, and the app reports `1 - distance` as the
  similarity the contract describes.
- At a million chunks I would still pick HNSW if memory allows: IVFFlat's build time and its drift as the data
  changes cost more than HNSW's larger index and slower build. I would then tune `ef_search` for recall against
  latency (the LLM takes seconds, so a few more milliseconds of search is cheap).
- Cost of this choice: the index is larger than IVFFlat's and slower to build, which does not matter at this size.

## What would show this was wrong

- A question whose right chunk exists but does not come back, while an exact scan (turning the index off for one
  query) finds it: the index loses recall.
- The ingest slowing down noticeably as the wiki grows because of index updates.
- The RDS instance running short of memory because of the index.
