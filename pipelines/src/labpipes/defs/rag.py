"""S6: the wiki RAG branch, in the `rag` database on RDS (pgvector), on a daily schedule.

- wiki_pages: the published pages from the ops wiki (resource `wiki`: labpipes/wiki.py). Each page's ETag is
  stored and sent next time: an unchanged page then costs a 304 and no body.
- wiki_chunks: split every stored page with labpipes.chunking.chunk_markdown (chunk size and overlap are
  decision 015) and keep the chunks; Chunk.content_hash is the id, so unchanged text keeps its id (and its
  vector), changed text gets a new id, and chunks of text that is gone are deleted.
- wiki_embeddings: embed only the chunks that have no vector yet (resource `embed`: nomic-embed-text,
  768 dimensions) and store them as pgvector `vector(768)`, with an HNSW index (decision 016).
- A blocking check that every chunk has a vector, and a daily schedule. Run it by hand with
  `scripts/dagster-run.sh ingest_wiki --wait`; a second run must fetch and embed nothing.

The RDS admin connection comes from the `rag` resource (RagStore.connect); `create extension vector` works
there. The app reads `rag.chunks` joined to `rag.pages` (rideapp/assistant.py).
"""

import dagster as dg

from ..chunking import Chunk, chunk_markdown
from ..resources import LlmEmbed, RagStore, WikiApi

# Decision 015: the wiki's pages are short (about 60 to 180 tokens), so a chunk is usually a whole page.
CHUNK_TOKENS = 200
CHUNK_OVERLAP = 30

SETUP_SQL = (
    "create extension if not exists vector",
    "create schema if not exists rag",
    "create table if not exists rag.pages ("
    " id text primary key, title text not null, updated_at text not null, etag text not null,"
    " body text not null, fetched_at timestamptz not null default now())",
    "create table if not exists rag.chunks ("
    " id text primary key, page_id text not null references rag.pages (id) on delete cascade,"
    " idx integer not null, heading text not null, content text not null, tokens integer not null,"
    " embedding vector(768))",
    # Decision 016: HNSW, cosine distance (the same measure nomic-embed-text is trained for).
    "create index if not exists chunks_embedding_hnsw on rag.chunks using hnsw (embedding vector_cosine_ops)",
)


def vector_literal(v: list[float]) -> str:
    """pgvector's text form of a vector, for a `%s::vector` parameter."""
    return "[" + ",".join(f"{x:.7g}" for x in v) + "]"


def page_chunks(page_id: str, title: str, body: str) -> dict[str, Chunk]:
    """The chunks a page should have, by id (content hash), in page order."""
    return {c.content_hash(page_id): c for c in chunk_markdown(title, body, CHUNK_TOKENS, CHUNK_OVERLAP)}


def plan_changes(existing: set[str], wanted: set[str]) -> tuple[set[str], set[str]]:
    """(ids to add, ids to remove) to turn the stored chunk ids into the wanted ones."""
    return wanted - existing, existing - wanted


def _setup(cur) -> None:
    for sql in SETUP_SQL:
        cur.execute(sql)


@dg.asset(group_name="rag", kinds={"postgres"})
def wiki_pages(wiki: WikiApi, rag: RagStore) -> dg.MaterializeResult:
    """The wiki's published pages, fetched with conditional GETs: only pages whose ETag changed come back."""
    client = wiki.client()
    refs = client.pages()
    conn = rag.connect()
    try:
        with conn, conn.cursor() as cur:
            _setup(cur)
            cur.execute("select id, etag from rag.pages")
            stored = dict(cur.fetchall())
            fetched = 0
            for ref in refs:
                page = client.page(ref.id, stored.get(ref.id))  # None: a 304
                if page is None:
                    continue
                fetched += 1
                cur.execute(
                    "insert into rag.pages (id, title, updated_at, etag, body) values (%s, %s, %s, %s, %s) "
                    "on conflict (id) do update set title = excluded.title, updated_at = excluded.updated_at,"
                    " etag = excluded.etag, body = excluded.body, fetched_at = now()",
                    (page.id, page.title, page.updated_at, page.etag, page.body),
                )
            gone = sorted(set(stored) - {r.id for r in refs})
            if gone:  # unpublished pages go, and their chunks with them (on delete cascade)
                cur.execute("delete from rag.pages where id = any(%s)", (gone,))
    finally:
        conn.close()
    return dg.MaterializeResult(metadata={
        "pages_listed": dg.MetadataValue.int(len(refs)),
        "pages_fetched": dg.MetadataValue.int(fetched),
        "pages_not_modified": dg.MetadataValue.int(len(refs) - fetched),
        "pages_removed": dg.MetadataValue.int(len(gone)),
    })


@dg.asset(deps=[wiki_pages], group_name="rag", kinds={"postgres"})
def wiki_chunks(rag: RagStore) -> dg.MaterializeResult:
    """Chunks of every stored page, keyed by content hash: only text not seen before is added."""
    conn = rag.connect()
    added = removed = total = 0
    try:
        with conn, conn.cursor() as cur:
            _setup(cur)
            cur.execute("select id, title, body from rag.pages order by id")
            for page_id, title, body in cur.fetchall():
                wanted = page_chunks(page_id, title, body)
                cur.execute("select id from rag.chunks where page_id = %s", (page_id,))
                new, old = plan_changes({r[0] for r in cur.fetchall()}, set(wanted))
                if old:
                    cur.execute("delete from rag.chunks where id = any(%s)", (sorted(old),))
                for cid, c in wanted.items():
                    # An existing chunk keeps its vector; only its position and heading are refreshed.
                    cur.execute(
                        "insert into rag.chunks (id, page_id, idx, heading, content, tokens) values (%s, %s, %s, %s, %s, %s) "
                        "on conflict (id) do update set idx = excluded.idx, heading = excluded.heading",
                        (cid, page_id, c.index, c.heading, c.text, c.tokens),
                    )
                added, removed, total = added + len(new), removed + len(old), total + len(wanted)
    finally:
        conn.close()
    return dg.MaterializeResult(metadata={
        "chunks_total": dg.MetadataValue.int(total),
        "chunks_added": dg.MetadataValue.int(added),
        "chunks_removed": dg.MetadataValue.int(removed),
        "chunk_tokens": dg.MetadataValue.int(CHUNK_TOKENS),
        "chunk_overlap": dg.MetadataValue.int(CHUNK_OVERLAP),
    })


@dg.asset(deps=[wiki_chunks], group_name="rag", kinds={"postgres", "llama.cpp"})
def wiki_embeddings(rag: RagStore, embed: LlmEmbed) -> dg.MaterializeResult:
    """A vector for every chunk that has none yet, from the embedding server on the LLM host."""
    conn = rag.connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("select id, content from rag.chunks where embedding is null order by id")
            todo = cur.fetchall()
            vectors = embed.embed_documents([content for _, content in todo]) if todo else []
            for (cid, _), v in zip(todo, vectors, strict=True):
                cur.execute("update rag.chunks set embedding = %s::vector where id = %s", (vector_literal(v), cid))
            cur.execute("select count(*) from rag.chunks")
            total = cur.fetchone()[0]
    finally:
        conn.close()
    return dg.MaterializeResult(metadata={
        "chunks_embedded": dg.MetadataValue.int(len(todo)),
        "chunks_total": dg.MetadataValue.int(total),
    })


@dg.asset_check(asset=wiki_embeddings, blocking=True, description="There are chunks, and every one has a vector.")
def every_chunk_has_a_vector(rag: RagStore) -> dg.AssetCheckResult:
    conn = rag.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select count(*), count(*) filter (where embedding is null) from rag.chunks")
            total, missing = cur.fetchone()
    finally:
        conn.close()
    return dg.AssetCheckResult(
        passed=total > 0 and missing == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"{total} chunk(s), {missing} without a vector",
        metadata={"chunks": dg.MetadataValue.int(total), "without_vector": dg.MetadataValue.int(missing)},
    )


ingest_wiki = dg.define_asset_job(
    "ingest_wiki",
    selection=dg.AssetSelection.assets(wiki_pages, wiki_chunks, wiki_embeddings),  # and their checks
    description="Fetch changed wiki pages, chunk them, embed new chunks.",
)

# Daily, early in the morning UTC: the wiki changes by the day, and an unchanged wiki costs 16 conditional GETs.
ingest_wiki_daily = dg.ScheduleDefinition(
    job=ingest_wiki,
    cron_schedule="0 3 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
