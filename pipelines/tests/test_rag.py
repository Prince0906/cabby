"""S6, my own tests: the pure parts of the wiki ingest (no network, no database)."""

from labpipes.defs.rag import CHUNK_OVERLAP, CHUNK_TOKENS, page_chunks, plan_changes, vector_literal

BODY = "Surcharge applies at Pier 76.\n\n## Fee\n\nIt is $4.50 and shows as HL-76 on receipts.\n"


def test_unchanged_text_keeps_its_ids_so_nothing_is_embedded_twice():
    first = page_chunks("harbor-line", "Harbor Line", BODY)
    again = page_chunks("harbor-line", "Harbor Line", BODY)
    assert list(first) == list(again) and len(first) >= 1
    new, old = plan_changes(set(first), set(again))
    assert new == set() and old == set()


def test_changed_text_gets_a_new_id_and_the_old_one_is_removed():
    before = page_chunks("harbor-line", "Harbor Line", BODY)
    after = page_chunks("harbor-line", "Harbor Line", BODY.replace("$4.50", "$5.00"))
    new, old = plan_changes(set(before), set(after))
    assert new and old and new.isdisjoint(old)


def test_the_same_text_on_two_pages_has_two_ids():
    assert set(page_chunks("a", "T", BODY)).isdisjoint(page_chunks("b", "T", BODY))


def test_chunk_sizes_are_decision_015_and_valid():
    assert (CHUNK_TOKENS, CHUNK_OVERLAP) == (200, 30)


def test_vector_literal_is_pgvectors_text_form():
    assert vector_literal([0.5, -1.0, 2.25]) == "[0.5,-1,2.25]"
