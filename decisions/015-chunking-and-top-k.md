# 015: Chunks of 200 tokens with 30 overlap, and the top 3 chunks per question

Stage: S6 · Date: 2026-11-22 · Author: Prince0906 · Drafted by agent: yes (the choice is the author's; rewrite the reasoning in your own words, you will be asked to defend it)

## Choice

`CHUNK_TOKENS = 200`, `CHUNK_OVERLAP = 30` in `pipelines/src/labpipes/defs/rag.py`, and `TOP_K = 3` in
`app/src/rideapp/assistant.py`.

## Rejected alternatives

- **Smaller chunks (120 tokens, 20 overlap).** 20 chunks instead of 16: a few pages split in two, and a
  question about one fact can then land on the half that lacks the title's context.
- **Top-k of 5.** More chance that a second page is included, but each chunk costs about 11 s of prompt reading on
  the CPU LLM host, so 5 chunks is about 20 s more per answer.
- **Top-k of 1.** The fastest, but one wrong first hit gives a confident wrong answer, and a question that needs
  two pages cannot be answered.

## Why

- I measured the wiki: 16 pages, 62 to 181 tokens each (median 94), 1,546 tokens in all. At 200 tokens or more
  every page is exactly one chunk (16 chunks, average 101 tokens), so overlap changes nothing today and only
  matters when a page grows past 200 tokens. 30 is a small safety margin for that day.
- A chunk is a whole page with its title and heading in front, so "It is $4.50" is findable.
- Top-k 3 costs about 3 x 11 s of prompt reading; the measured answers took 38 to 43 s end to end, and the
  right page was the first hit both times (similarity 0.75 and 0.85, the next hit 0.68 and 0.68).
- Chunk ids are content hashes, so changing these two numbers changes the ids of any page they re-split: the
  old chunks are deleted and the new ones embedded on the next run, and nothing else is touched.
- Cost of this choice: three chunks is three chances, not a guarantee. A question that needs a fourth page gets an
  answer from three, and the prompt tells the model to say "I do not know" rather than guess.

## What would show this was wrong

- A question whose answer is on a page that came 4th or lower (the trace lists the chunks it got).
- Pages that grow past 200 tokens and split in the middle of a rule (the overlap or size is too small).
- Answers that take longer than the 120 s the contract tells clients to wait: lower k or the chunk size.
