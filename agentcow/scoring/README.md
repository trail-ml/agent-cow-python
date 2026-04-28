# agent-cow scoring

**Score an agent's COW session against a ground truth recording.**

The module mirrors the structure of [`psudeocode.md`](./psudeocode.md):

- `extraction.py` — pull op IDs, rows, and table metadata from Postgres.
- `matching.py` — group rows by PK, pair GT with agent, derive UUID mapping, detect wasted ops.
- `compare.py` — `WriteComparator` Protocol and the default `DatatypeComparator` (which accepts per-table overrides).
- `scores.py` — `struct_score`, `content_score`, `efficiency`.
- `scorer.py` — the per-op iteration flow plus the public entry points.

## Quick example

```python
from agentcow.scoring import score_cow_sessions

result = await score_cow_sessions(
    executor=executor,
    ground_truth_session_id=gt_id,
    agent_session_id=agent_id,
    schema="public",
)

result.struct_score      # entity coverage in [0, 1]
result.content_score     # mean field-similarity across matched entities
result.efficiency        # min(1, gt_ops/a_ops) * (1 - wasted/a_ops)
result.op_struct_scores  # {op_id: delta in cumulative struct_score}
result.op_content_scores # {op_id: mean similarity over matched rows from this op}
result.counts            # {"matched", "missing", "extra", "gt_ops", "agent_ops"}
```

## Custom score functions

Pass any number of `(ScoringResult) -> float` callables and they get evaluated after the core signals:

```python
from agentcow.scoring import (
    score_cow_sessions, default_score_fn, precision, recall, f1,
)

result = await score_cow_sessions(
    executor=executor,
    ground_truth_session_id=gt_id,
    agent_session_id=agent_id,
    schema="public",
    score_fns={
        "overall": default_score_fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    },
)
result.scores["overall"]   # 0.5*struct + 0.3*content + 0.2*efficiency
```

`default_score_fn` is the built-in convenience weighting; you can swap it for anything that maps a `ScoringResult` to a float.

## `collapse=True`

By default the scorer evaluates every row the agent wrote, so a create-then-delete cycle penalizes efficiency (wasted ops) and possibly struct (unmatched extra). Pass `collapse=True` to drop entities that the agent created and later deleted within the session before scoring — useful when you only care about the agent's net intent:

```python
result = await score_cow_sessions(..., collapse=True)
```

Op IDs on surviving rows are preserved, so the per-op breakdown still makes sense.

## How it works

1. **Extraction** — rows are read from `*_changes` tables, grouped by `operation_id`, sorted by `_cow_updated_at`.
2. **Matching** — both sides are reduced to one row per `(table, pk)` (last write wins). Each GT entity greedily picks the best agent entity in the same table with matching `is_delete`. UUID mapping handles the fact that GT and agent create entities with different UUIDs.
3. **Field comparison** — each field is compared by SQL type. Text uses `SequenceMatcher.ratio()`, JSON is deep-equal, everything else is exact. PKs, FKs (compared via UUID mapping), timestamps, and configured `ignored_fields` are skipped for content scoring.
4. **Per-op flow** — for each agent op in topological order, the cumulative agent rows are re-scored against GT and the delta is recorded in `op_struct_scores`. `op_content_scores` is computed independently from the final matching: each op gets the mean per-field similarity over matched rows whose agent-side write came from it (ops with no matched rows are omitted, so a weighted mean recovers `content_score`).
5. **Efficiency** — `min(1, gt_ops/agent_ops) * (1 - wasted_ops/agent_ops)`, where a wasted op is one whose only effect was an unmatched create-then-delete cycle.
6. **Reduce** — registered `score_fns` reduce the `ScoringResult` into entries on `result.scores`.

## Custom row comparison

The default per-row comparator is `DatatypeComparator`. To override how a specific table compares, pass either:

- `row_similarity={"table_name": fn, ...}` where `fn(gt_row, agent_row) -> bool | float` is the simplest hook. Return `True`/`False`, a graded float in `[0, 1]`, or raise `AssertionError` for assertion-style helpers. Agent FK UUIDs are pre-remapped into GT space so direct `==` works.
- `comparator=DatatypeComparator(table_comparators={...})` for full control. Each table comparator implements the `WriteComparator` protocol (one `.compare(gt, agent, table_meta, uuid_mapping, ignored_fields) -> float` method); tables not in the map fall through to datatype-aware comparison.

```python
from agentcow.scoring import score_cow_sessions
from agentcow.scoring.sample_evaluators import metadata_priority_match

result = await score_cow_sessions(
    executor=executor,
    ground_truth_session_id=gt_id,
    agent_session_id=agent_id,
    schema="public",
    row_similarity={"tasks": metadata_priority_match},
)
```

`metadata_priority_match` is a sample `RowSimilarityFn` that compares only `metadata.priority` inside a `jsonb` column — see `sample_evaluators.py` for it plus a couple of name-equality variants (including an assertion-style one).

## TODO

- **`feedback_report`** — LLM-consumable narrative summarising matched / missing / extra rows.
