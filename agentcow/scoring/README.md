# agent-cow scoring

**Score an agent's COW session against a ground truth recording.**

Answers two questions at two levels:

- **Session level** — *did the agent produce the right world state?* (structural + content scores) and *did the agent do it efficiently?* (op count and waste).
- **Operation level** — for each individual agent op, *did this step move the world closer to GT?* (`structural_utility`) and *were the values this step wrote correct?* (`content_utility`).

## Install

Scoring ships with `agent-cow`:

```bash
pip install agent-cow
```

## Quick Example

Compare an agent session to a ground truth session using the default score function:

```python
import uuid
from agentcow.scoring import score_cow_sessions

result = await score_cow_sessions(
    executor=executor,
    ground_truth_session_id=uuid.UUID("..."),
    agent_session_id=uuid.UUID("..."),
    schema="public",
)

result.scores["overall"]   # float in [0, 1]
result.feedback_report     # LLM-ready narrative
```

That's it. `score_cow_sessions` pulls both sessions from the COW `*_changes` tables, compares them row-by-row, and returns one number plus a human/LLM readable report.

### The default score

When you don't pass a `ScoringConfig`, `result.scores["overall"]` is computed by `default_score_fn`:

```
overall = structural_score    * 0.5    # did the agent write to the right rows?
        + content_score       * 0.2    # were the field values correct?
        + avg_content_utility * 0.1    # was each step filling in correct values?
        + efficiency          * 0.2    # reasonable number of ops, no wasted work?
```

Weights sum to 1, and each term is in `[0, 1]`, so a perfect session hits exactly `1.0`. Each term is a raw signal exposed on `result.terms` — see below.

## Using Your Own Score

The default is a starting point. To weigh things differently, register your own `ScoreFn`:

```python
from agentcow.scoring import ScoringConfig, SessionScoringTerms, score_cow_sessions

def outcome_only(terms: SessionScoringTerms) -> float:
    return terms.structural_score * terms.content_score

result = await score_cow_sessions(
    executor=executor,
    ground_truth_session_id=gt_id,
    agent_session_id=agent_id,
    schema="public",
    config=ScoringConfig(score_fns={
        "overall": outcome_only,
    }),
)
```

`sample_scorers.py` ships ready-made `ScoreFn`s you can register by name: `default_score_fn`, `precision`, `recall`, `f1`.

## Custom per-table similarity

`ScoreFn`s only reweight *already-aggregated* signals (eg. extra_row_count, matched_row_count) — by the time one runs, every row has already been matched and compared. Writing a full `WriteComparator` reaches earlier in the pipeline but forces you to reimplement FK remapping, field-level dispatch, and structured result objects just to change one rule.

You can use a `row_similarity` function to override **how similar two rows are** for a given table:

```python
from agentcow.scoring import ScoringConfig, score_cow_sessions

def issues_similar(gt: dict, agent: dict) -> bool:
    return (
        gt["name"].strip().lower() == agent["name"].strip().lower()
        and gt["state_id"] == agent["state_id"]
    )

result = await score_cow_sessions(
    executor=executor,
    ground_truth_session_id=gt_id,
    agent_session_id=agent_id,
    schema="public",
    config=ScoringConfig(
        row_similarity={"issues": issues_similar},
    ),
)
```

The callable takes two plain dicts and returns `bool` (exact match / no match → 1.0 / 0.0), `float` (graded similarity in `[0, 1]`), or `SimilarityResult` (for custom feedback / per-field breakdown). Foreign-key UUIDs on the agent side are automatically remapped into GT space before your function runs, so direct `==` on FK columns works without thinking about UUID mapping.

Assertion-style helpers drop in unchanged — an `AssertionError` raised inside the function is caught and becomes mismatch feedback:

```python
def issues_similar(gt, agent):
    assert gt["name"] == agent["name"], f"name differs: {gt['name']!r} vs {agent['name']!r}"
    assert gt["state_id"] == agent["state_id"], "wrong state"
    return True
```

Graded similarity when a boolean is too blunt:

```python
from difflib import SequenceMatcher

def issues_similar(gt, agent) -> float:
    name_sim = SequenceMatcher(None, gt["name"], agent["name"]).ratio()
    state_match = 1.0 if gt["state_id"] == agent["state_id"] else 0.0
    return 0.6 * name_sim + 0.4 * state_match
```

`row_similarity` is the recommended hook for per-table customization. The fuller `WriteComparator` protocol (`CompositeComparator(table_comparators=...)`) is still available when you need access to `FieldConfig`, UUID mapping state, or want to emit structured per-field results.

## What's in `result`

```python
result.scores                   # dict[str, float] — one entry per registered ScoreFn
result.feedback_report          # LLM-consumable narrative string
result.terms                    # SessionScoringTerms — every raw signal
result.scored_graph             # agent graph with per-op structural + content utility
result.matched_writes           # rows the agent got right (with per-field similarities)
result.missing_writes           # GT rows the agent never produced
result.extra_writes             # agent rows with no GT counterpart
result.entity_state_comparisons # per-entity final-state diffs
```

`SessionScoringTerms` carries the raw signals every `ScoreFn` gets:

```python
terms.structural_score      # presence/recall across entities at the final state
terms.content_score         # mean per-field similarity across matched rows
terms.relationship_score    # fraction of GT foreign-key links preserved
terms.efficiency            # op count ratio × (1 − waste ratio)
terms.op_utilities          # list[OpUtility] — per-op structural + content utility
terms.matched_row_count
terms.missing_row_count
terms.extra_row_count
terms.gt_operation_count
terms.agent_operation_count
```

## How It Works

1. **Extraction** — rows are read from the COW `*_changes` tables and grouped by `operation_id` into graph nodes. Edges come from foreign-key relationships.
2. **Matching** — rows are matched across the full session (not per node): GT might use 1 bulk write where the agent used 3 individual calls. The scorer pairs GT rows with the best agent row by table, delete/upsert type, and field similarity. UUID mapping handles the fact that GT and agent create entities with different UUIDs.
3. **Field comparison** — each field is compared by SQL type. Text is fuzzy (`SequenceMatcher.ratio()`), enums/bool/int/uuid/json are exact, primary keys and timestamps are skipped. Every comparison produces a similarity in `[0, 1]`.
4. **Entity state** (the *destination*) — collapse all rows to each entity's final state, then run matching. Produces `structural_score`, `content_score`, `relationship_score`.
5. **Op-level utility** (the *journey*) — for each agent op, compute `structural_utility` (change in structural score) and `content_utility` (mean similarity of the rows that op contributed).
6. **Efficiency** — `op_count_ratio × (1 − waste_ratio)`, where waste is agent entities that were created *and* deleted in the same session without ever matching GT.
7. **Reduce** — the registered `ScoreFn`s reduce `SessionScoringTerms` into `result.scores`, and the `FeedbackFn` writes `result.feedback_report`.

## Config

All optional — pass a `ScoringConfig` to override defaults:

```python
from agentcow.scoring import ScoringConfig, CompositeComparator

ScoringConfig(
    score_fns={"overall": my_score_fn},  # default: {"overall": default_score_fn}
    feedback_fn=my_feedback_fn,          # default: default_feedback_fn (LLM-ready narrative)
    row_similarity={                     # per-table (gt_row, agent_row) -> bool|float
        "issues": my_issues_similar,
    },
    comparator=CompositeComparator(      # escape hatch: full WriteComparator protocol
        table_comparators={"my_table": MyTableComparator()},
    ),
    collapse=False,                      # True → evaluate final state only (pure outcome)
    match_threshold=0.8,                 # similarity cutoff for binary match/no-match
    exact_match_threshold=0.9999,        # pass-1 lock-in cutoff for near-perfect pairings
    ignored_fields={"updated_at"},       # fields to skip during comparison
)
```

`collapse=False` (default) keeps every raw row so create-then-delete waste is penalized. `collapse=True` removes cancelling writes before matching for a pure outcome-based evaluation.

## API

```python
from agentcow.scoring import (
    score_cow_sessions,   # executor-based: fetches both sessions, then scores
    score_sessions,       # graph-based: score two pre-built CowGraphs
    ScoringConfig,
    ScoringResult,
    SessionScoringTerms,
    default_score_fn, precision, recall, f1,
    default_feedback_fn,
    from_row_similarity, SimilarityResult, RowSimilarityFn,
    CompositeComparator, DatatypeComparator,
)
```

Use `score_sessions` directly when you already have `CowGraph`s in memory (e.g. from tests or a non-Postgres backend); use `score_cow_sessions` to pull from a live Postgres schema.
