"""
Sample :data:`ScoreFn` implementations for COW session scoring.
"""

from __future__ import annotations

from .types import SessionScoringTerms


def precision(terms: SessionScoringTerms) -> float:
    matched = terms.matched_row_count
    extra = terms.extra_row_count
    denom = matched + extra
    return matched / denom if denom else 1.0


def recall(terms: SessionScoringTerms) -> float:
    matched = terms.matched_row_count
    missing = terms.missing_row_count
    denom = matched + missing
    return matched / denom if denom else 1.0


def f1(terms: SessionScoringTerms) -> float:
    p = precision(terms)
    r = recall(terms)
    return (2 * p * r / (p + r)) if (p + r) else 0.0


def default_score_fn(terms: SessionScoringTerms) -> float:
    n = len(terms.op_utilities)
    if n:
        sum_structural_utility = sum(
            utility.structural_utility for utility in terms.op_utilities
        )
        avg_content_utility = (
            sum(utility.content_utility for utility in terms.op_utilities) / n
        )
    else:
        sum_structural_utility = 0.0
        avg_content_utility = 0.0
    clamped_structural_journey = max(0.0, min(1.0, sum_structural_utility))
    return (
        terms.structural_score * 0.4
        + terms.content_score * 0.2
        + clamped_structural_journey * 0.1
        + avg_content_utility * 0.1
        + terms.efficiency * 0.2
    )
