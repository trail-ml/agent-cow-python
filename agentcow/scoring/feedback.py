"""
Default LLM-consumable feedback report for COW session scoring.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .sample_scorers import f1, precision, recall

if TYPE_CHECKING:
    from .scorer import ScoringResult


MATCHED_ATTENTION_THRESHOLD = 0.9


def default_feedback_fn(result: "ScoringResult") -> str:
    terms = result.terms
    lines: list[str] = ["SCORING FEEDBACK REPORT", "", "== Scores =="]
    for name, value in result.scores.items():
        lines.append(f"{name}: {value:.2%}")

    n_ops = len(terms.op_utilities)
    if n_ops:
        avg_struct_util = sum(o.structural_utility for o in terms.op_utilities) / n_ops
        avg_content_util = sum(o.content_utility for o in terms.op_utilities) / n_ops
    else:
        avg_struct_util = 0.0
        avg_content_util = 0.0

    lines.extend(
        [
            "",
            "== Terms ==",
            (
                f"Entity state - structural: {terms.structural_score:.2%}, "
                f"content: {terms.content_score:.2%}, "
                f"relationship: {terms.relationship_score:.2%}"
            ),
            f"Efficiency: {terms.efficiency:.2%}",
            (
                f"Precision: {precision(terms):.2%}, "
                f"Recall: {recall(terms):.2%}, F1: {f1(terms):.2%}"
            ),
            (
                f"Op utility (avg) - structural: {avg_struct_util:+.2%}, "
                f"content: {avg_content_util:.2%}"
            ),
            (
                f"Rows - matched: {terms.matched_row_count}, "
                f"missing: {terms.missing_row_count}, "
                f"extra: {terms.extra_row_count}"
            ),
            (
                f"Operations - GT: {terms.gt_operation_count}, "
                f"agent: {terms.agent_operation_count}"
            ),
        ]
    )

    if result.missing_writes:
        lines.extend(["", "== Missing writes (GT rows the agent did not produce) =="])
        for miss in result.missing_writes:
            gt = miss.ground_truth
            action = "DELETE" if gt.is_delete else "WRITE"
            lines.append(f"[{gt.table_name} {action} {_format_pk(gt.primary_key)}]")
            if miss.feedback:
                lines.append(f"  {miss.feedback}")

    if result.extra_writes:
        lines.extend(["", "== Extra writes (agent rows with no GT counterpart) =="])
        for extra_write in result.extra_writes:
            agent = extra_write.agent
            action = "DELETE" if agent.is_delete else "WRITE"
            lines.append(f"[{agent.table_name} {action} {_format_pk(agent.primary_key)}]")
            if extra_write.feedback:
                lines.append(f"  {extra_write.feedback}")

    low_sim = [
        matched_write
        for matched_write in result.matched_writes
        if matched_write.comparison.similarity < MATCHED_ATTENTION_THRESHOLD
    ]
    if low_sim:
        lines.extend(
            [
                "",
                (
                    "== Matched writes needing attention (similarity < "
                    f"{MATCHED_ATTENTION_THRESHOLD:.0%}) =="
                ),
            ]
        )
        for matched_write in low_sim:
            gt = matched_write.ground_truth
            lines.append(
                f"[{gt.table_name} {_format_pk(gt.primary_key)}] "
                f"similarity {matched_write.comparison.similarity:.2%}"
            )
            if matched_write.comparison.overall_feedback:
                lines.append(f"  {matched_write.comparison.overall_feedback}")
            for field_result in matched_write.comparison.field_results:
                if field_result.matches:
                    continue
                lines.append(
                    f"  - {field_result.field_name}: "
                    f"gt={field_result.ground_truth_value!r} "
                    f"agent={field_result.agent_value!r} "
                    f"(sim {field_result.similarity:.2%})"
                )

    return "\n".join(lines)


def _format_pk(pk: dict[str, Any]) -> str:
    return "{" + ", ".join(f"{k}={v!r}" for k, v in pk.items()) + "}"
