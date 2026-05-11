"""
AI-assisted analysis of Great Expectations failures.

When a GX gate fails, the operator sees a JSON blob like:

  {
    "expectation_type": "expect_column_values_to_be_between",
    "kwargs": {"column": "metric_value", "min_value": 30, "max_value": 220},
    "result": {"unexpected_count": 47, "unexpected_percent": 0.05,
               "partial_unexpected_list": [225, 230, 250, ...]}
  }

That's true but unhelpful at 3am. This module passes the failure +
relevant table context to Claude and asks for:
  1. A plain-English diagnosis ("heart_rate_bpm spike above 220").
  2. Likely root causes ranked by probability.
  3. Suggested next steps for the operator.

Output is structured Markdown that gets posted to Slack alongside the
raw failure detail.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from ai.client import Prompt, complete

log = logging.getLogger(__name__)


SYSTEM_PROMPT = """\
You are a senior data engineer at a healthcare lakehouse company. You're \
analyzing Great Expectations data quality failures for a streaming \
pipeline (Spark → Iceberg → Snowflake). Your job is to help the on-call \
engineer triage the failure quickly.

For each failure, produce a Markdown response with three sections:

## Diagnosis
2-3 sentences describing what the failure means in plain English. \
Avoid jargon. Avoid restating the raw expectation/threshold — the \
operator already sees that.

## Likely causes (ranked)
1. **Cause 1** — one-line explanation of why it's likely.
2. **Cause 2** — etc.
Rank by probability given the context. List at most 4.

## Suggested next steps
- One concrete action (query / file / dashboard to check).
- One concrete action.
- One concrete action.

Keep total response under 400 words. Tone: terse, technical, no \
hedging. The operator wants to act, not read prose."""


def analyze_gx_failure(
    suite_name: str,
    layer: str,
    source: str,
    failure_payload: dict,
    table_context: Optional[dict] = None,
) -> str:
    """Analyze one GX expectation failure with Claude.

    Args:
        suite_name: e.g., 'bronze_sensor', 'silver_sensor'.
        layer: 'bronze' | 'silver' | 'gold'.
        source: 'sensor' | 'pharmacy' | 'ehr'.
        failure_payload: the GX expectation result dict
            (kwargs + result.unexpected_count etc.).
        table_context: optional dict with recent metrics
            (row_count, avg_value, recent_changes, etc.) to inform
            the analysis.

    Returns:
        Markdown diagnosis suitable for Slack.
    """
    context_lines = [
        f"Suite:  `{suite_name}`",
        f"Layer:  `{layer}`",
        f"Source: `{source}`",
        "",
        "## Failure payload",
        "```json",
        json.dumps(failure_payload, indent=2, default=str)[:3000],
        "```",
    ]
    if table_context:
        context_lines += [
            "",
            "## Table context",
            "```json",
            json.dumps(table_context, indent=2, default=str)[:1500],
            "```",
        ]

    return complete(
        Prompt(
            user="\n".join(context_lines),
            system=SYSTEM_PROMPT,
            max_tokens=1500,
        )
    ).text


def analyze_gx_run(
    suite_name: str,
    layer: str,
    source: str,
    run_results: dict,
) -> str:
    """Analyze the entire GX run (multiple expectation failures).

    Picks the top 3 failures by unexpected_percent and asks Claude
    for a consolidated diagnosis.
    """
    # Extract failed expectations.
    failures = [
        exp for exp in run_results.get("expectations", [])
        if not exp.get("success", True)
    ]
    if not failures:
        return "All expectations passed — no AI analysis needed."

    # Sort by unexpected_percent (worst first).
    failures.sort(
        key=lambda e: e.get("result", {}).get("unexpected_percent", 0.0),
        reverse=True,
    )
    top = failures[:3]

    summary_lines = [
        f"Suite:  `{suite_name}` (layer={layer}, source={source})",
        f"Total expectations: {len(run_results.get('expectations', []))}",
        f"Failed: {len(failures)}",
        "",
        "## Top failures",
    ]
    for i, f in enumerate(top, 1):
        kw = f.get("kwargs", {})
        res = f.get("result", {})
        summary_lines.append(
            f"### {i}. `{f.get('expectation_type', '?')}` on "
            f"column `{kw.get('column', '?')}`"
        )
        summary_lines.append(
            f"unexpected={res.get('unexpected_count', '?')} "
            f"({res.get('unexpected_percent', 0.0) * 100:.2f}%); "
            f"sample={res.get('partial_unexpected_list', [])[:5]}"
        )
        summary_lines.append("")

    return complete(
        Prompt(
            user="\n".join(summary_lines),
            system=SYSTEM_PROMPT,
            max_tokens=2000,
        )
    ).text
