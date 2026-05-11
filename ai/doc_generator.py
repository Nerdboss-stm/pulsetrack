"""
AI-assisted dbt documentation generator.

Reads a model's SQL + its compiled manifest entry, and produces a
``_<model>.yml`` snippet with:
  - A model-level ``description`` written in our voice.
  - A column entry for every column in the model with a one-line
    ``description``.
  - Suggested test stack per column (not_null, accepted_values,
    relationships, dbt_utils.accepted_range) based on heuristics
    + the SQL semantics.

Used as a developer aid: run before opening a PR to scaffold the
``_*.yml`` then hand-edit. NOT a substitute for human documentation;
just removes the boilerplate.

Usage:
    python -m ai.doc_generator \
        --model dbt_project/models/marts/core/dim_patient.sql

Output: prints the proposed YAML to stdout.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ai.client import Prompt, complete

log = logging.getLogger(__name__)


SYSTEM_PROMPT = """\
You are a senior data engineer documenting a dbt model. You're given a \
.sql file and asked to produce a `_<model>.yml` snippet with:

1. Model-level `description` (one paragraph, plain English).
2. A `columns:` block where every column has a `description` and at \
least one test.

Test selection heuristics:
- Surrogate keys (`*_key`): not_null + unique.
- FK columns referencing dim_*: not_null + relationships.
- Enum-like columns: accepted_values with the values from the SQL.
- Numeric ranges (`*_pct`, `*_score`, `*_rate`): dbt_utils.accepted_range.
- Timestamp columns: not_null where they appear non-nullable in the SQL.

Output format: pure YAML, no markdown fences, no commentary. Match the \
existing _stg.yml / _core.yml indentation: 2 spaces, dashed lists, \
column entries below `columns:`.

Example shape:

  version: 2
  models:
    - name: stg_X
      description: |
        Staging view ...
      columns:
        - name: x_key
          description: SHA-256 surrogate over (id). Primary key.
          tests: [not_null, unique]

Be terse. Description fields are ≤ 100 chars unless the column is \
genuinely complex."""


def generate_yaml(model_sql: str, model_name: str) -> str:
    """Generate a `_<model_name>.yml` snippet from the model SQL.

    Args:
        model_sql: full source of the .sql file.
        model_name: the model's name (filename without .sql).

    Returns:
        YAML string suitable to paste into `_<group>.yml`.
    """
    user_prompt = (
        f"Generate the YAML doc snippet for dbt model `{model_name}`.\n\n"
        f"Model SQL:\n```sql\n{model_sql[:8000]}\n```"
    )

    return complete(
        Prompt(
            user=user_prompt,
            system=SYSTEM_PROMPT,
            max_tokens=3000,
        )
    ).text


def main():
    parser = argparse.ArgumentParser(prog="ai.doc_generator")
    parser.add_argument(
        "--model",
        required=True,
        help="Path to the dbt model .sql file",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write to <dir>/_<model>.gen.yml instead of stdout",
    )
    args = parser.parse_args()

    path = Path(args.model)
    if not path.exists():
        log.error("file not found: %s", path)
        sys.exit(2)

    model_name = path.stem
    sql = path.read_text(encoding="utf-8")

    yaml_text = generate_yaml(sql, model_name)

    if args.write:
        out = path.parent / f"_{model_name}.gen.yml"
        out.write_text(yaml_text, encoding="utf-8")
        print(f"wrote {out}")
    else:
        print(yaml_text)


if __name__ == "__main__":
    main()
