"""
AI-assisted dbt PR reviewer.

Runs as a GitHub Action on every PR that touches ``dbt_project/``.
Reads the PR diff + the dbt manifest, and posts a structured review
comment with:

  1. Each model change summarized (added/modified/removed columns,
     test changes, lineage impact).
  2. Risk flags (PII columns added without masking, breaking changes
     to gold tables, missing tests).
  3. Suggestions for follow-up.

Run locally:
    python -m ai.pr_reviewer --pr 42 --repo myorg/pulsetrack

Run in CI: invoked by .github/workflows/ai-pr-review.yml with
PR_NUMBER + GITHUB_TOKEN + ANTHROPIC_API_KEY.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from typing import Optional

from ai.client import Prompt, complete

log = logging.getLogger(__name__)


SYSTEM_PROMPT = """\
You are a staff data engineer reviewing a dbt PR for PulseTrack — a \
healthcare lakehouse with PII-sensitive data. You're given:
- The PR title + description.
- The full diff for `dbt_project/`.
- The current dbt manifest (compiled state of main branch).

Produce a Markdown PR review comment with these sections:

## Summary
2-3 sentence overview of what the PR does and your overall \
assessment (LGTM / minor concerns / needs revision).

## Changes
Bullet list. For each changed model:
- `<model_name>`: what changed (columns added/removed, SQL refactor, \
test additions, etc.). One line per model.

## Risk flags
Only flag items that genuinely matter. Be conservative — false \
positives waste reviewer time. Categories to check:
- **PII**: new column with `email`, `mrn`, `ssn`, `dob`, `name`, \
`address` etc. → require masking via `generate_sha256_key` macro.
- **Breaking change**: removed/renamed column on a gold mart that \
downstream BI tools likely depend on.
- **Missing tests**: new column without any test in `_*.yml`.
- **Documentation**: new column without a `description`.
- **Performance**: cross-joins, full table scans on facts, missing \
incremental_strategy on large tables.

If no real risks, write "None — clean PR."

## Suggestions
2-3 concrete, actionable items. NOT nice-to-haves. Things that the \
reviewer should likely ask for.

Tone: terse, direct, no hedging. The reviewer wants signal not \
boilerplate. Total response ≤ 600 words."""


def get_pr_diff(pr_number: int, repo: str) -> str:
    """Fetch the PR diff via gh CLI."""
    result = subprocess.run(
        ["gh", "pr", "diff", str(pr_number), "--repo", repo],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh pr diff failed: {result.stderr}")
    return result.stdout


def get_pr_meta(pr_number: int, repo: str) -> dict:
    """Fetch PR title + body + author via gh CLI."""
    result = subprocess.run(
        [
            "gh", "pr", "view", str(pr_number),
            "--repo", repo,
            "--json", "title,body,author,headRefName,baseRefName",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh pr view failed: {result.stderr}")
    return json.loads(result.stdout)


def get_dbt_manifest(manifest_path: str) -> Optional[dict]:
    """Read the latest dbt manifest from disk (if available)."""
    if not os.path.exists(manifest_path):
        log.warning("manifest not found at %s — skipping lineage context", manifest_path)
        return None
    with open(manifest_path) as f:
        return json.load(f)


def review_pr(
    pr_number: int,
    repo: str,
    manifest_path: str = "dbt_project/target/manifest.json",
) -> str:
    """Generate a review comment for one PR.

    Returns:
        Markdown comment body suitable for ``gh pr comment``.
    """
    meta = get_pr_meta(pr_number, repo)
    diff = get_pr_diff(pr_number, repo)

    # Truncate large diffs — Claude has a context window but it
    # costs tokens.
    if len(diff) > 30_000:
        diff = diff[:30_000] + "\n\n... [truncated]"

    manifest = get_dbt_manifest(manifest_path)
    manifest_excerpt = ""
    if manifest:
        # Just the model names + their column lists — keeps token cost down.
        models = []
        for unique_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") != "model":
                continue
            cols = list(node.get("columns", {}).keys())
            models.append(f"  - {node['name']}: {len(cols)} columns")
        manifest_excerpt = (
            "## dbt manifest summary (main branch)\n"
            f"{len(models)} models:\n"
            + "\n".join(sorted(models)[:50])
        )

    user_prompt = f"""\
## PR #{pr_number}: {meta['title']}
**Author:** {meta['author']['login']}
**Base:** {meta['baseRefName']} ← **Head:** {meta['headRefName']}

**Description:**
{meta.get('body', '(no description)')[:2000]}

{manifest_excerpt}

## PR diff (`dbt_project/`)
```diff
{diff}
```
"""

    return complete(
        Prompt(
            user=user_prompt,
            system=SYSTEM_PROMPT,
            max_tokens=2500,
        )
    ).text


def post_comment(pr_number: int, repo: str, body: str) -> None:
    """Post the review as a PR comment via gh CLI."""
    # gh expects body via stdin for multi-line.
    result = subprocess.run(
        ["gh", "pr", "comment", str(pr_number), "--repo", repo, "--body-file", "-"],
        input=body,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh pr comment failed: {result.stderr}")


def main():
    parser = argparse.ArgumentParser(prog="ai.pr_reviewer")
    parser.add_argument("--pr", type=int, required=True, help="PR number")
    parser.add_argument(
        "--repo", default=os.environ.get("GITHUB_REPOSITORY", "myorg/pulsetrack"),
        help="org/repo (defaults to GITHUB_REPOSITORY env)",
    )
    parser.add_argument(
        "--manifest",
        default="dbt_project/target/manifest.json",
        help="Path to dbt manifest.json",
    )
    parser.add_argument(
        "--post",
        action="store_true",
        help="Post the review as a PR comment (vs print to stdout)",
    )
    args = parser.parse_args()

    body = review_pr(args.pr, args.repo, manifest_path=args.manifest)

    if args.post:
        post_comment(args.pr, args.repo, body)
        print(f"posted review to PR #{args.pr}")
    else:
        print(body)


if __name__ == "__main__":
    main()
