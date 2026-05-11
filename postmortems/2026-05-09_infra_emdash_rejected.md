# Postmortem: EC2 security group rejected description with em-dash; terraform apply failed

**Date:** 2026-05-09
**Severity:** SEV3 (cosmetic; cluster deploy delayed by 20 min)
**Status:** Resolved
**Authors:** PulseTrack DE
**Anchor commit / change:** [`5e08bb0`](../#) — `fix(infra): EC2 SG description rejects non-ASCII (em-dash); use plain hyphen`

## Summary

A `terraform apply` failed during EMR cluster spin-up with a cryptic AWS error: `InvalidParameterValue: Invalid description for security group`. The description contained an em-dash (`—`, U+2014) instead of a plain hyphen (`-`, U+002D). EC2 security group description fields silently reject non-ASCII characters with the unhelpful error above.

Filing this small SEV3 because:
1. It cost 20 minutes of debugging (small but real)
2. It's a class-of-bug worth documenting — Unicode in places that look ASCII-friendly
3. The fix is a 1-character change; the lesson is bigger than the bug.

## Impact

- **Blast radius:** terraform apply
- **Duration:** ~20 minutes from first failed apply to fix
- **Production impact:** none

## Timeline (UTC)

| Time | Event |
|---|---|
| 2026-05-09 13:09 | `terraform apply` fails on security group creation |
| 2026-05-09 13:12 | Error message: `InvalidParameterValue: Invalid description for security group`. No clue what's invalid. |
| 2026-05-09 13:15 | Inspect the security group block in TF: description reads "PulseTrack EMR core — allow Kafka + S3" |
| 2026-05-09 13:18 | Grep for non-ASCII characters: `grep -P "[^\x00-\x7F]" infrastructure/modules/networking/main.tf` — finds the em-dash |
| 2026-05-09 13:19 | Cross-check AWS docs: "Description can contain alphanumeric characters, spaces, periods, hyphens (-), and these special characters: `! \\ . , : ; @ # _`" |
| 2026-05-09 13:19 | The em-dash is NOT in the allowed set |
| 2026-05-09 13:20 | Fix: replace `—` with `-` |
| 2026-05-09 13:21 | `terraform apply` succeeds |

## Root cause

AWS EC2 security group description fields enforce ASCII subset matching the regex above. Em-dashes, typographic quotes, and other non-ASCII characters fail validation silently with "Invalid description for security group" — no character index, no helpful pointer.

The em-dash entered the source file because Markdown / docstring conventions in PulseTrack use em-dashes for readability (consistent with PEP 257-style docstrings I prefer). A code-comment style choice leaked into an AWS API value.

## 5 Whys

1. **Why did terraform apply fail?** Because the security group description contained an em-dash.
2. **Why did the description contain an em-dash?** Because I typed it that way following the project's docstring style.
3. **Why didn't a linter catch it?** Because terraform fmt doesn't validate field-content against AWS regex, only TF syntax.
4. **Why didn't a pre-commit hook catch it?** Because no hook checks for non-ASCII in TF files.
5. **Why isn't checking for non-ASCII in TF files standard practice?** Because most TF code is ASCII by default — the em-dash here was an outlier from the project's broader text-style convention.

## Trigger

The first `terraform apply` after adding the security group block. Pure latent bug — would have surfaced any time the SG was applied.

## Resolution

Commit `5e08bb0`: single character change, `—` → `-`. All AWS resource description fields audited; this was the only instance.

## What didn't go well

- AWS error message gave zero diagnostics. "Invalid description" without saying WHAT is invalid is poor UX.
- No pre-commit check for non-ASCII in TF files. Would catch this class of bug.

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | Add pre-commit hook: reject non-ASCII in `*.tf` files unless explicitly opted-in via header | PulseTrack DE | 2026-05-22 | P2 |
| 2 | Project style guide: ASCII-only in any string that goes into an AWS API call | PulseTrack DE | 2026-05-15 | P3 |

## Lessons learned

**Text-style conventions don't survive boundaries.** Em-dashes are great in Markdown comments and docstrings; they're poison in AWS API calls. When code crosses a system boundary, encoding matters.

**Cryptic error messages waste real time.** AWS could fix this error in one PR. We can't make them, but we can avoid the failure mode entirely with a pre-commit gate.

## References

- Related commits: `5e08bb0` (the fix)
- External: AWS EC2 SG description validation rules
