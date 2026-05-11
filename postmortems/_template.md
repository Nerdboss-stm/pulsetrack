# Postmortem: <one-line summary, imperative voice — "<system> <verb> <impact>">

**Date:** YYYY-MM-DD
**Severity:** SEV1 / SEV2 / SEV3
**Status:** Resolved | Mitigated | Open
**Authors:** <name(s)>
**Incident commander:** <name>
**Anchor commit / change:** [`<shortsha>`](https://github.com/.../commit/<sha>) — `<commit subject>`

> Blameless postmortem culture. The "5 Whys" and "what didn't go well" sections name systems, processes, and gaps — never people. The goal is org-learning.

## Summary

One paragraph (3-5 sentences). What broke, when, who was affected, how it was resolved, what changed permanently.

## Impact

- **Blast radius:** which layer, which downstream consumers, which time range
- **Customer-facing impact:** data delay / unavailability / quality regression
- **Data impact:** rows lost, rows duplicated, partition replays needed
- **Cost impact:** if any (overpay, wasted compute, retroactive replay)
- **Duration:** time from first symptom to full resolution

## Timeline (UTC, minute granular)

| Time | Event |
|---|---|
| HH:MM | First symptom |
| HH:MM | Alert fired / detection |
| HH:MM | On-call ack |
| HH:MM | Mitigation applied |
| HH:MM | Root-cause identified |
| HH:MM | Permanent fix deployed |
| HH:MM | Recovery verified |
| HH:MM | All-clear / customer comms |

## Root cause

Plain-language explanation of the bug, including the precise code path / config / data state that produced the failure. Quote exact log lines, error messages, commit hashes.

## 5 Whys

1. **Why did <symptom> happen?** Because <X>.
2. **Why did <X> happen?** Because <Y>.
3. **Why did <Y> happen?** Because <Z>.
4. **Why did <Z> happen?** Because <process-or-system gap>.
5. **Why did <process gap> exist?** Because <organizational reason>.

(The fifth "why" usually lands on a missing test, missing review gate, missing alert, or missing onboarding doc.)

## Trigger

What activated the latent bug. Often: a deploy, a config change, a traffic spike, a 3rd-party API change, a schema evolution.

## Resolution

The exact change(s) made to resolve:
- Commit `<sha>`: `<subject>`
- Config change: `<before>` → `<after>`
- Operational action: `<what was run>`

## What went well

(Aim for 3-5.)

- Alert fired within X seconds of the symptom — runbook X had the exact diagnosis steps
- On-call had recent training on Y, made the fix in Z minutes
- Mitigation was applied before customer impact spread further

## What didn't go well

(Aim for 3-5. Be honest. This is where the org learning happens.)

- The alert threshold was set too high — symptom existed for X minutes before detection
- No runbook existed for this failure mode
- Mitigation required logging into the EMR master via SSH, which X engineers can do — single point of failure
- A test that should have caught this didn't exist

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | <specific task> | <name> | YYYY-MM-DD | P0/P1/P2 |
| 2 | ... | | | |

Every action item must have an owner + a due date. "We should consider X" is not an action item.

## Lessons learned

(2-3 short paragraphs. What this incident teaches us about the system, the process, or the team.)

## References

- Runbook used: `runbooks/<...>.md`
- Related postmortems: ...
- Related ADRs: ...
- External docs / vendor advisories: ...
