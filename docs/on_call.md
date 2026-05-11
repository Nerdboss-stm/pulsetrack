# PulseTrack On-Call

The on-call rotation, escalation policy, and shift hand-off protocol. Designed for a data org of 10 — pragmatic, light on ceremony, heavy on the parts that matter (response SLA, postmortems, hand-off).

**Audience:** every DE on the rotation. Read once at onboarding, re-read before your first primary shift.

---

## 1. Severity definitions

| Severity | Definition | Examples | First response | Resolution target |
|---|---|---|---|---|
| **SEV1** | Production data outage, or data correctness incident with downstream blast radius. Customer-visible (where "customer" = ML team, BI team, the dashboard the eng-leadership Monday meeting opens). | Bronze ingestion fully stopped > 30 min; silver `is_valid=true` rows missing; gold MERGE produced corrupted rows; credential leak (active); identity-bridge dropped > 50% of patients; Snowflake views returning > 1h stale data | **15 min ack, page on-call**, war-room channel up | < 4h |
| **SEV2** | Degraded quality or partial outage. Pipeline is moving but slower / lossier than SLO. | Silver lag p95 > 90s sustained; DLQ rate > 1%; identity-resolution dropping to 90%; one EMR streaming app keeps failing & restarting; Snowflake view 20-30 min stale | **4h ack on business hours, next-day off-hours**, Slack channel post | < 24h |
| **SEV3** | Single failed run, cosmetic issue, or alert noise. | One Prefect daily-flow run failed and retried successfully; one EMR step had to be re-submitted; dashboard panel showing stale data due to known limitation | **Next business day** | Track on Linear; no formal target |

**Promotion rules:**

- If a SEV2 isn't acked within its window → auto-promote to SEV1
- If a SEV2 lasts > 24h → promote to SEV1
- If a SEV3 happens 3 times in 7 days → promote to SEV2 (it's a pattern, not an isolated event)
- SEVs go down only via explicit operator decision; don't silently de-escalate

---

## 2. Rotation cadence

| Role | Schedule | Responsibility |
|---|---|---|
| **Primary** | 1-week shift, Mon 09:00 → next Mon 09:00 (local time) | Owns all pages, runs the day-to-day ops, leads incidents |
| **Shadow** | 1-week shift, same schedule, paired with primary | Co-owns; takes over for vacation/sick; learning rotation for new joiners |
| **Eng lead** | Always (escalation tier 2) | Approves SEV1 declarations from the floor; cross-team coordination |
| **Director** | Always (tier 3) | Customer comms; external escalation; auth for emergency spend |

**Rotation calendar:** `docs/on_call_calendar.md` (TODO — currently in Google Calendar). 6-week rotation across 6 DEs; effectively each DE primaries once every 6 weeks and shadows once.

**Coverage gaps:**

- Vacations: swap with another DE; primary owner records swap in `#pulsetrack-on-call`
- Sick day: shadow takes over; if shadow unavailable, eng lead is primary for the day

---

## 3. Paging matrix

Where does each alert end up? Goal: SEV1 wakes you up; SEV2 interrupts your work; SEV3 waits until you check Slack.

| Severity | Channel | Sender | Note |
|---|---|---|---|
| SEV1 | **PagerDuty** (Events API v2) | `observability/alerting.py:page_pagerduty` | **Currently mocked** — secret exists at `pulsetrack/dev/pagerduty` but routes to a stub. Real PagerDuty contract is a Phase-2 enhancement. While mocked, SEV1 also fires Slack `@channel` to `#pulsetrack-alerts`. |
| SEV1 | Slack `#pulsetrack-alerts` with `@channel` | `observability/alerting.py:notify_slack` | The actual page mechanism today. |
| SEV1 | SMS via PagerDuty | (once unmocked) | Mobile fallback if Slack is down |
| SEV2 | Slack `#pulsetrack-alerts` (no `@channel`) | `observability/alerting.py:notify_slack` | DE on-call sees it; acks via emoji reaction |
| SEV2 | Email digest (hourly) | `observability/alerting.py:email_digest` (TODO) | Catch-all for unacked SEV2s |
| SEV3 | Slack `#pulsetrack-noise` | same | Low-volume channel; reviewed daily |
| SEV3 | Email digest (daily) | same | Asynchronous |

**Wiring:** `observability/alerting.py` reads from `pulsetrack/<env>/slack` and `pulsetrack/<env>/pagerduty`. SNS topic `pulsetrack-{env}-alerts` is the entry point — Lambdas / EMR alarms publish to it, the alerting module subscribes and routes.

**Common alert sources that page:**

| Alert | Sev | Where it comes from |
|---|---|---|
| `pulsetrack-{env}-emr-apps-failed` | SEV1 | CloudWatch alarm (`infrastructure/modules/monitoring/main.tf`) |
| `kafka_consumer_lag > 1M` | SEV1 | Spark `StreamingQueryListener` → `metrics.py` Prometheus → CloudWatch |
| `dlq_topic_rate > 5% of producer` | SEV2 | DLQ ingestion job → SNS |
| `identity_resolution_pct < 92%` | SEV2 | `observability/monitors.py` daily run |
| `gx_silver_sensor_suite failed` | SEV2 | `data_quality/run_all_suites.py` → SNS |
| `aws_budget_alarm > 80%` | SEV2 | AWS Budgets → SNS |
| `freshness_breach (any layer)` | SEV2 | `observability/monitors.py` → SNS |
| Any `monitor_runs` row with `status='error'` | SEV3 | `observability/state.py` post-write trigger |

---

## 4. Response procedure

### 4.1 You got paged (SEV1)

1. **Ack within 15 min.** In Slack `#pulsetrack-alerts`, react with `eyes_emoji` + thread "ack — investigating". This stops the auto-promote timer.
2. **Open the war room.** Create channel `#incident-YYYYMMDD-<short-title>` from `#pulsetrack-alerts`. Invite primary on-call (you), shadow, eng lead.
3. **Identify the runbook.** Each alert payload includes a `runbook:` link. Click it. If there isn't one, this incident's first deliverable is to write it.
4. **Snapshot state.** Run the relevant diagnostic block from the runbook — copy outputs into the war room. Don't fix anything until you have a snapshot; we need it for the postmortem.
5. **Triage:**
   - Is it actually SEV1? (Can you de-escalate to SEV2?)
   - Is the immediate fix safe? (Re-deploy / restart vs. data-modifying actions)
   - Does it need cross-team help? (Eng lead pulls in DBA / Snowflake admin / etc.)
6. **Execute the fix.** Follow the runbook. If the runbook is wrong, log it in the war room — that's the runbook update task.
7. **Verify.** Use the runbook's "verification" section. The incident isn't resolved until verification passes for 15 minutes.
8. **Stand down.** Post "all clear" in war room + `#pulsetrack-alerts`. Archive the war-room channel after 7 days.
9. **Postmortem in 48h.** See § 6.

### 4.2 You got pinged (SEV2)

1. **Ack within 4h on business hours / next morning off-hours.** Slack reaction or thread.
2. **Snooze if safely-degraded.** If the system is degraded but draining (Case A in `runbooks/kafka_consumer_lag.md`), set a 1-hour snooze, re-check.
3. **Otherwise:** follow the same runbook procedure as SEV1, no war room.
4. **Postmortem in 48h** if any of: it took > 8h to resolve, it affected gold output, it required a code change.

### 4.3 You got a notification (SEV3)

Triage at start-of-next-business-day. If recurring (same alert > 3× in 7 days), promote to SEV2 and write a runbook section if missing.

---

## 5. Escalation

If you can't resolve, escalate up the tree. Default escalation timer: **1 hour** between tiers — if SEV1 isn't trending toward resolution within an hour, page the next tier.

| Tier | Who | When |
|---|---|---|
| 1 | Primary on-call (you) | Default; resolves 95% of pages |
| 2 | Eng lead | After 1h on SEV1, or immediately on data-correctness incidents (you call them, they don't wait for paging) |
| 3 | Director | After 2h on SEV1, or on credential leak / external customer impact |
| 4 | AWS support (Enterprise tier) | On AWS-side outage (MSK, EMR, KMS) lasting > 30 min |

**How to escalate:**

- Tier 2/3: Slack DM with `URGENT: SEV1 escalation — <one-line>` + link to war-room channel
- Tier 4: open AWS support case at Severity = `Production-system-down`; share case URL in war room

---

## 6. Postmortems

Every SEV1 and SEV2 gets a postmortem within 48h. Blameless culture — focus on systems, not people.

### 6.1 Template

File: `postmortems/YYYY-MM-DD_<short-slug>.md`. Use this skeleton:

```markdown
# Incident YYYY-MM-DD: <short title>

**Severity:** SEV1
**Authors:** <primary>, <shadow>
**Status:** RESOLVED / OPEN

## TL;DR (2-3 sentences)

## Impact
- Customer-visible: <what did they see? what was wrong?>
- Duration: detection T+0 → mitigation T+Xm → resolution T+Ym
- Data loss / contamination: <rows affected, layers, tables>

## Timeline (UTC)
- 14:01 — first alert fired
- 14:03 — on-call acked
- ...

## Detection
What alerted us? Were the right monitors in place? Did the alert fire on time per SLO burn-rate?

## Root cause
What actually broke. Be specific — code path, infra component, third-party.

## Resolution
What you did. Include commands run, even if "rolled back to commit X".

## What went well
2-3 things.

## What went poorly
2-3 things — system / process gaps, not people.

## Action items
| Owner | Due | Description | Linear |
|---|---|---|---|
| ... | ... | ... | ... |
```

### 6.2 Review

- Author publishes draft in `#pulsetrack-on-call` within 48h
- Async review window: 3 business days
- Comments + action items finalized by week's end
- Action items must have owners and dates; tracked in Linear

### 6.3 Blameless culture (the actual rules)

- No name-calling in postmortems. Refer to roles ("the operator", "the producer process"), not people.
- Never the question "why didn't X do Y?". Always "what would have made Y the obvious next step?".
- Postmortems are public to all of eng. Don't hide your operator-mistakes — that's how the team learns.
- Action items must be system-fixes (a guardrail, an alert, a doc) not person-fixes ("be more careful").

---

## 7. Shift hand-off

End of each shift (Mon 09:00 for primaries), post in `#pulsetrack-on-call`:

```
## Hand-off — week ending 2026-05-10

Outgoing primary: @<name>
Incoming primary: @<name>
Outgoing shadow: @<name>
Incoming shadow: @<name>

### Active issues
- <Linear link> — still open; <one-line status>

### Resolved this week
- SEV2: silver lag spike — RESOLVED; postmortem in <link>

### Notable changes shipped
- Glacierbase V006 applied to gold; <link to PR>
- WHOOP refresh-token rotation completed; new tokens in pulsetrack/dev/whoop-tokens

### Watch-outs for next week
- Anthropic API budget at 60%; throttle if exceeds 80%
- Quarterly DR drill scheduled <date>
```

Incoming primary acks the hand-off with `eyes_emoji` reaction within 1 hour.

---

## 8. Mocked / TODO components

These are gaps the on-call docs assume will be filled but currently aren't. Don't be surprised when alerts route oddly:

- **PagerDuty is mocked.** `pulsetrack/dev/pagerduty` is a stub. The Phase-2 wiring is: real Events API v2 → on-call phone routing → escalation tree managed in PagerDuty's own UI.
- **Email digest is not wired.** SEV3 noise channel today is just Slack — daily email digest at 9 AM is a TODO.
- **`#pulsetrack-on-call`** channel exists; `#pulsetrack-alerts` exists; `#pulsetrack-noise` is a TODO (today, SEV3s go to `#pulsetrack-alerts` and pollute the SEV2 channel).
- **The on-call calendar is a Google Calendar**, not a PagerDuty schedule. When PD is unmocked, the schedule becomes authoritative.

---

## 9. References

- `observability/alerting.py` — Slack + PagerDuty routing
- `observability/monitors.py` — SLI checks that emit alerts
- `infrastructure/modules/monitoring/main.tf` — CloudWatch alarms
- `docs/slos.md` — burn-rate thresholds that drive paging
- `runbooks/` — symptom-keyed remediation procedures
- `postmortems/` — historical incidents (the institutional memory)
- Google SRE book ch. 14 (managing incidents) — original source of the war-room pattern
