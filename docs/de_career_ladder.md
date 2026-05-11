# Data Engineering Career Ladder — PulseTrack

A career ladder is not a checklist. It is a description of *scope*, *judgment*,
and *influence* at each level — calibrated against an actual lakehouse codebase
so the abstractions stay honest.

This document walks the six-level ladder used by WHOOP-tier shops (Carta DE
ladder, Meta E3–E7, Stripe IC1–IC7) and maps each level to artifacts in this
repository. Use it for self-assessment, hiring rubrics, and growth
conversations.

**A note on tone:** every level has its own kind of mastery. A great DE I
hand-codes a beautiful SCD2 dimension; a great Staff doesn't write any of it
because they decided which dimension to model and which team should own it.
Neither is harder; they are different jobs.

---

## Level 0 — Intern (3-month rotation, mentored)

**Equivalent:** E1 (Meta), IC0 (Stripe), L1 (Carta).

**Typical experience:** 0–1 years. CS or DS undergrad mid-program, bootcamp
graduate with one capstone, or career-switcher fresh from a course.

**What they own:**
- A single, scoped, two-week task with daily check-ins
- Documentation cleanup, code-comment improvement, README maintenance
- Adding one column or one test to an existing pipeline
- Reproducing a bug locally before a senior triages it

**Expected artifacts from PulseTrack:**
- A pre-commit run that passes (`black`, `ruff`, `gitleaks`)
- A green CI run on a trivial PR (e.g., add a column to `dim_metric.py` and
  the test in `tests/test_dim_metric.py`)
- A walkthrough doc proving they read one pipeline end-to-end —
  this can literally be `docs/onboarding_new_de.md` annotated with their
  notes

**Skills demonstrated:**
- Comfort with Git: clone, branch, commit, push, PR
- Reads Python without panic
- Can run `make test` and interpret pytest output
- Can read Spark UI well enough to find the slow stage
- Asks clarifying questions before writing code

**Sample interview questions:**
1. "Walk me through the difference between a `git pull` and a `git fetch`."
2. "I have a list of users with duplicate emails. Write SQL that returns
   one row per email."
3. "What does this Python code do?" — show 20 lines of dataclass + decorator
4. "If you had to verify our pipeline didn't drop any records, what's the
   first thing you'd check?"

**Sample PulseTrack task:**
> Add a `last_seen_firmware_version` column to `transformations/silver_to_gold/dim_device.py`,
> wire it through the SCD2 merge logic, and add a unit test that proves the
> column populates for an unchanged device record.

**How to be a great intern:** ask "what does success look like" before writing
code. Pair on your first PR. Read more than you write in week 1.

**Promotion to DE I:** mentor signs off that the intern can take a one-week
task with a written spec and ship it without daily check-ins.

---

## Level 1 — DE I (Junior, "ramp-up")

**Equivalent:** E3 (Meta), IC1–IC2 (Stripe), L2 (Carta).

**Typical experience:** 0–2 years post-graduation. Often a former intern.
Comfortable with the language and tools but new to production systems.

**What they own:**
- One or two pipelines end-to-end, but only after a senior has scoped the
  design
- All bug-fixes within their owned pipelines
- The test coverage for their owned pipelines
- Reading runbooks during on-call shadow shifts (not yet on-call alone)
- The "explain what you did this week" five-minute slot in standup

**Expected artifacts from PulseTrack:**
- A complete junior tutorial walkthrough (see `docs/first_pipeline_tutorial.md`)
- One full SCD2 or fact transform with tests, e.g. the kind of work in
  `transformations/silver_to_gold/dim_medication.py` (commit `9691570`)
- A non-trivial PR with green CI and at least one reviewer's nit addressed
- Comfort reading the bronze→silver pipeline files and saying which is which
  without looking at the filename

**Skills demonstrated:**
- Can write a Spark transform from a spec
- Can write pytest tests with fixtures (sees `tests/conftest.py` as a useful
  pattern, not a black box)
- Reads SQL, including window functions and CTEs
- Understands what `MERGE INTO` does and when to use it
- Knows what a watermark is and can describe `dropDuplicatesWithinWatermark`
- Can write a `prepare_for_validation()` function for a new GX suite

**Sample interview questions:**
1. "What's the difference between SCD1 and SCD2? When would you pick which?"
   Expected: SCD1 overwrites, SCD2 keeps history with effective_from /
   effective_to.
2. "Show me how you'd write a test for a Spark transform without spinning up
   a real cluster." Expected: pytest fixture with local SparkSession,
   `tmp_lakehouse` pattern.
3. "Walk me through what `foreachBatch` does."
4. "I write a Kafka producer. The downstream consumer says some records are
   missing. Where do you start?"
5. "What's a watermark in Spark Structured Streaming? Why do we need one?"

**Sample PulseTrack task:**
> The `dim_medication` dimension is missing a `therapeutic_class` field that
> our analytics team wants. The data exists in the FDA spl bundle in the
> bronze layer. Wire it through: bronze→silver (extract), silver→gold
> (denormalize into dim_medication), update the GX suite, add tests.

**How to be a great DE I:** read the existing code before asking
questions. When stuck for >30 min, write down what you tried and ask.
Don't be afraid to ask "why is this code structured this way?" — the
answer is often "history" and the senior will appreciate the question.

**Promotion to DE II:** the DE can scope and ship a one-month project
without a senior writing the design doc. They've reviewed at least one
junior's PR substantively. They've handled at least one production
incident under guidance.

---

## Level 2 — DE II (Mid, "core IC")

**Equivalent:** E4 (Meta), IC3 (Stripe), L3 (Carta).

**Typical experience:** 2–5 years. The level where most ICs sit for years
and where most of the actual engineering work happens.

**What they own:**
- A bounded context: a layer of the lakehouse (e.g., all of silver), or a
  full vertical (e.g., the wearable path bronze→silver→gold)
- The runbooks for their bounded context
- The on-call rotation alone (not shadowed) for their context
- The design docs for their next quarter of work
- Mentoring a DE I directly: one weekly 1:1, code reviews, pairing on the
  hard stuff
- Knowing when to push back on PM/data-analyst requests on technical grounds

**Expected artifacts from PulseTrack:**
- A multi-commit feature like the WHOOP API connector (commit `c1d8376`):
  OAuth flow, REST client, Avro transformer, Kafka producer wiring
- A real architectural decision documented and shipped, e.g. the silver gate
  refactor that dropped the absolute `event_timestamp` window (commit
  `78ddfea`) — sized to one engineer, one week
- An incident response done well: read the page, diagnose, fix, document
  postmortem (e.g., the silver cold-start postmortem referenced in
  `pulsetrack-study/PROMPT_4_REPORT.md` § 3.12)
- The Glacierbase migration framework's first iteration (commits `0aaf0e1`,
  `8028edb`) — pattern-recognition, framework thinking, willingness to read
  someone else's blog post and reimplement

**Skills demonstrated:**
- Owns the Spark + Iceberg / Delta / Kafka stack end-to-end
- Writes design docs that other engineers can implement
- Reviews PRs with substantive feedback, not just nits
- Knows when to ship a hack and when to invest in cleanup
- Has opinions on data modeling that are right *and* defensible
- Can debug a Spark Streaming hang without help

**Sample interview questions:**
1. "Tell me about a time you chose between two implementations and what
   tipped the decision." Expected: concrete tradeoff named, decision documented.
2. "Walk me through how you'd handle late-arriving data in a daily-summary
   fact table." Expected: watermark + window + grain-key recompute on
   touched slices (mirrors the pattern in `fact_vital_daily_summary.py`).
3. "Your silver streaming query is falling behind bronze. The bronze rate
   is steady. Where do you look?" Expected: see `runbooks/kafka_consumer_lag.md`
   — check Prometheus consumer_lag, check the foreachBatch duration,
   check S3 5xx, check executor count.
4. "What is `streaming-skip-overwrite-snapshots` and why does it exist?"
5. "When would you NOT use Iceberg time-travel and prefer a separate
   audit table?"

**Sample PulseTrack task:**
> Migrate the orchestration layer from Makefile-driven to Prefect Cloud
> (commit `02e5a2b`). Design the flow boundaries, decide which tasks
> are idempotent, write 7 deployments with schedules, ensure all
> dbt invocations resolve project_dir correctly.

**How to be a great DE II:** be the person who can both write the code AND
explain the design choice to a non-engineer. Take ownership for the
incidents in your bounded context — even when it wasn't your code that
broke. Pair with juniors regularly.

**Promotion to DE III:** the DE has consistently shipped projects that
required cross-team coordination. They've made at least one architectural
decision the team accepted and that survived contact with production
traffic. They've trained at least one junior to a stronger level.

---

## Level 3 — DE III (Senior, "the rock")

**Equivalent:** E5 (Meta), IC4 (Stripe), L4 (Carta).

**Typical experience:** 5–8 years. The level where you're trusted with the
hardest problems and where most senior engineers will spend the rest of
their careers (it's a terminal level for many).

**What they own:**
- A full vertical AND the architectural decisions inside it
- The choice of frameworks for their team (Spark vs. Flink, Iceberg vs.
  Delta, dbt vs. Spark-native gold)
- The on-call rotation as the senior responder (page-escalation target)
- Cross-team dependencies and contracts (e.g., "what does ML expect from
  fact_vital_reading")
- Hiring loops: drives them, gives the hire/no-hire call on candidates at
  DE I / DE II level
- The principal+ engineers' ear for technical debate

**Expected artifacts from PulseTrack:**
- The reversed-ID S3 partitioning refactor (commit `901bf01`) — read the
  WHOOP blog post, designed the abstraction (`lakehouse/partition_strategy.py`),
  ran the actual benchmark on real S3, decided between three strategies, picked
  reversed-ID over hash-bucket on operational grounds
- The Iceberg + Glacierbase rollout (commits `0aaf0e1`, `1e03a98`,
  `8028edb`) — recognized the schema-management problem as a class, picked
  WHOOP's pattern as the target architecture, implemented the lock /
  ledger / topo-sort, wrote the runbook
- A complete operational story like the EMR teardown design (`teardown-compute.sh`,
  commit `a286cda`) where the engineer recognized that EMR + MSK are 99% of
  the cost and that S3 storage persisting across compute lifecycle is
  worth the operational complexity of conditional teardown
- The streaming observability stack (commit `6d896ec`): wired
  `StreamingQueryListener` to Prometheus, defined the metrics that the
  on-call team would actually look at

**Skills demonstrated:**
- Can hold an architectural debate with another senior and either win it
  or change their own mind (both are wins)
- Recognizes patterns across systems — "this is a thundering-herd problem,
  not a Spark problem"
- Picks abstractions that don't leak (the `FormatWriter` class in
  `lakehouse/format_writer.py` is the right level of indirection between
  the streaming code and Iceberg / Delta)
- Comfortable presenting a design to skip-level leadership without help
- Comfortable saying "no" to a roadmap item on technical grounds

**Sample interview questions:**
1. "Tell me about an architectural decision you made that turned out to
   be wrong. What did you do?"
2. "Walk me through how you'd design a streaming pipeline for 100K events/s
   on a budget of $5K/month." Expected: MSK Serverless sizing, Spark
   executor sizing, S3 prefix strategy, observability hooks, cost math.
3. "Why Iceberg over Delta for our gold layer?" Expected: catalog
   independence (Glue native), partition evolution in-place, broader
   ecosystem (Snowflake reads Iceberg native), AND the honest answer
   that Delta is the better Databricks-native choice — the decision is
   about platform alignment, not technical superiority.
4. "Walk me through a postmortem you wrote. Why was that root cause
   the right thing to write down?"
5. "How would you decide whether to dbt-ify the silver layer or keep it
   in Spark?"

**Sample PulseTrack task:**
> Design the migration from the local Docker Compose stack to AWS EMR +
> MSK Serverless + S3 + Glue Catalog. Decide module boundaries for
> Terraform. Decide what stays local-dev vs. cloud-only. Write the
> bootstrap script that emits .env.cloud from terraform outputs. (Commit
> `3448d12` and the chain of preceding Terraform commits.)

**How to be a great DE III:** make the team better than you. Your code is
not the deliverable — the team's capability is. Spend time on hiring,
mentoring, design reviews, and architectural conversations. When you do
write code, make it the kind of code juniors will look at and learn from.

**Promotion to Staff:** the DE has demonstrated influence beyond their
direct team. They've made decisions that other teams adopt. They've
mentored at least one DE II to senior. They're the person other senior
engineers ask before they make their own decisions.

---

## Level 4 — Staff DE ("influence beyond team")

**Equivalent:** E6 (Meta), IC5 (Stripe), L5 (Carta).

**Typical experience:** 8–12 years. Rare and hard-won. Often the
"technical leader" of a 10–30 person org without managing anyone directly.

**What they own:**
- Cross-team technical strategy: e.g., "all of WHOOP's data platform will
  standardize on Iceberg by Q3"
- The hiring bar for senior engineers across the org
- The technical roadmap: 12-month outlook, with explicit dependencies and
  tradeoffs
- The "weird" problems no one else can solve: the production incident that
  no team owns, the cross-cutting refactor, the cost-overrun investigation
- Skip-level visibility: VPs and Directors trust their judgment

**Expected artifacts from PulseTrack-scale:**
- A platform-level architecture doc — the equivalent of
  `pulsetrack-study/INTERVIEW_PREP.md` § 1 and § 2 written as a forward-
  looking strategy, not a retrospective
- The decision to migrate from Makefile orchestration to Prefect Cloud
  (commit `02e5a2b`) framed as a multi-team strategy: "Prefect is what
  WHOOP migrated to; we'll align so engineers moving between teams
  recognize the patterns"
- The framework decisions: Glacierbase pattern adoption (commit `1e03a98`),
  the choice to dual-write Iceberg + Delta, the AI-assisted engineering
  rollout (commit `c5ea199`)
- A real cost-management story: budget alerts at 80%, m5.xlarge spot
  pricing, EMR 2h auto-terminate, teardown-compute.sh — recognizing
  that operational discipline IS the engineering work for cloud-native
  systems

**Skills demonstrated:**
- Can explain why a technical decision is the *right* decision for the
  *business*, not just the engineering team
- Influences without authority — gets two senior engineers to agree on
  an approach through reasoning, not seniority
- Writes documents that change behavior. The Architecture doc in
  `Architecture.md` is read by every new hire and shapes how they think
- Recognizes when a problem is organizational rather than technical
  (e.g., "we have three streaming pipelines because three teams own them;
  the fix is one team, not one framework")
- Comfortable saying "this isn't the right problem to solve" to a senior PM

**Sample interview questions:**
1. "Tell me about a time you changed someone's mind on a technical
   decision. How?"
2. "How do you decide what NOT to work on?"
3. "Walk me through how you'd handle this: an SVP wants us to migrate from
   AWS to GCP within 6 months. What's your process?"
4. "Describe a time you saw a technical decision that was wrong and waited
   to bring it up. Why did you wait? What was the right moment?"
5. "How do you measure whether you're succeeding as a Staff engineer?"

**Sample PulseTrack-equivalent task:**
> WHOOP wants a unified streaming + batch platform. There are 4 teams
> currently running 4 different stacks (Spark on EMR, Beam on Dataflow,
> Flink on K8s, dbt + Airflow). You have 12 months and a $2M budget.
> Write the strategy doc. Decide which patterns survive, which die. Get
> all 4 teams aligned without firing anyone.

**How to be a great Staff:** read more than you write. Listen more than
you speak. When you do speak, be the highest-signal voice in the room.
Have one or two strong technical opinions a year that you ship, not ten
opinions a quarter that nobody implements.

**Staff isn't "more code." It's influence across teams.**

**Promotion to Principal:** the Staff has set technical direction for the
entire data org for at least one major initiative, and that initiative
has shipped and worked.

---

## Level 5 — Principal DE ("influence beyond org")

**Equivalent:** E7 (Meta), IC6 (Stripe), L6 (Carta).

**Typical experience:** 12+ years. Extremely rare. At many companies there
are 1–3 of these total in the entire data org.

**What they own:**
- Industry-level technical strategy: "where is the data engineering field
  going, and how do we position ourselves"
- The conference talks, the blog posts, the open-source contributions
- The decisions that span multiple orgs: ML platform + data platform +
  infra platform alignment
- The 3–5 year technical roadmap

**Expected artifacts:**
- A published blog post that other companies copy. WHOOP's S3 partitioning
  post and Glacierbase post are real examples of Principal-level output
- An OSS contribution that lands in mainline Iceberg / Spark / dbt
- A talk at Strata, Data+AI Summit, or QCon that other engineers cite
- Recruiting pull: candidates apply because they want to work with this person

**Skills demonstrated:**
- Recognized by name in the industry
- Comfortable being wrong in public (writes about failures as well as
  successes)
- Mentors Staff engineers across other teams, including externally

**At PulseTrack-scale, this level doesn't quite fit — the project is one
person's portfolio. But the work informed by Principal-level public output
(WHOOP's published blogs on reversed-ID partitioning and Glacierbase) is
what makes this project possible. Reading Principal-level writing and
reimplementing it is one of the fastest ways to ramp from Senior to Staff.**

---

## Growth path — what's required to move from N to N+1

| Transition | Skills | Scope | Behavioral |
|------------|--------|-------|------------|
| Intern → DE I | Can run the stack locally; ships a 1-week task with a written spec | One column / one test / one bug fix | Asks "what does success look like" before writing code |
| DE I → DE II | Can scope a 1-month project; reviews PRs substantively | One pipeline end-to-end | Owns incidents in their pipeline. Mentors a fellow junior informally |
| DE II → DE III | Designs systems; influences team conventions | A vertical (e.g., all of silver) | Hiring loops; cross-team interfaces; says no when warranted |
| DE III → Staff | Influences across teams; writes the strategy docs | Org-level architecture | Mentors seniors; technical leader without title |
| Staff → Principal | Influences industry; OSS / talks / blogs | Multi-org strategy | Public technical voice; field-shaping work |

---

## How to read this document

If you're an **intern**, focus on Level 0–1. Don't pre-optimize for Level 3
moves; you'll burn out. Be a great intern, then a great junior.

If you're a **DE II considering DE III**: look at the artifacts column. Have
you owned a vertical end-to-end? Have you designed a system other engineers
implement? Have you been the senior responder on an incident? The promotion
is about scope, not seniority.

If you're a **DE III considering Staff**: the question is not "am I a better
engineer." The question is "have I made other engineers better." Staff is
not the next rung; it's a different job.

If you're a **Staff considering Principal**: ship the blog post. Give the
talk. Contribute to OSS. Influence doesn't compound inside one company
forever; eventually you need to influence the field.

---

## References

- Carta engineering ladder: https://carta.com/engineering-ladder/
- Stripe's IC track: described in their public engineering blog
- Meta's E3–E7 framework: publicly discussed in
  https://www.levels.fyi/blog/meta-engineer-levels.html
- Will Larson's "Staff Engineer: Leadership Beyond the Management Track"
- Tanya Reilly's "The Staff Engineer's Path"

PulseTrack itself is a single-person portfolio project, so the artifact
column maps to commits and files in this repo. At a real company those
artifacts would map to JIRA epics, design docs, and team-owned services.

---

*Last updated: 2026-05-10. Maintained alongside `docs/onboarding_new_de.md`
and `docs/war_stories.md`.*
