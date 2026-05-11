# ADR-004: Glacierbase — Own-Rolled Iceberg Migration Framework

## Status
Accepted

## Date
2026-05-09

## Context

PulseTrack's lakehouse DDL (Iceberg table creation, column additions,
partition-spec evolution, snapshot-strategy changes) needs the same
discipline as application-database migrations:

- **Versioned**: every DDL change is a numbered, immutable file in source
  control.
- **Idempotent**: re-running against an up-to-date catalog is a no-op.
- **Concurrency-safe**: two engineers running migrations against the same
  catalog simultaneously cannot corrupt state.
- **Rollback-aware**: every forward migration ships with an explicit
  rollback script.
- **Auditable**: state-of-applied migrations is queryable.

The existing tools in this space target relational databases:

- **Flyway** / **Liquibase**: mature, but Iceberg + Glue catalog support is
  shaky. Both rely on JDBC; the Iceberg JDBC catalog isn't first-class in
  our Glue-first setup.
- **dbt migrations**: dbt is designed for transformation models, not DDL
  operations like partition-spec evolution or table relocation.
- **Apache Atlas**: governance/lineage tool, not a migration framework.
- **Schemachange (Snowflake)**: Snowflake-only; doesn't help with Iceberg.

WHOOP has publicly documented their own-rolled `Glacierbase` migration
framework for exactly this problem (see their engineering blog). We adopted
the same pattern.

## Decision

Build **Glacierbase**, an own-rolled migration framework with this shape:

- Versioned SQL files: `V001__create_iceberg_gold_tables.sql`,
  `V001__create_iceberg_gold_tables__down.sql`, and so on.
- Statements split by a state-aware scanner (handles `;` inside string
  literals and SQL comments).
- Variable interpolation from a per-catalog YAML (`{{ .variables.X.Y }}`)
  and from environment (`${VAR}`).
- DynamoDB conditional-write lock per catalog, with TTL-based stale-lock
  reaping (default 30 min, configurable).
- State table (per catalog) recording every applied migration with timing
  and outcome.
- CLI wrapper (`migrations/cli.py`) for `apply`, `rollback`, `status`,
  `dry-run`.

Migrations live at `/Users/nerdboss-stm/pulsetrack-cm/migrations/versions/`
and run via Spark's `spark.sql` (Iceberg DDL is auto-committed per
statement).

## Consequences

**Positive**:
- Iceberg-aware. The framework handles partition-spec evolution (e.g. V004
  reversed-id rewrite) and snapshot-strategy changes that Flyway has no
  vocabulary for.
- Transparent. Migrations are plain SQL files; operators read them directly
  and can replay any single statement manually if needed.
- DynamoDB conditional-write lock is robust against parallel CI runs and
  concurrent operator runs. Stale locks self-clear via TTL.
- State table is queryable like any Iceberg table — `SELECT * FROM ...
  migration_state` shows the full applied history.
- CI integration is straightforward: `migrations/cli.py status` on
  every PR, `apply` on merge.

**Negative**:
- We own the code. ~600 lines of Python (`runner.py`, `lock.py`, `state.py`,
  `validator.py`, `catalog_config.py`) we have to maintain.
- No GUI. Operators read the state table directly; no Liquibase-style
  status dashboard.
- No declarative diff. Flyway/Liquibase have generators that emit forward
  migrations from a target schema; we write each migration by hand.

These costs were judged acceptable because:
1. Iceberg DDL is genuinely different enough from RDBMS DDL that a generic
   tool would buy little.
2. The framework code is small enough to read in an afternoon.
3. The transparency wins outweigh the missing UX in operational
   troubleshooting.

## Alternatives Considered

- **Flyway** with the Iceberg JDBC catalog: rejected. JDBC catalog isn't
  first-class in our Glue-only setup, and Flyway's lock implementation is
  RDBMS-row-based — it wouldn't translate cleanly to Iceberg-on-Glue.
- **Liquibase**: same Iceberg-on-Glue support gap. Liquibase's XML/YAML
  changelog format also adds verbosity over plain SQL files.
- **dbt-based migrations** (using dbt run-operation or on-run-start hooks):
  rejected. dbt is for transformation, not DDL ops. Coercing it into a
  migration framework would muddle the two concerns.
- **Apache Atlas**: governance/lineage, not migration. Overkill and not the
  right tool.
- **No framework, just SQL scripts run manually**: rejected. We need
  concurrency control (lock), state tracking, and rollback as first-class
  concerns. Bare scripts always grow into a framework eventually — better
  to design it.

## References

- `/Users/nerdboss-stm/pulsetrack-cm/migrations/runner.py` — statement
  splitter and Spark dispatch.
- `/Users/nerdboss-stm/pulsetrack-cm/migrations/lock.py` — DynamoDB
  conditional-write lock with TTL reaping.
- `/Users/nerdboss-stm/pulsetrack-cm/migrations/state.py` — applied-migration
  ledger.
- `/Users/nerdboss-stm/pulsetrack-cm/migrations/versions/` — V001..V005
  forward and rollback scripts.
- `/Users/nerdboss-stm/pulsetrack-cm/infrastructure/modules/iam/main.tf` —
  `aws_dynamodb_table.glacierbase_lock` definition.
- Commit `0aaf0e1` — "feat: Iceberg on Glue + Glacierbase migration framework".
- Commit `1e03a98` — "feat: modernize stack to EMR 7.13 + Iceberg 1.10 +
  WHOOP-aligned Glacierbase".
- Commit `8028edb` — "fix(iceberg,migrations): close 3 gaps".
- Related: ADR-001 (Iceberg).
