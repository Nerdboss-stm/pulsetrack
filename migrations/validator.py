"""
Migration validator — pure-stdlib parsing helpers + graph + conflict checks.

The validator exists in its own module so it can be exercised by lightweight
unit tests without bringing up Spark. It owns:

  * Discovering migration files in ``versions/``.
  * Computing SHA-256 of each forward file.
  * Substituting ``${ENV_VAR}`` placeholders.
  * Parsing ``-- depends_on: V001, V002`` directives.
  * Topologically sorting the migration set, respecting dependencies.
  * Detecting two unapplied migrations targeting the same table.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

# ── Constants ───────────────────────────────────────────────────────────────

# Strict pattern: V<NNN>__<name>.sql, optionally with __down before .sql.
_FILE_RE = re.compile(r"^V(\d{3,})__([A-Za-z0-9_]+?)(?P<down>__down)?\.sql$")

# Valid env-var references inside SQL: ${VAR}.
_ENV_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")

# Recognized table-touching DDL/DML keywords. Order matters for greedy regex
# alternation: longest first so e.g. "DELETE FROM" beats "DELETE".
_TABLE_OP_RE = re.compile(
    r"""(?ix)
    \b(
        CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)? |
        DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?         |
        ALTER\s+TABLE\s+                            |
        TRUNCATE\s+TABLE\s+                         |
        TRUNCATE\s+                                 |
        MERGE\s+INTO\s+                             |
        INSERT\s+INTO\s+                            |
        DELETE\s+FROM\s+                            |
        UPDATE\s+
    )
    ([A-Za-z_$\{\}][\w\.\$\{\}]*)
    """,
)

# ``-- depends_on: V001, V002`` — case-insensitive, anywhere in leading comments.
_DEPENDS_RE = re.compile(
    r"^\s*--\s*depends_on\s*:\s*([Vv]\d+(?:\s*,\s*[Vv]\d+)*)\s*$",
    re.MULTILINE,
)


# ── Data class ──────────────────────────────────────────────────────────────


@dataclass
class Migration:
    """A discovered migration on disk (forward script + optional rollback)."""

    version: str  # "V001"
    name: str  # "create_iceberg_gold_tables"
    path: Path
    down_path: Path | None
    sha256: str
    raw_sql: str
    depends_on: list[str] = field(default_factory=list)
    targets: set[str] = field(default_factory=set)

    @property
    def filename(self) -> str:
        return self.path.name

    @property
    def has_rollback(self) -> bool:
        return self.down_path is not None and self.down_path.exists()


# ── Discovery ───────────────────────────────────────────────────────────────


def _hash_text(text: str) -> str:
    """SHA-256 of the file's bytes (UTF-8). Used for tampering detection."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_depends_on(sql: str) -> list[str]:
    """Pull ``-- depends_on: V001, V002`` directives from comments."""
    deps: list[str] = []
    for match in _DEPENDS_RE.finditer(sql):
        for token in match.group(1).split(","):
            token = token.strip().upper()
            if token:
                deps.append(token)
    return deps


def _strip_sql_comments(sql: str) -> str:
    """Remove ``--`` line comments and ``/* */`` block comments before scanning."""
    no_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    no_line = re.sub(r"--[^\n]*", " ", no_block)
    return no_line


def _extract_targets(sql: str) -> set[str]:
    """Best-effort regex scan for tables touched by this migration.

    Strips the trailing ``;`` or ``(`` if matched, lowercases for comparison.
    Env-var placeholders stay in the identifier (they will be the same across
    migrations, so equality still works for conflict detection).
    """
    cleaned = _strip_sql_comments(sql)
    targets: set[str] = set()
    for match in _TABLE_OP_RE.finditer(cleaned):
        ident = match.group(2).rstrip(";,()").lower()
        if ident:
            targets.add(ident)
    return targets


def discover(versions_dir: Path) -> list[Migration]:
    """Walk ``versions_dir``, return every forward migration sorted by version."""
    if not versions_dir.exists():
        raise FileNotFoundError(f"versions directory not found: {versions_dir}")

    migrations: dict[str, Migration] = {}
    down_files: dict[str, Path] = {}

    for entry in sorted(versions_dir.iterdir()):
        if not entry.is_file() or not entry.name.endswith(".sql"):
            continue
        match = _FILE_RE.match(entry.name)
        if not match:
            raise ValueError(
                f"migration filename does not match V<NNN>__<name>.sql: {entry.name}"
            )
        version = f"V{int(match.group(1)):03d}"
        name = match.group(2)
        if match.group("down"):
            down_files[version] = entry
            continue
        if version in migrations:
            raise ValueError(f"duplicate migration version: {version}")
        raw = entry.read_text(encoding="utf-8")
        migrations[version] = Migration(
            version=version,
            name=name,
            path=entry,
            down_path=None,
            sha256=_hash_text(raw),
            raw_sql=raw,
            depends_on=_parse_depends_on(raw),
            targets=_extract_targets(raw),
        )

    for version, down_path in down_files.items():
        if version in migrations:
            migrations[version].down_path = down_path

    return sorted(migrations.values(), key=lambda m: m.version)


# ── Topological sort ────────────────────────────────────────────────────────


def topo_sort(migrations: list[Migration]) -> list[Migration]:
    """Stable topological sort. File order is the tiebreaker for independent nodes.

    Raises ``ValueError`` if a dependency is missing or a cycle exists.
    """
    by_version = {m.version: m for m in migrations}

    for m in migrations:
        for dep in m.depends_on:
            if dep not in by_version:
                raise ValueError(
                    f"{m.version} declares dependency on {dep} which does not exist"
                )

    # Kahn's algorithm with ordered ready set so file order is preserved.
    incoming: dict[str, set[str]] = {m.version: set(m.depends_on) for m in migrations}
    children: dict[str, list[str]] = {m.version: [] for m in migrations}
    for m in migrations:
        for dep in m.depends_on:
            children[dep].append(m.version)

    ordered: list[Migration] = []
    ready = sorted(v for v, deps in incoming.items() if not deps)
    while ready:
        v = ready.pop(0)
        ordered.append(by_version[v])
        for child in children[v]:
            incoming[child].discard(v)
            if not incoming[child]:
                # Insert in sorted position so file order is preserved among peers.
                ready.append(child)
                ready.sort()

    if len(ordered) != len(migrations):
        leftover = [v for v, deps in incoming.items() if deps]
        raise ValueError(f"circular dependency detected among migrations: {leftover}")

    return ordered


# ── Conflict detection ──────────────────────────────────────────────────────


def detect_conflicts(pending: list[Migration]) -> list[tuple[str, str, str]]:
    """Return ``(versionA, versionB, table)`` triples where two pending migrations
    touch the same table without an explicit dependency between them.

    Migrations that declare ``depends_on`` are saying "I intend to follow
    them in order" — that is **not** a conflict, even if they touch the
    same table. Conflicts are reserved for the *unordered* case where two
    independent edits would both write to one table and the runner has no
    way to know which should win.
    """
    by_version = {m.version: m for m in pending}

    def transitively_depends(child: str, parent: str, seen: set[str] | None = None) -> bool:
        if seen is None:
            seen = set()
        if child == parent:
            return True
        if child in seen:
            return False
        seen.add(child)
        node = by_version.get(child)
        if node is None:
            return False
        return any(transitively_depends(d, parent, seen) for d in node.depends_on)

    conflicts: list[tuple[str, str, str]] = []
    for i, a in enumerate(pending):
        for b in pending[i + 1 :]:
            shared = a.targets & b.targets
            if not shared:
                continue
            # Either direction of declared dependency resolves the ordering.
            if transitively_depends(b.version, a.version) or transitively_depends(
                a.version, b.version
            ):
                continue
            for table in sorted(shared):
                conflicts.append((a.version, b.version, table))
    return conflicts


# ── Env-var substitution ────────────────────────────────────────────────────


def render_sql(sql: str, env: dict[str, str] | None = None) -> str:
    """Replace ``${VAR}`` with the value from env (or ``os.environ`` if None).

    Raises ``KeyError`` listing every unset variable referenced in the file.
    """
    source = env if env is not None else dict(os.environ)
    referenced = {m.group(1) for m in _ENV_RE.finditer(sql)}
    missing = sorted(v for v in referenced if v not in source)
    if missing:
        raise KeyError(f"missing env vars referenced by SQL: {', '.join(missing)}")
    return _ENV_RE.sub(lambda m: source[m.group(1)], sql)


# ── Hash check ──────────────────────────────────────────────────────────────


def verify_hash(migration: Migration, applied_hash: str) -> None:
    """Raise ``RuntimeError`` if a previously applied migration has been edited."""
    if migration.sha256 != applied_hash:
        raise RuntimeError(
            f"migration tampering detected for {migration.version} "
            f"({migration.filename}): on-disk sha256 {migration.sha256} "
            f"does not match applied sha256 {applied_hash}. "
            "Migrations are immutable once applied — revert the edit or "
            "create a follow-up migration."
        )
