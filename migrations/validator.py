"""
Migration validator — pure-stdlib parsing helpers + graph + conflict checks.

The validator exists in its own module so it can be exercised by lightweight
unit tests without bringing up Spark. It owns:

  * Discovering migration files in ``versions/``.
  * Computing SHA-256 of each forward file.
  * Parsing migration metadata headers:
      - ``-- MIGRATION_DESCRIPTION: ...`` (WHOOP Glacierbase format)
      - ``-- MIGRATION_AUTHOR: ...`` (WHOOP Glacierbase format)
      - ``-- depends_on: V001, V002`` (PulseTrack extension — Glacierbase
        doesn't have explicit dependency declarations in the public API,
        but ordering edge cases at scale make this useful)
  * Rendering Go-template-style ``{{ .variables.X.Y.Z }}`` references from a
    nested config (loaded from ``catalogs/<name>.yaml``).
  * Topologically sorting the migration set, respecting dependencies.
  * Detecting two unapplied migrations targeting the same table.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ── Constants ───────────────────────────────────────────────────────────────

# Strict pattern: V<NNN>__<name>.sql, optionally with __down before .sql.
_FILE_RE = re.compile(r"^V(\d{3,})__([A-Za-z0-9_]+?)(?P<down>__down)?\.sql$")

# Env-var references in YAML config values: ${VAR} or ${VAR:-default}.
_ENV_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}")

# Go-template-style variable references in SQL: {{ .variables.path.to.key }}.
# Matches WHOOP Glacierbase blog post:
#     bucket({{ .variables.catalog.namespace.tableName.bucketSize }}, id)
_TEMPLATE_RE = re.compile(r"\{\{\s*\.variables\.([A-Za-z_][A-Za-z0-9_.]*)\s*\}\}")

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

# Header directives — case-insensitive, only on lines that start with ``--``.
_DEPENDS_RE = re.compile(
    r"^\s*--\s*depends_on\s*:\s*([Vv]\d+(?:\s*,\s*[Vv]\d+)*)\s*$",
    re.MULTILINE,
)
_DESCRIPTION_RE = re.compile(
    r"^\s*--\s*MIGRATION_DESCRIPTION\s*:\s*(.+?)\s*$",
    re.MULTILINE | re.IGNORECASE,
)
_AUTHOR_RE = re.compile(
    r"^\s*--\s*MIGRATION_AUTHOR\s*:\s*(.+?)\s*$",
    re.MULTILINE | re.IGNORECASE,
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
    description: str = ""
    author: str = ""

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


def _parse_header(sql: str, regex: re.Pattern[str]) -> str:
    """Return the first match of a single-value header directive, or empty."""
    match = regex.search(sql)
    return match.group(1).strip() if match else ""


def _strip_sql_comments(sql: str) -> str:
    """Remove ``--`` line comments and ``/* */`` block comments before scanning."""
    no_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    no_line = re.sub(r"--[^\n]*", " ", no_block)
    return no_line


def _normalize_templates_for_target_scan(sql: str) -> str:
    """Collapse ``{{ .variables.X.Y.Z }}`` to identifier-safe placeholders.

    The table-op regex matches identifier characters; ``{{`` and whitespace
    inside ``{{ ... }}`` would otherwise split the identifier and the scan
    would mis-attribute the "target" to the literal ``{{``. Replacing each
    template reference with ``__TPL_X_Y_Z__`` keeps the identifier
    contiguous AND deterministic across migrations — two migrations
    referencing the same Go-template path collapse to the same placeholder,
    so conflict detection still works.
    """

    def repl(m: re.Match[str]) -> str:
        path = m.group(1).replace(".", "_")
        return f"__TPL_{path}__"

    return _TEMPLATE_RE.sub(repl, sql)


def _extract_targets(sql: str) -> set[str]:
    """Best-effort regex scan for tables touched by this migration.

    Strips the trailing ``;`` or ``(`` if matched, lowercases for comparison.
    Go-template placeholders are first normalized to ``__TPL_X_Y_Z__`` so
    they survive the identifier-character regex and stay equal across
    migrations referencing the same template path.
    """
    cleaned = _strip_sql_comments(sql)
    cleaned = _normalize_templates_for_target_scan(cleaned)
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
            description=_parse_header(raw, _DESCRIPTION_RE),
            author=_parse_header(raw, _AUTHOR_RE),
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


# ── Variable rendering (Go-template style, matching WHOOP Glacierbase) ──────


def render_env_in_str(value: str, env: dict[str, str] | None = None) -> str:
    """Replace ``${VAR}`` and ``${VAR:-default}`` tokens with env values.

    Used for YAML config values at load time so the same YAML works across
    dev/staging/prod by varying environment. ``${VAR:-default}`` falls back
    to the default if the variable is unset; bare ``${VAR}`` raises if unset.
    """
    source = env if env is not None else dict(os.environ)

    def resolve(match: re.Match[str]) -> str:
        var = match.group(1)
        default = match.group(2)
        if var in source:
            return source[var]
        if default is not None:
            return default
        raise KeyError(f"unset env var referenced in config: ${{{var}}}")

    return _ENV_RE.sub(resolve, value)


def render_env_in_obj(obj: Any, env: dict[str, str] | None = None) -> Any:
    """Recursively render ``${VAR}`` tokens in any string value of a nested dict/list."""
    if isinstance(obj, str):
        return render_env_in_str(obj, env)
    if isinstance(obj, dict):
        return {k: render_env_in_obj(v, env) for k, v in obj.items()}
    if isinstance(obj, list):
        return [render_env_in_obj(v, env) for v in obj]
    return obj


def render_template(sql: str, variables: dict[str, Any]) -> str:
    """Render Go-template-style ``{{ .variables.X.Y.Z }}`` references from
    a nested ``variables`` dict.

    Mirrors the WHOOP Glacierbase blog post syntax exactly — each token is
    a dot-separated path into the ``variables`` block of the catalog YAML.
    Unresolved references raise ``KeyError`` with the full path so the
    operator sees what's missing.
    """

    def resolve(match: re.Match[str]) -> str:
        path = match.group(1).split(".")
        node: Any = variables
        for key in path:
            if not isinstance(node, dict) or key not in node:
                raise KeyError(
                    f"unresolved template variable: {{{{ .variables.{'.'.join(path)} }}}}"
                )
            node = node[key]
        return str(node)

    return _TEMPLATE_RE.sub(resolve, sql)


def render_sql(
    sql: str,
    variables: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
) -> str:
    """Two-pass renderer: ``{{ .variables.X.Y }}`` first, then ``${VAR}``.

    Migration SQL primarily uses the templated form (matches Glacierbase);
    ``${VAR}`` is supported as a fallback for cases where a value isn't in
    the catalog YAML's ``variables`` block (e.g., a one-off env override).
    """
    rendered = sql
    if variables is not None:
        rendered = render_template(rendered, variables)
    # ``${VAR}`` second so an env var can override a template if both reference
    # the same key (rarely useful, but the order is the deterministic choice).
    if _ENV_RE.search(rendered):
        rendered = render_env_in_str(rendered, env)
    return rendered


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
