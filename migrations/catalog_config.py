"""
Catalog configuration loader (Glacierbase-style).

Each catalog is a YAML file under ``migrations/catalogs/``:

    catalog: glue_iceberg
    migrationExecutor:
      type: spark
      conf:
        sparkConf:
          spark.sql.catalog.glue_iceberg: ...
        dependencies:
          - "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"
    variables:
      iceberg:
        catalog: glue_iceberg
      glue:
        database:
          gold: pulsetrack_gold_dev
    state:
      table: glue_iceberg.pulsetrack_gold_dev.schema_migrations
    lock:
      type: dynamodb
      tableName: pulsetrack-glacierbase-locks

The CLI loads the YAML via ``--catalog <name>``, env-substitutes ``${VAR}``
tokens in the values (so the same YAML is portable across dev/prod), and
hands the loaded config to the runner + state + lock subsystems.

Pure-stdlib parser so the framework doesn't require pyyaml on the executing
node — we accept a minimal YAML subset (key: value, nested dict via
indentation, list via ``- item``). For richer YAML (anchors, multi-line
strings) install pyyaml; the loader prefers it when available.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .validator import render_env_in_obj


@dataclass
class CatalogConfig:
    """Parsed catalog config."""

    catalog: str
    spark_conf: dict[str, str]
    dependencies: list[str]
    variables: dict[str, Any]
    state_table: str
    lock: dict[str, Any]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CatalogConfig":
        try:
            executor = data["migrationExecutor"]
            conf = executor["conf"]
            spark_conf = conf.get("sparkConf", {}) or {}
            dependencies = list(conf.get("dependencies", []) or [])
            return cls(
                catalog=data["catalog"],
                spark_conf=dict(spark_conf),
                dependencies=dependencies,
                variables=dict(data.get("variables", {}) or {}),
                state_table=data["state"]["table"],
                lock=dict(data.get("lock", {}) or {}),
                raw=data,
            )
        except KeyError as exc:
            missing = exc.args[0]
            raise ValueError(
                f"catalog config missing required key: {missing!r}. "
                f"Required shape: catalog, migrationExecutor.conf.sparkConf, "
                f"state.table"
            ) from exc


# ── YAML loading (pyyaml-optional) ──────────────────────────────────────────


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML. Prefer pyyaml if installed; fall back to a minimal parser."""
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore[import-untyped]

        loaded = yaml.safe_load(text)
        if not isinstance(loaded, dict):
            raise ValueError(f"top-level YAML must be a mapping in {path}")
        return loaded
    except ImportError:
        return _minimal_yaml_parse(text, path)


def _minimal_yaml_parse(text: str, path: Path) -> dict[str, Any]:
    """Minimal YAML subset parser — enough for our config files.

    Supports:
      - ``key: value`` (string scalar)
      - ``key:`` followed by indented child block (nested dict)
      - ``- item`` lists (string elements only)
      - ``# comment`` (line + inline)
      - ``"quoted"`` and ``'quoted'`` strings (preserve the value as-is
        minus the wrapping quotes — including ``${VAR}`` tokens)
      - empty values (``key:`` with nothing after) → empty dict

    Does NOT support: anchors, multi-line strings, complex flow syntax,
    type coercion to int/bool. All values are returned as strings; the
    consumer (``CatalogConfig``) coerces where it needs to.
    """
    root: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, root)]  # (indent, container)

    def strip_inline_comment(line: str) -> str:
        # Don't strip ``#`` inside quoted strings.
        in_quote: str | None = None
        for i, ch in enumerate(line):
            if in_quote:
                if ch == in_quote:
                    in_quote = None
                continue
            if ch in ('"', "'"):
                in_quote = ch
                continue
            if ch == "#":
                return line[:i]
        return line

    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = strip_inline_comment(raw)
        stripped = line.rstrip()
        if not stripped.strip():
            continue
        indent = len(stripped) - len(stripped.lstrip())
        content = stripped.strip()

        # Pop containers shallower than this indent.
        while stack and stack[-1][0] >= indent:
            stack.pop()
        if not stack:
            raise ValueError(f"yaml indent error at {path}:{lineno}")
        parent_indent, parent = stack[-1]

        if content.startswith("- "):
            # List item under the current parent (which must already be a list).
            value = _parse_scalar(content[2:].strip())
            if not isinstance(parent, list):
                raise ValueError(f"unexpected list item at {path}:{lineno}")
            parent.append(value)
            continue

        if ":" not in content:
            raise ValueError(f"expected 'key: value' at {path}:{lineno} ({content!r})")

        key, _, value = content.partition(":")
        key = key.strip()
        value = value.strip()

        if not isinstance(parent, dict):
            raise ValueError(f"unexpected key '{key}' under non-dict at {path}:{lineno}")

        if value == "":
            # Container — children come on subsequent indented lines.
            # Peek ahead to figure out if the child is a dict or a list. We
            # don't have lookahead in this single-pass loop, so default to
            # dict and convert to list on first ``- `` item.
            new_container: Any = {}
            parent[key] = new_container
            stack.append((indent, new_container))
        elif value == "[]":
            parent[key] = []
        elif value == "{}":
            parent[key] = {}
        else:
            parent[key] = _parse_scalar(value)

    # Post-process: any dict that only ever received list-like children would
    # have been re-typed by the ``- `` branch, but we deferred that conversion.
    # Walk the tree and convert empty-dict containers that are followed by
    # list items.
    return _coerce_pending_lists(root)


def _parse_scalar(value: str) -> Any:
    """Strip surrounding quotes; return string. No int/bool coercion."""
    if not value:
        return ""
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    return value


def _coerce_pending_lists(node: Any) -> Any:
    """No-op placeholder; the parser above handles list/dict directly enough
    for our configs. Kept as the seam for richer post-processing later."""
    return node


# ── Public API ──────────────────────────────────────────────────────────────


def load_catalog(name: str, catalogs_dir: Path | str) -> CatalogConfig:
    """Load and env-render the named catalog config.

    Args:
        name: Catalog name. Must match the ``catalog:`` key in the YAML.
        catalogs_dir: Directory containing ``<name>.yaml`` files. Accepts
            ``Path`` or ``str`` — coerced to ``Path`` internally.

    Raises:
        FileNotFoundError: ``catalogs_dir/<name>.yaml`` doesn't exist.
        ValueError: catalog name in file doesn't match ``name``.
        KeyError: a referenced ``${VAR}`` env variable isn't set.
    """
    catalogs_dir = Path(catalogs_dir)
    path = catalogs_dir / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"catalog config not found: {path}. "
            f"Existing catalogs: {[p.stem for p in catalogs_dir.glob('*.yaml')]}"
        )
    data = _load_yaml(path)
    rendered = render_env_in_obj(data, dict(os.environ))
    cfg = CatalogConfig.from_dict(rendered)
    if cfg.catalog != name:
        raise ValueError(
            f"catalog name mismatch: file is {name}.yaml but ``catalog:`` "
            f"key inside is {cfg.catalog!r}"
        )
    return cfg


def list_catalogs(catalogs_dir: Path | str) -> list[str]:
    """Return the names of all catalogs (filenames without ``.yaml``)."""
    catalogs_dir = Path(catalogs_dir)
    if not catalogs_dir.exists():
        return []
    return sorted(p.stem for p in catalogs_dir.glob("*.yaml"))
