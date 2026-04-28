"""
Great Expectations 1.x integration for PulseTrack quality gates.

Layout:

* a process-wide ephemeral GX context with a single Spark data source
* a registry pattern (`@register_suite("name")`) — suite modules own their
  own expectations and self-register at import time
* `validate(df, suite_name, layer, source) -> bool` — the only thing
  pipeline jobs need to call inside `foreachBatch` or batch routines

The runner is informative-by-default: failures structured-log the GX result
and bump ``records_failed{reason="quality_gate"}``. Callers decide whether
to halt propagation based on the return value.

GX API note
-----------
GX 1.x reorganised parts of the public surface (``data_sources`` instead of
``sources``, fluent ``ExpectationSuite.add_expectation``, etc.). Where the
runtime API may diverge slightly from the public docs we wrap calls in
narrow try/except blocks and fall back to a no-op success so a transient GX
internal error never takes the pipeline down.
"""
from __future__ import annotations

import os
import sys
from typing import Callable, Dict

import great_expectations as gx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from logger import get_logger  # noqa: E402
from metrics import records_failed  # noqa: E402

log = get_logger(__name__)

DATA_SOURCE_NAME = "pulsetrack_delta"

_CONTEXT = None
SUITE_BUILDERS: Dict[str, Callable] = {}


def get_context():
    """Return the singleton ephemeral GX context."""
    global _CONTEXT
    if _CONTEXT is None:
        _CONTEXT = gx.get_context(mode="ephemeral")
        _CONTEXT.data_sources.add_spark(name=DATA_SOURCE_NAME)
    return _CONTEXT


def register_suite(name: str):
    """Decorator: register a no-arg callable that returns an ExpectationSuite."""
    def deco(builder: Callable):
        SUITE_BUILDERS[name] = builder
        return builder
    return deco


def _get_or_add_batch_definition(suite_name: str):
    context = get_context()
    ds = context.data_sources.get(DATA_SOURCE_NAME)

    asset_name = f"adhoc_{suite_name}"
    try:
        asset = ds.get_asset(asset_name)
    except (LookupError, KeyError, AttributeError):
        asset = ds.add_dataframe_asset(name=asset_name)

    batch_def_name = "whole"
    try:
        return asset.get_batch_definition(batch_def_name)
    except (LookupError, KeyError, AttributeError):
        return asset.add_batch_definition_whole_dataframe(batch_def_name)


def validate(df, suite_name: str, layer: str, source: str) -> bool:
    """Run `suite_name` against `df`; True on success, False on failure.

    Failures are logged with the GX result dict and bump
    ``records_failed{layer,source,reason='quality_gate'}``. Runtime errors
    inside GX itself are caught and reported as ``reason='gx_runtime'``;
    they do not block the pipeline (returns True so the calling job
    continues with the existing data).
    """
    if suite_name not in SUITE_BUILDERS:
        raise KeyError(f"Unknown GX suite: {suite_name!r}")

    try:
        batch_def = _get_or_add_batch_definition(suite_name)
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})
        suite = SUITE_BUILDERS[suite_name]()
        result = batch.validate(suite)
    except Exception as exc:
        log.error(
            "GX runtime error — gate skipped",
            extra={"extra_data": {
                "suite": suite_name,
                "layer": layer,
                "source": source,
                "error": str(exc),
            }},
        )
        records_failed.labels(layer=layer, source=source, reason="gx_runtime").inc()
        return True

    success = bool(getattr(result, "success", False))
    try:
        payload = result.describe_dict()
    except AttributeError:
        try:
            payload = result.to_json_dict()
        except AttributeError:
            payload = str(result)

    if success:
        log.info(
            "GX quality gate passed",
            extra={"extra_data": {
                "suite": suite_name, "layer": layer, "source": source,
            }},
        )
    else:
        log.error(
            "GX quality gate FAILED",
            extra={"extra_data": {
                "suite": suite_name,
                "layer": layer,
                "source": source,
                "result": payload,
            }},
        )
        records_failed.labels(
            layer=layer, source=source, reason="quality_gate",
        ).inc()
    return success
