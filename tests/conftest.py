"""
Shared pytest fixtures for PulseTrack unit tests.

Usage:
    @pytest.fixture(scope="session") def spark   — local Delta-enabled Spark
    @pytest.fixture                  def tmp_lakehouse — isolated lakehouse root

Spark-heavy tests should declare ``spark`` as a parameter; the session
fixture is shared so we only pay the JVM startup cost once per test run.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _have_spark() -> bool:
    try:
        import pyspark  # noqa: F401
        from delta import configure_spark_with_delta_pip  # noqa: F401

        return True
    except Exception:
        return False


HAVE_SPARK = _have_spark()


@pytest.fixture(scope="session")
def spark():
    """Local Delta-enabled SparkSession for unit tests."""
    if not HAVE_SPARK:
        pytest.skip("pyspark/delta not available")
    from pyspark.sql import SparkSession

    # Use cached Ivy jars when available (offline-friendly). configure_spark_
    # with_delta_pip would re-resolve via Maven, which fails in CI sandboxes
    # without network access to Maven Central.
    ivy = os.path.expanduser("~/.ivy2/jars")
    delta_jars = [
        os.path.join(ivy, "io.delta_delta-spark_2.12-3.0.0.jar"),
        os.path.join(ivy, "io.delta_delta-storage-3.0.0.jar"),
    ]
    have_cached_jars = all(os.path.exists(j) for j in delta_jars)
    if not have_cached_jars:
        pytest.skip("Delta JARs not cached at ~/.ivy2/jars")

    builder = (
        SparkSession.builder.appName("PulseTrack-tests")
        .master("local[2]")
        .config("spark.jars", ",".join(delta_jars))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


_PROJECT_PREFIXES = (
    "config",
    "logger",
    "metrics",
    "data_quality",
    "streaming",
    "transformations",
    "data_generators",
    "schemas",
    "utils",
)


def _clear_prometheus_registry() -> None:
    """Module-level Prometheus collectors get re-instantiated when we reload
    project modules. The default registry rejects duplicates with ValueError,
    so we must unregister everything before the re-import."""
    try:
        from prometheus_client import REGISTRY
    except Exception:
        return
    for collector in list(REGISTRY._collector_to_names.keys()):
        try:
            REGISTRY.unregister(collector)
        except Exception:
            pass


def _nuke_project_modules() -> None:
    """Drop project modules from sys.modules so the next import picks up
    a fresh ``settings`` instance — works around `from config import settings`
    binding the pre-reload object."""
    for m in list(sys.modules):
        if any(m == p or m.startswith(p + ".") for p in _PROJECT_PREFIXES):
            sys.modules.pop(m, None)
    _clear_prometheus_registry()


@pytest.fixture
def tmp_lakehouse(tmp_path, monkeypatch):
    """Point the global ``settings`` at an isolated temp lakehouse for one test."""
    monkeypatch.setenv("PT_LAKEHOUSE_BASE", str(tmp_path))
    _nuke_project_modules()
    yield tmp_path
    _nuke_project_modules()


@pytest.fixture
def fresh_settings(monkeypatch):
    """Reload config with whatever env vars the caller sets via monkeypatch."""

    def _reload():
        import importlib

        import config as cfg

        importlib.reload(cfg)
        return cfg.settings

    return _reload
