"""Lakehouse abstractions — format-agnostic writers, table catalog helpers."""

from __future__ import annotations

from typing import Literal

from lakehouse.format_writer import FormatWriter, TableIdentity

Layer = Literal["bronze", "silver", "gold"]


def make_writer_for(
    spark,
    fmt: str,
    *,
    path: str,
    table_name: str,
    layer: Layer,
) -> FormatWriter:
    """Construct a ``FormatWriter`` + ``TableIdentity`` for a medallion table.

    The Glue database is derived from ``settings.glue_db_<layer>`` and the
    Iceberg catalog from ``settings.iceberg_catalog``. ``path`` is the Delta
    location used in ``fmt="delta"`` mode and informational in
    ``fmt="iceberg"`` mode (Iceberg manages its own storage layout under the
    catalog warehouse).

    Iceberg note: this helper does NOT call ``create_table``. Callers should
    rely on the Glacierbase migration framework (``migrations/versions/V00*``)
    to bring tables into existence ahead of writes — that's the source of
    truth for production schemas. Direct callers that want a transform to
    auto-create can still invoke ``writer.create_table`` after construction.
    """
    # Local import to avoid pulling pydantic-settings in the format_writer module.
    from config import settings

    db = {
        "bronze": settings.glue_db_bronze,
        "silver": settings.glue_db_silver,
        "gold": settings.glue_db_gold,
    }[layer]
    return FormatWriter(
        spark=spark,
        identity=TableIdentity(
            path=path,
            catalog=settings.iceberg_catalog,
            database=db,
            table=table_name,
        ),
        fmt=fmt,
    )


__all__ = ["FormatWriter", "TableIdentity", "Layer", "make_writer_for"]
