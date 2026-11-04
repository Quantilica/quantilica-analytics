"""Regression tests for the DatasetSpec converter and reader/geo/schema helpers."""

from __future__ import annotations

from quantilica.analytics.lake import DatasetSpec


def test_dataset_spec_partition_types_follow_core():
    from quantilica.core.lake import partition_types_for

    keys = ("ano", "mes", "trimestre", "semestre", "uf", "periodo", "year", "month")
    spec = DatasetSpec(
        dataset_id="x",
        partition_keys=keys,
        partitions_from=lambda p: {},
    )
    assert spec.partition_types == partition_types_for(keys)
    assert spec.partition_types["mes"] == "varchar"
    assert spec.partition_types["trimestre"] == "varchar"
    assert spec.partition_types["ano"] == "int"
