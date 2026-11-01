"""Unit tests for DatasetSpec, SpecLakeConverter, and convert_with_specs."""

from pathlib import Path

import pyarrow.parquet as pq

from quantilica.analytics.lake import (
    DatasetSpec,
    SpecLakeConverter,
    check_dataset,
    convert_with_specs,
    detect_encoding,
)


def test_detect_encoding(tmp_path: Path):
    utf8_file = tmp_path / "utf8.txt"
    utf8_file.write_text("olá mundo", encoding="utf-8")
    assert detect_encoding(utf8_file) == "utf-8"

    utf8_bom = tmp_path / "utf8_bom.txt"
    utf8_bom.write_bytes(b"\xef\xbb\xbfhello world")
    assert detect_encoding(utf8_bom) == "utf-8-sig"

    cp1252_file = tmp_path / "cp1252.txt"
    cp1252_file.write_bytes(b"\xe7\xe3\xf5\xe9")  # çãõé in cp1252 (invalid utf-8)
    assert detect_encoding(cp1252_file) == "cp1252"


def test_convert_with_specs_duckdb_and_parquet(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    lake_dir = tmp_path / "lake"
    raw_dir.mkdir(parents=True)

    # Dataset 1: vendas (CSV with preamble and decimal comma)
    vendas_dir = raw_dir / "vendas"
    vendas_dir.mkdir()
    vendas_file = vendas_dir / "vendas_2024@20261025.csv"
    vendas_file.write_text(
        "#PREAMBULO linha 1\n"
        "#PREAMBULO linha 2\n"
        "#DATA;PRODUTO;VALOR\n"
        "2024-01-01;ItemA;10,50\n"
        "2024-01-02;ItemB;20,00\n",
        encoding="utf-8",
    )

    specs = {
        "vendas": DatasetSpec(
            dataset_id="vendas",
            partition_keys=("ano",),
            partitions_from=lambda p: {"ano": 2024},
            reader="duckdb",
            separator=";",
            skip_until="#DATA",
            decimal_comma=True,
            rename={"#data": "data"},
            required=("data", "produto", "valor"),
        )
    }

    converter = SpecLakeConverter("demo", specs, version="1.0.0")
    assert converter.datasets() == ["vendas"]

    res = converter.convert(raw_dir, lake_dir)
    assert res.converted == 1
    assert res.skipped == 0
    assert not res.errors

    # Verify output in lake
    ds_dir = lake_dir / "demo" / "vendas"
    assert ds_dir.is_dir()
    parquet_path = ds_dir / "ano=2024" / "vendas_2024.parquet"
    assert parquet_path.is_file()

    table = pq.read_table(parquet_path)
    assert table.num_rows == 2
    assert "data" in table.column_names
    assert "produto" in table.column_names
    assert "valor" in table.column_names

    # Check lake conformance
    errors = check_dataset(ds_dir)
    assert not errors

    # Run again without force -> should be skipped
    res_second = converter.convert(raw_dir, lake_dir)
    assert res_second.converted == 0
    assert res_second.skipped == 1
    assert not res_second.errors

    # Run with force -> should reconvert
    res_force = converter.convert(raw_dir, lake_dir, force=True)
    assert res_force.converted == 1
    assert res_force.skipped == 0
    assert not res_force.errors


def test_convert_with_specs_polars_and_filter(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    lake_dir = tmp_path / "lake"
    raw_dir.mkdir(parents=True)

    csv_file = raw_dir / "clientes" / "clientes_2023@20261020.csv"
    csv_file.parent.mkdir()
    csv_file.write_text("codigo;nome\n101;Empresa X\n102;Empresa Y\n", encoding="utf-8")

    specs = {
        "clientes": DatasetSpec(
            dataset_id="clientes",
            partition_keys=("ano",),
            partitions_from=lambda p: {"ano": 2023},
            reader="polars",
            separator=";",
            required=("codigo", "nome"),
        ),
        "outros": DatasetSpec(
            dataset_id="outros",
            partition_keys=("ano",),
            partitions_from=lambda p: {"ano": 2023},
        ),
    }

    # Unknown dataset requested
    res_unknown = convert_with_specs(
        "demo", specs, raw_dir, lake_dir, datasets=["inexistente"]
    )
    assert "dataset desconhecido: inexistente" in res_unknown.errors

    # Filter to "clientes"
    res = convert_with_specs("demo", specs, raw_dir, lake_dir, datasets=["clientes"])
    assert res.converted == 1
    assert not res.errors

    ds_dir = lake_dir / "demo" / "clientes"
    assert (ds_dir / "ano=2023" / "clientes_2023.parquet").is_file()
