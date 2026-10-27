"""Tests for the Hive lake writers, checker and readers."""

from __future__ import annotations

import json
import shutil
import subprocess
import zipfile
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from quantilica.core.lake import DESCRIPTOR_FILENAME, DatasetDescriptor

from quantilica.analytics import (
    DataContract,
    Field,
    LakeError,
    LakeWriter,
    check_dataset,
    duckdb_view_sql,
    scan_dataset,
)

CSV_LATIN1 = "Município;Valor Total\nSão Paulo;10\nAraçatuba;20\n".encode("latin-1")


@pytest.fixture
def dataset_dir(tmp_path: Path) -> Path:
    return tmp_path / "lake" / "pdet" / "rais"


@pytest.fixture
def descriptor() -> DatasetDescriptor:
    return DatasetDescriptor(
        source_id="pdet",
        dataset_id="rais",
        alias="rais",
        partition_keys=["ano", "mes"],
        partition_types={"ano": "int", "mes": "varchar"},
    )


def make_zip(path: Path, member: str, content: bytes) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, content)
    return path


def footer(path: Path) -> dict[str, str]:
    raw = pq.ParquetFile(path).metadata.metadata
    return {k.decode(): v.decode() for k, v in raw.items()}


def parquets(dataset_dir: Path) -> list[str]:
    return sorted(
        str(p.relative_to(dataset_dir)) for p in dataset_dir.rglob("*.parquet")
    )


def big_csv(path: Path, rows: int = 300_000) -> Path:
    lines = ["id;texto"] + [f"{i};{'x' * 40}{i}" for i in range(rows)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# --- DuckDB engine -----------------------------------------------------------------


def test_duckdb_latin1_zip_streaming(tmp_path, dataset_dir, descriptor):
    src = make_zip(tmp_path / "rais_2025@20260513.zip", "dados.txt", CSV_LATIN1)
    writer = LakeWriter(dataset_dir, descriptor)

    result = writer.write_duckdb(
        src,
        partitions={"ano": 2025, "mes": "09"},
        encoding="latin-1",
        rename={"valor_total": "valor"},
        required=("municipio", "valor"),
    )

    assert result.status == "converted"
    assert result.rows == 2
    expected = dataset_dir / "ano=2025" / "mes=09" / "rais_2025.parquet"
    assert result.outputs == [expected]
    df = pl.read_parquet(expected)
    assert df.columns == ["municipio", "valor"]
    assert df["municipio"].to_list() == ["São Paulo", "Araçatuba"]
    meta = footer(expected)
    assert len(meta["quantilica.origin_sha256"]) == 64
    assert meta["quantilica.lake.layout_version"] == "1"
    assert meta["quantilica.source_id"] == "pdet"
    assert meta["quantilica.dataset_id"] == "rais"
    assert meta["quantilica.origin_file"] == "rais_2025@20260513.zip"
    assert meta["quantilica.file_key"] == "rais_2025"
    assert meta["quantilica.converter"] == "quantilica-analytics"
    assert "quantilica.converted_at" in meta
    # No scratch left behind and no sidecar next to the Parquet.
    assert not list(dataset_dir.rglob("*.tmp"))
    assert not (dataset_dir / ".tmp").exists()
    assert not list(dataset_dir.rglob("*.manifest.json"))


def test_duckdb_origin_sha256_is_files_hash(tmp_path, dataset_dir, descriptor):
    from quantilica.core.files import sha256_file

    src = tmp_path / "a.csv"
    src.write_bytes(CSV_LATIN1)
    result = LakeWriter(dataset_dir, descriptor).write_duckdb(
        src, partitions={"ano": 2025, "mes": "01"}, encoding="latin-1"
    )
    assert footer(result.output)["quantilica.origin_sha256"] == sha256_file(src)


def test_duckdb_updates_descriptor(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "a.csv"
    src.write_bytes(CSV_LATIN1)
    contract = DataContract("rais", [Field("municipio", pl.String)], version=3)

    LakeWriter(dataset_dir, descriptor).write_duckdb(
        src,
        partitions={"ano": 2025, "mes": "01"},
        encoding="latin-1",
        contract=contract,
        converter="pdet-fetcher 0.5.0",
    )

    saved = DatasetDescriptor.load(dataset_dir)
    assert saved.contract_id == "rais"
    assert saved.contract_version == 3
    assert saved.converter == "pdet-fetcher 0.5.0"
    assert saved.partition_types == {"ano": "int", "mes": "varchar"}
    # The writer works on a copy of the caller's descriptor.
    assert descriptor.contract_id is None


def test_duckdb_headerless_with_names(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("rfb", "cnaes", "cnaes")
    src = tmp_path / "K3241.CNAECSV.csv"
    src.write_text("0111;Cultivo de arroz\n0112;Cultivo de milho\n", encoding="utf-8")

    result = LakeWriter(dataset_dir, descriptor).write_duckdb(
        src, header=False, names=("codigo", "descricao")
    )

    assert result.output == dataset_dir / "K3241.CNAECSV.parquet"
    df = pl.read_parquet(result.output)
    assert df.columns == ["codigo", "descricao"]
    assert df["codigo"].to_list() == ["0111", "0112"]  # text keeps leading zeros


def test_duckdb_explicit_quote_and_escape(tmp_path, dataset_dir):
    # Formato da Receita: tudo entre aspas duplas, apóstrofo literal no texto e
    # aspas internas dobradas. Com quote/escape fixos, nada depende da detecção.
    descriptor = DatasetDescriptor("rfb", "empresas", "empresas")
    src = tmp_path / "K3241.EMPRECSV.csv"
    src.write_text(
        '"00000001";"D\'AGUA LTDA";"2062"\n"00000002";"BAR ""DO ZE"" O\'NEIL";"2135"\n',
        encoding="latin-1",
    )

    result = LakeWriter(dataset_dir, descriptor).write_duckdb(
        src,
        header=False,
        names=("cnpj_basico", "razao_social", "natureza_juridica"),
        encoding="latin-1",
        quote='"',
        escape='"',
    )

    df = pl.read_parquet(result.output)
    assert df["razao_social"].to_list() == ["D'AGUA LTDA", 'BAR "DO ZE" O\'NEIL']
    assert df["natureza_juridica"].to_list() == ["2062", "2135"]


def test_duckdb_headerless_requires_names(tmp_path, dataset_dir):
    src = tmp_path / "a.csv"
    src.write_text("1;2\n", encoding="utf-8")
    writer = LakeWriter(dataset_dir, DatasetDescriptor("s", "d", "d"))
    with pytest.raises(ValueError, match="names"):
        writer.write_duckdb(src, header=False)


def test_duckdb_rejects_html_error_page(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "rais_2025.csv"
    src.write_text(
        "<html>\n<head><title>403 Forbidden</title></head>\n"
        "<body><h1>Forbidden</h1></body>\n</html>\n",
        encoding="utf-8",
    )
    writer = LakeWriter(dataset_dir, descriptor)

    with pytest.raises(LakeError, match="missing"):
        writer.write_duckdb(
            src,
            partitions={"ano": 2025, "mes": "09"},
            required=("municipio", "valor_total"),
        )

    assert parquets(dataset_dir) == []
    assert not (dataset_dir / DESCRIPTOR_FILENAME).exists()
    assert not (dataset_dir / "ano=2025").exists()


def test_duckdb_rejects_unsupported_format(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "a.xlsx"
    src.write_bytes(b"not a csv")
    with pytest.raises(LakeError, match="Unsupported"):
        LakeWriter(dataset_dir, descriptor).write_duckdb(
            src, partitions={"ano": 2025, "mes": "01"}
        )
    assert parquets(dataset_dir) == []


@pytest.mark.skipif(shutil.which("7z") is None, reason="7z not installed")
def test_duckdb_7z(tmp_path, dataset_dir, descriptor):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "caged.txt").write_bytes(CSV_LATIN1)
    src = tmp_path / "caged_202401@20260101.7z"
    subprocess.run(
        ["7z", "a", "-bd", str(src), str(raw / "caged.txt")],
        check=True,
        capture_output=True,
    )

    result = LakeWriter(dataset_dir, descriptor).write_duckdb(
        src, partitions={"ano": 2024, "mes": "01"}, encoding="latin-1"
    )

    assert result.output.name == "caged_202401.parquet"
    assert result.rows == 2


def test_idempotent_skip_then_force(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "rais_2025.csv"
    src.write_bytes(CSV_LATIN1)
    kwargs = {"partitions": {"ano": 2025, "mes": "09"}, "encoding": "latin-1"}

    first = LakeWriter(dataset_dir, descriptor).write_duckdb(src, **kwargs)
    second = LakeWriter(dataset_dir, descriptor).write_duckdb(src, **kwargs)
    forced = LakeWriter(dataset_dir, descriptor, force=True).write_duckdb(src, **kwargs)

    assert first.status == "converted"
    assert second.status == "skipped"
    assert second.outputs == first.outputs
    assert second.rows == 0
    assert forced.status == "converted"
    assert forced.rows == 2
    assert len(parquets(dataset_dir)) == 1


def test_republished_origin_replaces_not_accumulates(tmp_path, dataset_dir, descriptor):
    kwargs = {"partitions": {"ano": 2025, "mes": "09"}, "encoding": "latin-1"}
    writer = LakeWriter(dataset_dir, descriptor)
    old = tmp_path / "rais_2025@20260101.csv"
    old.write_bytes(CSV_LATIN1)
    new = tmp_path / "rais_2025@20260201.csv"
    new.write_bytes(CSV_LATIN1 + "Santos;30\n".encode("latin-1"))

    writer.write_duckdb(old, **kwargs)
    result = writer.write_duckdb(new, **kwargs)

    assert result.status == "converted"
    assert parquets(dataset_dir) == ["ano=2025/mes=09/rais_2025.parquet"]
    assert pl.read_parquet(result.output).height == 3
    assert footer(result.output)["quantilica.origin_file"] == "rais_2025@20260201.csv"


def test_contract_version_change_reconverts(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "a.csv"
    src.write_bytes(CSV_LATIN1)
    kwargs = {"partitions": {"ano": 2025, "mes": "01"}, "encoding": "latin-1"}
    v1 = DataContract("rais", [Field("municipio", pl.String)], version=1)
    v2 = DataContract("rais", [Field("municipio", pl.String)], version=2)
    writer = LakeWriter(dataset_dir, descriptor)

    assert writer.write_duckdb(src, contract=v1, **kwargs).status == "converted"
    assert writer.write_duckdb(src, contract=v1, **kwargs).status == "skipped"
    assert writer.write_duckdb(src, contract=v2, **kwargs).status == "converted"
    assert writer.write_duckdb(src, **kwargs).status == "converted"  # contract dropped


def test_try_cast_counts_failures(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    src = tmp_path / "t.csv"
    src.write_text(
        "id;valor;data;nome\n1;1.5;2026-01-02;a\nx;abc;2026-13-45;b\n3;;2026-01-03;c\n",
        encoding="utf-8",
    )
    contract = DataContract(
        "d",
        [
            Field("id", pl.Int64),
            Field("valor", pl.Float64),
            Field("data", pl.Date),
            Field("nome", pl.String),
        ],
    )

    result = LakeWriter(dataset_dir, descriptor).write_duckdb(
        src, contract=contract, separator=";"
    )

    df = pl.read_parquet(result.output)
    assert df.schema["id"] == pl.Int64
    assert df.schema["valor"] == pl.Float64
    assert df.schema["data"] == pl.Date
    assert df.schema["nome"] == pl.String
    assert df["id"].to_list() == [1, None, 3]
    # Empty values were already null, so they are not cast failures.
    assert result.cast_nulls == {"id": 1, "valor": 1, "data": 1}
    meta = footer(result.output)
    assert json.loads(meta["quantilica.cast_nulls"]) == result.cast_nulls
    assert meta["quantilica.contract.id"] == "d"
    assert meta["quantilica.contract.version"] == "1"


def test_contract_missing_required_field(tmp_path, dataset_dir):
    src = tmp_path / "t.csv"
    src.write_text("a;b\n1;2\n", encoding="utf-8")
    contract = DataContract("d", [Field("c", pl.Int64)])
    with pytest.raises(LakeError, match="missing"):
        LakeWriter(dataset_dir, DatasetDescriptor("s", "d", "d")).write_duckdb(
            src, contract=contract
        )
    assert parquets(dataset_dir) == []


def test_partition_keys_must_match_descriptor(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "a.csv"
    src.write_bytes(CSV_LATIN1)
    with pytest.raises(LakeError, match="do not match"):
        LakeWriter(dataset_dir, descriptor).write_duckdb(
            src, partitions={"ano": 2025}, encoding="latin-1"
        )


def test_invalid_partition_value_is_rejected(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "a.csv"
    src.write_bytes(CSV_LATIN1)
    with pytest.raises(ValueError, match="Invalid partition value"):
        LakeWriter(dataset_dir, descriptor).write_duckdb(
            src, partitions={"ano": "../x", "mes": "01"}, encoding="latin-1"
        )


def test_descriptor_adopts_partition_keys(tmp_path, dataset_dir):
    src = tmp_path / "a.csv"
    src.write_bytes(CSV_LATIN1)
    LakeWriter(dataset_dir, DatasetDescriptor("s", "d", "d")).write_duckdb(
        src, partitions={"uf": "SP"}, encoding="latin-1"
    )
    saved = DatasetDescriptor.load(dataset_dir)
    assert saved.partition_keys == ["uf"]
    assert saved.partition_types == {"uf": "varchar"}


def test_missing_partition_value_uses_hive_default(tmp_path, dataset_dir):
    src = tmp_path / "a.csv"
    src.write_bytes(CSV_LATIN1)
    descriptor = DatasetDescriptor("s", "d", "d", partition_keys=["uf"])
    result = LakeWriter(dataset_dir, descriptor).write_duckdb(
        src, partitions={"uf": None}, encoding="latin-1"
    )
    assert result.output.parent.name == "uf=__HIVE_DEFAULT_PARTITION__"


def test_rows_partition_source_not_supported(dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d", partition_source="rows")
    with pytest.raises(NotImplementedError):
        LakeWriter(dataset_dir, descriptor)


def test_split_into_parts_and_prune_orphans(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    src = big_csv(tmp_path / "big.csv")
    other = tmp_path / "big-007.csv"  # a different key that looks like a part
    other.write_text("id;texto\n1;a\n", encoding="utf-8")

    LakeWriter(dataset_dir, descriptor).write_duckdb(other, separator=";")
    split = LakeWriter(dataset_dir, descriptor, max_part_bytes=50_000).write_duckdb(
        src, separator=";"
    )

    assert len(split.outputs) > 1
    names = [p.name for p in split.outputs]
    assert names == [f"big-{i:03d}.parquet" for i in range(1, len(names) + 1)]
    assert sum(pq.ParquetFile(p).metadata.num_rows for p in split.outputs) == 300_000
    assert split.rows == 300_000
    assert all(footer(p)["quantilica.file_key"] == "big" for p in split.outputs)
    # Same origin and same parts: nothing to do.
    again = LakeWriter(dataset_dir, descriptor, max_part_bytes=50_000).write_duckdb(
        src, separator=";"
    )
    assert again.status == "skipped"

    # Republished smaller and unsplit: the old -NNN parts must go.
    src.write_text("id;texto\n1;a\n2;b\n", encoding="utf-8")
    result = LakeWriter(dataset_dir, descriptor).write_duckdb(src, separator=";")

    assert result.rows == 2
    assert parquets(dataset_dir) == ["big-007.parquet", "big.parquet"]
    # big-007.parquet is the *other* file key and must survive the pruning.
    assert footer(dataset_dir / "big-007.parquet")["quantilica.file_key"] == "big-007"


def test_part_name_collision_with_other_key_is_refused(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    other = tmp_path / "big-001.csv"
    other.write_text("id;texto\n1;a\n", encoding="utf-8")
    LakeWriter(dataset_dir, descriptor).write_duckdb(other, separator=";")
    before = (dataset_dir / "big-001.parquet").read_bytes()

    with pytest.raises(LakeError, match="already belongs"):
        LakeWriter(dataset_dir, descriptor, max_part_bytes=50_000).write_duckdb(
            big_csv(tmp_path / "big.csv"), separator=";"
        )

    assert (dataset_dir / "big-001.parquet").read_bytes() == before
    assert parquets(dataset_dir) == ["big-001.parquet"]
    assert not list(dataset_dir.rglob("*.tmp"))


def test_incomplete_part_set_is_not_current(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    src = big_csv(tmp_path / "big.csv")
    writer = LakeWriter(dataset_dir, descriptor, max_part_bytes=50_000)
    result = writer.write_duckdb(src, separator=";")
    assert len(result.outputs) > 1

    result.outputs[0].unlink()  # simulate a crash mid-publish
    assert writer.write_duckdb(src, separator=";").status == "converted"


def test_failed_conversion_leaves_previous_output(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "rais_2025.csv"
    src.write_bytes(CSV_LATIN1)
    kwargs = {"partitions": {"ano": 2025, "mes": "09"}, "encoding": "latin-1"}
    good = LakeWriter(dataset_dir, descriptor).write_duckdb(src, **kwargs)
    before = good.output.read_bytes()

    src.write_text("<html>403</html>\n", encoding="utf-8")
    with pytest.raises(LakeError):
        LakeWriter(dataset_dir, descriptor).write_duckdb(
            src, required=("municipio",), **kwargs
        )

    assert good.output.read_bytes() == before
    assert not list(dataset_dir.rglob("*.tmp"))


# --- Polars engine -----------------------------------------------------------------


def test_polars_dataframe_with_source(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "inmet_2025@20260101.csv"
    src.write_text("raw", encoding="utf-8")
    df = pl.DataFrame({"estacao": ["A", "B"], "temp": [20.5, 21.0]})
    writer = LakeWriter(dataset_dir, descriptor)

    result = writer.write_polars(df, src, partitions={"ano": 2025, "mes": "01"})

    expected = dataset_dir / "ano=2025" / "mes=01" / "inmet_2025.parquet"
    assert result.outputs == [expected]
    assert result.status == "converted"
    assert result.rows == 2
    assert pl.read_parquet(expected).equals(df)
    meta = footer(expected)
    assert meta["quantilica.origin_file"] == "inmet_2025@20260101.csv"
    assert meta["quantilica.lake.layout_version"] == "1"
    assert "quantilica.origin_sha256" in meta
    assert DatasetDescriptor.load(dataset_dir).partition_keys == ["ano", "mes"]
    # Same raw file: skipped without evaluating the data.
    exploding = pl.LazyFrame({"x": [1]}).select(
        pl.col("x").map_elements(lambda _: 1 / 0, return_dtype=pl.Int64)
    )
    again = writer.write_polars(exploding, src, partitions={"ano": 2025, "mes": "01"})
    assert again.status == "skipped"


def test_polars_lazyframe_streams(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    src = tmp_path / "t.csv"
    src.write_text("raw", encoding="utf-8")
    lazy = pl.LazyFrame({"a": [1, 2, 3]}).with_columns(b=pl.col("a") * 2)

    result = LakeWriter(dataset_dir, descriptor).write_polars(lazy, src)

    assert result.rows == 3
    assert pl.read_parquet(result.output)["b"].to_list() == [2, 4, 6]
    assert not list(dataset_dir.rglob("*.tmp"))


def test_polars_without_source_needs_key_and_never_skips(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    writer = LakeWriter(dataset_dir, descriptor)
    df = pl.DataFrame({"a": [1]})

    with pytest.raises(ValueError, match="file_key_name"):
        writer.write_polars(df, None)

    first = writer.write_polars(df, None, file_key_name="snapshot")
    second = writer.write_polars(df, None, file_key_name="snapshot")
    assert first.output.name == "snapshot.parquet"
    assert second.status == "converted"
    assert "quantilica.origin_sha256" not in footer(first.output)


def test_polars_contract_validated_before_writing(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    contract = DataContract("d", [Field("a", pl.Int64)], version=2)
    writer = LakeWriter(dataset_dir, descriptor)

    with pytest.raises(TypeError):
        writer.write_polars(
            pl.DataFrame({"a": ["x"]}), None, file_key_name="k", contract=contract
        )
    assert parquets(dataset_dir) == []

    result = writer.write_polars(
        pl.LazyFrame({"a": [1]}), None, file_key_name="k", contract=contract
    )
    assert footer(result.output)["quantilica.contract.version"] == "2"


def test_polars_split_by_size_and_prune(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    df = pl.DataFrame({"a": list(range(10_000)), "b": ["x" * 20] * 10_000})
    writer = LakeWriter(dataset_dir, descriptor, max_part_bytes=100_000)

    result = writer.write_polars(df, None, file_key_name="k")
    assert len(result.outputs) > 1
    assert sum(pq.ParquetFile(p).metadata.num_rows for p in result.outputs) == 10_000

    small = LakeWriter(dataset_dir, descriptor).write_polars(
        df.head(3), None, file_key_name="k"
    )
    assert parquets(dataset_dir) == ["k.parquet"]
    assert small.rows == 3


# --- checker -----------------------------------------------------------------------


@pytest.fixture
def healthy(tmp_path, dataset_dir, descriptor) -> Path:
    writer = LakeWriter(dataset_dir, descriptor)
    for ano, mes in [(2025, "09"), (2025, "10")]:
        src = tmp_path / f"rais_{ano}_{mes}.csv"
        src.write_bytes(CSV_LATIN1)
        writer.write_duckdb(
            src, partitions={"ano": ano, "mes": mes}, encoding="latin-1"
        )
    polars_src = tmp_path / "extra.csv"
    polars_src.write_text("x", encoding="utf-8")
    writer.write_polars(
        pl.DataFrame({"municipio": ["Rio"], "valor_total": ["5"]}),
        polars_src,
        partitions={"ano": 2026, "mes": "01"},
    )
    return dataset_dir


def test_check_dataset_healthy(healthy):
    assert check_dataset(healthy) == []


def test_check_dataset_healthy_with_split_parts(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    LakeWriter(dataset_dir, descriptor, max_part_bytes=50_000).write_duckdb(
        big_csv(tmp_path / "big.csv"), separator=";"
    )
    assert check_dataset(dataset_dir) == []


def test_check_dataset_unpartitioned_and_empty(tmp_path):
    dataset_dir = tmp_path / "s" / "d"
    descriptor = DatasetDescriptor("s", "d", "d")
    LakeWriter(dataset_dir, descriptor).write_polars(
        pl.DataFrame({"a": [1]}), None, file_key_name="k"
    )
    assert check_dataset(dataset_dir) == []

    empty = tmp_path / "s" / "empty"
    descriptor.save(empty)
    assert check_dataset(empty) == []


def test_check_dataset_missing_descriptor(tmp_path):
    errors = check_dataset(tmp_path)
    assert len(errors) == 1 and DESCRIPTOR_FILENAME in errors[0]


def test_check_dataset_invalid_descriptor(tmp_path):
    (tmp_path / DESCRIPTOR_FILENAME).write_text("{not json", encoding="utf-8")
    errors = check_dataset(tmp_path)
    assert len(errors) == 1 and "invalid" in errors[0]

    (tmp_path / DESCRIPTOR_FILENAME).write_text('{"source_id": "x"}', encoding="utf-8")
    assert "missing required keys" in check_dataset(tmp_path)[0]


def test_check_dataset_bad_layout(healthy):
    (healthy / "ano=2025" / "mes=09" / "stray.csv").write_text("x")
    (healthy / "ano=2025" / "mes=09" / "k.parquet.tmp").write_text("x")
    (healthy / "2025").mkdir()
    (healthy / "Ano=2025").mkdir()
    (healthy / "ano=2025" / "uf=SP").mkdir()
    (healthy / "ano=20 25").mkdir()
    shutil.copy(
        healthy / "ano=2025" / "mes=09" / "rais_2025_09.parquet",
        healthy / "ano=2025" / "loose.parquet",
    )

    errors = "\n".join(check_dataset(healthy))

    assert "stray.csv: unexpected file" in errors
    assert "k.parquet.tmp: unexpected file" in errors
    assert "2025: not a valid Hive" in errors
    assert "Ano=2025: not a valid Hive" in errors
    assert "uf=SP: expected mes at level 2" in errors
    assert "ano=20 25: not a valid Hive" in errors
    assert "loose.parquet: expected 2 partition levels" in errors


def test_check_dataset_bad_file_names(healthy):
    mes = healthy / "ano=2025" / "mes=09"
    shutil.copy(mes / "rais_2025_09.parquet", mes / "rais_2025_09@20260513.parquet")
    shutil.copy(mes / "rais_2025_09.parquet", mes / "rais_2025_09-002.parquet")
    shutil.copy(mes / "rais_2025_09.parquet", mes / "dados.csv.parquet")

    errors = "\n".join(check_dataset(healthy))

    assert "keeps a raw extension or @date suffix" in errors
    assert "mixes an unsplit file with numbered parts" in errors
    assert "dados.csv" in errors


def test_check_dataset_non_contiguous_parts(tmp_path, dataset_dir):
    descriptor = DatasetDescriptor("s", "d", "d")
    LakeWriter(dataset_dir, descriptor, max_part_bytes=50_000).write_duckdb(
        big_csv(tmp_path / "big.csv"), separator=";"
    )
    (dataset_dir / "big-001.parquet").unlink()
    assert any("not contiguous" in e for e in check_dataset(dataset_dir))


def test_check_dataset_missing_and_wrong_metadata(healthy):
    plain = healthy / "ano=2025" / "mes=09" / "plain.parquet"
    pl.DataFrame({"a": [1]}).write_parquet(plain)  # no quantilica.* metadata
    wrong = healthy / "ano=2025" / "mes=10" / "wrong.parquet"
    pl.DataFrame({"a": [1]}).write_parquet(
        wrong,
        metadata={
            "quantilica.lake.layout_version": "1",
            "quantilica.source_id": "other",
            "quantilica.dataset_id": "rais",
            "quantilica.file_key": "wrong",
            "quantilica.converted_at": "2026-01-01T00:00:00+00:00",
        },
    )

    errors = check_dataset(healthy)
    joined = "\n".join(errors)

    assert "plain.parquet: missing metadata" in joined
    assert "wrong.parquet: quantilica.source_id is 'other', expected 'pdet'" in joined


def test_check_dataset_partition_column_inside_file(healthy):
    path = healthy / "ano=2025" / "mes=09" / "inside.parquet"
    table = pa.table({"a": [1], "ano": [2025]}).replace_schema_metadata(
        {
            "quantilica.lake.layout_version": "1",
            "quantilica.source_id": "pdet",
            "quantilica.dataset_id": "rais",
            "quantilica.file_key": "inside",
            "quantilica.converted_at": "2026-01-01T00:00:00+00:00",
        }
    )
    pq.write_table(table, path)
    assert any("stored inside the file" in e for e in check_dataset(healthy))


def test_check_dataset_unreadable_parquet(healthy):
    (healthy / "ano=2025" / "mes=09" / "broken.parquet").write_bytes(b"not parquet")
    assert any("unreadable Parquet" in e for e in check_dataset(healthy))


def test_check_dataset_unknown_partition_type_and_layout_version(tmp_path):
    descriptor = DatasetDescriptor(
        "s",
        "d",
        "d",
        partition_keys=["ano"],
        partition_types={"ano": "uuid", "ghost": "int"},
        layout_version=2,
    )
    descriptor.save(tmp_path)
    errors = "\n".join(check_dataset(tmp_path))
    assert "layout_version 2 is not supported" in errors
    assert "Unknown type 'uuid'" in errors
    assert "'ghost'" in errors


def test_check_dataset_contract_id_mismatch(tmp_path, dataset_dir, descriptor):
    contract = DataContract("rais", [Field("municipio", pl.String)])
    src = tmp_path / "a.csv"
    src.write_bytes(CSV_LATIN1)
    LakeWriter(dataset_dir, descriptor).write_duckdb(
        src,
        partitions={"ano": 2025, "mes": "01"},
        encoding="latin-1",
        contract=contract,
    )
    saved = DatasetDescriptor.load(dataset_dir)
    saved.contract_id = "another"
    saved.save(dataset_dir)
    assert any("quantilica.contract.id" in e for e in check_dataset(dataset_dir))


def test_check_dataset_partition_types_compared_across_readers(healthy):
    """Undeclared ``varchar`` for ``mes`` is only right if no reader infers 9."""
    descriptor = DatasetDescriptor.load(healthy)
    assert descriptor.partition_types["mes"] == "varchar"
    assert check_dataset(healthy) == []
    # Declaring it as int is a *different* contract: '09' is still readable as 9.
    descriptor.partition_types["mes"] = "int"
    descriptor.save(healthy)
    assert check_dataset(healthy) == []


# --- readers -----------------------------------------------------------------------


def test_scan_dataset_uses_declared_types(healthy):
    lazy = scan_dataset(healthy)
    schema = lazy.collect_schema()
    assert schema["ano"] == pl.Int64
    assert schema["mes"] == pl.String

    df = lazy.collect()
    assert df.height == 5
    assert sorted(df["mes"].unique().to_list()) == ["01", "09", "10"]  # '09' kept
    assert df.filter(pl.col("ano") == 2026).height == 1
    assert "municipio" in df.columns


def test_scan_dataset_without_descriptor_infers(tmp_path):
    pl.DataFrame({"a": [1]}).write_parquet(_mkpart(tmp_path / "ano=2025") / "f.parquet")
    df = scan_dataset(tmp_path).collect()
    assert df["ano"].to_list() == [2025]


def _mkpart(path: Path) -> Path:
    path.mkdir(parents=True)
    return path


def test_duckdb_view_sql(healthy):
    sql = duckdb_view_sql(healthy)

    assert sql.startswith('CREATE OR REPLACE VIEW "rais" AS')
    assert "hive_partitioning = true" in sql
    assert "hive_types = {'ano': 'BIGINT', 'mes': 'VARCHAR'}" in sql
    assert (str(healthy / "**/*.parquet")) in sql

    conn = duckdb.connect()
    conn.execute(sql)
    assert conn.execute("SELECT count(*) FROM rais").fetchone()[0] == 5
    types = dict(conn.execute("DESCRIBE rais").fetchall()[i][:2] for i in range(4))
    assert types["ano"] == "BIGINT"
    assert types["mes"] == "VARCHAR"
    months = conn.execute("SELECT DISTINCT mes FROM rais ORDER BY 1").fetchall()
    assert months == [("01",), ("09",), ("10",)]
    pruned = conn.execute("SELECT count(*) FROM rais WHERE ano = 2026").fetchone()[0]
    assert pruned == 1


def test_duckdb_view_sql_custom_name_and_unpartitioned(tmp_path):
    dataset_dir = tmp_path / "s" / "d"
    LakeWriter(dataset_dir, DatasetDescriptor("s", "d", "short")).write_polars(
        pl.DataFrame({"a": [1, 2]}), None, file_key_name="k"
    )

    sql = duckdb_view_sql(dataset_dir, 'my"view')

    assert 'VIEW "my""view"' in sql
    assert "hive_types" not in sql
    conn = duckdb.connect()
    conn.execute(sql)
    assert conn.execute('SELECT count(*) FROM "my""view"').fetchone()[0] == 2
    assert 'VIEW "short"' in duckdb_view_sql(dataset_dir)


# --- remediation tests (R-02, R-03, R-04) -------------------------------------------


def test_staging_multipart_polars_no_staged_parquet(tmp_path, dataset_dir, descriptor):
    writer = LakeWriter(dataset_dir, descriptor, max_part_bytes=20_000)
    df = pl.DataFrame({"texto": ["x" * 100 for _ in range(1000)]})
    staged_names = []
    orig_publish = writer._publish

    def spy_publish(target, key, staged):
        staged_names.extend([p.name for p in staged])
        # Staged files in .tmp must never end in .parquet
        assert all(p.name.endswith(".parquet.tmp") for p in staged)
        return orig_publish(target, key, staged)

    writer._publish = spy_publish
    result = writer.write_polars(
        df, None, file_key_name="parts_test", partitions={"ano": 2025, "mes": "01"}
    )
    assert len(result.outputs) > 1
    assert all(p.name.endswith(".parquet") for p in result.outputs)
    assert len(staged_names) == len(result.outputs)
    assert all(name.endswith(".parquet.tmp") for name in staged_names)


def test_staging_multipart_duckdb_no_staged_parquet(tmp_path, dataset_dir, descriptor):
    src = big_csv(tmp_path / "big_input.csv", rows=150_000)
    writer = LakeWriter(dataset_dir, descriptor, max_part_bytes=50_000)
    staged_names = []
    orig_publish = writer._publish

    def spy_publish(target, key, staged):
        staged_names.extend([p.name for p in staged])
        # Staged files must use temporary extension and not match *.parquet
        assert all(p.name.endswith(".parquet.tmp") for p in staged)
        return orig_publish(target, key, staged)

    writer._publish = spy_publish
    result = writer.write_duckdb(
        src, partitions={"ano": 2025, "mes": "01"}, separator=";"
    )
    assert len(result.outputs) > 1
    assert all(p.name.endswith(".parquet") for p in result.outputs)
    assert len(staged_names) == len(result.outputs)
    assert all(name.endswith(".parquet.tmp") for name in staged_names)


def test_file_key_ending_in_three_digits_caged_001(tmp_path, dataset_dir, descriptor):
    src = big_csv(tmp_path / "caged-001.csv", rows=150_000)
    writer = LakeWriter(dataset_dir, descriptor)

    # 1. First write converts
    res1 = writer.write_duckdb(
        src, partitions={"ano": 2024, "mes": "01"}, separator=";"
    )
    assert res1.status == "converted"
    assert res1.output.name == "caged-001.parquet"
    assert footer(res1.output)["quantilica.file_key"] == "caged-001"

    # 2. Second write detects origin_matches and skips
    res2 = writer.write_duckdb(
        src, partitions={"ano": 2024, "mes": "01"}, separator=";"
    )
    assert res2.status == "skipped"
    assert res2.outputs == res1.outputs

    # 3. check_dataset approves without part number or file_key mismatch errors
    errors = check_dataset(dataset_dir)
    assert errors == []

    # 4. Multi-part split with key ending in -001, then prune orphans
    writer_split = LakeWriter(
        dataset_dir, descriptor, force=True, max_part_bytes=50_000
    )
    res_split = writer_split.write_duckdb(
        src, partitions={"ano": 2024, "mes": "01"}, separator=";"
    )
    assert res_split.status == "converted"
    assert len(res_split.outputs) > 1
    assert res_split.outputs[0].name == "caged-001-001.parquet"
    assert check_dataset(dataset_dir) == []

    # Republish smaller unsplit and check orphan pruning
    src.write_text("id;texto\n1;a\n2;b\n", encoding="utf-8")
    res_unsplit = writer.write_duckdb(
        src, partitions={"ano": 2024, "mes": "01"}, separator=";"
    )
    assert res_unsplit.status == "converted"
    assert res_unsplit.outputs == [res1.output]
    target_dir = dataset_dir / "ano=2024" / "mes=01"
    target_files = [p.name for p in target_dir.glob("*.parquet")]
    assert target_files == ["caged-001.parquet"]
    assert check_dataset(dataset_dir) == []


def test_drop_partition_columns_duckdb(tmp_path, dataset_dir, descriptor):
    src = tmp_path / "data_with_partitions.csv"
    src.write_text("ano;mes;municipio;valor\n2025;09;Campinas;15\n", encoding="utf-8")
    writer = LakeWriter(dataset_dir, descriptor)

    result = writer.write_duckdb(
        src,
        partitions={"ano": 2025, "mes": "09"},
        separator=";",
        required=("ano", "mes", "municipio", "valor"),
    )
    assert result.status == "converted"
    # Physical parquet schema must omit partition columns
    df = pl.read_parquet(result.output)
    assert "ano" not in df.columns
    assert "mes" not in df.columns
    assert df.columns == ["municipio", "valor"]
    # check_dataset must pass cleanly
    assert check_dataset(dataset_dir) == []
    # scan_dataset reads partition columns back from Hive folder structure
    scanned = scan_dataset(dataset_dir).collect()
    assert set(scanned.columns) == {"ano", "mes", "municipio", "valor"}
    assert scanned["ano"].to_list() == [2025]
    assert scanned["mes"].to_list() == ["09"]


def test_drop_partition_columns_polars(tmp_path, dataset_dir, descriptor):
    df = pl.DataFrame(
        {
            "ano": [2025, 2025],
            "mes": ["01", "01"],
            "estacao": ["A", "B"],
            "temp": [19.0, 22.5],
        }
    )
    writer = LakeWriter(dataset_dir, descriptor)
    result = writer.write_polars(
        df,
        None,
        file_key_name="inmet_part_test",
        partitions={"ano": 2025, "mes": "01"},
    )
    assert result.status == "converted"
    # Physical parquet schema must omit partition columns
    pq_df = pl.read_parquet(result.output)
    assert "ano" not in pq_df.columns
    assert "mes" not in pq_df.columns
    assert pq_df.columns == ["estacao", "temp"]
    # check_dataset approves
    assert check_dataset(dataset_dir) == []


def test_drop_partition_columns_polars_lazy(tmp_path, dataset_dir, descriptor):
    lf = pl.DataFrame(
        {
            "ano": [2025],
            "mes": ["02"],
            "valor": [42],
        }
    ).lazy()
    writer = LakeWriter(dataset_dir, descriptor)
    result = writer.write_polars(
        lf,
        None,
        file_key_name="lazy_part_test",
        partitions={"ano": 2025, "mes": "02"},
    )
    assert result.status == "converted"
    pq_df = pl.read_parquet(result.output)
    assert pq_df.columns == ["valor"]
    assert check_dataset(dataset_dir) == []


def test_drop_partition_columns_error_when_no_payload(
    tmp_path, dataset_dir, descriptor
):
    df = pl.DataFrame({"ano": [2025], "mes": ["01"]})
    writer = LakeWriter(dataset_dir, descriptor)
    with pytest.raises(LakeError, match="No payload columns left to write"):
        writer.write_polars(
            df,
            None,
            file_key_name="empty_payload",
            partitions={"ano": 2025, "mes": "01"},
        )
