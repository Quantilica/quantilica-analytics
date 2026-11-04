"""Regression tests for the DatasetSpec converter and reader/geo/schema helpers."""

from __future__ import annotations

import gzip
import zipfile
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from quantilica.analytics.lake import (
    DatasetSpec,
    convert_with_specs,
    find_skip_until,
)
from quantilica.analytics.reader import DEFAULT_BR_NA, read_csv_resilient


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


# --- convert_with_specs ----------------------------------------------------------


def _spec(**kwargs) -> DatasetSpec:
    base = {
        "dataset_id": "demo",
        "partition_keys": ("ano",),
        "partitions_from": lambda p: {"ano": 2024},
    }
    base.update(kwargs)
    return DatasetSpec(**base)


def _convert(tmp_path: Path, spec: DatasetSpec, **kwargs):
    lake = tmp_path / "lake"
    res = convert_with_specs("src", {"demo": spec}, tmp_path / "raw", lake, **kwargs)
    return res, lake / "src" / "demo"


def _read_all(ds_dir: Path):
    files = sorted(ds_dir.rglob("*.parquet"))
    assert files, f"no parquet under {ds_dir}"
    return pq.read_table(files[0])


def _raw(tmp_path: Path) -> Path:
    raw = tmp_path / "raw" / "demo"
    raw.mkdir(parents=True)
    return raw


def test_convert_gz_csv(tmp_path: Path):
    raw = _raw(tmp_path)
    with gzip.open(raw / "vendas_2024.csv.gz", "wb") as fh:
        fh.write(b"a;b\n1;x\n2;y\n")
    res, ds = _convert(tmp_path, _spec())
    assert not res.errors, res.errors
    assert res.converted == 1
    assert _read_all(ds).num_rows == 2


def _write_dbf(path: Path, rows: list[tuple[str, str]], encoding="latin-1") -> None:
    fields = [("NOME", 10), ("UF", 2)]
    header_len = 32 + 32 * len(fields) + 1
    rec_len = 1 + sum(w for _, w in fields)
    head = bytearray(32)
    head[0] = 0x03
    head[4:8] = len(rows).to_bytes(4, "little")
    head[8:10] = header_len.to_bytes(2, "little")
    head[10:12] = rec_len.to_bytes(2, "little")
    out = bytearray(head)
    for name, width in fields:
        d = bytearray(32)
        d[0 : len(name)] = name.encode()
        d[11] = ord("C")
        d[16] = width
        out += d
    out += b"\r"
    for nome, uf in rows:
        out += b" " + nome.ljust(10).encode(encoding) + uf.ljust(2).encode(encoding)
    out += b"\x1a"
    path.write_bytes(bytes(out))


def test_convert_dbf(tmp_path: Path):
    raw = _raw(tmp_path)
    _write_dbf(raw / "cad_2024.dbf", [("São Paulo", "SP"), ("Rio", "RJ")])
    res, ds = _convert(tmp_path, _spec())
    assert not res.errors, res.errors
    assert res.converted == 1
    table = _read_all(ds)
    assert table.column("nome").to_pylist() == ["São Paulo", "Rio"]
    assert table.column("uf").to_pylist() == ["SP", "RJ"]


def test_convert_dbc_without_decoder_reports_actionable_error(tmp_path: Path):
    raw = _raw(tmp_path)
    (raw / "cad_2024.dbc").write_bytes(b"not really a dbc")
    res, _ = _convert(tmp_path, _spec())
    assert res.converted == 0
    assert len(res.errors) == 1
    assert "dbc" in res.errors[0].lower()


def test_separator_fallback(tmp_path: Path):
    raw = _raw(tmp_path)
    (raw / "v_2024.csv").write_text("a,b\n1,x\n2,y\n", encoding="utf-8")
    res, ds = _convert(tmp_path, _spec(separator=";"))
    assert not res.errors, res.errors
    assert _read_all(ds).column_names[:2] == ["a", "b"]


def test_separator_is_strict_when_fallback_disabled(tmp_path: Path):
    raw = _raw(tmp_path)
    (raw / "v_2024.csv").write_text("a,b\n1,x\n2,y\n", encoding="utf-8")
    _, ds = _convert(tmp_path, _spec(separator=";", fallback=False))
    assert _read_all(ds).column_names[:1] == ["a_b"]  # one column: "a,b" normalized


def test_encoding_fallback_when_utf8_sniff_is_wrong(tmp_path: Path):
    raw = _raw(tmp_path)
    body = b"a;b\n" + b"1;aaaaaaaaaa\n" * 8000 + "2;ação\n".encode("latin-1")
    assert len(body) > 65536
    (raw / "v_2024.csv").write_bytes(body)
    res, ds = _convert(tmp_path, _spec())
    assert not res.errors, res.errors
    assert _read_all(ds).column("b").to_pylist()[-1] == "ação"


def test_members_filters_archive_members(tmp_path: Path):
    raw = _raw(tmp_path)
    with zipfile.ZipFile(raw / "v_2024.zip", "w") as zf:
        zf.writestr("dados_a.csv", "a;b\n1;x\n")
        zf.writestr("dados_b.csv", "a;b\n2;y\n3;z\n")
    res, ds = _convert(tmp_path, _spec(members="dados_b*"))
    assert not res.errors, res.errors
    assert _read_all(ds).column("a").to_pylist() == ["2", "3"]


def test_members_without_match_is_an_error(tmp_path: Path):
    raw = _raw(tmp_path)
    with zipfile.ZipFile(raw / "v_2024.zip", "w") as zf:
        zf.writestr("dados_a.csv", "a;b\n1;x\n")
    res, _ = _convert(tmp_path, _spec(members="nada*"))
    assert res.converted == 0
    assert res.errors


def test_polars_reader_uses_real_latin1(tmp_path: Path):
    raw = _raw(tmp_path)
    # 0x81 is undefined in cp1252 but is U+0081 in latin-1
    (raw / "v_2024.csv").write_bytes(b"a;b\n1;x\x81y\n2;ma\xe7\xe3\n")
    res, ds = _convert(tmp_path, _spec(reader="polars", encoding="latin-1"))
    assert not res.errors, res.errors
    assert _read_all(ds).column("b").to_pylist() == ["x\x81y", "maçã"]


def test_find_skip_until_latin1_is_real_latin1(tmp_path: Path):
    p = tmp_path / "f.csv"
    p.write_bytes(b"\x81\x8d\nHEAD;x\n1;2\n")
    assert find_skip_until(p, "HEAD", "latin-1") == 1


def test_fixed_width_layout_found_with_stacked_extensions(tmp_path: Path):
    raw = _raw(tmp_path)
    with gzip.open(raw / "tab@20261025.txt.gz", "wb") as fh:
        fh.write(b"0001AB\n0002CD\n")
    (raw / "tab_layout.txt").write_text(
        "Coluna,Tamanho,Inicio,Fim,Tipo\nCOD,4,1,4,CHAR\nSIG,2,5,6,CHAR\n",
        encoding="latin-1",
    )
    res, ds = _convert(tmp_path, _spec(reader="fixed_width"))
    assert not res.errors, res.errors
    assert _read_all(ds).column("cod").to_pylist() == ["0001", "0002"]


# --- read_csv_resilient ----------------------------------------------------------


def test_resilient_does_not_impose_null_values_by_default(tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_text("a;b\nNA;1\n-;2\n", encoding="utf-8")
    df = read_csv_resilient(p, separator=";")
    assert df["a"].to_list() == ["NA", "-"]


def test_resilient_br_nulls_opt_in(tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_text("a;b\nNA;1\n-;2\nx;3\n", encoding="utf-8")
    df = read_csv_resilient(p, separator=";", br_nulls=True)
    assert df["a"].to_list() == [None, None, "x"]
    explicit = read_csv_resilient(p, separator=";", null_values=["x"])
    assert explicit["a"].to_list() == ["NA", "-", None]
    assert "NA" in DEFAULT_BR_NA


def test_resilient_explicit_encoding_does_not_fall_back_silently(tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_bytes("a;b\nação;1\n".encode("latin-1"))
    with pytest.raises(Exception):  # noqa: B017 - polars raises ComputeError
        read_csv_resilient(p, separator=";", encoding="utf-8")


def test_resilient_explicit_fallback_encodings(tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_bytes("a;b\nação;1\n".encode("latin-1"))
    df = read_csv_resilient(
        p, separator=";", encoding="utf-8", fallback_encodings=["latin-1"]
    )
    assert df["a"].to_list() == ["ação"]


def test_resilient_auto_encoding_still_detects(tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_bytes("a;b\nação;1\n".encode("latin-1"))
    assert read_csv_resilient(p, separator=";")["a"].to_list() == ["ação"]


def test_resilient_lazy_streams_latin1_and_null_bytes(tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_bytes("cod;nome\x00\n0123;ação\n0456;maçã\n".encode("latin-1"))
    lf = read_csv_resilient(p, separator=";", code_columns=["cod"], lazy=True)
    assert isinstance(lf, pl.LazyFrame)
    df = lf.collect()
    assert df["cod"].to_list() == ["0123", "0456"]
    assert df["nome"].to_list() == ["ação", "maçã"]


def test_resilient_lazy_utf8_scans_the_file_directly(tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_text("a;b\n1;ç\n2;d\n", encoding="utf-8")
    lf = read_csv_resilient(p, separator=";", lazy=True)
    assert lf.head(1).collect().shape == (1, 2)


def test_resilient_max_bytes_guards_eager_reads(tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_text("a;b\n" + "1;2\n" * 1000, encoding="utf-8")
    with pytest.raises(ValueError, match="lazy"):
        read_csv_resilient(p, separator=";", max_bytes=100)
    assert read_csv_resilient(p, separator=";", max_bytes=100, lazy=True) is not None
    assert read_csv_resilient(p, separator=";", n_rows=3).height == 3


# --- EraSchemaResolver -----------------------------------------------------------


def _resolver():
    from quantilica.analytics import EraSchemaResolver

    return EraSchemaResolver({"ano": pl.Int64, "valor": pl.Float64})


def test_era_resolver_warns_and_counts_values_lost_in_cast(caplog):
    resolver = _resolver()
    df = pl.DataFrame({"ano": ["2020", "oops", None], "valor": ["1.5", "2", "x"]})
    with caplog.at_level("WARNING", logger="quantilica.analytics.schema"):
        out = resolver.resolve(df)
    # lenient mode keeps the contract: unconvertible values become null ...
    assert out["ano"].to_list() == [2020, None, None]
    assert out["valor"].to_list() == [1.5, 2.0, None]
    # ... but never silently
    assert resolver.cast_failures == {"ano": 1, "valor": 1}
    assert "ano" in caplog.text and "valor" in caplog.text


def test_era_resolver_no_warning_when_cast_is_clean(caplog):
    resolver = _resolver()
    df = pl.DataFrame({"ano": ["2020", None], "valor": ["1", "2"]})
    with caplog.at_level("WARNING", logger="quantilica.analytics.schema"):
        resolver.resolve(df)
    assert resolver.cast_failures == {}
    assert caplog.text == ""


def test_era_resolver_strict_cast_raises():
    resolver = _resolver()
    df = pl.DataFrame({"ano": ["2020", "oops"], "valor": ["1", "2"]})
    with pytest.raises(ValueError, match="ano"):
        resolver.resolve(df, strict_cast=True)
