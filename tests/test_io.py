import io
import json

import polars as pl
import pyarrow.parquet as pq
import pytest
from quantilica.core.manifests import DownloadManifest

from quantilica.analytics.reader import (
    SmartReader,
    normalize_brazilian_numbers,
    read_brazilian_csv,
)
from quantilica.analytics.schema import DEFAULT_NULL_SENTINELS
from quantilica.analytics.writer import manifest_to_metadata, to_parquet


def _manifest(source_id: str = "test-source") -> DownloadManifest:
    return DownloadManifest(
        source_id=source_id,
        dataset_id="test-dataset",
        url="http://example.com/test.csv",
        sha256="fake-sha256",
        size_bytes=123,
        fetched_at="2026-05-09T00:00:00Z",
        path="test.csv",
        producer="test-producer",
    )


def test_to_parquet_with_manifest(tmp_path):
    # Create dummy data
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    manifest = _manifest()

    output_file = tmp_path / "test.parquet"
    to_parquet(df, output_file, manifest=manifest)

    assert output_file.exists()

    # Verify metadata via Parquet read
    df_read = pl.read_parquet(output_file)
    assert df_read.equals(df)


def test_to_parquet_atomic_no_tmp_file_leftovers(tmp_path):
    df = pl.DataFrame({"a": [1, 2, 3]})
    output_file = tmp_path / "out.parquet"

    to_parquet(df, output_file, manifest=_manifest())

    assert output_file.exists()
    assert list(tmp_path.glob("*.tmp.parquet")) == []


def test_to_parquet_atomic_preserves_target_on_failure(tmp_path, monkeypatch):
    df = pl.DataFrame({"a": [1, 2, 3]})
    output_file = tmp_path / "out.parquet"

    to_parquet(df, output_file)  # first (successful) write
    original_bytes = output_file.read_bytes()

    def failing_write(self, *args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", failing_write)

    with pytest.raises(RuntimeError, match="disk full"):
        to_parquet(df, output_file)  # second (failing) write

    # The original file survives untouched and no temp file is left behind.
    assert output_file.read_bytes() == original_bytes
    assert list(tmp_path.glob("*.tmp.parquet")) == []


def test_to_parquet_persists_quantilica_metadata(tmp_path):
    df = pl.DataFrame({"a": [1]})
    manifest = _manifest()
    output_file = tmp_path / "meta.parquet"

    to_parquet(df, output_file, manifest=manifest)

    kv = {
        key.decode(): value.decode()
        for key, value in pq.read_metadata(output_file).metadata.items()
        if key.startswith(b"quantilica.")
    }
    assert kv == manifest_to_metadata(manifest)
    assert kv["quantilica.source_id"] == "test-source"


def test_to_parquet_writes_manifest_sidecar(tmp_path):
    df = pl.DataFrame({"a": [1]})
    manifest = _manifest()
    output_file = tmp_path / "out.parquet"

    to_parquet(df, output_file, manifest=manifest)

    sidecar = tmp_path / "out.parquet.manifest.json"
    assert sidecar.exists()
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "test-dataset"
    assert payload["sha256"] == "fake-sha256"


def test_to_parquet_without_manifest_writes_no_sidecar(tmp_path):
    df = pl.DataFrame({"a": [1]})
    output_file = tmp_path / "plain.parquet"

    to_parquet(df, output_file)

    assert output_file.exists()
    assert not (tmp_path / "plain.parquet.manifest.json").exists()


def test_to_parquet_lazyframe(tmp_path):
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    output_file = tmp_path / "lazy.parquet"

    to_parquet(df.lazy(), output_file)

    assert pl.read_parquet(output_file).equals(df)


def test_smart_reader_csv(tmp_path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("col1;col2\nval1;123", encoding="latin-1")

    reader = SmartReader()
    df = reader.read(csv_file)

    assert df.columns == ["col1", "col2"]
    assert df.height == 1
    assert df[0, "col2"] == 123


def test_read_brazilian_csv_polars_defaults(tmp_path):
    csv_file = tmp_path / "br.csv"
    csv_file.write_text("nome;valor\nfoo;1,5\nbar;-9999\n", encoding="latin-1")

    df = read_brazilian_csv(csv_file)

    assert df.columns == ["nome", "valor"]
    assert df.height == 2
    # comma decimal parsed as float; -9999 treated as null
    assert df[0, "valor"] == 1.5
    assert df[1, "valor"] is None


def test_read_brazilian_csv_polars_kwargs_passthrough(tmp_path):
    csv_file = tmp_path / "nohdr.csv"
    csv_file.write_text("foo;10\nbar;20\n", encoding="latin-1")

    df = read_brazilian_csv(csv_file, has_header=False, new_columns=["k", "v"])

    assert df.columns == ["k", "v"]
    assert df.height == 2


def test_read_brazilian_csv_pandas_engine(tmp_path):
    pd = pytest.importorskip("pandas")

    csv_file = tmp_path / "br.csv"
    csv_file.write_text("nome;valor\nfoo;1,5\n", encoding="latin-1")

    df = read_brazilian_csv(csv_file, engine="pandas")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["nome", "valor"]
    assert df.loc[0, "valor"] == 1.5


def test_read_brazilian_csv_utf8_explicit_encoding(tmp_path):
    csv_file = tmp_path / "u8.csv"
    csv_file.write_text("nome;valor\nSão Paulo;2,5\n", encoding="utf-8")

    df = read_brazilian_csv(csv_file, encoding="utf-8")

    assert df["nome"].to_list() == ["São Paulo"]
    assert df["valor"].to_list() == [2.5]


def test_read_brazilian_csv_falls_back_to_latin1_on_decode_error(tmp_path):
    # Invalid UTF-8 content: default encoding fails and latin-1 kicks in.
    csv_file = tmp_path / "l1.csv"
    csv_file.write_text("nome;valor\nOtimização;2,5\nAção;3,0\n", encoding="latin-1")

    df = read_brazilian_csv(csv_file, encoding="utf-8")

    assert df["nome"].to_list() == ["Otimização", "Ação"]
    assert df["valor"].to_list() == [2.5, 3.0]


def test_read_brazilian_csv_fallback_rewinds_bytesio():
    # A consumed file-like source must still be readable on the fallback path.
    source = io.BytesIO("nome;valor\nAção;1,5\n".encode("latin-1"))

    df = read_brazilian_csv(source, encoding="utf-8")

    assert df["nome"].to_list() == ["Ação"]
    assert df["valor"].to_list() == [1.5]


def test_normalize_brazilian_numbers():
    df = pl.DataFrame(
        {
            "valor": ["1.234,56", "2,5", "-9999", "N/D", "n.d.", "", None],
        }
    )

    out = normalize_brazilian_numbers(df, ["valor"])

    assert out.schema == {"valor": pl.Float64}
    assert out["valor"].to_list() == [1234.56, 2.5, None, None, None, None, None]


def test_normalize_brazilian_numbers_negative_and_grouped():
    df = pl.DataFrame({"valor": ["-1.234,56", "1.000.000,25"]})

    out = normalize_brazilian_numbers(df, ["valor"])

    assert out["valor"].to_list() == [-1234.56, 1000000.25]


def test_normalize_brazilian_numbers_empty_column_list_unchanged():
    df = pl.DataFrame({"a": ["1,5"]})

    out = normalize_brazilian_numbers(df, [])

    assert out.equals(df)


def test_normalize_brazilian_numbers_missing_column_raises():
    df = pl.DataFrame({"valor": ["1,5"]})

    with pytest.raises(ValueError, match="inexistente"):
        normalize_brazilian_numbers(df, ["inexistente"])


def test_normalize_brazilian_numbers_multiple_columns_at_once():
    df = pl.DataFrame({"qtde": ["1.000", "500"], "peso": ["2,5", "3,75"]})

    out = normalize_brazilian_numbers(df, ["qtde", "peso"])

    assert out.schema == {"qtde": pl.Float64, "peso": pl.Float64}
    assert out["qtde"].to_list() == [1000.0, 500.0]
    assert out["peso"].to_list() == [2.5, 3.75]


def test_default_null_sentinels_canonical_constant():
    assert DEFAULT_NULL_SENTINELS == [
        "-9999",
        "N/D",
        "NA",
        "n.a.",
        "n.d.",
        "-",
        "não disp",
        "",
    ]


def test_top_level_exports():
    import quantilica.analytics as qa

    for name in (
        "DataContract",
        "DEFAULT_BR_NA",
        "DEFAULT_NULL_SENTINELS",
        "Field",
        "SmartReader",
        "manifest_to_metadata",
        "normalize_brazilian_numbers",
        "read_brazilian_csv",
        "to_parquet",
    ):
        assert hasattr(qa, name), name
    assert qa.DEFAULT_NULL_SENTINELS is qa.DEFAULT_BR_NA
