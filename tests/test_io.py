import polars as pl
from quantilica.core.manifests import DownloadManifest

from quantilica.analytics.reader import SmartReader, read_brazilian_csv
from quantilica.analytics.writer import to_parquet


def test_to_parquet_with_manifest(tmp_path):
    # Create dummy data
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    # Create dummy manifest
    manifest = DownloadManifest(
        source_id="test-source",
        dataset_id="test-dataset",
        url="http://example.com/test.csv",
        sha256="fake-sha256",
        size_bytes=123,
        fetched_at="2026-05-09T00:00:00Z",
        path="test.csv",
        producer="test-producer",
    )

    output_file = tmp_path / "test.parquet"
    to_parquet(df, output_file, manifest=manifest)

    assert output_file.exists()

    # Verify metadata via Parquet read
    df_read = pl.read_parquet(output_file)
    assert df_read.equals(df)


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
    csv_file.write_text(
        "nome;valor\nfoo;1,5\nbar;-9999\n", encoding="latin-1"
    )

    df = read_brazilian_csv(csv_file)

    assert df.columns == ["nome", "valor"]
    assert df.height == 2
    # comma decimal parsed as float; -9999 treated as null
    assert df[0, "valor"] == 1.5
    assert df[1, "valor"] is None


def test_read_brazilian_csv_polars_kwargs_passthrough(tmp_path):
    csv_file = tmp_path / "nohdr.csv"
    csv_file.write_text("foo;10\nbar;20\n", encoding="latin-1")

    df = read_brazilian_csv(
        csv_file, has_header=False, new_columns=["k", "v"]
    )

    assert df.columns == ["k", "v"]
    assert df.height == 2


def test_read_brazilian_csv_pandas_engine(tmp_path):
    import pytest

    pd = pytest.importorskip("pandas")

    csv_file = tmp_path / "br.csv"
    csv_file.write_text("nome;valor\nfoo;1,5\n", encoding="latin-1")

    df = read_brazilian_csv(csv_file, engine="pandas")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["nome", "valor"]
    assert df.loc[0, "valor"] == 1.5
