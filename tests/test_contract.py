"""Tests for DataContract validation, nullability and PyArrow interop."""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from quantilica.core.manifests import DownloadManifest

from quantilica.analytics.schema import DataContract, Field
from quantilica.analytics.writer import to_parquet


@pytest.fixture
def contract() -> DataContract:
    return DataContract(
        dataset_id="br_test",
        fields=[
            Field("id", pl.Int64, nullable=False),
            Field("valor", pl.Float64, nullable=True),
            Field("nome", pl.String, nullable=True),
        ],
    )


@pytest.fixture
def valid_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "valor": [1.5, None, 3.0],
            "nome": ["a", "b", "c"],
        }
    )


class TestFieldNullable:
    def test_field_defaults_nullable(self):
        field = Field("a", pl.Int64)

        assert field.nullable is True

    def test_field_nullable_false(self):
        field = Field("a", pl.Int64, nullable=False)

        assert field.nullable is False

    def test_field_to_arrow_respects_nullable(self):
        field = Field("a", pl.Int64, nullable=False)

        arrow_field = field.to_arrow()

        assert arrow_field.name == "a"
        assert not arrow_field.nullable

    def test_field_to_arrow_nullable_true(self):
        field = Field("a", pl.Float64)

        arrow_field = field.to_arrow()

        assert arrow_field.nullable is True
        assert pa.types.is_float64(arrow_field.type)

    def test_field_positional_backward_compatibility(self):
        # Former signature: Field(name, dtype, required=True, description=None)
        field = Field("col_a", pl.Int64, True, "Column description")
        assert field.name == "col_a"
        assert field.dtype == pl.Int64
        assert field.required is True
        assert field.description == "Column description"
        assert field.nullable is True


class TestArrowInterop:
    def test_contract_positional_backward_compatibility(self):
        # Former signature: DataContract(dataset_id, fields, metadata=dict)
        fields = [Field("id", pl.Int64)]
        meta = {"source": "ibge"}
        contract = DataContract("pop_table", fields, meta)
        assert contract.dataset_id == "pop_table"
        assert contract.fields == fields
        assert contract.metadata == meta
        assert contract.version == 1

    def test_contract_to_arrow_schema(self, contract):
        schema = contract.to_arrow_schema()

        assert isinstance(schema, pa.Schema)
        assert schema.names == ["id", "valor", "nome"]
        assert not schema.field("id").nullable
        assert schema.field("valor").nullable

    def test_contract_to_polars_schema(self, contract):
        schema = contract.to_polars_schema()

        assert schema == {
            "id": pl.Int64,
            "valor": pl.Float64,
            "nome": pl.String,
        }


class TestValidateDataFrame:
    def test_valid_dataframe_passes(self, contract, valid_df):
        contract.validate(valid_df)

    def test_valid_lazyframe_passes(self, contract, valid_df):
        contract.validate(valid_df.lazy())

    def test_missing_required_field_raises_value_error(self):
        contract = DataContract(dataset_id="x", fields=[Field("obrigatorio", pl.Int64)])
        df = pl.DataFrame({"outro": [1]})

        with pytest.raises(ValueError, match="Missing required field: obrigatorio"):
            contract.validate(df)

    def test_missing_optional_field_passes(self):
        contract = DataContract(
            dataset_id="x",
            fields=[Field("opcional", pl.Int64, required=False)],
        )
        df = pl.DataFrame({"outro": [1]})

        contract.validate(df)

    def test_wrong_type_raises_type_error(self, contract):
        df = pl.DataFrame({"id": [1.5, 2.5], "valor": [1.0, 2.0], "nome": ["a", "b"]})

        with pytest.raises(TypeError, match="Field 'id' has type"):
            contract.validate(df)

    def test_nullable_false_with_null_raises_value_error(self):
        contract = DataContract(
            dataset_id="x", fields=[Field("id", pl.Int64, nullable=False)]
        )
        df = pl.DataFrame({"id": [1, None, 3]})

        with pytest.raises(ValueError, match="contains null values"):
            contract.validate(df)

    def test_nullable_false_with_null_lazyframe_raises(self):
        contract = DataContract(
            dataset_id="x", fields=[Field("id", pl.Int64, nullable=False)]
        )
        df = pl.DataFrame({"id": [1, None]}).lazy()

        with pytest.raises(ValueError, match="contains null values"):
            contract.validate(df)

    def test_nullable_false_no_null_passes(self):
        contract = DataContract(
            dataset_id="x", fields=[Field("id", pl.Int64, nullable=False)]
        )
        df = pl.DataFrame({"id": [1, 2, 3]})

        contract.validate(df)

    def test_nullable_true_allows_null(self, contract, valid_df):
        assert valid_df["valor"].null_count() == 1
        contract.validate(valid_df)


class TestValidateArrowTable:
    def test_valid_arrow_table_passes(self, contract):
        table = pa.table({"id": [1, 2], "valor": [1.5, 2.5], "nome": ["a", "b"]})

        contract.validate(table)

    def test_arrow_table_missing_required_field_raises(self, contract):
        table = pa.table({"valor": [1.5]})

        with pytest.raises(ValueError, match="Missing required field: id"):
            contract.validate(table)

    def test_arrow_table_wrong_type_raises(self, contract):
        table = pa.table({"id": ["x"], "valor": [1.5], "nome": ["a"]})

        with pytest.raises(TypeError, match="Field 'id' has type"):
            contract.validate(table)

    def test_arrow_table_null_violation_raises(self, contract):
        table = pa.table({"id": [1, None], "valor": [1.5, 2.5], "nome": ["a", "b"]})

        with pytest.raises(ValueError, match="contains null values"):
            contract.validate(table)

    def test_arrow_schema_names_align_with_data_table(self, contract):
        table = pa.table({"id": [1, 2], "valor": [1.5, 2.5], "nome": ["a", "b"]})

        contract.validate(table)
        assert contract.to_arrow_schema().names == table.schema.names


class TestToParquetWithContract:
    def test_writes_file_with_contract_metadata(self, contract, valid_df, tmp_path):
        output = tmp_path / "out.parquet"

        result = to_parquet(valid_df, output, contract=contract)

        assert isinstance(result, Path)
        assert result.exists()
        kv = {
            key.decode(): value.decode()
            for key, value in pq.read_metadata(output).metadata.items()
        }
        assert kv["quantilica.contract.id"] == "br_test"
        assert kv["quantilica.contract.version"] == "1"

    def test_contract_version_integer_serialized(self, tmp_path):
        contract = DataContract(
            dataset_id="br_v2",
            version=2,
            fields=[Field("a", pl.Int64)],
        )
        output = tmp_path / "v2.parquet"

        to_parquet(pl.DataFrame({"a": [1]}), output, contract=contract)

        kv = {
            key.decode(): value.decode()
            for key, value in pq.read_metadata(output).metadata.items()
        }
        assert kv["quantilica.contract.version"] == "2"

    def test_invalid_dataframe_blocked_before_write(self, contract, tmp_path):
        df = pl.DataFrame({"valor": [1.0]})
        output = tmp_path / "blocked.parquet"

        with pytest.raises(ValueError, match="Missing required field: id"):
            to_parquet(df, output, contract=contract)

        assert not output.exists()
        assert list(tmp_path.glob("*")) == []

    def test_null_violation_blocked_before_write(self, contract, tmp_path):
        df = pl.DataFrame(
            {"id": [None, None], "valor": [1.0, 2.0], "nome": ["a", "b"]},
            schema_overrides={"id": pl.Int64},
        )
        output = tmp_path / "nulls.parquet"

        with pytest.raises(ValueError, match="contains null values"):
            to_parquet(df, output, contract=contract)

        assert not output.exists()

    def test_manifest_and_contract_metadata_coexist(self, contract, valid_df, tmp_path):
        manifest = DownloadManifest(
            source_id="test-source",
            dataset_id="test-dataset",
            url="http://example.com/test.csv",
            sha256="fake-sha256",
            size_bytes=123,
            fetched_at="2026-05-09T00:00:00Z",
            path="test.csv",
        )
        output = tmp_path / "both.parquet"

        to_parquet(valid_df, output, manifest=manifest, contract=contract)

        kv = {
            key.decode(): value.decode()
            for key, value in pq.read_metadata(output).metadata.items()
            if key.startswith(b"quantilica.")
        }
        assert kv["quantilica.contract.id"] == "br_test"
        assert kv["quantilica.contract.version"] == "1"
        assert kv["quantilica.source_id"] == "test-source"
