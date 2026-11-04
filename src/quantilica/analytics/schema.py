"""Data contracts and schema validation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import polars as pl
import pyarrow as pa

logger = logging.getLogger("quantilica.analytics.schema")

DEFAULT_NULL_SENTINELS = ["-9999", "N/D", "NA", "n.a.", "n.d.", "-", "não disp", ""]
"""Canonical sentinel strings treated as null in Brazilian public data.

Tokens such as ``-9999`` (numeric hole), ``N/D`` (não disponível), ``n.a.``
(não aplicável), ``n.d.`` (não disponível) and ``não disp`` appear across
IBGE, Banco Central and ministry CSV/Excel extracts. Reader and writer
modules import this constant so every entry point shares one definition.
"""


def _arrow_types_compatible(actual: pa.DataType, expected: pa.DataType) -> bool:
    """Check Arrow type compatibility across Polars' large/non-large variants.

    Polars maps ``pl.String`` to ``large_string`` while PyArrow inference uses
    plain ``string`` (and likewise for binary); both are semantically valid
    representations of the same logical type.
    """
    if actual.equals(expected):
        return True
    large_pair = {
        (pa.string(), pa.large_string()),
        (pa.large_string(), pa.string()),
        (pa.binary(), pa.large_binary()),
        (pa.large_binary(), pa.binary()),
    }
    return (actual, expected) in large_pair


@dataclass(frozen=True)
class Field:
    """A field in a data contract.

    Attributes:
        name (str): The name of the field.
        dtype (pl.DataType): The expected Polars data type.
        required (bool, optional): Whether the field must be present. Defaults to
            True.
        description (str | None, optional): An optional description of the field.
            Defaults to None.
        nullable (bool, optional): Whether the field may contain null values.
            Defaults to True.
    """

    name: str
    dtype: pl.DataType
    required: bool = True
    description: str | None = None
    nullable: bool = True

    def to_arrow(self) -> pa.Field:
        """Convert the field to a PyArrow field.

        Returns:
            pa.Field: The equivalent PyArrow field, with nullability derived
            from :attr:`nullable`.
        """
        arrow_type = (
            pl.DataFrame(schema={self.name: self.dtype})
            .to_arrow()
            .schema.field(self.name)
            .type
        )
        return pa.field(self.name, arrow_type, nullable=self.nullable)


@dataclass(frozen=True)
class DataContract:
    """A contract defining the expected structure and types of a dataset.

    Attributes:
        dataset_id (str): A unique identifier for the dataset.
        fields (list[Field]): A list of field specifications.
        metadata (dict[str, Any], optional): Additional metadata for the contract.
            Defaults to an empty dict.
        version (int, optional): Contract schema version. Defaults to 1.
    """

    dataset_id: str
    fields: list[Field]
    metadata: dict[str, Any] = field(default_factory=dict)
    version: int = 1

    def to_arrow_schema(self) -> pa.Schema:
        """Build an equivalent PyArrow schema from the contract.

        Returns:
            pa.Schema: A schema with one :class:`pa.Field` per contract field.
        """
        return pa.schema([field_spec.to_arrow() for field_spec in self.fields])

    def to_polars_schema(self) -> pl.Schema:
        """Build an equivalent Polars schema from the contract.

        Returns:
            pl.Schema: A schema mapping each field name to its expected dtype.
        """
        return pl.Schema(
            {field_spec.name: field_spec.dtype for field_spec in self.fields}
        )

    def validate(self, df: pl.DataFrame | pl.LazyFrame | pa.Table) -> None:
        """Validate a table against this contract.

        Args:
            df (pl.DataFrame | pl.LazyFrame | pa.Table): The table to validate.

        Raises:
            ValueError: If a required field is missing from the table schema, or
                if a non-nullable field contains null values.
            TypeError: If a field's type does not match the expected type in the
                contract.
        """
        if isinstance(df, pa.Table):
            self._validate_arrow(df)
            return

        schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema

        for field_spec in self.fields:
            if field_spec.required and field_spec.name not in schema:
                raise ValueError(f"Missing required field: {field_spec.name}")

            if field_spec.name in schema:
                actual_type = schema[field_spec.name]
                if actual_type != field_spec.dtype:
                    raise TypeError(
                        f"Field '{field_spec.name}' has type "
                        f"{actual_type}, expected {field_spec.dtype}"
                    )
                self._validate_nullability(df, field_spec)

    def _validate_arrow(self, table: pa.Table) -> None:
        """Validate a PyArrow table against this contract."""
        schema = table.schema
        for field_spec in self.fields:
            if field_spec.required and field_spec.name not in schema.names:
                raise ValueError(f"Missing required field: {field_spec.name}")

            if field_spec.name in schema.names:
                index = schema.get_field_index(field_spec.name)
                column = table.column(index)
                if not _arrow_types_compatible(column.type, field_spec.to_arrow().type):
                    raise TypeError(
                        f"Field '{field_spec.name}' has type "
                        f"{column.type}, expected {field_spec.dtype}"
                    )
                if field_spec.nullable is False and column.null_count > 0:
                    raise ValueError(
                        f"Field '{field_spec.name}' contains null values but "
                        "contract specifies nullable=False"
                    )

    def _validate_nullability(self, df, field_spec) -> None:
        """Check a column for null values when the contract is not nullable."""
        if field_spec.nullable is not False:
            return

        if isinstance(df, pl.LazyFrame):
            null_count = df.select(
                pl.col(field_spec.name).is_null().sum().alias("_null_count")
            ).collect()["_null_count"][0]
        else:
            null_count = df[field_spec.name].null_count()

        if null_count > 0:
            raise ValueError(
                f"Field '{field_spec.name}' contains null values but contract "
                "specifies nullable=False"
            )

    def cast(self, df: pl.DataFrame) -> pl.DataFrame:
        """Cast a DataFrame to match the contract types.

        Args:
            df (pl.DataFrame): The DataFrame to cast.

        Returns:
            pl.DataFrame: A new DataFrame with columns cast to the expected types.
        """
        expressions = []
        for field_spec in self.fields:
            if field_spec.name in df.columns:
                expressions.append(pl.col(field_spec.name).cast(field_spec.dtype))
        return df.with_columns(expressions)


class EraSchemaResolver:
    """Mapeia esquemas de múltiplas eras históricas para contrato canônico (L3).

    Renomeia colunas de eras históricas para o nome canônico, assegura casting para
    os dtypes esperados e preenche colunas ausentes em eras antigas com nulos tipados.
    """

    def __init__(
        self,
        canonical_schema: DataContract | dict[str, pl.DataType],
        eras: dict[str, dict[str, str]] | None = None,
        *,
        era_mappings: dict[str, dict[str, str]] | None = None,
        default_values: dict[str, Any] | None = None,
    ) -> None:
        if isinstance(canonical_schema, DataContract):
            self.schema: dict[str, pl.DataType] = {
                f.name: f.dtype for f in canonical_schema.fields
            }
        else:
            self.schema = dict(canonical_schema)
        self.eras: dict[str, dict[str, str]] = dict(eras or era_mappings or {})
        self.default_values: dict[str, Any] = dict(default_values or {})
        self.cast_failures: dict[str, int] = {}

    @staticmethod
    def _count_cast_losses(series: pl.Series, dtype: pl.DataType) -> int:
        """Non-null values of ``series`` that a lenient cast to ``dtype`` nulls."""
        casted = series.cast(dtype, strict=False)
        return int((casted.is_null() & series.is_not_null()).sum())

    def register_era(self, era: str, mapping: dict[str, str]) -> None:
        """Registra mapeamento {coluna_antiga: coluna_canonica} para uma era."""
        self.eras[era] = mapping

    def resolve(
        self,
        df: pl.DataFrame,
        era: str | None = None,
        *,
        fill_missing: bool = True,
        strict_columns: bool = False,
        strict_cast: bool = False,
    ) -> pl.DataFrame:
        """Normaliza um DataFrame da era histórica para o schema canônico.

        Contrato do cast para o dtype canônico: um valor não nulo que não
        converte **vira nulo, mas nunca em silêncio**. Com ``strict_cast=False``
        (o padrão, que preserva o comportamento anterior de não interromper a
        carga de uma série histórica) cada coluna com perdas gera um aviso no
        logger ``quantilica.analytics.schema`` e a contagem fica em
        :attr:`cast_failures` (``{coluna: valores perdidos}``, refeito a cada
        chamada). Com ``strict_cast=True`` a primeira coluna com perdas levanta
        ``ValueError``, sem devolver dados alterados.
        """
        res = df
        self.cast_failures = {}
        if era and era in self.eras:
            mapping = {k: v for k, v in self.eras[era].items() if k in res.columns}
            if mapping:
                res = res.rename(mapping)

        exprs: list[pl.Expr] = []
        for col_name, dtype in self.schema.items():
            if col_name in res.columns:
                if res[col_name].dtype != dtype:
                    lost = self._count_cast_losses(res[col_name], dtype)
                    if lost:
                        if strict_cast:
                            raise ValueError(
                                f"{lost} value(s) of column {col_name!r} cannot "
                                f"be cast to {dtype}"
                            )
                        self.cast_failures[col_name] = lost
                        logger.warning(
                            "EraSchemaResolver: %d value(s) of column %r cannot be "
                            "cast to %s and became null",
                            lost,
                            col_name,
                            dtype,
                        )
                    exprs.append(pl.col(col_name).cast(dtype, strict=False))
            elif fill_missing:
                val = self.default_values.get(col_name)
                if val is not None:
                    exprs.append(pl.lit(val).cast(dtype, strict=False).alias(col_name))
                else:
                    exprs.append(pl.lit(None, dtype=dtype).alias(col_name))

        if exprs:
            res = res.with_columns(exprs)

        if strict_columns:
            res = res.select(list(self.schema.keys()))

        return res
