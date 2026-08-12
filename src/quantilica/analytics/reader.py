"""Smart file reading integrated with Quantilica manifests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import polars as pl
from quantilica.core.manifests import DownloadManifest

DEFAULT_BR_NA = ["-9999", "N/D", "NA", ""]
"""Default sentinel strings treated as null in Brazilian public-data CSVs."""


def read_brazilian_csv(
    source: Any,
    *,
    engine: Literal["polars", "pandas"] = "polars",
    separator: str = ";",
    decimal: str = ",",
    encoding: str = "latin-1",
    na_values: list[str] | None = None,
    **kwargs: Any,
) -> Any:
    """Read a Brazilian public-data CSV with the common defaults.

    Brazilian government CSVs typically use ``;`` separators, comma decimals
    and latin-1 encoding. ``source`` may be a path, bytes or file-like object
    (whatever the chosen engine accepts). Extra ``kwargs`` are forwarded to the
    underlying reader using that engine's native names (e.g. ``skip_rows`` /
    ``new_columns`` for polars, ``skiprows`` for pandas).

    Args:
        source (Any): The source CSV file. May be a path, bytes, or file-like object.
        engine (Literal["polars", "pandas"], optional): The processing engine to use. Defaults to "polars".
        separator (str, optional): The column separator. Defaults to ";".
        decimal (str, optional): The decimal separator. Defaults to ",".
        encoding (str, optional): The file encoding. Defaults to "latin-1".
        na_values (list[str] | None, optional): A list of strings to interpret as missing values. Defaults to None, which uses `DEFAULT_BR_NA`.
        **kwargs (Any): Extra keyword arguments forwarded to the underlying reader.

    Returns:
        Any: A ``polars.DataFrame`` (default) or ``pandas.DataFrame`` when ``engine="pandas"``.

    Raises:
        ValueError: If an unknown engine is provided.
    """
    na = list(DEFAULT_BR_NA if na_values is None else na_values)

    if engine == "polars":
        kwargs.setdefault("separator", separator)
        kwargs.setdefault("encoding", encoding)
        kwargs.setdefault("null_values", na)
        if decimal == ",":
            kwargs.setdefault("decimal_comma", True)
        return pl.read_csv(source, **kwargs)

    if engine == "pandas":
        import pandas as pd

        kwargs.setdefault("sep", separator)
        kwargs.setdefault("decimal", decimal)
        kwargs.setdefault("encoding", encoding)
        kwargs.setdefault("na_values", na)
        return pd.read_csv(source, **kwargs)

    raise ValueError(f"Unknown engine: {engine!r}")


class SmartReader:
    """A reader that understands Quantilica manifests and optimizes Polars loading."""

    def __init__(self, default_encoding: str = "utf-8"):
        """Initialize the SmartReader.

        Args:
            default_encoding (str, optional): The default encoding to use if not specified. Defaults to "utf-8".
        """
        self.default_encoding = default_encoding

    def read(
        self,
        path_or_manifest: str | Path | DownloadManifest,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """Read a file into a Polars DataFrame, optionally guided by a manifest.

        Args:
            path_or_manifest (str | Path | DownloadManifest): The file path or a download manifest pointing to the file.
            **kwargs (Any): Extra keyword arguments forwarded to the underlying polars reader.

        Returns:
            pl.DataFrame: The loaded data as a Polars DataFrame.

        Raises:
            ValueError: If the file format (extension) is not supported.
        """
        if isinstance(path_or_manifest, DownloadManifest):
            path = Path(path_or_manifest.path)
        else:
            path = Path(path_or_manifest)

        suffix = path.suffix.lower()

        if suffix == ".csv":
            # Brazilian Gov data: default to semicolon and latin-1
            if "separator" not in kwargs:
                kwargs["separator"] = ";"
            if "encoding" not in kwargs:
                kwargs["encoding"] = "latin-1"
            return pl.read_csv(path, **kwargs)

        if suffix == ".parquet":
            return pl.read_parquet(path, **kwargs)

        if suffix == ".json":
            return pl.read_json(path, **kwargs)

        if suffix in (".xlsx", ".xls"):
            return pl.read_excel(path, **kwargs)

        raise ValueError(f"Unsupported file format: {suffix}")

    def scan(
        self,
        path_or_manifest: str | Path | DownloadManifest,
        **kwargs: Any,
    ) -> pl.LazyFrame:
        """Lazily scan a file (optimized for large CSVs/Parquet).

        Args:
            path_or_manifest (str | Path | DownloadManifest): The file path or a download manifest pointing to the file.
            **kwargs (Any): Extra keyword arguments forwarded to the underlying polars scanner.

        Returns:
            pl.LazyFrame: The loaded data as a Polars LazyFrame.

        Raises:
            ValueError: If the file format (extension) does not support lazy scanning.
        """
        if isinstance(path_or_manifest, DownloadManifest):
            path = Path(path_or_manifest.path)
        else:
            path = Path(path_or_manifest)

        suffix = path.suffix.lower()

        if suffix == ".csv":
            if "separator" not in kwargs:
                kwargs["separator"] = ";"
            if "encoding" not in kwargs:
                kwargs["encoding"] = "latin-1"
            return pl.scan_csv(path, **kwargs)

        if suffix == ".parquet":
            return pl.scan_parquet(path, **kwargs)

        raise ValueError(f"Lazy scanning not supported for: {suffix}")
