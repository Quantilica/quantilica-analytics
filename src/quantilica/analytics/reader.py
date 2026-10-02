"""Smart file reading integrated with Quantilica manifests."""

from __future__ import annotations

import codecs
import re
from pathlib import Path
from typing import Any, Literal

import polars as pl
from quantilica.core.manifests import DownloadManifest

from .schema import DEFAULT_NULL_SENTINELS

DEFAULT_BR_NA = DEFAULT_NULL_SENTINELS
"""Default sentinel strings treated as null in Brazilian public-data CSVs.

Kept as an alias of :data:`quantilica.analytics.schema.DEFAULT_NULL_SENTINELS`
for backwards compatibility with the 0.1.x/0.2.x import surface.
"""

_FALLBACK_ENCODING = "latin-1"


def _strip_thousands(value: str) -> str:
    """Strip thousand separators so only the decimal marker remains.

    Dots are removed because they are the canonical Brazilian thousand
    separator (``1.234,56``). Grouping spaces (regular, U+00A0 and U+202F)
    and spreadsheet apostrophes are stripped too. Accounting sign variants
    are folded: parenthesized negatives ``(1.234,56)``, trailing-sign
    negatives ``1.234,56-`` and leading ``+`` become a leading ``-`` (or are
    removed). The decimal comma is swapped for a dot at the end.
    """
    value = value.strip()

    negative = False
    if value.startswith("(") and value.endswith(")"):
        negative = True
        value = value[1:-1].strip()
    if value.endswith("-"):
        negative = True
        value = value[:-1].strip()
    if value.startswith("-"):
        negative = True
        value = value[1:].strip()
    if value.startswith("+"):
        value = value[1:].strip()

    value = value.replace(".", "").replace(",", ".")
    value = (
        value.replace(" ", "")
        .replace("\u00a0", "")
        .replace("\u202f", "")
        .replace("'", "")
    )
    return f"-{value}" if negative and value else value


_GROUPED_NUMBER_RE = re.compile(r"^-?\d{1,3}(?:\.\d{3})+$")

_SENTINEL_KEYS = frozenset(
    sentinel.lower().replace(".", "").replace(",", ".").replace(" ", "")
    for sentinel in DEFAULT_NULL_SENTINELS
)


def _clean_number_text(value: str | None) -> str:
    """Rewrite one raw cell from Brazilian to canonical float text.

    The dot is ambiguous in the wild: ``1.234,56`` is a thousand-separated
    Brazilian number while ``1234.56`` is already canonical float text.
    Both shapes survive: dots are removed only when (a) a decimal comma is
    present (the canonical Brazilian format) or (b) the value matches a
    ``1.234``-style thousand grouping. Values matching the canonical null
    sentinels (case-insensitively) normalize to an empty string, which the
    caller's strictness-free cast turns into null.
    """
    if value is None:
        return ""

    text = value.strip()

    if "," in text or _GROUPED_NUMBER_RE.match(text):
        text = _strip_thousands(text)
    else:
        # Grouping spaces/apostrophes only, keeping the dot as the decimal
        # marker; sign folding stays in _strip_thousands.
        for sep in (" ", "\u00a0", "\u202f", "'"):
            text = text.replace(sep, "")

    if text.lower() in _SENTINEL_KEYS or text in _SENTINEL_KEYS:
        return ""
    return text


def _normalize_number_column(column: pl.String) -> pl.String:
    """Rewrite one String column from Brazilian to canonical float text.

    The acceptable shapes (``1.234,56``, ``1234,56``, ``1234.56``, sentinels)
    are folded by :func:`_clean_number_text`; the caller then performs the
    strictness-free Float64 cast.
    """
    return column.map_elements(_clean_number_text, return_dtype=pl.String)


def _to_float64(df: pl.DataFrame, name: str) -> pl.DataFrame:
    """Return ``df`` with column ``name`` safely cast to Float64.

    Text columns are normalized (thousand separators removed, commas turned
    into dots) and cast with ``strict=False`` so unparsable values become
    null. Columns that are already Float64 are kept as is; other non-string
    dtypes (including Decimal) go through their string representation first,
    which keeps Decimal-scale integers exactly representable.
    """
    if name not in df.columns:
        raise ValueError(f"Column not found in DataFrame: {name!r}")

    series = df.get_column(name)
    if series.dtype == pl.Float64:
        return df
    if series.dtype == pl.String:
        return df.with_columns(
            _normalize_number_column(series).cast(pl.Float64, strict=False)
        )
    return df.with_columns(
        _normalize_number_column(series.cast(pl.String)).cast(
            pl.Float64,
            strict=False,
        )
    )


def normalize_brazilian_numbers(
    df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    """Convert Brazilian-formatted numeric columns to :class:`pl.Float64`.

    Brazilian public data renders numbers as ``1.234,56`` (dot thousands,
    comma decimals). This function rewrites those columns to canonical float
    text (thousand separators removed, commas replaced by dots) and casts to
    :class:`pl.Float64`. Values that remain unparsable after normalization
    become null (``strict=False``), which mirrors CSV reading of sentinel
    tokens such as ``"N/D"`` or ``"-"``.

    Args:
        df (pl.DataFrame): The DataFrame whose columns are to be normalized.
        columns (list[str]): The names of the columns to convert.

    Returns:
        pl.DataFrame: A new DataFrame with the selected columns as Float64.
            Columns already stored as Float64 are passed through unchanged;
            other dtypes are cast through their string representation first.

    Raises:
        ValueError: If a requested column is not present in the DataFrame.
    """
    result = df
    for name in columns:
        result = _to_float64(result, name)
    return result


def _encoding_candidates(encoding: str) -> list[str]:
    """Return the concrete encodings to try for decoding, in order.

    The requested encoding is always attempted first, followed by latin-1.
    A latin-1-family request returns only latin-1 (it decodes every byte
    sequence, so a fallback could never trigger). Unknown, unregistered
    encodings are returned as requested, without guessing. Encodings are
    compared through :func:`codecs.lookup` names so aliases (``latin1``,
    ``iso-8859-1``, ``LATIN-1``) all resolve to the same family.
    """
    try:
        canonical = codecs.lookup(encoding).name
    except (LookupError, TypeError):
        return [encoding, _FALLBACK_ENCODING]

    latin1 = codecs.lookup(_FALLBACK_ENCODING).name
    if canonical == latin1:
        return [_FALLBACK_ENCODING]
    return [encoding, _FALLBACK_ENCODING]


def _rewind_source(source: Any) -> Any:
    """Best-effort rewind of a consumable source between read attempts."""
    seek = getattr(source, "seek", None)
    if seek is not None:
        try:
            seek(0)
        except (OSError, ValueError):  # pragma: no cover - exotic files
            pass
    return source


def _read_csv_polars_with_fallback(source: Any, **kwargs: Any) -> pl.DataFrame:
    """Read a CSV with polars, retrying with latin-1 on decoding errors.

    Polars' native reader only understands ``utf8``/``utf8-lossy``; other
    encodings are decoded in Python first and fail with
    :class:`UnicodeDecodeError`, which drives the fallback. Consumable
    sources are rewound before each retry so the fallback sees the whole
    payload.
    """
    encoding = str(kwargs.pop("encoding", "utf8"))
    candidates = _encoding_candidates(encoding)
    last_error: UnicodeDecodeError | None = None

    for index, candidate in enumerate(candidates):
        attempt = dict(kwargs)
        attempt["encoding"] = candidate
        active = _rewind_source(source) if index else source
        try:
            return pl.read_csv(active, **attempt)
        except UnicodeDecodeError as error:
            last_error = error
    raise last_error  # type: ignore[misc]  # pragma: no cover - unreachable


def _read_csv_pandas_with_fallback(
    source: Any,
    pandas_module: Any,
    **kwargs: Any,
) -> Any:
    """Read a CSV with ``pandas_module`` (pandas), retrying with latin-1."""
    encoding = str(kwargs.pop("encoding", "utf8"))
    candidates = _encoding_candidates(encoding)
    last_error: UnicodeDecodeError | None = None

    for index, candidate in enumerate(candidates):
        attempt = dict(kwargs)
        attempt["encoding"] = candidate
        active = _rewind_source(source) if index else source
        try:
            return pandas_module.read_csv(active, **attempt)
        except UnicodeDecodeError as error:
            last_error = error
    raise last_error  # type: ignore[misc]  # pragma: no cover - unreachable


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

    The requested encoding is tried first; when decoding fails with
    :class:`UnicodeDecodeError` the latin-1 fallback is attempted. File-like
    sources are rewound between attempts. Files in the latin-1 family are
    read directly since latin-1 decodes every byte sequence.

    Args:
        source (Any): The source CSV file. May be a path, bytes, or file-like object.
        engine (Literal["polars", "pandas"], optional): The processing engine to use.
            Defaults to "polars".
        separator (str, optional): The column separator. Defaults to ";".
        decimal (str, optional): The decimal separator. Defaults to ",".
        encoding (str, optional): The file encoding. Defaults to "latin-1".
        na_values (list[str] | None, optional): A list of strings to interpret as
            missing values. Defaults to None, which uses `DEFAULT_BR_NA`.
        **kwargs (Any): Extra keyword arguments forwarded to the underlying reader.

    Returns:
        Any: A ``polars.DataFrame`` (default) or ``pandas.DataFrame`` when
            ``engine="pandas"``.

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
        return _read_csv_polars_with_fallback(source, **kwargs)

    if engine == "pandas":
        import pandas as pd

        kwargs.setdefault("sep", separator)
        kwargs.setdefault("decimal", decimal)
        kwargs.setdefault("encoding", encoding)
        kwargs.setdefault("na_values", na)
        return _read_csv_pandas_with_fallback(source, pd, **kwargs)

    raise ValueError(f"Unknown engine: {engine!r}")


class SmartReader:
    """A reader that understands Quantilica manifests and optimizes Polars loading."""

    def __init__(self, default_encoding: str = "utf-8"):
        """Initialize the SmartReader.

        Args:
            default_encoding (str, optional): The default encoding to use if not
                specified. Defaults to "utf-8".
        """
        self.default_encoding = default_encoding

    def read(
        self,
        path_or_manifest: str | Path | DownloadManifest,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """Read a file into a Polars DataFrame, optionally guided by a manifest.

        Args:
            path_or_manifest (str | Path | DownloadManifest): The file path or a
                download manifest pointing to the file.
            **kwargs (Any): Extra keyword arguments forwarded to the underlying polars
                reader.

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
            return _read_csv_polars_with_fallback(path, **kwargs)

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
            path_or_manifest (str | Path | DownloadManifest): The file path or a
                download manifest pointing to the file.
            **kwargs (Any): Extra keyword arguments forwarded to the underlying polars
                scanner.

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
