"""Analytical data processing and conversion layer for Quantilica."""

from importlib.metadata import PackageNotFoundError, version

from .reader import (
    DEFAULT_BR_NA,
    SmartReader,
    normalize_brazilian_numbers,
    read_brazilian_csv,
)
from .schema import DEFAULT_NULL_SENTINELS, DataContract, Field
from .writer import manifest_to_metadata, to_parquet

try:
    __version__ = version("quantilica-analytics")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "DEFAULT_BR_NA",
    "DEFAULT_NULL_SENTINELS",
    "DataContract",
    "Field",
    "SmartReader",
    "__version__",
    "manifest_to_metadata",
    "normalize_brazilian_numbers",
    "read_brazilian_csv",
    "to_parquet",
]
