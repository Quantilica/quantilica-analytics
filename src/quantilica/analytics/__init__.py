"""Analytical data processing and conversion layer for Quantilica."""

from importlib.metadata import PackageNotFoundError, version

from .reader import DEFAULT_BR_NA, SmartReader, read_brazilian_csv
from .schema import DataContract, Field
from .writer import to_parquet

try:
    __version__ = version("quantilica-analytics")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "DEFAULT_BR_NA",
    "DataContract",
    "Field",
    "SmartReader",
    "__version__",
    "read_brazilian_csv",
    "to_parquet",
]
