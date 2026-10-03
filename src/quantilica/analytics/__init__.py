"""Analytical data processing and conversion layer for Quantilica."""

from importlib.metadata import PackageNotFoundError, version

from .geo import (
    DIM_GEO_CONTRACT,
    DIM_UF_CONTRACT,
    IBGE_DV_EXCEPTIONS,
    calc_ibge_dv,
    get_dim_geo,
    get_dim_uf,
    mun_6_to_7,
    mun_7_to_6,
)
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
    "DIM_GEO_CONTRACT",
    "DIM_UF_CONTRACT",
    "DataContract",
    "Field",
    "IBGE_DV_EXCEPTIONS",
    "SmartReader",
    "__version__",
    "calc_ibge_dv",
    "get_dim_geo",
    "get_dim_uf",
    "manifest_to_metadata",
    "mun_6_to_7",
    "mun_7_to_6",
    "normalize_brazilian_numbers",
    "read_brazilian_csv",
    "to_parquet",
]
