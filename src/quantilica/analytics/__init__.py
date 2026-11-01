"""Analytical data processing and conversion layer for Quantilica."""

from importlib.metadata import PackageNotFoundError, version

from .fixed_width import (
    FixedField,
    fixed_width_sql,
    layout_from_datasus,
    layout_from_sas_input,
)
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
from .lake import (
    ConvertResult,
    DatasetSpec,
    LakeError,
    LakeWriter,
    LakeWriteResult,
    SpecLakeConverter,
    check_dataset,
    convert_with_specs,
    detect_encoding,
    duckdb_view_sql,
    scan_dataset,
)
from .lake_audit import AuditFinding, AuditReport, lake_audit
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
    "AuditFinding",
    "AuditReport",
    "ConvertResult",
    "DEFAULT_BR_NA",
    "DEFAULT_NULL_SENTINELS",
    "DIM_GEO_CONTRACT",
    "DIM_UF_CONTRACT",
    "DataContract",
    "DatasetSpec",
    "Field",
    "FixedField",
    "IBGE_DV_EXCEPTIONS",
    "LakeError",
    "LakeWriteResult",
    "LakeWriter",
    "SmartReader",
    "SpecLakeConverter",
    "__version__",
    "calc_ibge_dv",
    "check_dataset",
    "convert_with_specs",
    "detect_encoding",
    "duckdb_view_sql",
    "fixed_width_sql",
    "get_dim_geo",
    "get_dim_uf",
    "lake_audit",
    "layout_from_datasus",
    "layout_from_sas_input",
    "manifest_to_metadata",
    "mun_6_to_7",
    "mun_7_to_6",
    "normalize_brazilian_numbers",
    "read_brazilian_csv",
    "scan_dataset",
    "to_parquet",
]
