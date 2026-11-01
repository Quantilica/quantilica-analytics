"""Canonical geographic dimension and geospatial WKB helpers.

Provides the module-level contracts for the ``dim_geo`` and ``dim_uf``
datasets, IBGE municipality code converters between 6 and 7 digit forms
(checked digit per IBGE Módulo 10), accessors for the bundled
municipality dimension, and helpers for converting GeoJSON geometry to WKB
and extracting bounding boxes for the Lake.
"""

from __future__ import annotations

import json
import struct
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import polars as pl

from .schema import DataContract, Field

__all__ = [
    "BBOX_COLUMNS",
    "DIM_GEO_CONTRACT",
    "DIM_UF_CONTRACT",
    "GEOMETRY_WKB_COLUMN",
    "IBGE_DV_EXCEPTIONS",
    "calc_ibge_dv",
    "extract_wkb_and_bbox",
    "geometry_to_wkb",
    "get_dim_geo",
    "get_dim_uf",
    "mun_6_to_7",
    "mun_7_to_6",
]

IBGE_DV_EXCEPTIONS: frozenset[int] = frozenset(
    {2201919, 2201988, 2202251, 2611533, 3117836, 3152131, 4305871, 5203939, 5203962}
)
"""Official IBGE municipalities whose 7th check digit violates Módulo 10.

These 9 codes are documented historical quirks in the IBGE territorial
registry. For these municipalities, :func:`mun_7_to_6` safely derives the
6-digit code, but :func:`mun_6_to_7` (which computes Módulo 10) produces a
check digit that differs from the official registry.
"""

_IntStr = int | str

DIM_GEO_CONTRACT = DataContract(
    dataset_id="dim_geo",
    version=1,
    fields=[
        Field("ibge_mun_id", pl.Int64, nullable=False),
        Field("ibge_mun_id_6", pl.Int64, nullable=False),
        Field("mun_nome", pl.String, nullable=False),
        Field("uf_sigla", pl.String, nullable=False),
        Field("uf_nome", pl.String, nullable=False),
        Field("ibge_uf_id", pl.Int64, nullable=False),
        Field("regiao", pl.String, nullable=False),
    ],
)

DIM_UF_CONTRACT = DataContract(
    dataset_id="dim_uf",
    version=1,
    fields=[
        Field("uf_sigla", pl.String, nullable=False),
        Field("uf_nome", pl.String, nullable=False),
        Field("ibge_uf_id", pl.Int64, nullable=False),
        Field("regiao", pl.String, nullable=False),
    ],
)

_DATA_DIR = Path(__file__).parent / "data"
_DIM_GEO_PATH = _DATA_DIR / "dim_geo.parquet"

_UF_COLUMNS = ["uf_sigla", "uf_nome", "ibge_uf_id", "regiao"]


def _validate_digits(value: _IntStr, expected: int, label: str) -> str:
    """Validate that *value* has exactly *expected* numeric digits.

    Args:
        value (int | str): The code to validate. Integers must have exactly
            ``expected`` digits in their natural decimal representation;
            strings must contain only ASCII digits.
        expected (int): The exact number of digits required.
        label (str): Human-readable parameter name used in error messages.

    Returns:
        str: The canonical digit string.

    Raises:
        ValueError: If the value is out of range or contains non-digit
            characters, or does not have exactly ``expected`` digits.
    """
    if isinstance(value, int):
        if not 10 ** (expected - 1) <= value < 10**expected:
            raise ValueError(
                f"{label} must have exactly {expected} digits, got {value!r}"
            )
        return str(value)

    text = str(value)
    if not text.isascii() or not text.isdigit():
        raise ValueError(f"{label} must contain only digits, got {value!r}")
    if len(text) != expected:
        raise ValueError(f"{label} must have exactly {expected} digits, got {text!r}")
    return text


def calc_ibge_dv(code_6: _IntStr) -> int:
    """Compute the IBGE Módulo 10 check digit for a 6-digit municipality code.

    Weights ``[1, 2, 1, 2, 1, 2]`` are applied left to right; products
    greater than or equal to 10 are reduced by summing their digits
    (equivalent to subtracting 9). If the sum's remainder over 10 is 0 the
    digit is 0, otherwise it is ``10 - remainder``.

    Args:
        code_6 (int | str): Municipality code without the check digit.

    Returns:
        int: The check digit (0-9).

    Raises:
        ValueError: If ``code_6`` does not have exactly 6 numeric digits.
    """
    digits = _validate_digits(code_6, 6, "code_6")
    total = 0
    for index, char in enumerate(digits):
        product = int(char) * (1 if index % 2 == 0 else 2)
        total += product - 9 if product >= 10 else product
    remainder = total % 10
    return 0 if remainder == 0 else 10 - remainder


def mun_6_to_7(code_6: _IntStr) -> int:
    """Convert a 6-digit municipality code to the full 7-digit IBGE code.

    The 6-digit form (used by DATASUS, without the check digit) gains the
    Módulo 10 check digit as the seventh digit.

    Args:
        code_6 (int | str): Municipality code without the check digit.

    Returns:
        int: The complete 7-digit IBGE code.

    Raises:
        ValueError: If ``code_6`` does not have exactly 6 numeric digits.
    """
    return int(_validate_digits(code_6, 6, "code_6")) * 10 + calc_ibge_dv(code_6)


def mun_7_to_6(code_7: _IntStr) -> int:
    """Convert a 7-digit municipality code to the 6-digit form.

    Args:
        code_7 (int | str): Complete IBGE code with check digit.

    Returns:
        int: The 6-digit code without the check digit.

    Raises:
        ValueError: If ``code_7`` does not have exactly 7 numeric digits.
    """
    return int(_validate_digits(code_7, 7, "code_7")) // 10


def get_dim_geo() -> pl.DataFrame:
    """Load the canonical municipality dimension.

    Returns:
        pl.DataFrame: One row per official Brazilian municipality (>= 5570)
            with columns ``ibge_mun_id`` (7-digit code with DV),
            ``ibge_mun_id_6``, ``mun_nome``, ``uf_sigla``, ``uf_nome``,
            ``ibge_uf_id`` and ``regiao``.

    Raises:
        ValueError: If the bundled parquet violates :data:`DIM_GEO_CONTRACT`.
        FileNotFoundError: If the bundled parquet is missing.
    """
    if not _DIM_GEO_PATH.is_file():
        raise FileNotFoundError(f"dim_geo.parquet not found at {_DIM_GEO_PATH}")
    df = pl.read_parquet(_DIM_GEO_PATH)
    DIM_GEO_CONTRACT.validate(df)
    return df


def get_dim_uf() -> pl.DataFrame:
    """Derive the federative unit dimension from the municipality dimension.

    Returns:
        pl.DataFrame: 27 rows (UF + DF) with columns ``uf_sigla``,
            ``uf_nome``, ``ibge_uf_id`` and ``regiao``, deduplicated and
            ordered by ``ibge_uf_id``.

    Raises:
        ValueError: If the derived table violates :data:`DIM_UF_CONTRACT`.
    """
    df = get_dim_geo().select(_UF_COLUMNS).unique().sort("ibge_uf_id")
    DIM_UF_CONTRACT.validate(df)
    return df


# --- Geospatial WKB and Bounding Box Lake Helpers (Dimensão L5) ---

GEOMETRY_WKB_COLUMN = "geom_wkb"
"""Nome canônico da coluna binária WKB para geometrias no Lake."""

BBOX_COLUMNS = ("bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy")
"""Nomes canônicos das colunas escalares de bounding box no Lake."""


def _coord_to_wkb_point(coords: list[Any] | tuple[Any, ...]) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(coords[0]), float(coords[1]))


def _coords_to_wkb_linestring(coords: list[Any]) -> bytes:
    header = struct.pack("<BII", 1, 2, len(coords))
    pts = b"".join(struct.pack("<dd", float(c[0]), float(c[1])) for c in coords)
    return header + pts


def _coords_to_wkb_polygon(rings: list[Any]) -> bytes:
    parts = [struct.pack("<BII", 1, 3, len(rings))]
    for ring in rings:
        parts.append(struct.pack("<I", len(ring)))
        parts.append(
            b"".join(struct.pack("<dd", float(c[0]), float(c[1])) for c in ring)
        )
    return b"".join(parts)


def _coords_to_wkb_multipoint(coords: list[Any]) -> bytes:
    parts = [struct.pack("<BII", 1, 4, len(coords))]
    for c in coords:
        parts.append(_coord_to_wkb_point(c))
    return b"".join(parts)


def _coords_to_wkb_multilinestring(lines: list[Any]) -> bytes:
    parts = [struct.pack("<BII", 1, 5, len(lines))]
    for line in lines:
        parts.append(_coords_to_wkb_linestring(line))
    return b"".join(parts)


def _coords_to_wkb_multipolygon(polys: list[Any]) -> bytes:
    parts = [struct.pack("<BII", 1, 6, len(polys))]
    for poly in polys:
        parts.append(_coords_to_wkb_polygon(poly))
    return b"".join(parts)


def geometry_to_wkb(geom: Mapping[str, Any] | None) -> bytes | None:
    """Converte um dicionário de geometria GeoJSON para bytes WKB (little-endian).

    Suporta Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon.
    Retorna None se a geometria for nula, inválida ou de tipo não suportado.
    """
    if not geom or not isinstance(geom, Mapping):
        return None
    gtype = str(geom.get("type", "")).strip()
    coords = geom.get("coordinates")
    if coords is None:
        return None

    if gtype == "Point":
        return _coord_to_wkb_point(coords)
    if gtype == "LineString":
        return _coords_to_wkb_linestring(coords)
    if gtype == "Polygon":
        return _coords_to_wkb_polygon(coords)
    if gtype == "MultiPoint":
        return _coords_to_wkb_multipoint(coords)
    if gtype == "MultiLineString":
        return _coords_to_wkb_multilinestring(coords)
    if gtype == "MultiPolygon":
        return _coords_to_wkb_multipolygon(coords)
    return None


def _calc_bbox(
    geom: Mapping[str, Any] | None,
) -> tuple[float | None, float | None, float | None, float | None]:
    """Calcula (minx, miny, maxx, maxy) para uma geometria GeoJSON."""
    if not geom or not isinstance(geom, Mapping):
        return (None, None, None, None)

    coords = geom.get("coordinates")
    if not coords:
        return (None, None, None, None)

    def _extract_all_points(data: Any) -> list[tuple[float, float]]:
        if not isinstance(data, (list, tuple)) or not data:
            return []
        if isinstance(data[0], (int, float)) and len(data) >= 2:
            return [(float(data[0]), float(data[1]))]
        pts = []
        for item in data:
            pts.extend(_extract_all_points(item))
        return pts

    pts = _extract_all_points(coords)
    if not pts:
        return (None, None, None, None)

    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return (min(xs), min(ys), max(xs), max(ys))


def extract_wkb_and_bbox(
    features_or_path: str | Path | Mapping[str, Any] | Iterable[dict[str, Any]],
    *,
    target_epsg: int = 4674,
) -> pl.DataFrame:
    """Extrai feições GeoJSON para DataFrame com geom_wkb e colunas escalares de BBox.

    Permite indexar e consultar dados espaciais no Parquet Lake sem depender de PostGIS.
    """
    feature_list: list[dict[str, Any]] = []

    if isinstance(features_or_path, (str, Path)):
        p = Path(features_or_path)
        with open(p, "rb") as f:
            data = json.load(f)
        if isinstance(data, dict):
            feature_list = data.get("features", [])
    elif isinstance(features_or_path, Mapping):
        feature_list = features_or_path.get("features", [])
    elif isinstance(features_or_path, Iterable):
        feature_list = list(features_or_path)

    rows: list[dict[str, Any]] = []
    for feat in feature_list:
        if not isinstance(feat, dict):
            continue
        props = dict(feat.get("properties") or {})
        geom = feat.get("geometry")
        wkb = geometry_to_wkb(geom)
        minx, miny, maxx, maxy = _calc_bbox(geom)

        props[GEOMETRY_WKB_COLUMN] = wkb
        props[BBOX_COLUMNS[0]] = minx
        props[BBOX_COLUMNS[1]] = miny
        props[BBOX_COLUMNS[2]] = maxx
        props[BBOX_COLUMNS[3]] = maxy
        rows.append(props)

    if not rows:
        schema = {
            GEOMETRY_WKB_COLUMN: pl.Binary,
            BBOX_COLUMNS[0]: pl.Float64,
            BBOX_COLUMNS[1]: pl.Float64,
            BBOX_COLUMNS[2]: pl.Float64,
            BBOX_COLUMNS[3]: pl.Float64,
        }
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(rows)
    return df
