"""Canonical geographic dimension (municipalities and federative units).

Provides the module-level contracts for the ``dim_geo`` and ``dim_uf``
datasets, IBGE municipality code converters between 6 and 7 digit forms
(checked digit per IBGE Módulo 10), and accessors for the bundled
municipality dimension.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .schema import DataContract, Field

__all__ = [
    "DIM_GEO_CONTRACT",
    "DIM_UF_CONTRACT",
    "IBGE_DV_EXCEPTIONS",
    "calc_ibge_dv",
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
