"""Fixed-width file layout parsing and DuckDB SQL query generator."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class FixedField:
    """Definition of a single fixed-width field."""

    name: str
    start: int  # 1-based start position
    width: int
    kind: Literal["str", "int", "decimal"] = "str"
    scale: int = 0


def layout_from_datasus(text: str) -> list[FixedField]:
    """Parse DATASUS layout table (e.g. SIGTAP *_layout.txt).

    Expected columns: Coluna,Tamanho,Inicio,Fim,Tipo (comma or semicolon separated).
    """
    fields: list[FixedField] = []
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    for line in lines:
        parts = [p.strip() for p in re.split(r"[,;]", line)]
        if not parts:
            continue
        first_lower = parts[0].lower()
        if first_lower in ("coluna", "nome", "campo"):
            continue
        if len(parts) >= 5:
            name = parts[0].lower()
            width = int(parts[1])
            start = int(parts[2])
            tipo = parts[4].upper()
            scale = 0
            if (
                "DEC" in tipo
                or "VAL" in tipo
                or "MOEDA" in tipo
                or name.startswith("vl_")
                or name.startswith("val_")
            ):
                kind = "decimal"
                scale = 2
            elif "NUM" in tipo or "INT" in tipo:
                kind: Literal["str", "int", "decimal"] = "int"
            else:
                kind = "str"
            fields.append(
                FixedField(name=name, start=start, width=width, kind=kind, scale=scale)
            )
    return fields


def layout_from_sas_input(text: str) -> list[FixedField]:
    """Parse SAS INPUT statement layout (e.g. IBGE / PNAD).

    Format: @pos NOME $w. or @pos NOME w. or @pos NOME w.d
    """
    fields: list[FixedField] = []
    pattern = re.compile(
        r"@(?P<pos>\d+)\s+(?P<name>\w+)\s+(?P<is_char>\$)?(?P<width>\d+)(?:\.(?P<scale>\d+)?)?"
    )
    for match in pattern.finditer(text):
        start = int(match.group("pos"))
        name = match.group("name").lower()
        width = int(match.group("width"))
        is_char = bool(match.group("is_char"))
        scale_str = match.group("scale")
        scale = int(scale_str) if scale_str else 0
        if is_char:
            kind: Literal["str", "int", "decimal"] = "str"
        elif scale > 0:
            kind = "decimal"
        else:
            kind = "int"
        fields.append(
            FixedField(name=name, start=start, width=width, kind=kind, scale=scale)
        )
    return fields


def fixed_width_sql(
    path: str, layout: list[FixedField], *, encoding: str = "utf-8"
) -> str:
    """Generate a DuckDB SQL SELECT query that extracts fields from a fixed file."""
    select_items = []
    for f in layout:
        raw_expr = f"substr(column0, {f.start}, {f.width})"
        trimmed = f"trim({raw_expr})"
        if f.kind == "int":
            expr = f"TRY_CAST(NULLIF({trimmed}, '') AS BIGINT)"
        elif f.kind == "decimal":
            if f.scale > 0:
                divisor = 10**f.scale
                expr = f"TRY_CAST(NULLIF({trimmed}, '') AS DOUBLE) / {divisor}"
            else:
                expr = f"TRY_CAST(NULLIF({trimmed}, '') AS DOUBLE)"
        else:
            expr = trimmed
        select_items.append(f'{expr} AS "{f.name}"')

    projections = ",\n    ".join(select_items)
    path_lit = "'" + str(path).replace("'", "''") + "'"
    enc_lit = "'" + encoding.replace("'", "''") + "'"
    return (
        f"SELECT\n    {projections}\n"
        f"FROM read_csv({path_lit}, header=false, delim=NULL, "
        f"all_varchar=true, encoding={enc_lit})"
    )
