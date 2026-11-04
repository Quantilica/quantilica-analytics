"""Leitura de arquivos dBase (``.dbf``) e DBC do DATASUS, sem dependências extras.

Os valores saem como texto (strip aplicado, vazio vira nulo), como nas demais
leituras do lake; a tipagem fica a cargo do contrato. Campos memo não são lidos.
"""

from __future__ import annotations

import struct
import tempfile
from pathlib import Path

import polars as pl

# Byte "language driver" do cabeçalho dBase -> codec do Python.
_LDID_CODECS = {
    0x01: "cp437",
    0x02: "cp850",
    0x03: "cp1252",
    0x57: "cp1252",
    0x58: "cp1252",
    0x59: "cp1252",
    0x64: "cp852",
    0x65: "cp866",
    0xC8: "cp1250",
    0xC9: "cp1251",
}
_DEFAULT_CODEC = "latin-1"


def read_dbf(path: str | Path, *, encoding: str | None = None) -> pl.DataFrame:
    """Read a dBase file into a DataFrame of text columns.

    ``encoding`` defaults to the codec the header declares (``latin-1`` when it
    declares none). Column names are lower-cased; deleted records are dropped.
    """
    data = Path(path).read_bytes()
    if len(data) < 32:
        raise ValueError(f"{Path(path).name}: not a dBase file (too short)")
    n_records, header_len, record_len = struct.unpack("<IHH", data[4:12])
    codec = encoding or _LDID_CODECS.get(data[29], _DEFAULT_CODEC)

    fields: list[tuple[str, int]] = []
    pos = 32
    while pos + 32 <= len(data) and data[pos] != 0x0D:
        name = data[pos : pos + 11].split(b"\x00", 1)[0].decode(codec).strip().lower()
        fields.append((name, data[pos + 16]))
        pos += 32
    if not fields:
        raise ValueError(f"{Path(path).name}: dBase header declares no fields")

    columns: dict[str, list[str | None]] = {name: [] for name, _ in fields}
    for i in range(n_records):
        start = header_len + i * record_len
        record = data[start : start + record_len]
        if len(record) < record_len:
            break
        if record[:1] == b"*":  # deleted
            continue
        offset = 1
        for name, width in fields:
            text = record[offset : offset + width].decode(codec, errors="replace")
            columns[name].append(text.strip() or None)
            offset += width
    return pl.DataFrame(columns, schema={name: pl.String for name, _ in fields})


def read_dbc(path: str | Path, *, encoding: str | None = None) -> pl.DataFrame:
    """Read a DATASUS ``.dbc`` (a DBF compressed with PKWare blast).

    Needs ``datasus-dbc`` or ``pyreaddbc``; without either, raises an
    :class:`ImportError` that says what to install.
    """
    src = Path(path)
    with tempfile.TemporaryDirectory() as tmp:
        dst = Path(tmp) / (src.stem + ".dbf")
        try:
            import datasus_dbc

            datasus_dbc.decompress(str(src), str(dst))
        except ImportError:
            try:
                import pyreaddbc

                pyreaddbc.dbc2dbf(str(src), str(dst))
            except ImportError:
                raise ImportError(
                    f"{src.name}: reading .dbc files needs the 'datasus-dbc' "
                    "(or 'pyreaddbc') package; install it with "
                    "`uv add datasus-dbc`"
                ) from None
        return read_dbf(dst, encoding=encoding)
