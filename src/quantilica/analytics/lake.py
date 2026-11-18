"""Writers, conformance checker and readers for the Hive-partitioned lake.

The layout contract lives in :mod:`quantilica.core.lake`; this module holds
the pieces that need heavy dependencies:

* :class:`LakeWriter` converts a raw file into ``<file_key>[-NNN].parquet``
  inside its Hive partition, with two engines: DuckDB (streaming CSV/TXT,
  ZIP and 7z, any encoding) and Polars (DataFrame/LazyFrame).
* :func:`check_dataset` verifies a dataset against the contract, including
  that DuckDB, Polars and PyArrow read the same rows and partition types.
* :func:`scan_dataset` and :func:`duckdb_view_sql` read a dataset back.
"""

from __future__ import annotations

import atexit
import codecs
import errno
import fnmatch
import gzip
import json
import logging
import math
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
import zipfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

import polars as pl
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
from quantilica.core.lake import (
    DESCRIPTOR_FILENAME,
    HIVE_DEFAULT_PARTITION,
    LAYOUT_VERSION,
    DatasetDescriptor,
    dataset_lock,
    file_key,
    partition_dir,
    partition_types_for,
)
from quantilica.core.manifests import read_manifest_sidecar, trusted_sha256

from quantilica.analytics.schema import DataContract
from quantilica.analytics.writer import manifest_to_metadata

try:
    __version__ = version("quantilica-analytics")
except PackageNotFoundError:  # pragma: no cover - source checkout without install
    __version__ = "0.0.0"

logger = logging.getLogger("quantilica.analytics.lake")


def sha256_file(path: str | os.PathLike[str]) -> str:
    """SHA-256 of a raw file, reusing the digest of its sidecar when it is trusted.

    Every write path of :class:`LakeWriter` hashes its raw input through this
    module-level name, which delegates to
    :func:`quantilica.core.manifests.trusted_sha256`: with a valid
    ``<file>.manifest.json`` whose size and mtime agree with the file, the bytes
    are not read (they may live on a network mount, where reading them on every
    build means downloading the source again). It stays a plain function of this
    module, looked up at call time, so that callers can still replace
    ``quantilica.analytics.lake.sha256_file`` to count or cache hashes; the
    audit (``lake_audit``) keeps its own, always-reading hash.
    """
    return trusted_sha256(path)


TMP_DIRNAME = ".tmp"
TMP_SUFFIX = ".tmp"
TEXT_SUFFIXES = frozenset({".csv", ".txt"})
# Suffixes of package members that are known not to be data (documentation,
# office files, images, web pages). Used only when no member has a text suffix.
NON_DATA_SUFFIXES = frozenset(
    {
        ".pdf", ".doc", ".docx", ".odt", ".rtf", ".xls", ".xlsx", ".xlsm",
        ".ods", ".ppt", ".pptx", ".odp", ".htm", ".html", ".xml", ".md",
        ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".svg",
        ".ico", ".db", ".ini", ".lnk", ".log", ".sas", ".sps", ".dta", ".sav",
        ".zip", ".7z", ".rar", ".gz", ".json", ".dbf", ".dbc", ".parquet", ".sql",
        ".md5", ".sha1", ".sha256", ".sig", ".do", ".r", ".py", ".yaml", ".yml",
        ".exe", ".dll", ".bat", ".sh",
    }
)  # fmt: skip
# Member names that are documentation, with or without a suffix (``LEIA-ME``,
# ``leiame.docx``, ``README``, ``Licença``, ``layout_campos``, ``Dicionario``).
_NON_DATA_NAME = re.compile(
    r"^(leia[-_ ]?me|read[-_ ]?me|licen[sc]|layout|dicion[aá]rio)"
)
# Diretório opcional para a extração de ZIP/7z e o temporário do DuckDB. Sem ele,
# ambos ficam no staging do dataset, no mesmo disco do lake.
LAKE_SCRATCH_ENV = "QUANTILICA_LAKE_SCRATCH"
_LATIN1 = frozenset({"latin-1", "latin1", "iso-8859-1", "iso8859-1", "l1"})
_C1_BYTES = re.compile(rb"[\x80-\x9f]")

META_ORIGIN_SHA = "quantilica.origin_sha256"
META_LAYOUT = "quantilica.lake.layout_version"
META_CONTRACT_ID = "quantilica.contract.id"
META_CONTRACT_VERSION = "quantilica.contract.version"
META_CAST_NULLS = "quantilica.cast_nulls"
META_FILE_KEY = "quantilica.file_key"
META_CONVERTED_AT = "quantilica.converted_at"
# Grouped files (one Parquet per partition holding several raw files): the
# footer maps each member's file key to its provenance, and every row carries
# the member's file key in ORIGIN_COLUMN.
META_MEMBERS = "quantilica.members"
META_MEMBER_COLUMNS = "quantilica.member_columns"
ORIGIN_COLUMN = "_origem"
# Metadata that every Parquet in the lake must carry.
REQUIRED_METADATA = (
    META_LAYOUT,
    "quantilica.source_id",
    "quantilica.dataset_id",
    META_FILE_KEY,
    META_CONVERTED_AT,
)

_DATE_IN_NAME = re.compile(r"@[0-9T]+")
_PART_NAME = re.compile(r"^(?P<key>.+?)(?:-(?P<part>\d{3}))?\.parquet$")
_PARTITION_DIR = re.compile(r"^(?P<key>[a-z][a-z0-9_]*)=(?P<value>[0-9A-Za-z._-]+)$")
_DUCKDB_PART_FILE = re.compile(r"(\d+)")


class LakeError(RuntimeError):
    """A raw file cannot be converted, or a dataset violates the contract."""


def _first_value(cursor: Any) -> Any:
    """First column of the single row a DuckDB scalar query returns."""
    row = cursor.fetchone()
    if row is None:
        raise LakeError("DuckDB returned no row for a scalar query")
    return row[0]


def _duckdb_part_number(part: Path) -> int:
    """Numeric index in the name of a part file written by DuckDB."""
    match = _DUCKDB_PART_FILE.search(part.stem)
    if match is None:
        raise LakeError(f"{part.name}: no part number in the file name")
    return int(match.group(1))


@dataclass
class LakeWriteResult:
    """Outcome of a :class:`LakeWriter` call.

    ``status`` is ``"converted"`` or ``"skipped"`` (origin already in the
    lake). ``rows`` is 0 for skipped files, ``outputs`` lists the existing
    parts in that case. ``cast_nulls`` counts, per typed column, the values
    that ``TRY_CAST`` turned into null.
    """

    source_file: Path | None
    outputs: list[Path]
    status: str
    rows: int = 0
    cast_nulls: dict[str, int] = field(default_factory=dict)

    @property
    def output(self) -> Path:
        return self.outputs[0]


@dataclass(frozen=True)
class DuckSource:
    """Rows of a DuckDB ``SELECT``, for :class:`GroupMember` (read in streaming).

    The query runs on the writer's DuckDB connection (its memory limit, thread
    count and temporary directory), so it can read the raw file itself:
    ``DuckSource("SELECT * FROM read_csv('/raw/a.csv', all_varchar=true)")``.
    """

    sql: str


MemberData = pl.DataFrame | pl.LazyFrame | Path | DuckSource


@dataclass(frozen=True)
class GroupMember:
    """One raw file (or one ZIP member) of a group for :meth:`LakeWriter.write_group`.

    Attributes:
        src: The raw file. Its ``sha256`` decides whether the member changed.
        data: The member's rows: a ``DataFrame``, a ``LazyFrame`` (streamed with
            ``sink_parquet``, never collected), a ``Path`` to a Parquet file, a
            :class:`DuckSource`, or a function of no arguments returning any of
            those. The function is called only for a member that has to be
            written, so an expensive decode (a DBC, a ZIP) is skipped for the
            members already in the group.
        key: The member's key in ``quantilica.members`` and in ``_origem``;
            default ``file_key(src)``. Use ``composite_file_key(src, member)``
            for a member of a ZIP.
        columns: The member columns (``{"uf": "sp", "parte": "03"}``), written
            as literal text columns. Every member of a group has the same column
            names, and none of them may be the name of a data column.
        sha256: The digest of ``src`` when the caller already has it.
    """

    src: Path
    data: MemberData | Callable[[], MemberData]
    key: str | None = None
    columns: Mapping[str, str] = field(default_factory=dict)
    sha256: str | None = None


@dataclass
class GroupWriteResult(LakeWriteResult):
    """Outcome of :meth:`LakeWriter.write_group`.

    ``outputs`` lists the file; ``rows`` is the rows written (0 when skipped).
    ``written`` are the keys of the members converted in this call, ``kept``
    those carried over from the existing file, ``removed`` the members that left
    it (the orphans), ``replaced`` the per-raw-file Parquets deleted by
    ``replaces``/``replace_partition`` and ``not_replaced`` the candidates that
    were kept because they failed the safety test (see :meth:`write_group`).
    """

    written: list[str] = field(default_factory=list)
    kept: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    replaced: list[Path] = field(default_factory=list)
    not_replaced: list[Path] = field(default_factory=list)


# Fields of a ``quantilica.members`` entry: a member column cannot take these names.
ENTRY_FIELDS = frozenset(
    {
        "origin_file",
        "origin_sha256",
        "rows",
        "converted_at",
        "contract_version",
        "converter_version",
    }
)


@dataclass
class _GroupState:
    """What the lake holds for a group.

    ``members`` is empty unless the file is ``usable``; ``known`` lists the keys
    its footer names anyway (for ``removed``).
    """

    members: dict[str, dict[str, Any]] = field(default_factory=dict)
    parts: list[Path] = field(default_factory=list)
    usable: bool = True
    known: list[str] = field(default_factory=list)


@dataclass
class _GroupPlan:
    target: Path
    group_key: str
    state: _GroupState
    members: list[tuple[GroupMember, str, str]]  # (member, key, sha256)
    columns: list[str]
    changed: list[tuple[GroupMember, str, str]]
    kept: list[str]  # in the list and current
    carried: list[str]  # not in the list, kept because prune=False
    orphans: list[str]  # not in the list, dropped because prune=True
    blocked: bool = False  # prune=False over a file that cannot be carried over

    @property
    def is_current(self) -> bool:
        return (
            not self.changed
            and not self.orphans
            and not self.blocked
            and bool(self.state.parts)
            and self.state.usable
        )


# --- partition type vocabulary -------------------------------------------------

# Descriptor type name -> (DuckDB type, Polars dtype, Arrow type).
_PARTITION_TYPES: dict[str, tuple[str, Any, pa.DataType]] = {
    "varchar": ("VARCHAR", pl.String, pa.string()),
    "string": ("VARCHAR", pl.String, pa.string()),
    "str": ("VARCHAR", pl.String, pa.string()),
    "int": ("BIGINT", pl.Int64, pa.int64()),
    "integer": ("BIGINT", pl.Int64, pa.int64()),
    "bigint": ("BIGINT", pl.Int64, pa.int64()),
    "int64": ("BIGINT", pl.Int64, pa.int64()),
    "smallint": ("SMALLINT", pl.Int16, pa.int16()),
    "int16": ("SMALLINT", pl.Int16, pa.int16()),
    "double": ("DOUBLE", pl.Float64, pa.float64()),
    "float": ("DOUBLE", pl.Float64, pa.float64()),
    "float64": ("DOUBLE", pl.Float64, pa.float64()),
    "date": ("DATE", pl.Date, pa.date32()),
    "boolean": ("BOOLEAN", pl.Boolean, pa.bool_()),
    "bool": ("BOOLEAN", pl.Boolean, pa.bool_()),
}


def _partition_type(name: str, declared: str) -> tuple[str, Any, pa.DataType]:
    try:
        return _PARTITION_TYPES[declared.strip().lower()]
    except KeyError:
        raise LakeError(
            f"Unknown type {declared!r} for partition key {name!r}; "
            f"expected one of {sorted(_PARTITION_TYPES)}."
        ) from None


def _declared_types(descriptor: DatasetDescriptor) -> dict[str, str]:
    """Declared type per partition key; undeclared keys are ``varchar``."""
    return {
        key: descriptor.partition_types.get(key, "varchar")
        for key in descriptor.partition_keys
    }


# --- DuckDB helpers ---------------------------------------------------------------


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _connect(temp_dir: Path):
    import duckdb

    conn = duckdb.connect()
    conn.execute(f"SET temp_directory = {_lit(str(temp_dir))}")
    memory = os.environ.get("QUANTILICA_DUCKDB_MEMORY")
    if memory:
        conn.execute(f"SET memory_limit = {_lit(memory)}")
    threads = os.environ.get("QUANTILICA_DUCKDB_THREADS")
    if threads:
        conn.execute(f"SET threads = {int(threads)}")
    return conn


def _normalize_name(raw: str) -> str:
    """Lowercase snake_case ASCII name (``"Município (UF)"`` -> ``municipio_uf``)."""
    import unicodedata

    text = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode()
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text).strip("_").lower()
    return text or "col"


def _unique_names(names: list[str]) -> list[str]:
    """Normalize and disambiguate duplicates (``col``, ``col_2``...)."""
    seen: dict[str, int] = {}
    out = []
    for raw in names:
        base = _normalize_name(raw)
        seen[base] = seen.get(base, 0) + 1
        out.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
    return out


def _duckdb_type(dtype: Any) -> str:
    """DuckDB type for a Polars dtype used in a ``DataContract``."""
    simple = {
        pl.Int8: "TINYINT",
        pl.Int16: "SMALLINT",
        pl.Int32: "INTEGER",
        pl.Int64: "BIGINT",
        pl.UInt8: "UTINYINT",
        pl.UInt16: "USMALLINT",
        pl.UInt32: "UINTEGER",
        pl.UInt64: "UBIGINT",
        pl.Float32: "FLOAT",
        pl.Float64: "DOUBLE",
        pl.Boolean: "BOOLEAN",
        pl.Date: "DATE",
        pl.Datetime: "TIMESTAMP",
        pl.Time: "TIME",
        pl.String: "VARCHAR",
    }
    for polars_type, duck in simple.items():
        if dtype == polars_type:
            return duck
    if isinstance(dtype, pl.Decimal) or dtype == pl.Decimal:
        precision = getattr(dtype, "precision", None) or 38
        scale = getattr(dtype, "scale", None) or 0
        return f"DECIMAL({precision},{scale})"
    raise LakeError(f"Contract dtype {dtype} has no DuckDB cast.")


def _try_cast(column: str, duck: str, *, decimal_comma: bool = True) -> str:
    """``TRY_CAST`` of a text column that survives DuckDB's CSV cast pushdown.

    A bare ``TRY_CAST(col AS T)`` over ``read_csv`` is pushed into the reader,
    which then *raises* on the first bad value. ``NULLIF(col, '')`` (a no-op:
    empty fields are already null) keeps the cast in the query plan.
    """
    ident = _ident(column)
    if decimal_comma and duck in ("DOUBLE", "FLOAT", "DECIMAL"):
        return f"TRY_CAST(replace(NULLIF({ident}, ''), ',', '.') AS {duck})"
    return f"TRY_CAST(NULLIF({ident}, '') AS {duck})"


STAGE_PREFIX = "lake-stage-"  # one write's staging directory in the scratch
WORK_PREFIX = "lake-work-"  # extraction / DuckDB temporary directory in the scratch
OWNER_FILE = "owner.json"
STALE_SECONDS = 24 * 3600  # a directory of a dead process this old is removed
ORPHAN_SECONDS = 7 * 24 * 3600  # a directory with no owner file this old is removed
_scratch_configured = False


def _make_scratch_dir(base: Path, prefix: str) -> Path:
    """A fresh directory in the scratch, with an ``owner.json`` (pid and host)."""
    base.mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(dir=base, prefix=prefix))
    (path / OWNER_FILE).write_text(
        json.dumps({"pid": os.getpid(), "hostname": socket.gethostname()})
    )
    return path


def _pid_alive(pid: int) -> bool:
    """Whether ``pid`` is a running process on this host.

    Never uses ``os.kill(pid, 0)`` on Windows: there signal ``0`` is
    ``CTRL_C_EVENT`` and the call sends Ctrl+C to the process group, which
    interrupts the whole ``lake build`` sharing the console.
    """
    if pid <= 0:
        return False
    if os.name == "nt":
        return _pid_alive_windows(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:  # PermissionError: it exists, it is someone else's
        return True
    return True


def _pid_alive_windows(pid: int) -> bool:
    import ctypes
    from ctypes import wintypes

    query_limited_information = 0x1000
    still_active = 259
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)  # pyright: ignore[reportAttributeAccessIssue]  # ctypes.WinDLL só existe no Windows
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    k32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    k32.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = k32.OpenProcess(query_limited_information, False, pid)
    if not handle:
        # Access denied means the process exists (someone else's); anything else
        # (invalid parameter) means there is no such process.
        return ctypes.get_last_error() == 5  # pyright: ignore[reportAttributeAccessIssue]  # idem, só no Windows
    try:
        code = wintypes.DWORD()
        if not k32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return True
        return code.value == still_active
    finally:
        k32.CloseHandle(handle)


def _is_stale(entry: Path, now: float) -> bool:
    """Whether a scratch directory is a dead process's leftover.

    Removed only when it is older than 24 h *and* its ``owner.json`` names a pid
    that no longer exists on this host; a directory of another host, or of a live
    process (a ``COPY`` can run for hours without touching the directory's
    mtime), stays. Without an ``owner.json`` it takes 7 days.
    """
    try:
        age = now - entry.stat().st_mtime
        if age < STALE_SECONDS:
            return False
        try:
            owner = json.loads((entry / OWNER_FILE).read_text())
            pid, host = int(owner["pid"]), str(owner["hostname"])
        except (OSError, ValueError, KeyError, TypeError):
            return age >= ORPHAN_SECONDS
        return host == socket.gethostname() and not _pid_alive(pid)
    except OSError:
        return False


def _remove_stale_scratch(scratch: Path) -> None:
    """Remove the leftovers of dead processes from the scratch directory."""
    if not scratch.is_dir():
        return
    now = time.time()
    for prefix in (STAGE_PREFIX, WORK_PREFIX):
        for entry in scratch.glob(f"{prefix}*"):
            if entry.is_dir() and _is_stale(entry, now):
                shutil.rmtree(entry, ignore_errors=True)
    for entry in scratch.glob("polars-*"):  # POLARS_TEMP_DIR of a dead process
        suffix = entry.name.removeprefix("polars-")
        if not (entry.is_dir() and suffix.isdigit()):
            continue
        # Age first: a fresh directory is never removed, whatever its pid.
        if now - entry.stat().st_mtime < STALE_SECONDS:
            continue
        if not _pid_alive(int(suffix)):
            shutil.rmtree(entry, ignore_errors=True)


def configure_scratch() -> None:
    """Prepare the process for ``QUANTILICA_LAKE_SCRATCH`` (once per process).

    Removes the leftovers of dead processes from the scratch and points Polars'
    spill directory (``POLARS_TEMP_DIR``) at ``<scratch>/polars-<pid>``, removed
    at exit, unless the variable is already set. ``LakeWriter`` calls it when it
    is created. Limitation: Polars reads ``POLARS_TEMP_DIR`` once per process, so
    the setting only takes effect if no spill happened before this call; create
    the first writer before running other Polars work that can spill. DuckDB's
    spill needs no such call: its temporary directory is set on each connection.
    """
    global _scratch_configured
    base = os.environ.get(LAKE_SCRATCH_ENV)
    if not base or _scratch_configured:
        return
    _scratch_configured = True
    scratch = Path(base)
    scratch.mkdir(parents=True, exist_ok=True)
    _remove_stale_scratch(scratch)
    if os.environ.get("POLARS_TEMP_DIR"):
        logger.debug("lake: POLARS_TEMP_DIR is already set; leaving it")
        return
    spill = scratch / f"polars-{os.getpid()}"
    spill.mkdir(parents=True, exist_ok=True)
    os.environ["POLARS_TEMP_DIR"] = str(spill)
    atexit.register(shutil.rmtree, spill, ignore_errors=True)
    logger.debug("lake: Polars spill directory set to %s", spill)


def _scratch_dir(staging: Path) -> Path:
    """Work directory for extraction: a fresh one under ``QUANTILICA_LAKE_SCRATCH``
    when set, else ``staging`` itself."""
    base = os.environ.get(LAKE_SCRATCH_ENV)
    if not base:
        return staging
    return _make_scratch_dir(Path(base), WORK_PREFIX)


def _materialize(src: Path, tmp: Path, members: str | None = None) -> list[Path]:
    """Text files to read: the file itself, or the members of a ZIP/7z.

    A ``.gz`` is decompressed to ``tmp``. ``members`` is a case-insensitive
    glob (``fnmatch``) on the member's path inside the archive; only the
    matching members are read, whatever their suffix. It is an error for it to
    match none. Without ``members`` the data members are chosen by
    :func:`_data_member_names`.
    """
    suffix = src.suffix.lower()
    if suffix in TEXT_SUFFIXES:
        return [src]
    if suffix == ".gz":
        tmp.mkdir(parents=True, exist_ok=True)
        name = src.name[:-3]
        if Path(name).suffix.lower() not in TEXT_SUFFIXES:
            name += ".csv"
        out = tmp / name
        with gzip.open(src, "rb") as reader, open(out, "wb") as writer:
            shutil.copyfileobj(reader, writer, 1 << 24)
        return [out]
    if suffix == ".zip":
        with zipfile.ZipFile(src) as archive:
            entries = [m for m in archive.infolist() if not m.is_dir()]
            archive.extractall(tmp, members=entries)
    elif suffix == ".7z":
        tool = shutil.which("7z") or shutil.which("7zz") or shutil.which("7za")
        if tool is None:
            raise LakeError("7z not found in PATH (required for .7z files)")
        proc = subprocess.run(
            [tool, "e", "-y", "-bd", f"-o{tmp}", str(src)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise LakeError(f"7z failed on {src.name}: {proc.stderr.strip()[-300:]}")
    else:
        raise LakeError(f"Unsupported format: {src.name}")
    extracted = {
        p.relative_to(tmp).as_posix(): p for p in tmp.rglob("*") if p.is_file()
    }
    chosen = _select_member_names(extracted, members, src.name)
    return [extracted[name] for name in chosen]


def _select_member_names(
    names: Iterable[str], members: str | None, package: str | None = None
) -> list[str]:
    """The members of a package to read, in reading order (sorted by path).

    With ``members`` (a case-insensitive ``fnmatch`` glob on the member's path
    or on its base name) the matching members, whatever their suffix; it is an
    error for it to match none. Without it, :func:`_data_member_names`; it is an
    error for it to find none. ``package`` (the archive's name) is for the
    messages; when given, a fallback that picks several members is logged.
    """
    ordered = sorted(names, key=lambda n: PurePosixPath(n).parts)
    label = package or "package"
    if members:
        pattern = members.lower()
        chosen = [
            n
            for n in ordered
            if fnmatch.fnmatchcase(n.lower(), pattern)
            or fnmatch.fnmatchcase(PurePosixPath(n).name.lower(), pattern)
        ]
        if not chosen:
            raise LakeError(f"{label} has no member matching {members!r}")
        return chosen
    chosen = _data_member_names(ordered)
    if not chosen:
        raise LakeError(f"{label} contains no data files")
    if (
        package
        and len(chosen) > 1
        and not any(PurePosixPath(n).suffix.lower() in TEXT_SUFFIXES for n in chosen)
    ):
        logger.warning(
            "lake: %s has no .csv/.txt member; reading %d members by exclusion: %s",
            package,
            len(chosen),
            ", ".join(chosen),
        )
    return chosen


def _data_member_names(names: Iterable[str]) -> list[str]:
    """The names of ``names`` that hold data, for a package without ``members``.

    Members with a text suffix (``.csv``/``.txt``) win. If there is none, the
    data is in members with another (or no) suffix, as in the Receita Federal
    CNPJ files (``K3241.K03200Y0.D60711.EMPRECSV``) or the RAIS ``.COMT``; then
    everything is kept except hidden files, ``__MACOSX`` entries, members with
    a suffix that is known not to be data (see ``NON_DATA_SUFFIXES``) and
    documentation names (``LEIA-ME``, ``README``, ...).
    """
    candidates: list[str] = []
    for name in names:
        path = PurePosixPath(name)
        if path.name.startswith(".") or "__MACOSX" in path.parts:
            continue
        candidates.append(name)
    preferred = [
        n for n in candidates if PurePosixPath(n).suffix.lower() in TEXT_SUFFIXES
    ]
    if preferred:
        return preferred
    return [
        n
        for n in candidates
        if PurePosixPath(n).suffix.lower() not in NON_DATA_SUFFIXES
        and not _NON_DATA_NAME.match(PurePosixPath(n).name.lower())
    ]


def _has_c1_bytes(path: Path) -> bool:
    """Whether ``path`` holds a byte in 0x80-0x9F (a C1 control in Latin-1)."""
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 24):
            if _C1_BYTES.search(chunk):
                return True
    return False


def _latin1_to_utf8(files: list[Path], tmp: Path) -> list[Path]:
    """UTF-8 copies of Latin-1 ``files``.

    DuckDB's ``latin-1`` decoder rejects C1 bytes (0x80-0x9F) with "File is
    not latin-1 encoded", although Latin-1 maps every byte; Python's codec
    reads them as the control characters U+0080-U+009F.
    """
    tmp.mkdir(parents=True, exist_ok=True)
    out = []
    for i, src in enumerate(files):
        dst = tmp / f"{i:04d}-{src.name}"
        with (
            open(src, encoding="latin-1", newline="") as reader,
            open(dst, "w", encoding="utf-8", newline="") as writer,
        ):
            shutil.copyfileobj(reader, writer, 1 << 24)
        out.append(dst)
    return out


def _read_csv_sql(
    files: list[Path],
    *,
    separator: str,
    encoding: str,
    header: bool,
    names: tuple[str, ...],
    quote: str | None = None,
    escape: str | None = None,
    parallel: bool = True,
    skip: int = 0,
) -> str:
    args = [
        "[" + ", ".join(_lit(str(f)) for f in files) + "]",
        f"delim={_lit(separator)}",
        f"header={'true' if header else 'false'}",
        "all_varchar=true",
        f"encoding={_lit(encoding)}",
        "null_padding=true",
        "strict_mode=false",
        "comment=''",
    ]
    if skip > 0:
        args.append(f"skip={skip}")
    if quote is not None:
        args.append(f"quote={_lit(quote)}")
    if escape is not None:
        args.append(f"escape={_lit(escape)}")
    if not parallel:
        args.append("parallel=false")
    if not header:
        args.append("names=[" + ", ".join(_lit(n) for n in names) + "]")
    return f"read_csv({', '.join(args)})"


# --- parquet footer helpers --------------------------------------------------------


def _footer(path: Path) -> dict[str, str]:
    """File-level key/value metadata of a Parquet file (empty on any error).

    Read from the Parquet footer itself: Polars stores custom metadata only
    there, so ``pq.read_schema(...).metadata`` would miss it.
    """
    try:
        return footer_from_metadata(pq.ParquetFile(path).metadata)
    except (OSError, pa.ArrowException):
        return {}


def footer_from_metadata(metadata: pq.FileMetaData) -> dict[str, str]:
    """File-level key/value metadata of an already opened Parquet footer.

    For callers that open the file themselves (the lake audit reads the footer
    and the last row group in one open) and would otherwise parse it twice.
    """
    return {
        k.decode(): v.decode(errors="replace")
        for k, v in (metadata.metadata or {}).items()
    }


def _part_files(directory: Path, key: str) -> list[Path]:
    """Parquet files in ``directory`` that belong to ``key`` (by footer)."""
    if not directory.is_dir():
        return []
    pattern = re.compile(rf"^{re.escape(key)}(?:-(\d{{3}}))?\.parquet$")
    found = []
    for path in sorted(directory.glob("*.parquet")):
        match = pattern.match(path.name)
        if match and _footer(path).get(META_FILE_KEY) == key:
            found.append(path)
    return found


def origin_matches(
    directory: Path,
    key: str,
    sha256: str,
    *,
    layout_version: int = LAYOUT_VERSION,
    contract_version: int | None = None,
) -> bool:
    """Whether ``key`` is already in ``directory`` for this origin.

    True when the existing parts form one complete set (a lone ``<key>.parquet``
    or ``-001..-N`` without gaps) written in a single run, and every part
    records the same ``sha256``, layout version and contract version.
    """
    parts = _part_files(directory, key)
    if not parts:
        return False
    pattern = re.compile(rf"^{re.escape(key)}(?:-(\d{{3}}))?\.parquet$")
    numbers = []
    for p in parts:
        m = pattern.match(p.name)
        numbers.append(m.group(1) if m else None)
    if numbers == [None]:
        pass
    elif None in numbers or [int(n) for n in numbers] != list(
        range(1, len(numbers) + 1)
    ):
        return False
    expected_contract = None if contract_version is None else str(contract_version)
    runs = set()
    for part in parts:
        meta = _footer(part)
        if (
            meta.get(META_ORIGIN_SHA) != sha256
            or meta.get(META_LAYOUT) != str(layout_version)
            or meta.get(META_CONTRACT_VERSION) != expected_contract
        ):
            return False
        runs.add(meta.get(META_CONVERTED_AT))
    return len(runs) == 1


def _is_complete_set(key: str, parts: Sequence[_OriginRecord]) -> bool:
    """Whether footers of ``key`` form a complete set written in a single run.

    The same rule as :func:`origin_matches`, applied to already read footers: a
    lone ``<key>.parquet`` or ``-001..-N`` without gaps, one ``converted_at``.
    """
    pattern = re.compile(rf"^{re.escape(key)}(?:-(\d{{3}}))?\.parquet$")
    numbers = []
    for rec in sorted(parts, key=lambda r: r.path.name):
        m = pattern.match(rec.path.name)
        if m is None:
            return False
        numbers.append(m.group(1))
    if numbers != [None] and (
        None in numbers or [int(n) for n in numbers] != list(range(1, len(numbers) + 1))
    ):
        return False
    return len({rec.converted_at for rec in parts}) == 1


def _members(path: Path, group_key: str) -> dict[str, dict[str, Any]] | None:
    """Members of the grouped file ``path``; ``None`` if it is not ``group_key``."""
    meta = _footer(path)
    if meta.get(META_FILE_KEY) != group_key or META_MEMBERS not in meta:
        return None
    return json.loads(meta[META_MEMBERS])


def _entry_is_current(
    entry: Mapping[str, Any] | None,
    sha256: str,
    *,
    columns: Mapping[str, Any] | None = None,
    contract_version: int | None = None,
    converter_version: str | None = None,
) -> bool:
    """Whether a ``quantilica.members`` entry is up to date for the raw file.

    The digest must match. A version (contract, converter) is compared only
    when the caller passes one: an entry written before versions were recorded
    has none, so it is stale for a caller that passes a version and current for
    one that does not. ``columns`` (the member columns' values) are compared
    when given.
    """
    if entry is None or entry.get("origin_sha256") != sha256:
        return False
    if contract_version is not None and entry.get("contract_version") != (
        contract_version
    ):
        return False
    if converter_version is not None and entry.get("converter_version") != (
        converter_version
    ):
        return False
    return all(entry.get(k) == v for k, v in (columns or {}).items())


def member_matches(
    path: Path,
    group_key: str,
    key: str,
    sha256: str,
    *,
    layout_version: int = LAYOUT_VERSION,
    contract_version: int | None = None,
    converter_version: str | None = None,
) -> bool:
    """Whether member ``key`` is in the grouped file ``path`` with ``sha256``.

    ``key`` is the member's key in ``quantilica.members``: ``file_key(src)``, or
    a composite key (``composite_file_key``) for a member of a ZIP. With
    ``contract_version``/``converter_version`` the recorded versions must match
    too; see :func:`_entry_is_current` for the rule on entries without them.
    """
    if not path.is_file():
        return False
    if _footer(path).get(META_LAYOUT) != str(layout_version):
        return False
    members = _members(path, group_key) or {}
    return _entry_is_current(
        members.get(key),
        sha256,
        contract_version=contract_version,
        converter_version=converter_version,
    )


def _existing_ancestor(path: Path) -> Path:
    while not path.exists() and path != path.parent:
        path = path.parent
    return path


def _same_volume(a: Path, b: Path) -> bool:
    """Whether ``a`` and ``b`` (or their nearest existing ancestors) share a device."""
    try:
        return (
            os.stat(_existing_ancestor(a)).st_dev
            == os.stat(_existing_ancestor(b)).st_dev
        )
    except OSError:
        return False


def _copy_to(tmp: Path, hop_dir: Path) -> Path:
    """Copy ``tmp`` into ``hop_dir`` (flushed to disk); returns the copy's path."""
    hop_dir.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=hop_dir, prefix=".", suffix=TMP_SUFFIX)
    os.close(fd)
    hop = Path(name)
    try:
        shutil.copyfile(tmp, hop)
        with open(hop, "rb+") as fh:
            os.fsync(fh.fileno())
    except BaseException:
        hop.unlink(missing_ok=True)
        raise
    return hop


def _rmdir_if_empty_tmp(directory: Path) -> None:
    """Remove a ``.tmp`` directory that a cross-volume copy left empty."""
    if directory.name == TMP_DIRNAME:
        try:
            directory.rmdir()
        except OSError:
            pass


_CROSS_VOLUME_ERRNOS = frozenset(
    {errno.EXDEV, errno.EINVAL, errno.ENOTSUP, errno.EPERM}
)


def _durable_replace(tmp: Path, final: Path, hop_dir: Path | None = None) -> None:
    """``os.replace`` after flushing ``tmp`` to disk.

    Without the flush, a power loss right after the rename can leave ``final``
    with a truncated body (seen on Windows: "Parquet magic bytes not found"),
    and in a grouped file the old per-member files are already gone.

    When ``tmp`` is on another volume (the scratch directory), the rename is
    not possible: the file is copied next to ``final`` first (into ``hop_dir``,
    which must be on ``final``'s volume; default ``final``'s directory) and the
    rename is done there, so ``final`` is never seen half written. The volumes
    are compared by device before the rename is tried; a rename that fails with
    ``EXDEV``, ``EINVAL``, ``ENOTSUP`` or ``EPERM`` (what a virtual or network
    disk says for "not on the same volume") also falls back to the copy. Any
    other error (a ``PermissionError`` on an open file, ``ENOSPC``) is raised.
    An empty ``.tmp`` directory left by the copy is removed.
    """
    with open(tmp, "rb+") as fh:
        os.fsync(fh.fileno())
    if _same_volume(tmp, final):
        try:
            os.replace(tmp, final)
            return
        except OSError as exc:
            if exc.errno not in _CROSS_VOLUME_ERRNOS:
                raise
    hop_dir = hop_dir or final.parent
    hop = _copy_to(tmp, hop_dir)
    try:
        os.replace(hop, final)
    except BaseException:
        hop.unlink(missing_ok=True)
        raise
    finally:
        _rmdir_if_empty_tmp(hop_dir)
    tmp.unlink(missing_ok=True)


def _kv_options(meta: dict[str, str]) -> str:
    kv = ", ".join(f"{_lit(k)}: {_lit(v)}" for k, v in meta.items())
    return (
        f"FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880, KV_METADATA {{{kv}}}"
    )


def _member_literal(name: str, value: str) -> pl.Expr:
    """Literal text column for a member column."""
    if not isinstance(value, str):
        raise TypeError(f"member column {name!r}: expected str, got {value!r}")
    return pl.lit(value, dtype=pl.String).alias(name)


def _version_fields(
    contract_version: int | None, converter_version: str | None
) -> dict[str, Any]:
    """The versions to record in a ``quantilica.members`` entry (only those given)."""
    out: dict[str, Any] = {}
    if contract_version is not None:
        out["contract_version"] = contract_version
    if converter_version is not None:
        out["converter_version"] = converter_version
    return out


def _group_files(directory: Path, group_key: str) -> list[tuple[Path, dict, int]]:
    """``(path, footer, rows)`` of the files of ``group_key``, one open each."""
    if not directory.is_dir():
        return []
    pattern = re.compile(rf"^{re.escape(group_key)}(?:-(\d{{3}}))?\.parquet$")
    found = []
    for path in sorted(directory.glob("*.parquet")):
        if not pattern.match(path.name):
            continue
        try:
            meta = pq.ParquetFile(path).metadata
        except (OSError, pa.ArrowException):
            continue
        footer = footer_from_metadata(meta)
        if footer.get(META_FILE_KEY) == group_key:
            found.append((path, footer, meta.num_rows))
    return found


def _canonical_type(conn, dtype: str | pl.DataType) -> str:
    """DuckDB's canonical name of a type given as a DuckDB name or a Polars dtype."""
    name = dtype if isinstance(dtype, str) else _duckdb_type(dtype)
    try:
        return conn.execute(f"SELECT typeof(CAST(NULL AS {name}))").fetchall()[0][0]
    except Exception as exc:  # duckdb.Error
        raise LakeError(f"schema: unknown type {dtype!r}") from exc


def _describe(conn, source: str) -> dict[str, str]:
    """``{column: type}`` of ``read_parquet(source)``; ``source`` is a SQL literal."""
    rows = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet({source}, hive_partitioning=false, "
        "union_by_name=true)"
    ).fetchall()
    # DuckDB names the type of an all-null column ``"NULL"`` (with the quotes).
    return {r[0]: ("NULL" if r[1] == '"NULL"' else r[1]) for r in rows}


def _unify_types(
    sources: Sequence[tuple[str, Mapping[str, str]]], wanted: Mapping[str, str]
) -> dict[str, str]:
    """The column types to cast to: ``wanted``, plus the divergences that are safe.

    A column of type ``NULL`` (all null) takes the type the column has in the
    other sources, and ``TIMESTAMP_NS`` meeting ``TIMESTAMP`` becomes
    ``TIMESTAMP`` (the cast is checked for loss). Any other divergence is left
    for :func:`_check_same_schema` to refuse.
    """
    seen: dict[str, list[str]] = {}
    for _label, desc in sources:
        for column, kind in desc.items():
            seen.setdefault(column, []).append(kind)
    targets = dict(wanted)
    for column, kinds in seen.items():
        if column in wanted:
            continue
        real = {k for k in kinds if k != "NULL"}
        if len(real) == 1 and "NULL" in kinds:
            targets[column] = next(iter(real))
        elif real == {"TIMESTAMP", "TIMESTAMP_NS"}:
            targets[column] = "TIMESTAMP"
    return targets


def _needs_cast(
    present: Mapping[str, str], targets: Mapping[str, str], fill: Mapping[str, str]
) -> bool:
    return any(c in present and present[c] != t for c, t in targets.items()) or any(
        c not in present for c in fill
    )


def _apply_targets(
    present: Mapping[str, str], targets: Mapping[str, str], fill: Mapping[str, str]
) -> dict[str, str]:
    """The description of a source after :func:`_schema_select`."""
    out = dict(present)
    for column, kind in targets.items():
        if column in present or column in fill:
            out[column] = kind
    return out


def _schema_select(
    present: Mapping[str, str], targets: Mapping[str, str], fill: Mapping[str, str]
) -> str:
    """Select list casting the ``targets`` present (nulls for a missing ``fill``)."""
    items = []
    for column, kind in targets.items():
        if column in present:
            items.append(f"CAST({_ident(column)} AS {kind}) AS {_ident(column)}")
        elif column in fill:
            items.append(f"CAST(NULL AS {kind}) AS {_ident(column)}")
    items += [_ident(c) for c in present if c not in targets]
    return ", ".join(items)


def _check_roundtrip(
    conn,
    label: str,
    source: str,
    present: Mapping[str, str],
    targets: Mapping[str, str],
) -> None:
    """Refuse a cast that loses information.

    Counts the rows whose non-null value, cast to the target type and back to
    the original one, is not the original (a ``DOUBLE`` rounded to ``BIGINT``,
    ``'007'`` made ``7``, a ``DECIMAL`` losing scale, a ``TIMESTAMP`` cut to a
    ``DATE``, a value the target cannot hold). Widening casts pass.
    """
    checks = {
        c: (present[c], t)
        for c, t in targets.items()
        if c in present and present[c] not in ("NULL", t)
    }
    if not checks:
        return
    counts = ", ".join(
        f"count(*) FILTER (WHERE {_ident(c)} IS NOT NULL AND "
        f"TRY_CAST(TRY_CAST({_ident(c)} AS {t}) AS {s}) IS DISTINCT FROM {_ident(c)})"
        for c, (s, t) in checks.items()
    )
    row = conn.execute(f"SELECT {counts} FROM {source}").fetchall()[0]
    for (column, (was, now)), lost in zip(checks.items(), row, strict=True):
        if lost:
            raise LakeError(
                f"{label}: casting column {column!r} from {was} to {now} loses "
                f"information in {lost} row(s) (checked by casting back)"
            )


def _check_same_schema(
    who: str, desc: Mapping[str, str], reference: Mapping[str, str], label: str
) -> None:
    """Refuse a member (``who``) whose columns or types differ from the group's."""
    for column, kind in desc.items():
        if column not in reference:
            raise LakeError(
                f"{who} has column {column!r} ({kind}) that the group "
                f"({label}) does not have; declare it in `schema` to fill it with "
                "nulls in the others"
            )
        if reference[column] != kind:
            raise LakeError(
                f"{who}: column {column!r} is {kind} but the group "
                f"({label}) has {reference[column]}; declare the type in `schema` "
                "to cast every member to it"
            )
    for column, kind in reference.items():
        if column not in desc:
            raise LakeError(
                f"{who} has no column {column!r} ({kind} in the group, "
                f"{label}); declare it in `schema` to fill it with nulls"
            )


def _refuse_member_named(src: Path, names: Iterable[str], own: set[str]) -> None:
    """A data column with the name of a member column would be silently lost."""
    if clash := sorted(own.intersection(names)):
        raise LakeError(
            f"{src.name}: data column(s) {clash} have the name of a member column "
            "(or of the provenance column); rename them in the member's data"
        )


@dataclass(frozen=True)
class _OriginRecord:
    """What one Parquet footer says about one raw file (see ``origin_recorded``).

    ``entry`` is the ``quantilica.members`` entry for a grouped file and ``None``
    for a file written per raw file, where the same facts sit in the footer's own
    keys (``contract_version`` is then the footer's text, ``None`` when absent).
    """

    path: Path
    file_key: str | None
    sha256: str | None
    layout: str | None
    converted_at: str | None
    contract_version: str | None
    entry: Mapping[str, Any] | None


# --- writer ------------------------------------------------------------------------


class LakeWriter:
    """Write raw files into one dataset of the lake.

    Every write is idempotent per ``file_key`` (same ``sha256``, layout version
    and contract version is skipped unless ``force``), holds the dataset lock,
    publishes with ``os.replace`` and keeps ``_dataset.json`` up to date.

    Publishing a multi-part output replaces the parts one at a time, highest
    number first, and only then deletes the leftovers of a previous, larger
    split. A crash in between leaves a set that :func:`origin_matches` rejects,
    so the next run reconverts. The lake is not transactional across files:
    a concurrent reader may briefly see old and new parts together.

    Args:
        dataset_dir: ``<lake>/<source_id>/<dataset_id>``.
        descriptor: Descriptor to maintain in ``_dataset.json``. The writer
            works on a copy. Empty ``partition_keys`` are adopted from the
            first write.
        force: Reconvert even when the origin is already in the lake.
        max_part_bytes: Split the output into ``-NNN`` parts of about this
            size. ``None`` writes a single file.
    """

    def __init__(
        self,
        dataset_dir: Path,
        descriptor: DatasetDescriptor,
        *,
        force: bool = False,
        max_part_bytes: int | None = None,
    ) -> None:
        if descriptor.partition_source != "file":
            raise NotImplementedError(
                "partition_source='rows' is not supported by LakeWriter yet."
            )
        if max_part_bytes is not None and max_part_bytes <= 0:
            raise ValueError("max_part_bytes must be positive.")
        self.dataset_dir = Path(dataset_dir)
        self.descriptor = replace(
            descriptor,
            partition_keys=list(descriptor.partition_keys),
            partition_types=dict(descriptor.partition_types),
        )
        self.force = force
        self.max_part_bytes = max_part_bytes
        self._origin_cache: dict[str, list[_OriginRecord]] | None = None
        configure_scratch()

    # -- public API --

    def write_duckdb(
        self,
        src: Path,
        *,
        partitions: dict[str, Any] | None = None,
        file_key_name: str | None = None,
        src_origin: Path | None = None,
        separator: str = ";",
        encoding: str = "utf-8",
        header: bool = True,
        names: tuple[str, ...] = (),
        rename: dict[str, str] | None = None,
        required: tuple[str, ...] = (),
        contract: DataContract | None = None,
        converter: str = "quantilica-analytics",
        quote: str | None = None,
        escape: str | None = None,
        parallel: bool = True,
        skip: int = 0,
        decimal_comma: bool = True,
        members: str | None = None,
    ) -> LakeWriteResult:
        """Convert a CSV/TXT (or the members of a ZIP/7z) with DuckDB, streaming.

        ``members`` (a case-insensitive glob) picks which text members of a
        ZIP/7z are read; a ``.gz`` is decompressed first.

        Columns are read as text, named from the header (normalized, then
        ``rename``) or from ``names`` when ``header`` is false. ``quote`` and
        ``escape`` fix DuckDB's CSV dialect; ``None`` keeps its auto-detection,
        which can pick ``'`` as the escape of a file quoted with ``"``.
        ``parallel=False`` reads with a single thread, which DuckDB requires for
        quoted fields spanning lines. Latin-1 input with C1 bytes (0x80-0x9F),
        which DuckDB's decoder rejects, is first transcoded to UTF-8.
        ``QUANTILICA_LAKE_SCRATCH`` moves the archive extraction, DuckDB's
        temporary files and the staged Parquet to another directory (a faster
        disk, say); only the finished file is moved into the lake. ``required``
        columns missing from the header raise :class:`LakeError` before
        anything is written, which rejects an HTML error page saved as CSV.
        Columns typed by ``contract`` go through ``TRY_CAST``; the failures
        are counted in ``quantilica.cast_nulls`` (one extra pass over the
        input).
        """
        src = Path(src)
        origin = Path(src_origin) if src_origin is not None else src
        rename = rename or {}
        partitions = dict(partitions or {})
        if not header and not names:
            raise ValueError("names is required when header is False.")
        digest = sha256_file(origin)
        key = file_key_name or file_key(origin)
        with dataset_lock(self.dataset_dir):
            target = self._target(partitions)
            skipped = self._skip_if_current(target, key, origin, digest, contract)
            if skipped:
                return skipped
            staging = self._staging_dir()
            try:
                result = self._duckdb_convert(
                    src,
                    target,
                    key,
                    digest,
                    staging,
                    origin=origin,
                    partitions=partitions,
                    separator=separator,
                    encoding=encoding,
                    header=header,
                    names=names,
                    rename=rename,
                    required=required,
                    contract=contract,
                    converter=converter,
                    quote=quote,
                    escape=escape,
                    parallel=parallel,
                    skip=skip,
                    decimal_comma=decimal_comma,
                    members=members,
                )
            finally:
                self._cleanup_staging(staging)
            self._save_descriptor(partitions, contract, converter)
        logger.info("lake: %s -> %s (%d rows)", src.name, target, result.rows)
        return result

    def is_current(
        self,
        src: Path,
        *,
        partitions: dict[str, Any] | None = None,
        file_key_name: str | None = None,
        src_origin: Path | None = None,
        contract: DataContract | None = None,
    ) -> bool:
        """Whether ``src`` is already in the lake with its current ``sha256``.

        The same test the writers run before writing, exposed so a converter
        can skip an expensive read (a DBC decode, say) for a file that would be
        skipped anyway. Always ``False`` with ``force``.
        """
        if self.force:
            return False
        src = Path(src)
        origin = Path(src_origin) if src_origin is not None else src
        target = self._target(dict(partitions or {}))
        key = file_key_name or file_key(origin)
        skipped = self._skip_if_current(
            target, key, origin, sha256_file(origin), contract
        )
        return skipped is not None

    def origin_recorded(
        self,
        raw: Path,
        *,
        member: str | None = None,
        contract_version: int | None = None,
        converter_version: str | None = None,
    ) -> bool:
        """Whether ``raw`` is already in the lake, whatever partition it went to.

        The partition-free counterpart of :meth:`is_current`, for a converter
        that only learns the partition after reading the data (an ``ano`` column,
        say) and so cannot call :meth:`is_current` before reading. It looks at
        the footers of every Parquet of the dataset, never at their columns: the
        footers are read once per writer (lazily, and again after this writer
        writes, replaces or removes something), so asking for many raw files
        costs one pass over the footers, not one per call. Files written by
        another process after the first call are not seen by this instance.

        True only when:

        * at least one Parquet cites ``member or raw.name`` as its origin file
          (``quantilica.origin_file`` of a file written per raw file, or
          ``origin_file`` of a ``quantilica.members`` entry of a grouped file);
        * **every** Parquet that cites it records the digest of ``raw``
          (:func:`sha256_file`, the sidecar's when it is trusted, so the raw file
          is not read) and this writer's layout version. A raw file that produced
          one Parquet per year is therefore current only when all the years are
          in the lake with that digest; a changed raw file, even one that only
          adds a year, is not current;
        * for files written per raw file, the parts of each file key in a
          partition folder form one complete set (a lone ``<key>.parquet`` or
          ``-001..-N`` without gaps) written in a single run, and every part
          records the same contract version as ``contract_version`` (``None``
          when the file was written without a contract), as
          :func:`origin_matches` requires;
        * for grouped files, the entry's ``contract_version`` and
          ``converter_version`` equal the ones given, when given (an entry
          without a recorded version is stale for a caller that passes one), as
          :func:`member_matches` requires.

        ``member`` is the name the footers cite when it is not ``raw.name``.
        Always ``False`` with ``force``, and for a dataset with no Parquet.
        """
        if self.force:
            return False
        raw = Path(raw)
        records = self._origin_index().get(member or raw.name)
        if not records:
            return False
        digest = sha256_file(raw)
        layout = str(self.descriptor.layout_version)
        expected_contract = None if contract_version is None else str(contract_version)
        sets: dict[tuple[Path, str], list[_OriginRecord]] = {}
        for rec in records:
            if rec.sha256 != digest or rec.layout != layout:
                return False
            if rec.entry is not None:
                if not _entry_is_current(
                    rec.entry,
                    digest,
                    contract_version=contract_version,
                    converter_version=converter_version,
                ):
                    return False
            elif rec.contract_version != expected_contract or rec.file_key is None:
                return False
            else:
                sets.setdefault((rec.path.parent, rec.file_key), []).append(rec)
        return all(_is_complete_set(key, parts) for (_, key), parts in sets.items())

    def _origin_index(self) -> dict[str, list[_OriginRecord]]:
        """``{origin file name: records}`` of the dataset's footers (built once)."""
        if self._origin_cache is None:
            index: dict[str, list[_OriginRecord]] = {}
            for path in _dataset_files(self.dataset_dir):
                try:
                    meta = pq.read_metadata(path)
                except (OSError, pa.ArrowException):
                    continue
                footer = footer_from_metadata(meta)
                common = {
                    "path": path,
                    "file_key": footer.get(META_FILE_KEY),
                    "layout": footer.get(META_LAYOUT),
                    "converted_at": footer.get(META_CONVERTED_AT),
                }
                if META_MEMBERS in footer:
                    try:
                        members = json.loads(footer[META_MEMBERS])
                    except ValueError:
                        continue
                    for entry in members.values():
                        name = entry.get("origin_file")
                        if name:
                            index.setdefault(name, []).append(
                                _OriginRecord(
                                    sha256=entry.get("origin_sha256"),
                                    contract_version=None,
                                    entry=entry,
                                    **common,
                                )
                            )
                elif name := footer.get("quantilica.origin_file"):
                    index.setdefault(name, []).append(
                        _OriginRecord(
                            sha256=footer.get(META_ORIGIN_SHA),
                            contract_version=footer.get(META_CONTRACT_VERSION),
                            entry=None,
                            **common,
                        )
                    )
            self._origin_cache = index
        return self._origin_cache

    def _forget_origins(self) -> None:
        """Drop the footer index: the dataset's files are about to change."""
        self._origin_cache = None

    def write_polars(
        self,
        data: pl.DataFrame | pl.LazyFrame,
        src: Path | None,
        *,
        partitions: dict[str, Any] | None = None,
        file_key_name: str | None = None,
        contract: DataContract | None = None,
        converter: str = "quantilica-analytics",
    ) -> LakeWriteResult:
        """Write a DataFrame/LazyFrame that was read from ``src``.

        ``src`` is the raw file the data came from: it provides the
        ``sha256`` for idempotency (a skipped file never evaluates the plan),
        the default file key and the provenance sidecar. Without ``src``,
        ``file_key_name`` is required and the write is never skipped.

        A LazyFrame is streamed with ``sink_parquet`` (Polars falls back to
        the in-memory engine when the plan is not streamable). With
        ``max_part_bytes`` the frame is collected and split by rows, sized by
        its uncompressed estimate, so parts stay at or below the limit.
        """
        partitions = dict(partitions or {})
        if src is None and not file_key_name:
            raise ValueError("file_key_name is required when src is None.")
        src = Path(src) if src is not None else None
        key = file_key_name or file_key(src)  # type: ignore[arg-type]
        digest = sha256_file(src) if src is not None else None
        with dataset_lock(self.dataset_dir):
            target = self._target(partitions)
            if digest is not None:
                skipped = self._skip_if_current(target, key, src, digest, contract)
                if skipped:
                    return skipped
            if contract is not None:
                contract.validate(data)
            staging = self._staging_dir()
            try:
                result = self._polars_convert(
                    data,
                    src,
                    target,
                    key,
                    digest,
                    staging,
                    contract,
                    converter,
                    partitions=partitions,
                )
            finally:
                self._cleanup_staging(staging)
            self._save_descriptor(partitions, contract, converter)
        logger.info("lake: %s -> %s (%d rows)", key, target, result.rows)
        return result

    def is_current_member(
        self,
        src: Path,
        *,
        group_key: str,
        partitions: dict[str, Any] | None = None,
        member_key: str | None = None,
        contract_version: int | None = None,
        converter_version: str | None = None,
    ) -> bool:
        """Whether ``src`` is already a member of the grouped file with its ``sha256``.

        The grouped counterpart of :meth:`is_current`. Always ``False`` with ``force``.

        ``member_key`` is the member's key when it is not ``file_key(src)`` (a
        ZIP member: ``composite_file_key(src, member)``). With
        ``contract_version``/``converter_version`` the member counts as current
        only if it was written with the same versions; a member written
        without versions (an older footer, or a caller that did not pass any)
        is then stale. Without them, versions are not compared.
        """
        if self.force:
            return False
        src = Path(src)
        target = self._target(dict(partitions or {}))
        return member_matches(
            target / f"{group_key}.parquet",
            group_key,
            member_key or file_key(src),
            sha256_file(src),
            layout_version=self.descriptor.layout_version,
            contract_version=contract_version,
            converter_version=converter_version,
        )

    def write_member(
        self,
        data: pl.DataFrame | pl.LazyFrame,
        src: Path,
        *,
        group_key: str,
        partitions: dict[str, Any] | None = None,
        member_columns: Mapping[str, str] | None = None,
        converter: str = "quantilica-analytics",
        member_key: str | None = None,
        contract_version: int | None = None,
        converter_version: str | None = None,
    ) -> LakeWriteResult:
        """Write ``src`` into the partition's grouped file ``<group_key>.parquet``.

        One Parquet per partition holds the rows of several raw files (the
        DATASUS publishes one file per state and month, most of them tiny).
        ``member_columns`` become literal columns (``{"uf": "sp"}``), every row
        gets the member's key in ``_origem`` (``member_key``, default
        ``file_key(src)``) and the rows are sorted by the member columns, so a
        filter on them skips row groups by their statistics. Writing a member
        replaces only its own rows; the footer's ``quantilica.members`` keeps
        the provenance of each one, and the versions when given. The merge runs
        in DuckDB, spilling to disk, under the dataset lock.

        This rewrites the whole group for each member. To write a group of many
        members, use :meth:`write_group`, which rewrites it once.
        """
        src = Path(src)
        partitions = dict(partitions or {})
        member_columns = dict(member_columns or {})
        key = member_key or file_key(src)
        digest = sha256_file(src)
        with dataset_lock(self.dataset_dir):
            target = self._target(partitions)
            path = target / f"{group_key}.parquet"
            if not self.force and member_matches(
                path,
                group_key,
                key,
                digest,
                layout_version=self.descriptor.layout_version,
                contract_version=contract_version,
                converter_version=converter_version,
            ):
                return LakeWriteResult(src, [path], "skipped")
            staging = self._staging_dir()
            try:
                result = self._member_convert(
                    data,
                    src,
                    key,
                    digest,
                    path,
                    staging,
                    group_key=group_key,
                    partitions=partitions,
                    member_columns=member_columns,
                    converter=converter,
                    contract_version=contract_version,
                    converter_version=converter_version,
                )
            finally:
                self._cleanup_staging(staging)
            self._save_descriptor(partitions, None, converter)
        logger.info("lake: %s -> %s (%d rows)", src.name, path, result.rows)
        return result

    def _member_convert(
        self,
        data: pl.DataFrame | pl.LazyFrame,
        src: Path,
        key: str,
        digest: str,
        path: Path,
        staging: Path,
        *,
        group_key: str,
        partitions: dict[str, Any],
        member_columns: dict[str, str],
        converter: str,
        contract_version: int | None = None,
        converter_version: str | None = None,
    ) -> LakeWriteResult:
        frame = data.collect() if isinstance(data, pl.LazyFrame) else data
        reserved = [*partitions, *member_columns, ORIGIN_COLUMN]
        frame = frame.drop([c for c in reserved if c in frame.columns])
        if not frame.columns:
            raise LakeError(f"{src.name}: no payload columns to write")
        frame = frame.with_columns(
            *(_member_literal(k, v) for k, v in member_columns.items()),
            pl.lit(key, dtype=pl.String).alias(ORIGIN_COLUMN),
        )
        members: dict[str, dict[str, Any]] = {}
        existing = None
        if path.exists():
            found = _members(path, group_key)
            if found is None:
                owner = _footer(path).get(META_FILE_KEY)
                raise LakeError(
                    f"{path.name} is not the grouped file {group_key!r} "
                    f"(file key {owner!r}); refusing to overwrite it"
                )
            members = found
            existing = path
        members[key] = {
            "origin_file": src.name,
            "origin_sha256": digest,
            "rows": frame.height,
            "converted_at": datetime.now(UTC).isoformat(timespec="seconds"),
            **member_columns,
            **_version_fields(contract_version, converter_version),
        }
        meta = self._metadata(group_key, None, None, None, converter)
        meta[META_MEMBERS] = json.dumps(members, sort_keys=True, ensure_ascii=False)
        meta[META_MEMBER_COLUMNS] = json.dumps(list(member_columns))
        order = ", ".join(_ident(c) for c in [*member_columns, ORIGIN_COLUMN])
        query = "SELECT * FROM novo"
        if existing is not None:
            query = (
                f"SELECT * FROM read_parquet({_lit(str(existing))}, "
                "hive_partitioning=false) "
                f"WHERE {_ident(ORIGIN_COLUMN)} <> {_lit(key)} "
                f"UNION ALL BY NAME {query}"
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_out = staging / f"{group_key}.parquet{TMP_SUFFIX}"
        scratch = _scratch_dir(staging)
        conn = _connect(scratch)
        try:
            conn.register("novo", frame.to_arrow())
            conn.execute(
                f"COPY ({query} ORDER BY {order}) TO {_lit(str(tmp_out))} "
                f"({_kv_options(meta)})"
            )
            rows = pq.ParquetFile(tmp_out).metadata.num_rows
            self._move(tmp_out, path)
        except BaseException:
            tmp_out.unlink(missing_ok=True)
            raise
        finally:
            conn.close()
            if scratch != staging:
                shutil.rmtree(scratch, ignore_errors=True)
        return LakeWriteResult(src, [path], "converted", int(rows))

    def is_current_group(
        self,
        group_key: str,
        members: Sequence[GroupMember],
        *,
        partitions: dict[str, Any] | None = None,
        contract_version: int | None = None,
        converter_version: str | None = None,
        prune: bool = True,
    ) -> bool:
        """Whether :meth:`write_group` would skip this group.

        Hashes the raw files and reads the footer once, nothing else: a
        converter can ask before decoding anything. Always ``False`` with
        ``force``.
        """
        if self.force:
            return False
        plan = self._plan_group(
            group_key,
            members,
            dict(partitions or {}),
            contract_version,
            converter_version,
            prune,
        )
        return plan.is_current

    def write_group(
        self,
        group_key: str,
        members: Sequence[GroupMember],
        *,
        partitions: dict[str, Any] | None = None,
        converter: str = "quantilica-analytics",
        contract_version: int | None = None,
        converter_version: str | None = None,
        prune: bool = True,
        sort_by: Sequence[str] | None = None,
        schema: Mapping[str, str | pl.DataType] | None = None,
        replaces: Sequence[str] = (),
        replace_partition: bool = False,
    ) -> GroupWriteResult:
        """Write all the members of a group into ``<group_key>.parquet``, once.

        The group is the set of raw files of one partition that share a Parquet.
        ``members`` is the complete list: the group is skipped when every member
        is in the file with the same ``sha256`` (and versions, see below), and
        otherwise rewritten in a single pass, whatever the number of members
        that changed. :meth:`write_member` would rewrite it once per member.

        * Members already in the file and unchanged keep their rows, copied
          from the existing file; only the changed ones are read from their
          ``data`` (a function is called only then).
        * With ``prune`` (the default), a member of the existing file that is no
          longer in ``members`` is dropped from the rows and the footer, and
          listed in ``removed``. With ``prune=False`` the list may be partial:
          the members of the file that are not in it are carried over untouched.
          ``prune=False`` refuses (``LakeError``) to rewrite a file that is not
          in a usable state (see below), since it could not carry its members.
        * ``contract_version`` and ``converter_version`` are recorded in each
          written member's entry. When given, a member counts as current only
          if its entry has the same value; an entry from before the versions
          were recorded has none and is therefore rewritten. When ``None``, the
          versions are not compared.
        * A changed member's ``columns`` (``uf``, ``parte``...) also make it
          stale, and so does a different set of column names in the footer.
        * An existing file that is not usable (another layout version, other
          member columns, members that do not add up to its rows, or ``-NNN``
          parts) is rewritten whole, as if every member had changed; with
          ``prune`` the members it listed that are not in ``members`` are
          reported in ``removed``.

        Schema: a group holds one schema. The staged members are compared, by
        column name and type, with each other and with the existing file; a
        difference (a type, a missing or an extra column) is a ``LakeError``
        naming the member, the column and both types, instead of a silent
        upcast. Two differences are settled without being asked: a column of
        type ``NULL`` (all null) takes the type the other members have, and
        ``TIMESTAMP_NS`` meeting ``TIMESTAMP`` becomes ``TIMESTAMP``.
        ``schema`` (column -> DuckDB type name or Polars dtype; not partition
        keys, member columns or ``_origem``) lifts the comparison for the
        columns it lists: every member and the kept rows are cast to it, and a
        column a member lacks is filled with nulls. Every cast is checked by
        casting back: a column whose non-null values do not survive the round
        trip (a ``DOUBLE`` rounded to ``BIGINT``, ``'007'`` made ``7``, a
        narrower ``DECIMAL``, a ``TIMESTAMP`` cut to a ``DATE``) is a
        ``LakeError`` with the column, the types and the count. Widening casts
        pass. ``schema`` only acts when the group is rewritten.

        Memory: nothing is collected. A ``LazyFrame`` is streamed to a staged
        Parquet, a ``Path`` or :class:`DuckSource` goes through DuckDB, and the
        file is written by DuckDB from those staged files plus the kept rows
        of the existing file, spilling to disk. The staging and spill
        directories are under ``QUANTILICA_LAKE_SCRATCH`` when set (see
        :meth:`write_duckdb`), never in the lake.

        One file: a group is always a single ``<group_key>.parquet``.
        ``max_part_bytes`` does not apply to it (it is ignored, with a warning)
        and a set of ``-NNN`` parts left by an older run is replaced.

        Order: without ``sort_by`` the rows are written with
        ``preserve_insertion_order`` off, so the order of the rows, and of the
        members among themselves, is not guaranteed (a member's rows are not
        necessarily contiguous). ``sort_by`` (column names) adds an
        ``ORDER BY``, which costs a sort with spill to disk.

        Migration: ``replaces`` (file keys of per-raw-file Parquets of the same
        partition) or ``replace_partition`` (every other Parquet directly in the
        partition folder) name *candidates* for deletion after the group is
        published, under the same lock. A candidate is deleted only if all of
        these hold: its footer has no ``quantilica.members`` (it is not a grouped
        file), it has an ``origin_sha256`` that is the digest of one of the
        group's members (so every row it holds is in the group), and it has this
        dataset's ``source_id`` and ``dataset_id``. Anything else stays, with a
        warning in the log, and is listed in ``not_replaced``; so another group of
        the same partition, or a Parquet written later by another path, is never
        deleted. Deleted files are listed in ``replaced``. A skipped group runs the
        same test (a rerun after a crash between the publish and the removal).

        The dataset lock is held while the members' ``data`` are evaluated.
        """
        partitions = dict(partitions or {})
        if self.max_part_bytes is not None:
            logger.warning(
                "lake: max_part_bytes does not apply to a group; %s is one file",
                group_key,
            )
        with dataset_lock(self.dataset_dir):
            plan = self._plan_group(
                group_key,
                members,
                partitions,
                contract_version,
                converter_version,
                prune,
            )
            if plan.blocked:
                raise LakeError(
                    f"group {group_key!r}: the existing file is not in a usable "
                    "state (another layout version, other member columns, rows that "
                    "do not add up, or numbered parts); with prune=False its "
                    "members cannot be carried over. Rewrite it with prune=True."
                )
            if reserved := {
                *partitions,
                *self.descriptor.partition_keys,
                *plan.columns,
                ORIGIN_COLUMN,
            }.intersection(schema or ()):
                raise ValueError(
                    f"schema cannot declare partition keys, member columns or "
                    f"{ORIGIN_COLUMN!r}: {sorted(reserved)}"
                )
            if plan.is_current and not self.force:
                logger.info("lake: group %s is current", group_key)
                result = GroupWriteResult(
                    None,
                    plan.state.parts,
                    "skipped",
                    kept=[*plan.kept, *plan.carried],
                )
            else:
                staging = self._staging_dir()
                try:
                    result = self._group_convert(
                        plan,
                        staging,
                        partitions=partitions,
                        converter=converter,
                        contract_version=contract_version,
                        converter_version=converter_version,
                        sort_by=list(sort_by or ()),
                        schema=schema,
                    )
                finally:
                    self._cleanup_staging(staging)
                self._save_descriptor(partitions, None, converter)
            result.replaced, result.not_replaced = self._remove_replaced(
                plan.target, group_key, result.outputs, replaces, replace_partition
            )
        if result.status == "converted":
            logger.info(
                "lake: group %s -> %d rows (%d written, %d kept, %d removed)",
                group_key,
                result.rows,
                len(result.written),
                len(result.kept),
                len(result.removed),
            )
        return result

    def _remove_replaced(
        self,
        target: Path,
        group_key: str,
        outputs: list[Path],
        replaces: Sequence[str],
        replace_partition: bool,
    ) -> tuple[list[Path], list[Path]]:
        """Delete the per-raw-file Parquets the published group replaces.

        Returns ``(removed, kept)``; see :meth:`write_group` for the test.
        """
        self._forget_origins()
        if not replaces and not replace_partition:
            return [], []
        if replace_partition:
            candidates = sorted(target.glob("*.parquet"))
        else:
            candidates = [p for key in replaces for p in _part_files(target, key)]
            for key in replaces:
                if not (target / f"{key}.parquet").exists() and not _part_files(
                    target, key
                ):
                    logger.warning("lake: replaces: no Parquet with key %r", key)
        try:
            published = json.loads(_footer(outputs[0]).get(META_MEMBERS, "{}"))
        except ValueError:
            published = {}
        shas = {m.get("origin_sha256") for m in published.values()} - {None, ""}
        removed: list[Path] = []
        kept: list[Path] = []
        for path in candidates:
            if path in outputs or not path.is_file():
                continue
            meta = _footer(path)
            if (
                meta.get("quantilica.source_id") != self.descriptor.source_id
                or meta.get("quantilica.dataset_id") != self.descriptor.dataset_id
            ):
                reason = "another dataset"
            elif meta.get(META_FILE_KEY) == group_key:
                continue
            elif META_MEMBERS in meta:
                reason = "it is a grouped file (another group)"
            elif not meta.get(META_ORIGIN_SHA):
                reason = "it has no origin_sha256"
            elif meta[META_ORIGIN_SHA] not in shas:
                reason = "its origin is not a member of the group"
            else:
                path.unlink()
                removed.append(path)
                continue
            logger.warning("lake: %s kept, not replaced: %s", path, reason)
            kept.append(path)
        return removed, kept

    def _plan_group(
        self,
        group_key: str,
        members: Sequence[GroupMember],
        partitions: dict[str, Any],
        contract_version: int | None,
        converter_version: str | None,
        prune: bool,
    ) -> _GroupPlan:
        if not members:
            raise ValueError("write_group needs at least one member.")
        columns = list(members[0].columns)
        reserved = {*partitions, *self.descriptor.partition_keys, ORIGIN_COLUMN}
        if clash := reserved.intersection(columns):
            raise ValueError(
                f"member columns {sorted(clash)} clash with reserved names"
            )
        if clash := ENTRY_FIELDS.intersection(columns):
            raise ValueError(
                f"member columns {sorted(clash)} clash with the fields of a "
                "quantilica.members entry"
            )
        digests: dict[Path, str] = {}
        resolved: list[tuple[GroupMember, str, str]] = []
        seen: set[str] = set()
        for member in members:
            if list(member.columns) != columns:
                raise ValueError(
                    "every member of a group must have the same member columns; "
                    f"got {list(member.columns)} and {columns}"
                )
            for name, value in member.columns.items():
                if not isinstance(value, str):
                    raise TypeError(
                        f"member column {name!r}: expected str, got {value!r}"
                    )
            src = Path(member.src)
            key = member.key or file_key(src)
            if key in seen:
                raise ValueError(f"duplicate member key {key!r} in group {group_key!r}")
            seen.add(key)
            if member.sha256:
                digest = member.sha256
            else:
                if src not in digests:
                    digests[src] = sha256_file(src)
                digest = digests[src]
            resolved.append((member, key, digest))
        target = self._target(partitions)
        state = self._group_state(target, group_key, columns)
        changed: list[tuple[GroupMember, str, str]] = []
        kept: list[str] = []
        for item in resolved:
            member, key, digest = item
            if not self.force and _entry_is_current(
                state.members.get(key),
                digest,
                columns=member.columns,
                contract_version=contract_version,
                converter_version=converter_version,
            ):
                kept.append(key)
            else:
                changed.append(item)
        if prune:
            orphans = sorted(set(state.known) - seen)
            carried: list[str] = []
        else:
            orphans = []
            carried = sorted(set(state.members) - seen)
        return _GroupPlan(
            target,
            group_key,
            state,
            resolved,
            columns,
            changed,
            kept,
            carried,
            orphans,
            blocked=not prune and bool(state.parts) and not state.usable,
        )

    def _group_state(
        self, target: Path, group_key: str, columns: list[str]
    ) -> _GroupState:
        """The group as the lake holds it, reading each footer once.

        An existing file that is not a grouped file is an error. A grouped file
        that is not usable (see :meth:`write_group`) is reported with its files
        and the keys its footer lists (``known``) but no ``members``, so every
        member is rewritten and the files are replaced.
        """
        files = _group_files(target, group_key)
        if not files:
            return _GroupState()
        for path, kv, _rows in files:
            if META_MEMBERS not in kv:
                raise LakeError(
                    f"{path.name} is not the grouped file {group_key!r} "
                    f"(file key {kv.get(META_FILE_KEY)!r}); refusing to overwrite it"
                )
        parts = [path for path, _, _ in files]
        known: set[str] = set()
        merged: dict[str, dict[str, Any]] = {}
        usable = [path.name for path in parts] == [f"{group_key}.parquet"]
        for _path, kv, rows in files:
            try:
                entries = json.loads(kv[META_MEMBERS])
                known.update(entries)
                if kv.get(META_LAYOUT) != str(self.descriptor.layout_version):
                    usable = False
                if json.loads(kv.get(META_MEMBER_COLUMNS, "[]")) != columns:
                    usable = False
                if sum(int(e.get("rows", 0)) for e in entries.values()) != rows:
                    usable = False
                if merged.keys() & entries.keys():
                    usable = False
                merged.update(entries)
            except (ValueError, AttributeError, TypeError):
                usable = False
        return _GroupState(
            merged if usable else {}, parts, usable=usable, known=sorted(known)
        )

    def _group_convert(
        self,
        plan: _GroupPlan,
        staging: Path,
        *,
        partitions: dict[str, Any],
        converter: str,
        contract_version: int | None,
        converter_version: str | None,
        sort_by: list[str],
        schema: Mapping[str, str | pl.DataType] | None,
    ) -> GroupWriteResult:
        group_key = plan.group_key
        reserved = [*partitions, *self.descriptor.partition_keys]
        work = staging / "duck"
        work.mkdir()
        conn = _connect(work)
        try:
            if not sort_by:
                conn.execute("SET preserve_insertion_order = false")
            wanted = {
                name: _canonical_type(conn, dtype)
                for name, dtype in (schema or {}).items()
            }
            carried = [*plan.kept, *plan.carried]
            entries: dict[str, dict[str, Any]] = {
                k: plan.state.members[k] for k in carried
            }
            staged: dict[str, Path] = {}
            for i, (member, key, digest) in enumerate(plan.changed):
                data = member.data() if callable(member.data) else member.data
                out = staging / f"member-{i:06d}.parquet{TMP_SUFFIX}"
                rows = self._stage_member(
                    conn,
                    data,
                    Path(member.src),
                    key,
                    dict(member.columns),
                    reserved,
                    out,
                )
                staged[key] = out
                entries[key] = {
                    "origin_file": Path(member.src).name,
                    "origin_sha256": digest,
                    "rows": rows,
                    "converted_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    **member.columns,
                    **_version_fields(contract_version, converter_version),
                }
            existing = (
                "[" + ", ".join(_lit(str(p)) for p in plan.state.parts) + "]"
                if carried
                else None
            )
            carried_select = "*"
            # (label, member key or None for the existing file, description)
            sources: list[tuple[str, str | None, dict[str, str]]] = []
            if existing is not None:
                conn.register("keep", pa.table({"k": sorted(carried)}))
                sources.append(("the existing file", None, _describe(conn, existing)))
            for key in sorted(staged):
                desc = _describe(conn, _lit(str(staged[key])))
                sources.append((f"member {key!r}", key, desc))
            targets = _unify_types([(lb, d) for lb, _, d in sources], wanted)
            normalized: list[tuple[str, dict[str, str]]] = []
            for label, key, desc in sources:
                if _needs_cast(desc, targets, wanted):
                    if key is None:
                        _check_roundtrip(
                            conn,
                            label,
                            f"(SELECT * FROM read_parquet({existing}, "
                            "hive_partitioning=false, union_by_name=true) WHERE "
                            f"{_ident(ORIGIN_COLUMN)} IN (SELECT k FROM keep))",
                            desc,
                            targets,
                        )
                        carried_select = _schema_select(desc, targets, wanted)
                    else:
                        staged[key] = self._cast_staged(
                            conn, label, staged[key], desc, targets, wanted
                        )
                    desc = _apply_targets(desc, targets, wanted)
                normalized.append((label, desc))
            for label, desc in normalized[1:]:
                _check_same_schema(label, desc, normalized[0][1], normalized[0][0])

            def order_of(k: str) -> tuple:
                return tuple(entries[k].get(c) for c in plan.columns), k

            order_new = sorted(staged, key=order_of)
            meta = self._metadata(group_key, None, None, None, converter)
            meta[META_MEMBER_COLUMNS] = json.dumps(plan.columns)
            meta[META_MEMBERS] = json.dumps(entries, sort_keys=True, ensure_ascii=False)
            tmp_out = staging / f"{group_key}.parquet{TMP_SUFFIX}"
            branches = []
            if carried:
                conn.register("keep", pa.table({"k": sorted(carried)}))
                branches.append(
                    f"SELECT {carried_select} FROM read_parquet({existing}, "
                    "hive_partitioning=false, union_by_name=true) "
                    f"WHERE {_ident(ORIGIN_COLUMN)} IN (SELECT k FROM keep)"
                )
            if staged:
                files = "[" + ", ".join(_lit(str(staged[k])) for k in order_new) + "]"
                branches.append(
                    f"SELECT * FROM read_parquet({files}, "
                    "hive_partitioning=false, union_by_name=true)"
                )
            query = " UNION ALL BY NAME ".join(branches)
            if sort_by:
                order_by = ", ".join(_ident(c) for c in sort_by)
                query = f"SELECT * FROM ({query}) ORDER BY {order_by}"
            conn.execute(
                f"COPY ({query}) TO {_lit(str(tmp_out))} ({_kv_options(meta)})"
            )
            total = pq.ParquetFile(tmp_out).metadata.num_rows
            expected = sum(int(e["rows"]) for e in entries.values())
            if total != expected:
                tmp_out.unlink(missing_ok=True)
                raise LakeError(
                    f"group {group_key!r}: the file has {total} rows but its "
                    f"members add up to {expected}"
                )
            outputs = self._publish(plan.target, group_key, [tmp_out])
        finally:
            conn.close()
            shutil.rmtree(work, ignore_errors=True)
        return GroupWriteResult(
            None,
            outputs,
            "converted",
            total,
            written=list(staged),
            kept=list(carried),
            removed=list(plan.orphans),
        )

    @staticmethod
    def _cast_staged(
        conn,
        label: str,
        path: Path,
        present: dict[str, str],
        targets: dict[str, str],
        fill: dict[str, str],
    ) -> Path:
        """Rewrite a staged member with the ``targets`` columns cast (checked)."""
        source = f"read_parquet({_lit(str(path))}, hive_partitioning=false)"
        _check_roundtrip(conn, label, source, present, targets)
        out = path.with_name(path.name + ".cast")
        conn.execute(
            f"COPY (SELECT {_schema_select(present, targets, fill)} FROM {source}) "
            f"TO {_lit(str(out))} (FORMAT parquet, COMPRESSION zstd)"
        )
        path.unlink()
        return out

    def _stage_member(
        self,
        conn,
        data: MemberData,
        src: Path,
        key: str,
        columns: dict[str, str],
        reserved: list[str],
        out: Path,
    ) -> int:
        """Write the member's rows, with its columns and ``_origem``, to ``out``.

        Partition keys among the data columns are dropped (they live in the
        folder); a data column named like a member column or ``_origem`` is an
        error.
        """
        own = {*columns, ORIGIN_COLUMN}
        if isinstance(data, pl.LazyFrame | pl.DataFrame):
            names = (
                data.collect_schema().names()
                if isinstance(data, pl.LazyFrame)
                else data.columns
            )
            _refuse_member_named(src, names, own)
            frame = data.drop([c for c in reserved if c in names])
            remaining = (
                frame.collect_schema().names()
                if isinstance(frame, pl.LazyFrame)
                else frame.columns
            )
            if not remaining:
                raise LakeError(f"{src.name}: no payload columns to write")
            frame = frame.with_columns(
                *(_member_literal(k, v) for k, v in columns.items()),
                pl.lit(key, dtype=pl.String).alias(ORIGIN_COLUMN),
            )
            if isinstance(frame, pl.LazyFrame):
                frame.sink_parquet(out, compression="zstd")  # streaming
            else:
                frame.write_parquet(out, compression="zstd")
        elif isinstance(data, Path | DuckSource):
            source = (
                f"read_parquet({_lit(str(data))}, hive_partitioning=false)"
                if isinstance(data, Path)
                else f"({data.sql})"
            )
            names = [
                r[0]
                for r in conn.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
            ]
            _refuse_member_named(src, names, own)
            drop = [c for c in reserved if c in names]
            if len(drop) == len(names):
                raise LakeError(f"{src.name}: no payload columns to write")
            star = f"* EXCLUDE ({', '.join(_ident(c) for c in drop)})" if drop else "*"
            extra = [f"{_lit(v)}::VARCHAR AS {_ident(k)}" for k, v in columns.items()]
            extra.append(f"{_lit(key)}::VARCHAR AS {_ident(ORIGIN_COLUMN)}")
            conn.execute(
                f"COPY (SELECT {star}, {', '.join(extra)} FROM {source}) "
                f"TO {_lit(str(out))} (FORMAT parquet, COMPRESSION zstd)"
            )
        else:
            raise TypeError(
                f"{src.name}: member data must be a DataFrame, LazyFrame, Path or "
                f"DuckSource (or a function returning one), got {type(data).__name__}"
            )
        return int(pq.ParquetFile(out).metadata.num_rows)

    # -- shared plumbing --

    def _target(self, partitions: dict[str, Any]) -> Path:
        """Partition directory for the write; validates keys against the descriptor."""
        keys = list(partitions)
        declared = self.descriptor.partition_keys
        if declared and keys != declared:
            raise LakeError(
                f"Partitions {keys} do not match the dataset's keys {declared}."
            )
        return partition_dir(self.dataset_dir, partitions)

    def _staging_dir(self) -> Path:
        """Work directory of one write: the Parquet is staged here, then moved.

        Under ``QUANTILICA_LAKE_SCRATCH`` when set (``lake build --scratch``
        passes it to the converter processes), so the temporary Parquet stays
        off the lake's volume and only the finished file is moved there (see
        :func:`_durable_replace`). Otherwise ``<dataset>/.tmp``.
        """
        scratch = os.environ.get(LAKE_SCRATCH_ENV)
        if scratch:
            return _make_scratch_dir(Path(scratch), STAGE_PREFIX)
        base = self.dataset_dir / TMP_DIRNAME
        base.mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(dir=base))

    def _cleanup_staging(self, staging: Path) -> None:
        shutil.rmtree(staging, ignore_errors=True)
        if staging.parent == self.dataset_dir / TMP_DIRNAME:
            try:
                staging.parent.rmdir()  # only when no other writer left anything
            except OSError:
                pass

    def _move(self, tmp: Path, final: Path) -> None:
        """Publish a staged file (see :func:`_durable_replace`)."""
        self._forget_origins()
        _durable_replace(tmp, final, self.dataset_dir / TMP_DIRNAME)
        self._forget_origins()

    def _skip_if_current(
        self,
        target: Path,
        key: str,
        src: Path | None,
        digest: str,
        contract: DataContract | None,
    ) -> LakeWriteResult | None:
        if self.force:
            return None
        version = contract.version if contract is not None else None
        if origin_matches(
            target,
            key,
            digest,
            layout_version=self.descriptor.layout_version,
            contract_version=version,
        ):
            return LakeWriteResult(src, _part_files(target, key), "skipped")
        return None

    def _metadata(
        self,
        key: str,
        src: Path | None,
        digest: str | None,
        contract: DataContract | None,
        converter: str,
    ) -> dict[str, str]:
        manifest = read_manifest_sidecar(src) if src is not None else None
        meta = manifest_to_metadata(manifest)
        meta.update(
            {
                "quantilica.source_id": self.descriptor.source_id,
                "quantilica.dataset_id": self.descriptor.dataset_id,
                META_LAYOUT: str(self.descriptor.layout_version),
                META_FILE_KEY: key,
                "quantilica.converter": converter,
                "quantilica.writer": f"quantilica-analytics/{__version__}",
                META_CONVERTED_AT: datetime.now(UTC).isoformat(timespec="seconds"),
            }
        )
        if src is not None:
            meta["quantilica.origin_file"] = src.name
        if digest is not None:
            meta[META_ORIGIN_SHA] = digest  # the file's own hash beats the manifest's
        if contract is not None:
            meta[META_CONTRACT_ID] = contract.dataset_id
            meta[META_CONTRACT_VERSION] = str(contract.version)
        return meta

    def _publish(self, target: Path, key: str, staged: list[Path]) -> list[Path]:
        """Move staged parts into ``target`` and drop leftovers of older splits."""
        self._forget_origins()
        target.mkdir(parents=True, exist_ok=True)
        names = (
            [f"{key}.parquet"]
            if len(staged) == 1
            else [f"{key}-{i:03d}.parquet" for i in range(1, len(staged) + 1)]
        )
        outputs = [target / name for name in names]
        for path in outputs:  # "a" split in parts collides with the key "a-001"
            owner = _footer(path).get(META_FILE_KEY) if path.exists() else key
            if owner not in (None, key):
                raise LakeError(
                    f"{path.name} already belongs to file key {owner!r}; "
                    f"refusing to overwrite it with {key!r}"
                )
        hop_dir = self.dataset_dir / TMP_DIRNAME
        if len(staged) > 1:
            # Parts staged on another volume (the scratch) are first copied next
            # to the destination, so the renames below run back to back and the
            # window in which old and new parts coexist stays short.
            moved: list[Path] = []
            copies: list[Path] = []
            try:
                for tmp in staged:
                    if _same_volume(tmp, target):
                        moved.append(tmp)
                    else:
                        copy = _copy_to(tmp, hop_dir)
                        copies.append(copy)
                        moved.append(copy)
            except BaseException:
                for copy in copies:  # no half-copied set left in the dataset
                    copy.unlink(missing_ok=True)
                _rmdir_if_empty_tmp(hop_dir)
                raise
            for tmp, new in zip(staged, moved, strict=True):
                if new is not tmp:
                    tmp.unlink(missing_ok=True)
            staged = moved
        # Highest part first: a partial publish never looks like a complete set.
        for tmp, final in reversed(list(zip(staged, outputs, strict=True))):
            self._move(tmp, final)
        keep = set(outputs)
        for orphan in _part_files(target, key):
            if orphan not in keep:
                orphan.unlink()
        _rmdir_if_empty_tmp(hop_dir)
        self._forget_origins()
        return outputs

    def _save_descriptor(
        self,
        partitions: dict[str, Any],
        contract: DataContract | None,
        converter: str,
    ) -> None:
        descriptor = self.descriptor
        if not descriptor.partition_keys and partitions:
            descriptor.partition_keys = list(partitions)
        descriptor.partition_types = _declared_types(descriptor)
        if contract is not None:
            descriptor.contract_id = contract.dataset_id
            descriptor.contract_version = contract.version
        descriptor.converter = converter
        descriptor.save(self.dataset_dir)

    # -- DuckDB engine --

    def _duckdb_convert(
        self,
        src: Path,
        target: Path,
        key: str,
        digest: str,
        staging: Path,
        *,
        origin: Path | None = None,
        partitions: dict[str, Any],
        separator: str,
        encoding: str,
        header: bool,
        names: tuple[str, ...],
        rename: dict[str, str],
        required: tuple[str, ...],
        contract: DataContract | None,
        converter: str,
        quote: str | None = None,
        escape: str | None = None,
        parallel: bool = True,
        skip: int = 0,
        decimal_comma: bool = True,
        members: str | None = None,
    ) -> LakeWriteResult:
        scratch = _scratch_dir(staging)
        conn = _connect(scratch)
        try:
            if members:
                files = _materialize(src, scratch / "raw", members)
            else:
                files = _materialize(src, scratch / "raw")
            if encoding.lower() in _LATIN1 and any(map(_has_c1_bytes, files)):
                files = _latin1_to_utf8(files, scratch / "utf8")
                encoding = "utf-8"
            read = _read_csv_sql(
                files,
                separator=separator,
                encoding=encoding,
                header=header,
                names=names,
                quote=quote,
                escape=escape,
                parallel=parallel,
                skip=skip,
            )
            columns = self._columns(conn, read, header, names, rename, required)
            partition_keys = set(self.descriptor.partition_keys) | set(
                partitions.keys()
            )
            if partition_keys:
                columns = [
                    (src_col, out_col)
                    for src_col, out_col in columns
                    if out_col not in partition_keys
                ]
            if not columns:
                raise LakeError(
                    "No payload columns left to write after dropping partition "
                    f"keys {partition_keys}"
                )
            cast_nulls: dict[str, int] = {}
            select = self._select_list(
                columns,
                contract,
                partition_keys=partition_keys,
                decimal_comma=decimal_comma,
            )
            if contract is not None:
                cast_nulls = self._count_cast_nulls(
                    conn,
                    read,
                    columns,
                    contract,
                    partition_keys=partition_keys,
                    decimal_comma=decimal_comma,
                )
            meta = self._metadata(key, origin or src, digest, contract, converter)
            if cast_nulls:
                meta[META_CAST_NULLS] = json.dumps(cast_nulls, sort_keys=True)
            kv = ", ".join(f"{_lit(k)}: {_lit(v)}" for k, v in meta.items())
            options = f"FORMAT parquet, COMPRESSION zstd, KV_METADATA {{{kv}}}"
            query = f"SELECT {select} FROM {read}"
            target.mkdir(parents=True, exist_ok=True)
            if self.max_part_bytes is None:
                tmp_out = staging / f"{key}.parquet{TMP_SUFFIX}"
                try:
                    rows = _first_value(
                        conn.execute(
                            f"COPY ({query}) TO {_lit(str(tmp_out))} ({options})"
                        )
                    )
                    outputs = self._publish(target, key, [tmp_out])
                except BaseException:
                    tmp_out.unlink(missing_ok=True)
                    raise
            else:
                with tempfile.TemporaryDirectory() as duck_tmp:
                    out_dir = Path(duck_tmp) / "parts"
                    rows = _first_value(
                        conn.execute(
                            f"COPY ({query}) TO {_lit(str(out_dir))} "
                            f"({options}, FILE_SIZE_BYTES {self.max_part_bytes})"
                        )
                    )
                    raw_parts = sorted(
                        out_dir.glob("*.parquet"), key=_duckdb_part_number
                    )
                    staged = []
                    for idx, raw_part in enumerate(raw_parts, 1):
                        part_tmp = staging / f"{key}-{idx:03d}.parquet{TMP_SUFFIX}"
                        shutil.move(str(raw_part), str(part_tmp))
                        staged.append(part_tmp)
                    if not staged:  # empty input: still publish one (empty) file
                        tmp_out = staging / f"{key}.parquet{TMP_SUFFIX}"
                        conn.execute(
                            f"COPY ({query}) TO {_lit(str(tmp_out))} ({options})"
                        )
                        staged = [tmp_out]
                    outputs = self._publish(target, key, staged)
        finally:
            conn.close()
            if scratch != staging:
                shutil.rmtree(scratch, ignore_errors=True)
        return LakeWriteResult(
            origin or src, outputs, "converted", int(rows or 0), cast_nulls
        )

    def _columns(
        self,
        conn,
        read: str,
        header: bool,
        names: tuple[str, ...],
        rename: dict[str, str],
        required: tuple[str, ...],
    ) -> list[tuple[str, str]]:
        """``(source column, output column)`` pairs, validating ``required``."""
        if not header:
            return [(n, n) for n in names]
        raw = [r[0] for r in conn.execute(f"DESCRIBE SELECT * FROM {read}").fetchall()]
        out = [rename.get(n, n) for n in _unique_names(raw)]
        missing = [c for c in required if c not in out]
        if missing:
            raise LakeError(
                f"Unexpected header in {self.descriptor.source_id}/"
                f"{self.descriptor.dataset_id}: missing {missing} "
                f"(columns: {out[:6]}...)"
            )
        return list(zip(raw, out, strict=True))

    @staticmethod
    def _typed(
        columns: list[tuple[str, str]],
        contract: DataContract | None,
        partition_keys: set[str] | None = None,
    ) -> list[tuple[str, str, str | None]]:
        """``(source, output, duckdb type or None)`` for each column."""
        types: dict[str, str] = {}
        if contract is not None:
            present = {out for _, out in columns}
            ignored = partition_keys or set()
            absent = [
                f.name
                for f in contract.fields
                if f.required and f.name not in present and f.name not in ignored
            ]
            if absent:
                raise LakeError(f"Contract {contract.dataset_id}: missing {absent}")
            types = {
                f.name: _duckdb_type(f.dtype)
                for f in contract.fields
                if f.name in present
            }
        return [(src, out, types.get(out)) for src, out in columns]

    def _select_list(
        self,
        columns: list[tuple[str, str]],
        contract: DataContract | None,
        partition_keys: set[str] | None = None,
        decimal_comma: bool = True,
    ) -> str:
        parts = []
        for src, out, duck in self._typed(
            columns, contract, partition_keys=partition_keys
        ):
            expr = (
                _ident(src)
                if duck in (None, "VARCHAR")
                else _try_cast(src, duck, decimal_comma=decimal_comma)
            )
            parts.append(f"{expr} AS {_ident(out)}")
        return ", ".join(parts)

    def _count_cast_nulls(
        self,
        conn,
        read: str,
        columns: list[tuple[str, str]],
        contract: DataContract,
        partition_keys: set[str] | None = None,
        decimal_comma: bool = True,
    ) -> dict[str, int]:
        typed = [
            (src, out, duck)
            for src, out, duck in self._typed(
                columns, contract, partition_keys=partition_keys
            )
            if duck not in (None, "VARCHAR")
        ]
        if not typed:
            return {}
        counts = ", ".join(
            f"count(*) FILTER (WHERE {_ident(src)} IS NOT NULL "
            f"AND {_try_cast(src, duck, decimal_comma=decimal_comma)} IS NULL)"
            for src, _, duck in typed
        )
        row = conn.execute(f"SELECT {counts} FROM {read}").fetchone()
        return {out: int(n) for (_, out, _), n in zip(typed, row, strict=True)}

    # -- Polars engine --

    def _polars_convert(
        self,
        data: pl.DataFrame | pl.LazyFrame,
        src: Path | None,
        target: Path,
        key: str,
        digest: str | None,
        staging: Path,
        contract: DataContract | None,
        converter: str,
        partitions: dict[str, Any] | None = None,
    ) -> LakeWriteResult:
        partition_keys = set(self.descriptor.partition_keys)
        if partitions:
            partition_keys.update(partitions.keys())
        if partition_keys:
            if isinstance(data, pl.LazyFrame):
                cols = set(data.collect_schema().names())
                drop_cols = [c for c in partition_keys if c in cols]
                if drop_cols:
                    data = data.drop(drop_cols)
            else:
                drop_cols = [c for c in partition_keys if c in data.columns]
                if drop_cols:
                    data = data.drop(drop_cols)
        remaining_cols = (
            data.collect_schema().names()
            if isinstance(data, pl.LazyFrame)
            else data.columns
        )
        if not remaining_cols:
            raise LakeError(
                "No payload columns left to write after dropping partition "
                f"keys {partition_keys}"
            )
        meta = self._metadata(key, src, digest, contract, converter)
        target.mkdir(parents=True, exist_ok=True)
        if self.max_part_bytes is None:
            tmp_out = staging / f"{key}.parquet{TMP_SUFFIX}"
            try:
                if isinstance(data, pl.LazyFrame):
                    data.sink_parquet(tmp_out, compression="zstd", metadata=meta)
                else:
                    data.write_parquet(tmp_out, compression="zstd", metadata=meta)
                rows = pq.ParquetFile(tmp_out).metadata.num_rows
                outputs = self._publish(target, key, [tmp_out])
            except BaseException:
                tmp_out.unlink(missing_ok=True)
                raise
        else:
            frame = data.collect() if isinstance(data, pl.LazyFrame) else data
            rows = frame.height
            chunks = max(1, math.ceil(frame.estimated_size() / self.max_part_bytes))
            size = max(1, math.ceil(rows / chunks))
            staged = []
            for i, offset in enumerate(range(0, max(rows, 1), size), 1):
                path = staging / f"{key}-{i:03d}.parquet{TMP_SUFFIX}"
                frame.slice(offset, size).write_parquet(
                    path, compression="zstd", metadata=meta
                )
                staged.append(path)
            outputs = self._publish(target, key, staged)
        return LakeWriteResult(src, outputs, "converted", int(rows))


# --- compaction --------------------------------------------------------------------


def compact_members(
    dataset_dir: Path,
    member_keys: tuple[str, ...] = ("uf",),
    *,
    group_key: Any = None,
    converter: str = "quantilica-analytics",
    workers: int = 1,
) -> dict[str, int]:
    """Rewrite a dataset partitioned by ``member_keys`` into grouped files.

    The migration path to :meth:`LakeWriter.write_member`: the member keys stop
    being partitions (``uf=sp/ano=2020/mes=01`` becomes ``ano=2020/mes=01``),
    each remaining partition gets one Parquet with the rows of all its files,
    sorted by the member keys and ``_origem``, and ``quantilica.members`` is
    built from the old footers, so nothing is converted again. Each group is
    written before its old files are deleted; a rerun after a crash rebuilds
    the groups that still have old files.

    Args:
        dataset_dir: ``<lake>/<source_id>/<dataset_id>``.
        member_keys: Partition keys that become columns.
        group_key: ``f(dataset_id, values) -> str`` naming each grouped file;
            defaults to ``<dataset_id>_<value1><value2>...``.
        converter: Recorded in the footer and the descriptor.
        workers: Groups written at the same time, each with its own DuckDB
            connection (writing one Parquet is single-threaded).

    Returns:
        Counts of ``groups`` written and old ``files`` removed.
    """
    dataset_dir = Path(dataset_dir)
    descriptor = DatasetDescriptor.load(dataset_dir)
    old_keys = list(descriptor.partition_keys)
    members_in = [k for k in member_keys if k in old_keys]
    if not members_in:
        return {"groups": 0, "files": 0}
    new_keys = [k for k in old_keys if k not in members_in]
    name_for = group_key or (
        lambda ds, values: "_".join([ds, "".join(values)]) if values else ds
    )
    groups: dict[tuple[str, ...], list[tuple[Path, dict[str, str]]]] = {}
    for path in sorted(dataset_dir.rglob("*.parquet")):
        rel = path.relative_to(dataset_dir)
        if TMP_DIRNAME in rel.parts:
            continue
        values = dict(part.split("=", 1) for part in rel.parts[:-1] if "=" in part)
        if not all(k in values for k in old_keys):
            continue  # already grouped
        groups.setdefault(tuple(values[k] for k in new_keys), []).append((path, values))

    def compact_group(group_values: tuple[str, ...], files: list) -> int:
        partitions = dict(zip(new_keys, group_values, strict=True))
        gkey = name_for(descriptor.dataset_id, list(group_values))
        target = partition_dir(dataset_dir, partitions)
        # Por chave do bruto, só a versão mais nova: antes de o file_key tirar
        # a data dos DBCs, cada republicação ficava ao lado da anterior.
        newest: dict[str, tuple[str, str]] = {}
        footers = {}
        for path, _values in files:
            meta = footers[path] = _footer(path)
            origin = meta.get("quantilica.origin_file") or path.stem
            key = file_key(origin)
            version = (_DATE_IN_NAME.search(origin) or [""])[0]
            rank = (version, meta.get(META_CONVERTED_AT) or "")
            if key not in newest or rank > newest[key]:
                newest[key] = rank
        members: dict[str, dict[str, Any]] = {}
        origin_of: list[tuple[str, str]] = []
        for path, values in files:
            meta = footers[path]
            origin = meta.get("quantilica.origin_file") or path.stem
            key = file_key(origin)
            version = (_DATE_IN_NAME.search(origin) or [""])[0]
            if (version, meta.get(META_CONVERTED_AT) or "") != newest[key]:
                continue  # versão superada: só é apagada
            entry = members.setdefault(
                key,
                {
                    "origin_file": meta.get("quantilica.origin_file"),
                    "origin_sha256": meta.get(META_ORIGIN_SHA),
                    "rows": 0,
                    "converted_at": meta.get(META_CONVERTED_AT),
                    **{k: values[k] for k in members_in},
                },
            )
            entry["rows"] += pq.ParquetFile(path).metadata.num_rows
            origin_of.append((path.as_posix(), key, *(values[k] for k in members_in)))
        # Os brutos já vêm separados por membro: lidos nessa ordem, com a ordem de
        # inserção preservada, o arquivo sai ordenado sem ORDER BY (que, num mês do
        # SIA-PA, despeja gigabytes em disco).
        origin_of.sort(key=lambda item: (*item[2:], item[1], item[0]))
        meta = {
            "quantilica.source_id": descriptor.source_id,
            "quantilica.dataset_id": descriptor.dataset_id,
            META_LAYOUT: str(descriptor.layout_version),
            META_FILE_KEY: gkey,
            "quantilica.converter": converter,
            "quantilica.writer": f"quantilica-analytics/{__version__}",
            META_CONVERTED_AT: datetime.now(UTC).isoformat(timespec="seconds"),
            META_MEMBERS: json.dumps(members, sort_keys=True, ensure_ascii=False),
            META_MEMBER_COLUMNS: json.dumps(members_in),
        }
        staging = dataset_dir / TMP_DIRNAME
        staging.mkdir(parents=True, exist_ok=True)
        tmp_out = staging / f"{gkey}.parquet{TMP_SUFFIX}"
        sources = "[" + ", ".join(_lit(item[0]) for item in origin_of) + "]"
        cases = " ".join(
            f"WHEN {_lit(item[0])} THEN {_lit(item[1])}" for item in origin_of
        )
        hive_types = ", ".join(f"{_lit(k)}: 'VARCHAR'" for k in old_keys)
        drop = ", ".join(_ident(k) for k in ["filename", *new_keys])
        query = (
            f"SELECT t.* EXCLUDE ({drop}), "
            f"CASE replace(t.filename, '\\', '/') {cases} END "
            f"AS {_ident(ORIGIN_COLUMN)} "
            f"FROM read_parquet({sources}, hive_partitioning=true, "
            f"hive_types={{{hive_types}}}, union_by_name=true, filename=true) t"
        )
        scratch = _scratch_dir(staging)
        if scratch == staging:  # um temporário por grupo: os grupos rodam juntos
            scratch = Path(tempfile.mkdtemp(dir=staging))
        conn = _connect(scratch)
        try:
            conn.execute("SET preserve_insertion_order = true")
            conn.execute(
                f"COPY ({query}) TO {_lit(str(tmp_out))} ({_kv_options(meta)})"
            )
            target.mkdir(parents=True, exist_ok=True)
            _durable_replace(tmp_out, target / f"{gkey}.parquet")
        except BaseException:
            tmp_out.unlink(missing_ok=True)
            raise
        finally:
            conn.close()
            shutil.rmtree(scratch, ignore_errors=True)
        for path, _ in files:
            path.unlink()
            parent = path.parent
            while parent != dataset_dir:
                try:
                    parent.rmdir()  # só vazio; outro mês pode ter acabado de apagar
                except OSError:
                    break
                parent = parent.parent
        return len(files)

    written = removed = 0
    with dataset_lock(dataset_dir):
        items = sorted(groups.items())
        if workers <= 1:
            for group_values, files in items:
                removed += compact_group(group_values, files)
                written += 1
        else:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(workers) as pool:
                for count in pool.map(lambda item: compact_group(*item), items):
                    removed += count
                    written += 1
        shutil.rmtree(dataset_dir / TMP_DIRNAME, ignore_errors=True)
        descriptor.partition_keys = new_keys
        descriptor.partition_types = {
            k: v for k, v in descriptor.partition_types.items() if k in new_keys
        }
        descriptor.converter = converter
        descriptor.save(dataset_dir)
    return {"groups": written, "files": removed}


# --- readers ---------------------------------------------------------------------


def _dataset_files(dataset_dir: Path) -> list[Path]:
    return sorted(
        p
        for p in dataset_dir.rglob("*.parquet")
        if TMP_DIRNAME not in p.relative_to(dataset_dir).parts
    )


def scan_dataset(dataset_dir: Path) -> pl.LazyFrame:
    """Lazily read a dataset with Hive partitions as columns.

    Partition columns take the types declared in ``_dataset.json`` (so
    ``mes=09`` stays ``"09"`` when declared ``varchar``); without a readable
    descriptor Polars infers them.

    When the files do not share one schema (a column the source added in later
    years, a type that changed), the files are read one by one and combined by
    column name, with supertypes and nulls for the missing columns, as DuckDB's
    ``union_by_name`` does. ``pl.scan_parquet`` alone takes the first file's
    schema and rejects the others.
    """
    dataset_dir = Path(dataset_dir)
    try:
        descriptor = DatasetDescriptor.load(dataset_dir)
    except (OSError, ValueError):
        descriptor = None
    hive_schema: dict[str, Any] = {}
    if descriptor is not None and descriptor.partition_keys:
        hive_schema = {
            key: _partition_type(key, declared)[1]
            for key, declared in _declared_types(descriptor).items()
        }
    files = _dataset_files(dataset_dir)
    schemas = {
        pq.read_schema(p).remove_metadata() for p in files
    }  # set: pyarrow schemas hash by fields
    if len(schemas) <= 1:
        kwargs: dict[str, Any] = {"hive_schema": hive_schema} if hive_schema else {}
        return pl.scan_parquet(
            dataset_dir / "**/*.parquet", hive_partitioning=True, **kwargs
        )
    frames = []
    for path in files:
        values = dict(
            part.split("=", 1)
            for part in path.relative_to(dataset_dir).parts[:-1]
            if "=" in part
        )
        frame = pl.scan_parquet(path, hive_partitioning=False)
        frames.append(
            frame.with_columns(
                pl.lit(None if value == HIVE_DEFAULT_PARTITION else value)
                .cast(hive_schema.get(key, pl.String))
                .alias(key)
                for key, value in values.items()
            )
        )
    return pl.concat(frames, how="diagonal_relaxed")


def duckdb_view_sql(dataset_dir: Path, view_name: str | None = None) -> str:
    """``CREATE OR REPLACE VIEW`` over a dataset, typed by its descriptor.

    The view is named ``view_name`` or the descriptor's ``alias``, and reads
    ``read_parquet('<dataset>/**/*.parquet', hive_partitioning = true,
    hive_types = {...})`` with the declared partition types.
    """
    dataset_dir = Path(dataset_dir)
    descriptor = DatasetDescriptor.load(dataset_dir)
    name = view_name or descriptor.alias
    options = ["hive_partitioning = true"]
    if descriptor.partition_keys:
        types = ", ".join(
            f"{_lit(key)}: {_lit(_partition_type(key, declared)[0])}"
            for key, declared in _declared_types(descriptor).items()
        )
        options.append(f"hive_types = {{{types}}}")
    glob = _lit(str(dataset_dir / "**/*.parquet"))
    return (
        f"CREATE OR REPLACE VIEW {_ident(name)} AS\n"
        f"SELECT * FROM read_parquet({glob}, {', '.join(options)});"
    )


# --- conformance checker ------------------------------------------------------------


def check_dataset(dataset_dir: Path) -> list[str]:
    """Verify a dataset against the lake contract; ``[]`` means conforming.

    Checks, in order: ``_dataset.json`` loads; directory names are Hive
    ``k=v`` following the descriptor's keys; Parquet names are
    ``<file_key>[-NNN].parquet`` with contiguous parts; required ``quantilica.*``
    footer metadata is present and consistent with the descriptor; partition
    columns are not stored inside the files; and DuckDB, Polars and PyArrow
    read the same number of rows with the declared partition types.
    """
    dataset_dir = Path(dataset_dir)
    try:
        descriptor = DatasetDescriptor.load(dataset_dir)
    except FileNotFoundError:
        return [f"{DESCRIPTOR_FILENAME} not found in {dataset_dir}"]
    except (OSError, ValueError, TypeError) as exc:
        return [f"{DESCRIPTOR_FILENAME} is invalid: {exc}"]

    errors = _check_descriptor(descriptor)
    files, layout_errors = _check_layout(dataset_dir, descriptor)
    errors += layout_errors
    errors += _check_footers(dataset_dir, descriptor, files)
    if errors or not files:
        return errors
    return _check_readers(dataset_dir, descriptor, files)


def _check_descriptor(descriptor: DatasetDescriptor) -> list[str]:
    errors = []
    if descriptor.layout_version != LAYOUT_VERSION:
        errors.append(
            f"layout_version {descriptor.layout_version} is not supported "
            f"(expected {LAYOUT_VERSION})"
        )
    stray = sorted(set(descriptor.partition_types) - set(descriptor.partition_keys))
    if stray:
        errors.append(f"partition_types declares keys that are not partitions: {stray}")
    for key, declared in _declared_types(descriptor).items():
        try:
            _partition_type(key, declared)
        except LakeError as exc:
            errors.append(str(exc))
    return errors


def _check_layout(
    dataset_dir: Path, descriptor: DatasetDescriptor
) -> tuple[list[Path], list[str]]:
    errors: list[str] = []
    files: list[Path] = []
    expected = descriptor.partition_keys

    def walk(directory: Path, depth: int) -> None:
        for entry in sorted(directory.iterdir()):
            rel = entry.relative_to(dataset_dir)
            if entry.name.startswith("."):  # .lock, .tmp
                continue
            if entry.is_dir():
                match = _PARTITION_DIR.match(entry.name)
                if match is None:
                    errors.append(f"{rel}: not a valid Hive 'key=value' directory")
                    continue
                if depth >= len(expected) or match["key"] != expected[depth]:
                    want = expected[depth] if depth < len(expected) else "a file"
                    errors.append(f"{rel}: expected {want} at level {depth + 1}")
                    continue
                walk(entry, depth + 1)
            elif directory == dataset_dir and entry.name == DESCRIPTOR_FILENAME:
                continue
            elif entry.suffix == ".parquet" and _PART_NAME.match(entry.name):
                if depth != len(expected):
                    errors.append(f"{rel}: expected {len(expected)} partition levels")
                else:
                    files.append(entry)
            else:
                errors.append(f"{rel}: unexpected file (want <file_key>[-NNN].parquet)")

    walk(dataset_dir, 0)

    by_key: dict[tuple[Path, str], list[str | None]] = {}
    for path in files:
        key = _footer(path).get(META_FILE_KEY)
        part = None
        if key:
            key_pattern = re.compile(rf"^{re.escape(key)}(?:-(\d{{3}}))?\.parquet$")
            m = key_pattern.match(path.name)
            if m:
                part = m.group(1)
            else:
                match = _PART_NAME.match(path.name)
                key = match["key"] if match else path.stem
                part = match["part"] if match else None
        else:
            match = _PART_NAME.match(path.name)
            key = match["key"] if match else path.stem
            part = match["part"] if match else None

        if file_key(key) != key:
            errors.append(
                f"{path.relative_to(dataset_dir)}: file key {key!r} keeps a raw "
                "extension or @date suffix"
            )
        by_key.setdefault((path.parent, key), []).append(part)
    for (directory, key), parts in by_key.items():
        where = directory.relative_to(dataset_dir) / key
        if None in parts and len(parts) > 1:
            errors.append(f"{where}: mixes an unsplit file with numbered parts")
        elif None not in parts and sorted(
            int(p) for p in parts if p is not None
        ) != list(range(1, len(parts) + 1)):
            errors.append(f"{where}: part numbers are not contiguous from 001")
    return files, errors


def _check_footers(
    dataset_dir: Path, descriptor: DatasetDescriptor, files: list[Path]
) -> list[str]:
    errors = []
    for path in files:
        rel = path.relative_to(dataset_dir)
        try:
            columns = pq.ParquetFile(path).schema_arrow.names
        except (OSError, pa.ArrowException) as exc:
            errors.append(f"{rel}: unreadable Parquet ({exc})")
            continue
        meta = _footer(path)
        missing = [k for k in REQUIRED_METADATA if k not in meta]
        if missing:
            errors.append(f"{rel}: missing metadata {missing}")
        file_key_val = meta.get(META_FILE_KEY)
        expectations = {
            META_LAYOUT: str(descriptor.layout_version),
            "quantilica.source_id": descriptor.source_id,
            "quantilica.dataset_id": descriptor.dataset_id,
        }
        if descriptor.contract_id is not None:
            expectations[META_CONTRACT_ID] = descriptor.contract_id
        for name, want in expectations.items():
            if name in meta and meta[name] != want:
                errors.append(f"{rel}: {name} is {meta[name]!r}, expected {want!r}")
        if file_key_val is not None:
            key_pattern = re.compile(
                rf"^{re.escape(file_key_val)}(?:-(\d{{3}}))?\.parquet$"
            )
            if not key_pattern.match(path.name):
                errors.append(
                    f"{rel}: {META_FILE_KEY} is {file_key_val!r}, "
                    "which does not match filename"
                )
        inside = sorted(set(descriptor.partition_keys) & set(columns))
        if inside:
            errors.append(f"{rel}: partition columns stored inside the file: {inside}")
    return errors


# Linhas acima das quais o Polars (índice u32) não conta o dataset de uma vez.
_POLARS_MAX_COUNT = 2**32


def _check_readers(
    dataset_dir: Path, descriptor: DatasetDescriptor, files: list[Path]
) -> list[str]:
    # DuckDB spills to ".tmp" in the working directory by default: with the
    # process inside a (read-only) lake that would write there. Each call gets
    # its own spill folder, outside the lake, removed at the end.
    base = Path(os.environ.get(LAKE_SCRATCH_ENV) or tempfile.gettempdir())
    base.mkdir(parents=True, exist_ok=True)
    spill = Path(tempfile.mkdtemp(prefix="duckdb-check-", dir=base))
    try:
        return _check_readers_in(dataset_dir, descriptor, files, spill)
    finally:
        shutil.rmtree(spill, ignore_errors=True)


def _check_readers_in(
    dataset_dir: Path,
    descriptor: DatasetDescriptor,
    files: list[Path],
    spill: Path,
) -> list[str]:
    import duckdb

    errors: list[str] = []
    expected_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
    declared = _declared_types(descriptor)
    counts: dict[str, int] = {}

    # DuckDB, through the same view the consumers create.
    try:
        conn = duckdb.connect()
        conn.execute(f"SET temp_directory = {_lit(str(spill))}")
        conn.execute(duckdb_view_sql(dataset_dir, "_lake_check"))
        counts["duckdb"] = _first_value(
            conn.execute("SELECT count(*) FROM _lake_check")
        )
        described = {
            r[0]: r[1].upper()
            for r in conn.execute("DESCRIBE SELECT * FROM _lake_check").fetchall()
        }
        for key, declared_type in declared.items():
            want = _partition_type(key, declared_type)[0]
            if described.get(key) != want:
                got = described.get(key)
                errors.append(f"duckdb: partition {key!r} is {got}, expected {want}")
    except Exception as exc:
        errors.append(f"duckdb cannot read the dataset: {exc}")

    # Polars, through scan_dataset.
    try:
        lazy = scan_dataset(dataset_dir)
        if declared and expected_rows >= _POLARS_MAX_COUNT:
            # Sem o build de 64 bits, o Polars conta em u32 e estoura acima de
            # 4,29 bilhões de linhas (o SIA-PA tem 7,6 bilhões): conta uma partição
            # por vez. Um group_by sobre o dataset inteiro materializa tudo.
            first = next(iter(declared))
            values = sorted(
                {
                    part.split("=", 1)[1]
                    for p in files
                    for part in p.relative_to(dataset_dir).parts[:-1]
                    if part.startswith(f"{first}=")
                }
            )
            want = _partition_type(first, declared[first])[1]
            counts["polars"] = sum(
                lazy.filter(pl.col(first) == pl.lit(v).cast(want))
                .select(pl.len())
                .collect()
                .item()
                for v in values
            )
        else:
            counts["polars"] = lazy.select(pl.len()).collect().item()
        schema = lazy.collect_schema()
        for key, declared_type in declared.items():
            want = _partition_type(key, declared_type)[1]
            if schema.get(key) != want:
                errors.append(
                    f"polars: partition {key!r} is {schema.get(key)}, expected {want}"
                )
    except Exception as exc:
        errors.append(f"polars cannot read the dataset: {exc}")

    # PyArrow, with the declared partition schema.
    try:
        partitioning = None
        if declared:
            partitioning = pa_ds.partitioning(
                pa.schema(
                    [(key, _partition_type(key, t)[2]) for key, t in declared.items()]
                ),
                flavor="hive",
            )
        dataset = pa_ds.dataset(
            dataset_dir, format="parquet", partitioning=partitioning
        )
        counts["pyarrow"] = dataset.count_rows()
        for key, declared_type in declared.items():
            want = _partition_type(key, declared_type)[2]
            got = dataset.schema.field(key).type
            if got != want:
                errors.append(f"pyarrow: partition {key!r} is {got}, expected {want}")
    except Exception as exc:
        errors.append(f"pyarrow cannot read the dataset: {exc}")

    for reader, count in counts.items():
        if count != expected_rows:
            errors.append(
                f"{reader} reads {count} rows, the Parquet footers add up to "
                f"{expected_rows}"
            )
    return errors


# --- Declarative DatasetSpec & SpecLakeConverter framework -----------------------


def detect_encoding(path_or_bytes: Path | bytes) -> str:
    """Detect encoding: BOM -> utf-8-sig; valid UTF-8 in 64KB -> utf-8; else cp1252.

    A ``.gz`` and the first data member of a ``.zip`` are sampled decompressed.
    A multibyte character cut by the 64 KB boundary does not count as invalid.
    """
    return _detect_encoding(path_or_bytes)


def _detect_encoding(path_or_bytes: Path | bytes, members: str | None = None) -> str:
    """:func:`detect_encoding`, sampling the ``members`` glob of a ZIP."""
    if isinstance(path_or_bytes, Path):
        try:
            chunk = _sample_bytes(path_or_bytes, members=members)
        except (OSError, zipfile.BadZipFile, EOFError):
            with open(path_or_bytes, "rb") as f:
                chunk = f.read(65536)
    else:
        chunk = path_or_bytes[:65536]
    if chunk.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        codecs.getincrementaldecoder("utf-8")().decode(chunk, final=False)
        return "utf-8"
    except UnicodeDecodeError:
        return "cp1252"


def find_skip_until(file_path: Path, prefix: str, encoding: str) -> int:
    """Count how many preamble lines to skip until a line starts with ``prefix``.

    The file is decoded with ``encoding`` itself (Latin-1 is real Latin-1, not
    cp1252); a ``.gz`` and the first data member of a ``.zip`` are read decompressed.
    """
    return _find_skip_until(file_path, prefix, encoding)


def _find_skip_until(
    file_path: Path, prefix: str, encoding: str, members: str | None = None
) -> int:
    """:func:`find_skip_until`, reading the ``members`` glob of a ZIP."""
    enc = _py_codec(encoding)
    try:
        if file_path.suffix.lower() == ".gz":
            with gzip.open(file_path, "rt", encoding=enc, errors="replace") as gz:
                for idx, line in enumerate(gz):
                    if line.startswith(prefix):
                        return idx
        elif file_path.suffix.lower() == ".zip":
            with zipfile.ZipFile(file_path) as zf:
                names = [i.filename for i in zf.infolist() if not i.is_dir()]
                name = _select_member_names(names, members)[0]
                with zf.open(name) as member_file:
                    for idx, line in enumerate(member_file):
                        decoded = line.decode(enc, errors="replace")
                        if decoded.startswith(prefix):
                            return idx
        else:
            with open(file_path, encoding=enc, errors="replace") as f:
                for idx, line in enumerate(f):
                    if line.startswith(prefix):
                        return idx
    except Exception:
        pass
    return 0


@dataclass
class ConvertResult:
    """Summary of a conversion run across one or more datasets."""

    converted: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DatasetSpec:
    """How one raw dataset is read and converted to the lake."""

    dataset_id: str
    partition_keys: tuple[str, ...]
    partitions_from: Callable[[Path], dict[str, str | int | None]]
    reader: Literal["duckdb", "polars", "fixed_width", "parquet", "dbf"] = "duckdb"
    encoding: str | Literal["detect"] = "detect"
    separator: str = ";"
    header: bool = True
    decimal_comma: bool = True
    skip_until: str | None = None
    rename: Mapping[str, str] = field(default_factory=dict)
    required: tuple[str, ...] = ()
    members: str | None = None
    contract: DataContract | None = None
    normalize: Callable[[Path], Iterable[bytes]] | None = None
    fallback: bool = True
    """Tolerate a wrong guess about the file.

    With ``True`` (the default), a header that lacks ``separator`` but uses
    another one of ``; , tab |`` is read with that one, and a failed UTF-8
    guess (``encoding="detect"``) is retried as Latin-1; both log a warning.
    With ``False`` the spec is applied exactly as written.

    ``members`` is a case-insensitive glob choosing which text members of a
    ZIP/7z archive are read (all of them when ``None``).
    """

    @property
    def alias(self) -> str:
        """Short view name: dataset_id as a SQL-friendly identifier."""
        return self.dataset_id.replace("-", "_")

    @property
    def partition_types(self) -> dict[str, str]:
        """Partition types, as ``quantilica.core.lake.partition_types_for`` defines.

        ``ano`` is an integer; ``mes``, ``trimestre`` and every other key are
        ``varchar`` (the layout writes ``mes=01``, which an integer would drop).
        """
        return partition_types_for(self.partition_keys)


_SEPARATOR_CANDIDATES = (";", ",", "\t", "|")
_ENCODING_ERROR_HINTS = ("unicode", "utf", "encod", "codec", "decode")


def _py_codec(encoding: str) -> str:
    """Python codec name for ``encoding``; Latin-1 stays real Latin-1.

    ``latin-1`` (and its aliases) is *not* ``cp1252``: the two differ in
    0x80-0x9F, and cp1252 leaves 0x81, 0x8D, 0x8F, 0x90 and 0x9D undefined.
    """
    norm = encoding.lower().replace("_", "-")
    if norm in _LATIN1:
        return "latin-1"
    if norm in ("utf8", "utf-8"):
        return "utf-8"
    if norm in ("utf8-sig", "utf-8-sig"):
        return "utf-8-sig"
    return encoding


def _sample_bytes(path: Path, limit: int = 65536, members: str | None = None) -> bytes:
    """The first ``limit`` bytes of the text in ``path`` (decompressed if needed).

    Looks inside ``.gz`` files and into the first member of a ``.zip`` that
    :func:`_materialize` would read (same choice and order, ``members``
    included).
    """
    suffix = path.suffix.lower()
    if suffix == ".gz":
        with gzip.open(path, "rb") as fh:
            return fh.read(limit)
    if suffix == ".zip":
        with zipfile.ZipFile(path) as zf:
            infos = {i.filename: i for i in zf.infolist() if not i.is_dir()}
            try:
                chosen = _select_member_names(infos, members)
            except LakeError:
                return b""
            with zf.open(infos[chosen[0]]) as fh:
                return fh.read(limit)
    with open(path, "rb") as fh:
        return fh.read(limit)


def _sniff_separator(
    path: Path, encoding: str, skip: int, current: str, members: str | None = None
) -> str:
    """The delimiter of the header line when ``current`` does not occur in it.

    Returns ``current`` when it is present in the line (the declared
    separator is trusted), when no other candidate is, or when the file cannot
    be sampled (7z, for one).
    """
    if path.suffix.lower() not in (*TEXT_SUFFIXES, ".gz", ".zip"):
        return current
    try:
        chunk = _sample_bytes(path, members=members)
        lines = chunk.decode(_py_codec(encoding), errors="replace").splitlines()
    except Exception:
        return current
    if len(lines) <= skip:
        return current
    line = lines[skip]
    if current in line:
        return current
    counts = {c: line.count(c) for c in _SEPARATOR_CANDIDATES if c != current}
    best = max(counts, key=lambda c: counts[c])
    if counts[best] == 0:
        return current
    logger.warning(
        "lake: %s: separator %r not found in the header, using %r",
        path.name,
        current,
        best,
    )
    return best


def _is_encoding_error(exc: Exception) -> bool:
    if isinstance(exc, UnicodeError):
        return True
    text = str(exc).lower()
    return any(hint in text for hint in _ENCODING_ERROR_HINTS)


def _convert_spec_file(
    writer: LakeWriter,
    spec: DatasetSpec,
    path: Path,
    partitions: dict[str, Any],
    converter: str,
) -> LakeWriteResult:
    """Convert one raw file according to ``spec``.

    With ``spec.encoding == "detect"`` and ``spec.fallback``, a UTF-8 guess
    that fails to decode (the sniff only sees the first 64 KB) is retried as
    Latin-1.
    """
    detect = spec.encoding == "detect"
    enc = _detect_encoding(path, spec.members) if detect else spec.encoding
    candidates = [enc]
    if detect and spec.fallback and enc.lower().startswith("utf"):
        candidates.append("latin-1")
    for i, candidate in enumerate(candidates):
        try:
            return _convert_spec_file_with(
                writer, spec, path, partitions, converter, candidate
            )
        except Exception as exc:
            if i + 1 < len(candidates) and _is_encoding_error(exc):
                logger.warning(
                    "lake: %s: decoding as %s failed (%s), retrying as %s",
                    path.name,
                    candidate,
                    str(exc).splitlines()[0] if str(exc) else exc,
                    candidates[i + 1],
                )
                continue
            raise
    raise AssertionError("unreachable")  # pragma: no cover


def _convert_spec_file_with(
    writer: LakeWriter,
    spec: DatasetSpec,
    path: Path,
    partitions: dict[str, Any],
    converter: str,
    enc: str,
) -> LakeWriteResult:
    suffix = path.suffix.lower()

    def write_eager(read: Callable[[], pl.DataFrame]) -> LakeWriteResult:
        # An unchanged file is skipped before the plan runs, so do not decode it.
        current = writer.is_current(path, partitions=partitions, contract=spec.contract)
        return writer.write_polars(
            pl.DataFrame() if current else read(),
            path,
            partitions=partitions,
            contract=spec.contract,
            converter=converter,
        )

    if spec.reader == "parquet" or suffix == ".parquet":
        return writer.write_polars(
            pl.scan_parquet(path),
            path,
            partitions=partitions,
            contract=spec.contract,
            converter=converter,
        )

    if spec.reader == "dbf" or suffix in (".dbf", ".dbc"):
        from quantilica.analytics.dbf import read_dbc, read_dbf

        codec = None if spec.encoding == "detect" else _py_codec(spec.encoding)
        reader_fn = read_dbc if suffix == ".dbc" else read_dbf
        return write_eager(lambda: reader_fn(path, encoding=codec))

    if spec.reader == "polars":
        if suffix in (".xlsx", ".xls"):
            return write_eager(lambda: pl.read_excel(path))
        codec = _py_codec(enc)
        separator = spec.separator
        if spec.fallback:
            separator = _sniff_separator(path, codec, 0, separator)
        if codec in ("utf-8", "utf-8-sig"):
            return writer.write_polars(
                pl.scan_csv(
                    path,
                    separator=separator,
                    encoding="utf8",
                    has_header=spec.header,
                ),
                path,
                partitions=partitions,
                contract=spec.contract,
                converter=converter,
            )
        # Polars' lazy CSV scan only decodes UTF-8; other codecs are read eagerly.
        return write_eager(
            lambda: pl.read_csv(
                path,
                separator=separator,
                encoding=codec,
                has_header=spec.header,
            )
        )

    if spec.reader == "duckdb":
        skip = 0
        if spec.skip_until:
            skip = _find_skip_until(path, spec.skip_until, enc, spec.members)
        separator = spec.separator
        if spec.fallback:
            separator = _sniff_separator(path, enc, skip, separator, spec.members)
        return writer.write_duckdb(
            path,
            partitions=partitions,
            separator=separator,
            encoding=enc,
            header=spec.header,
            rename=dict(spec.rename),
            required=spec.required,
            contract=spec.contract,
            converter=converter,
            skip=skip,
            decimal_comma=spec.decimal_comma,
            members=spec.members,
        )

    if spec.reader == "fixed_width":
        from quantilica.analytics.fixed_width import (
            fixed_width_sql,
            layout_from_datasus,
        )

        # Look for a layout file next to the data, named after its file key.
        layout_file = path.parent / f"{file_key(path)}_layout.txt"
        if not layout_file.is_file():
            layout_file = path.parent / "layout.txt"
        if not layout_file.is_file():
            raise LakeError(f"Layout file not found for {path.name}")
        layout = layout_from_datasus(layout_file.read_text(encoding="latin-1"))

        def read_fixed() -> pl.DataFrame:
            import duckdb

            conn = duckdb.connect()
            try:
                sql = fixed_width_sql(str(path), layout, encoding=enc)
                # A relation's .pl() exists in every DuckDB; the connection's
                # was removed in 2.0.
                return conn.sql(sql).pl()
            finally:
                conn.close()

        return write_eager(read_fixed)

    raise LakeError(f"Unsupported reader: {spec.reader}")


def convert_with_specs(
    source_id: str,
    specs: Mapping[str, DatasetSpec],
    input_dir: Path | str,
    lake_dir: Path | str,
    *,
    datasets: list[str] | None = None,
    force: bool = False,
    converter: str = "quantilica-analytics",
) -> ConvertResult:
    """Convert raw files for specified datasets into Lake using DatasetSpecs."""
    from quantilica.core.lake import select_current

    input_dir = Path(input_dir)
    lake_dir = Path(lake_dir)
    result = ConvertResult()
    if not input_dir.is_dir():
        result.errors.append(f"diretório '{input_dir}' não existe")
        return result

    specs_to_run: list[DatasetSpec] = []
    if datasets is not None:
        for name in datasets:
            spec = specs.get(name)
            if spec is None:
                result.errors.append(f"dataset desconhecido: {name}")
            elif spec not in specs_to_run:
                specs_to_run.append(spec)
    else:
        specs_to_run = list(specs.values())

    supported_suffixes = frozenset(
        {
            ".csv",
            ".txt",
            ".zip",
            ".7z",
            ".gz",
            ".parquet",
            ".xlsx",
            ".xls",
            ".json",
            ".dbf",
            ".dbc",
        }
    )

    def is_layout_file(p: Path) -> bool:
        return p.stem.lower().endswith("_layout") or p.name.lower() == "layout.txt"

    for spec in specs_to_run:
        # Locate files for this spec
        candidate_dirs = [input_dir / spec.dataset_id]
        if spec.alias != spec.dataset_id:
            candidate_dirs.append(input_dir / spec.alias)

        target_dir = None
        for d in candidate_dirs:
            if d.is_dir():
                target_dir = d
                break

        if target_dir is not None:
            raw_files = [
                p
                for p in target_dir.rglob("*")
                if p.is_file()
                and p.suffix.lower() in supported_suffixes
                and not p.name.endswith(".manifest.json")
                and not p.name.endswith(".tmp")
                and not p.name.startswith(".")
                and not (spec.reader == "fixed_width" and is_layout_file(p))
            ]
        else:
            # Compare lexically, never through ``resolve()``: the input may be a
            # view of links into a mount, and resolving a link leaves the view
            # (the file would then look like it belongs to no other dataset).
            other_dataset_dirs = {
                Path(os.path.abspath(input_dir / s.dataset_id))
                for s in specs.values()
                if s.dataset_id != spec.dataset_id
                and (input_dir / s.dataset_id).is_dir()
            }
            raw_files = [
                p
                for p in input_dir.rglob("*")
                if p.is_file()
                and p.suffix.lower() in supported_suffixes
                and not p.name.endswith(".manifest.json")
                and not p.name.endswith(".tmp")
                and not p.name.startswith(".")
                and not (spec.reader == "fixed_width" and is_layout_file(p))
                and not any(
                    Path(os.path.abspath(p)).is_relative_to(d)
                    for d in other_dataset_dirs
                )
            ]

        if not raw_files:
            continue

        # select_current has constrained TypeVar (str, PurePath): it returns PurePath.
        selected_files = cast("list[Path]", select_current(raw_files))

        descriptor = DatasetDescriptor(
            source_id=source_id,
            dataset_id=spec.dataset_id,
            alias=spec.alias,
            partition_keys=list(spec.partition_keys),
            partition_types=spec.partition_types,
        )
        writer = LakeWriter(
            lake_dir / source_id / spec.dataset_id,
            descriptor,
            force=force,
        )

        for path in selected_files:
            try:
                partitions = spec.partitions_from(path)
            except Exception as exc:
                result.errors.append(f"{path.name}: {exc}")
                continue

            try:
                written = _convert_spec_file(writer, spec, path, partitions, converter)

                if written.status == "skipped":
                    result.skipped += 1
                else:
                    result.converted += 1
            except Exception as exc:
                message = str(exc).strip()
                first_line = message.splitlines()[0] if message else repr(exc)
                result.errors.append(f"{path.name}: {first_line}")

    return result


class SpecLakeConverter:
    """Standard LakeConverter backed by declarative DatasetSpec table."""

    def __init__(
        self,
        source_id: str,
        specs: Mapping[str, DatasetSpec],
        *,
        version: str = "0.0.0",
    ) -> None:
        self.source_id = source_id
        self.specs = dict(specs)
        self.version = version

    def datasets(self) -> list[str]:
        """Dataset ids this converter writes to the lake."""
        return list(self.specs)

    def convert(
        self,
        input_dir: Path | str,
        lake_dir: Path | str,
        *,
        datasets: list[str] | None = None,
        force: bool = False,
    ) -> ConvertResult:
        """Convert raw files into the lake according to specs."""
        return convert_with_specs(
            self.source_id,
            self.specs,
            Path(input_dir),
            Path(lake_dir),
            datasets=datasets,
            force=force,
            converter=f"{self.source_id}-fetcher {self.version}",
        )
