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

import json
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
from quantilica.core.files import sha256_file
from quantilica.core.lake import (
    DESCRIPTOR_FILENAME,
    LAYOUT_VERSION,
    DatasetDescriptor,
    dataset_lock,
    file_key,
    partition_dir,
)
from quantilica.core.manifests import read_manifest_sidecar

from quantilica.analytics.schema import DataContract
from quantilica.analytics.writer import manifest_to_metadata

try:
    __version__ = version("quantilica-analytics")
except PackageNotFoundError:  # pragma: no cover - source checkout without install
    __version__ = "0.0.0"

logger = logging.getLogger("quantilica.analytics.lake")

TMP_DIRNAME = ".tmp"
TMP_SUFFIX = ".tmp"
TEXT_SUFFIXES = frozenset({".csv", ".txt"})
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


def _try_cast(column: str, duck: str) -> str:
    """``TRY_CAST`` of a text column that survives DuckDB's CSV cast pushdown.

    A bare ``TRY_CAST(col AS T)`` over ``read_csv`` is pushed into the reader,
    which then *raises* on the first bad value. ``NULLIF(col, '')`` (a no-op:
    empty fields are already null) keeps the cast in the query plan.
    """
    return f"TRY_CAST(NULLIF({_ident(column)}, '') AS {duck})"


def _scratch_dir(staging: Path) -> Path:
    """Work directory for extraction: a fresh one under ``QUANTILICA_LAKE_SCRATCH``
    when set, else ``staging`` itself."""
    base = os.environ.get(LAKE_SCRATCH_ENV)
    if not base:
        return staging
    Path(base).mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(dir=base))


def _materialize(src: Path, tmp: Path) -> list[Path]:
    """Text files to read: the file itself, or the members of a ZIP/7z."""
    suffix = src.suffix.lower()
    if suffix in TEXT_SUFFIXES:
        return [src]
    if suffix == ".zip":
        with zipfile.ZipFile(src) as archive:
            members = [m for m in archive.infolist() if not m.is_dir()]
            archive.extractall(tmp, members=members)
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
    files = sorted(p for p in tmp.rglob("*") if p.is_file())
    if not files:
        raise LakeError(f"{src.name} contains no files")
    return files


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
) -> str:
    args = [
        "[" + ", ".join(_lit(str(f)) for f in files) + "]",
        f"delim={_lit(separator)}",
        f"header={'true' if header else 'false'}",
        "all_varchar=true",
        f"encoding={_lit(encoding)}",
        "null_padding=true",
        "strict_mode=false",
    ]
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
        raw = pq.ParquetFile(path).metadata.metadata or {}
    except (OSError, pa.ArrowException):
        return {}
    return {k.decode(): v.decode(errors="replace") for k, v in raw.items()}


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


def _members(path: Path, group_key: str) -> dict[str, dict[str, Any]] | None:
    """Members of the grouped file ``path``; ``None`` if it is not ``group_key``."""
    meta = _footer(path)
    if meta.get(META_FILE_KEY) != group_key or META_MEMBERS not in meta:
        return None
    return json.loads(meta[META_MEMBERS])


def member_matches(
    path: Path,
    group_key: str,
    key: str,
    sha256: str,
    *,
    layout_version: int = LAYOUT_VERSION,
) -> bool:
    """Whether raw file ``key`` is in the grouped file ``path`` with ``sha256``."""
    if not path.is_file():
        return False
    if _footer(path).get(META_LAYOUT) != str(layout_version):
        return False
    members = _members(path, group_key) or {}
    return members.get(key, {}).get("origin_sha256") == sha256


def _kv_options(meta: dict[str, str]) -> str:
    kv = ", ".join(f"{_lit(k)}: {_lit(v)}" for k, v in meta.items())
    return (
        f"FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880, KV_METADATA {{{kv}}}"
    )


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

    # -- public API --

    def write_duckdb(
        self,
        src: Path,
        *,
        partitions: dict[str, Any] | None = None,
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
    ) -> LakeWriteResult:
        """Convert a CSV/TXT (or the members of a ZIP/7z) with DuckDB, streaming.

        Columns are read as text, named from the header (normalized, then
        ``rename``) or from ``names`` when ``header`` is false. ``quote`` and
        ``escape`` fix DuckDB's CSV dialect; ``None`` keeps its auto-detection,
        which can pick ``'`` as the escape of a file quoted with ``"``.
        ``parallel=False`` reads with a single thread, which DuckDB requires for
        quoted fields spanning lines. Latin-1 input with C1 bytes (0x80-0x9F),
        which DuckDB's decoder rejects, is first transcoded to UTF-8.
        ``QUANTILICA_LAKE_SCRATCH`` moves the archive extraction and DuckDB's
        temporary files to another directory (a faster disk, say); the Parquet
        output is still staged next to the dataset and published by rename. ``required``
        columns missing from the header raise :class:`LakeError` before
        anything is written, which rejects an HTML error page saved as CSV.
        Columns typed by ``contract`` go through ``TRY_CAST``; the failures
        are counted in ``quantilica.cast_nulls`` (one extra pass over the
        input).
        """
        src = Path(src)
        rename = rename or {}
        partitions = dict(partitions or {})
        if not header and not names:
            raise ValueError("names is required when header is False.")
        digest = sha256_file(src)
        key = file_key(src)
        with dataset_lock(self.dataset_dir):
            target = self._target(partitions)
            skipped = self._skip_if_current(target, key, src, digest, contract)
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
        target = self._target(dict(partitions or {}))
        skipped = self._skip_if_current(
            target, file_key(src), src, sha256_file(src), contract
        )
        return skipped is not None

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
    ) -> bool:
        """Whether ``src`` is already a member of the grouped file with its ``sha256``.

        The grouped counterpart of :meth:`is_current`. Always ``False`` with ``force``.
        """
        if self.force:
            return False
        src = Path(src)
        target = self._target(dict(partitions or {}))
        return member_matches(
            target / f"{group_key}.parquet",
            group_key,
            file_key(src),
            sha256_file(src),
            layout_version=self.descriptor.layout_version,
        )

    def write_member(
        self,
        data: pl.DataFrame | pl.LazyFrame,
        src: Path,
        *,
        group_key: str,
        partitions: dict[str, Any] | None = None,
        member_columns: dict[str, str] | None = None,
        converter: str = "quantilica-analytics",
    ) -> LakeWriteResult:
        """Write ``src`` into the partition's grouped file ``<group_key>.parquet``.

        One Parquet per partition holds the rows of several raw files (the
        DATASUS publishes one file per state and month, most of them tiny).
        ``member_columns`` become literal columns (``{"uf": "sp"}``), every row
        gets the raw file's key in ``_origem`` and the rows are sorted by the
        member columns, so a filter on them skips row groups by their
        statistics. Writing a member replaces only its own rows; the footer's
        ``quantilica.members`` keeps the provenance of each one. The merge runs
        in DuckDB, spilling to disk, under the dataset lock.
        """
        src = Path(src)
        partitions = dict(partitions or {})
        member_columns = dict(member_columns or {})
        key = file_key(src)
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
    ) -> LakeWriteResult:
        frame = data.collect() if isinstance(data, pl.LazyFrame) else data
        reserved = [*partitions, *member_columns, ORIGIN_COLUMN]
        frame = frame.drop([c for c in reserved if c in frame.columns])
        if not frame.columns:
            raise LakeError(f"{src.name}: no payload columns to write")
        frame = frame.with_columns(
            *(pl.lit(v, dtype=pl.String).alias(k) for k, v in member_columns.items()),
            pl.lit(key, dtype=pl.String).alias(ORIGIN_COLUMN),
        )
        members: dict[str, dict[str, Any]] = {}
        existing = None
        if path.exists():
            members = _members(path, group_key)
            if members is None:
                owner = _footer(path).get(META_FILE_KEY)
                raise LakeError(
                    f"{path.name} is not the grouped file {group_key!r} "
                    f"(file key {owner!r}); refusing to overwrite it"
                )
            existing = path
        members[key] = {
            "origin_file": src.name,
            "origin_sha256": digest,
            "rows": frame.height,
            "converted_at": datetime.now(UTC).isoformat(timespec="seconds"),
            **member_columns,
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
            os.replace(tmp_out, path)
        except BaseException:
            tmp_out.unlink(missing_ok=True)
            raise
        finally:
            conn.close()
            if scratch != staging:
                shutil.rmtree(scratch, ignore_errors=True)
        return LakeWriteResult(src, [path], "converted", int(rows))

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
        base = self.dataset_dir / TMP_DIRNAME
        base.mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(dir=base))

    def _cleanup_staging(self, staging: Path) -> None:
        shutil.rmtree(staging, ignore_errors=True)
        try:
            staging.parent.rmdir()  # only when no other writer left anything
        except OSError:
            pass

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
        # Highest part first: a partial publish never looks like a complete set.
        for tmp, final in reversed(list(zip(staged, outputs, strict=True))):
            os.replace(tmp, final)
        keep = set(outputs)
        for orphan in _part_files(target, key):
            if orphan not in keep:
                orphan.unlink()
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
    ) -> LakeWriteResult:
        scratch = _scratch_dir(staging)
        conn = _connect(scratch)
        try:
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
            select = self._select_list(columns, contract, partition_keys=partition_keys)
            if contract is not None:
                cast_nulls = self._count_cast_nulls(
                    conn, read, columns, contract, partition_keys=partition_keys
                )
            meta = self._metadata(key, src, digest, contract, converter)
            if cast_nulls:
                meta[META_CAST_NULLS] = json.dumps(cast_nulls, sort_keys=True)
            kv = ", ".join(f"{_lit(k)}: {_lit(v)}" for k, v in meta.items())
            options = f"FORMAT parquet, COMPRESSION zstd, KV_METADATA {{{kv}}}"
            query = f"SELECT {select} FROM {read}"
            target.mkdir(parents=True, exist_ok=True)
            if self.max_part_bytes is None:
                tmp_out = staging / f"{key}.parquet{TMP_SUFFIX}"
                try:
                    rows = conn.execute(
                        f"COPY ({query}) TO {_lit(str(tmp_out))} ({options})"
                    ).fetchone()[0]
                    outputs = self._publish(target, key, [tmp_out])
                except BaseException:
                    tmp_out.unlink(missing_ok=True)
                    raise
            else:
                with tempfile.TemporaryDirectory() as duck_tmp:
                    out_dir = Path(duck_tmp) / "parts"
                    rows = conn.execute(
                        f"COPY ({query}) TO {_lit(str(out_dir))} "
                        f"({options}, FILE_SIZE_BYTES {self.max_part_bytes})"
                    ).fetchone()[0]
                    raw_parts = sorted(
                        out_dir.glob("*.parquet"),
                        key=lambda p: int(_DUCKDB_PART_FILE.search(p.stem).group(1)),
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
        return LakeWriteResult(src, outputs, "converted", int(rows or 0), cast_nulls)

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
    ) -> str:
        parts = []
        for src, out, duck in self._typed(
            columns, contract, partition_keys=partition_keys
        ):
            expr = _ident(src) if duck in (None, "VARCHAR") else _try_cast(src, duck)
            parts.append(f"{expr} AS {_ident(out)}")
        return ", ".join(parts)

    def _count_cast_nulls(
        self,
        conn,
        read: str,
        columns: list[tuple[str, str]],
        contract: DataContract,
        partition_keys: set[str] | None = None,
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
            f"AND {_try_cast(src, duck)} IS NULL)"
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
    written = removed = 0
    with dataset_lock(dataset_dir):
        for group_values, files in sorted(groups.items()):
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
                origin_of.append((path.as_posix(), key))
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
            sources = "[" + ", ".join(_lit(p) for p, _ in origin_of) + "]"
            mapping = ", ".join(f"({_lit(p)}, {_lit(k)})" for p, k in origin_of)
            hive_types = ", ".join(f"{_lit(k)}: 'VARCHAR'" for k in old_keys)
            drop = ", ".join(_ident(k) for k in ["filename", *new_keys])
            order = ", ".join(_ident(c) for c in [*members_in, ORIGIN_COLUMN])
            query = (
                f"SELECT t.* EXCLUDE ({drop}), m.origem AS {_ident(ORIGIN_COLUMN)} "
                f"FROM read_parquet({sources}, hive_partitioning=true, "
                f"hive_types={{{hive_types}}}, union_by_name=true, filename=true) t "
                f"JOIN (VALUES {mapping}) m(arquivo, origem) "
                f"ON replace(t.filename, '\\', '/') = m.arquivo"
            )
            scratch = _scratch_dir(staging)
            conn = _connect(scratch)
            try:
                conn.execute(
                    f"COPY ({query} ORDER BY {order}) TO {_lit(str(tmp_out))} "
                    f"({_kv_options(meta)})"
                )
                target.mkdir(parents=True, exist_ok=True)
                os.replace(tmp_out, target / f"{gkey}.parquet")
            except BaseException:
                tmp_out.unlink(missing_ok=True)
                raise
            finally:
                conn.close()
                if scratch != staging:
                    shutil.rmtree(scratch, ignore_errors=True)
            written += 1
            for path, _ in files:
                path.unlink()
                removed += 1
                parent = path.parent
                while parent != dataset_dir and not any(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent
        shutil.rmtree(dataset_dir / TMP_DIRNAME, ignore_errors=True)
        descriptor.partition_keys = new_keys
        descriptor.partition_types = {
            k: v for k, v in descriptor.partition_types.items() if k in new_keys
        }
        descriptor.converter = converter
        descriptor.save(dataset_dir)
    return {"groups": written, "files": removed}


# --- readers ---------------------------------------------------------------------


def scan_dataset(dataset_dir: Path) -> pl.LazyFrame:
    """Lazily read a dataset with Hive partitions as columns.

    Partition columns take the types declared in ``_dataset.json`` (so
    ``mes=09`` stays ``"09"`` when declared ``varchar``); without a readable
    descriptor Polars infers them.
    """
    dataset_dir = Path(dataset_dir)
    kwargs: dict[str, Any] = {}
    try:
        descriptor = DatasetDescriptor.load(dataset_dir)
    except (OSError, ValueError):
        descriptor = None
    if descriptor is not None and descriptor.partition_keys:
        kwargs["hive_schema"] = {
            key: _partition_type(key, declared)[1]
            for key, declared in _declared_types(descriptor).items()
        }
    return pl.scan_parquet(
        dataset_dir / "**/*.parquet", hive_partitioning=True, **kwargs
    )


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
        elif None not in parts and sorted(int(p) for p in parts) != list(
            range(1, len(parts) + 1)
        ):
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


def _check_readers(
    dataset_dir: Path, descriptor: DatasetDescriptor, files: list[Path]
) -> list[str]:
    import duckdb

    errors: list[str] = []
    expected_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
    declared = _declared_types(descriptor)
    counts: dict[str, int] = {}

    # DuckDB, through the same view the consumers create.
    try:
        conn = duckdb.connect()
        conn.execute(duckdb_view_sql(dataset_dir, "_lake_check"))
        counts["duckdb"] = conn.execute("SELECT count(*) FROM _lake_check").fetchone()[
            0
        ]
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
