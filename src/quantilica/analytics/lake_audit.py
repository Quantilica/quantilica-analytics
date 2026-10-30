"""Audit of a Hive-partitioned lake: is it complete, intact and traceable?

:func:`lake_audit` reads the footer of every Parquet in the lake (members of
grouped files included) and checks, in this order:

1. **Footers and integrity** (``parquet_corrompido``, ``membros_divergentes``,
   ``sem_proveniencia``): each file opens, its last row group is readable (a
   file with rows but no row groups is a defect; an empty file is not), and a
   grouped file's member rows add up to the file's rows.
2. **Provenance** (``sha256_divergente``, ``origem_sem_bruto``,
   ``origem_superada``, ``origem_fora_do_ledger``): the ``sha256`` each footer
   records for its raw origin must match the raw file in ``input_dir`` (the
   manifest sidecar's digest, else the file's hash) and/or the ``ledger`` export.
3. **Duplicates** (``versao_duplicada``): the same raw file (by file key) must
   not feed two Parquets of a dataset, as when two versions of it were converted
   into different partitions.
4. **Coverage** (``sem_parquet``): every current raw file of ``input_dir`` has a
   Parquet, unless it is *ignored*: the converter's ``inventory()`` says so
   (:class:`quantilica.core.lake.Planned`), or, without one, the heuristic does
   (format twins and non-tabular files, see :data:`NON_TABULAR_SUFFIXES`), or
   the caller listed it in ``known_exceptions``.
5. **Structure** (``particao_mista``, ``tmp_sobrando``, ``fora_do_layout``): one
   partition depth per dataset, no staging leftovers.
6. **Readers** (``leitores``): :func:`~quantilica.analytics.lake.check_dataset`
   on every dataset (DuckDB, Polars and PyArrow agree). Expensive;
   ``readers=False`` skips it.

Only footers are read (plus one column of one row group per file), never whole
files, and the footers are read by a thread pool. Directory symlinks and
junctions are never counted twice: one that points inside the lake is skipped
(its target is reached in place), and one that points outside is followed once
per real destination.

Ledger export
-------------
``ledger`` is a UTF-8 file with one raw file per record, a path relative to
the raw root (``<source>/.../<name>``, as in the view) and its ``sha256``:

* JSON Lines (``.jsonl``/``.ndjson``/``.json``)::

      {"path": "inmet/dados/2026@20261021.zip", "sha256": "ab12..."}

* CSV with a header (``.csv``)::

      path,sha256
      inmet/dados/2026@20261021.zip,ab12...

Extra fields are ignored; a path may appear with several digests. Without an
``input_dir``, the ledger also stands for the expected raw files in the
coverage check (the heuristic only: there are no files to give a converter).
"""

from __future__ import annotations

import csv
import fnmatch
import json
import os
import re
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from quantilica.core.files import sha256_file
from quantilica.core.lake import (
    DatasetDescriptor,
    converter_inventory,
    file_key,
    load_lake_converter,
    select_current,
)
from quantilica.core.manifests import MANIFEST_SIDECAR_SUFFIX, read_manifest_sidecar

from quantilica.analytics.lake import (
    META_FILE_KEY,
    META_MEMBERS,
    META_ORIGIN_SHA,
    TMP_DIRNAME,
    TMP_SUFFIX,
    check_dataset,
    footer_from_metadata,
)

# Finding categories, with what each one means.
CATEGORIES: dict[str, str] = {
    "parquet_corrompido": "Parquet que não abre ou com row group ilegível",
    "membros_divergentes": "arquivo agrupado cujos membros não somam as suas linhas",
    "sem_proveniencia": "Parquet sem origin_file e origin_sha256 no rodapé",
    "sha256_divergente": "sha256 da origem difere do bruto ou do ledger",
    "origem_sem_bruto": "origem que não está no bruto",
    "origem_superada": "origem é uma versão anterior à vigente",
    "origem_fora_do_ledger": "origem que não está no ledger",
    "versao_duplicada": "o mesmo arquivo bruto em mais de um Parquet",
    "sem_parquet": "arquivo bruto vigente sem Parquet e não ignorado",
    "particao_mista": "dataset com Parquets em profundidades de partição diferentes",
    "tmp_sobrando": "restos de escrita (.tmp) no lake",
    "fora_do_layout": "Parquet fora de <fonte>/<dataset>/",
    "leitores": "erro do check_dataset (descritor, layout, rodapé ou leitores)",
}

# Raw files that carry no table, so having no Parquet is expected (heuristic used
# when the converter has no ``inventory()``): documents, images and office files.
NON_TABULAR_SUFFIXES = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".odt",
        ".rtf",
        ".ppt",
        ".pptx",
        ".odp",
        ".gif",
        ".png",
        ".jpg",
        ".jpeg",
        ".bmp",
        ".tif",
        ".tiff",
        ".svg",
        ".html",
        ".htm",
        ".md",
        ".rst",
    }
)
# Text files that are documentation, not data (".txt" is tabular for many sources,
# so it is the name that decides): "leia-me", "readme", "licenca"/"license".
NON_TABULAR_NAME = re.compile(r"^(leia[-_ ]?me|readme|licen[cs]a|licen[cs]e)", re.I)

_PART_NAME = re.compile(r"^(?P<key>.+?)(?:-(?P<part>\d{3}))?\.parquet$")

Progress = Callable[[str, int, int], None]


@dataclass(frozen=True)
class AuditFinding:
    """One defect found by the audit.

    ``path`` is relative to the lake for everything found in it, and relative to
    the raw input for ``sem_parquet``. ``source`` and ``dataset`` are empty when
    the finding does not belong to a dataset.
    """

    category: str
    message: str
    path: str | None = None
    source: str | None = None
    dataset: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "source": self.source,
            "dataset": self.dataset,
            "path": self.path,
            "message": self.message,
        }


@dataclass
class AuditReport:
    """Result of :func:`lake_audit`.

    ``ok`` is true when there are no findings; checks that did not run (no raw
    input, no ledger, readers off) are listed in ``checks`` and do not fail the
    audit. ``datasets`` maps ``"<source>/<dataset>"`` to its counts.
    """

    lake_dir: str
    input_dir: str | None = None
    ledger: str | None = None
    started_at: str = ""
    seconds: float = 0.0
    checks: dict[str, str] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    ignored: dict[str, int] = field(default_factory=dict)
    datasets: dict[str, dict[str, int]] = field(default_factory=dict)
    findings: list[AuditFinding] = field(default_factory=list)
    links: list[dict[str, str]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.findings

    def by_category(self) -> dict[str, int]:
        """Number of findings per category (only the categories with findings)."""
        counter = Counter(f.category for f in self.findings)
        return {c: counter[c] for c in CATEGORIES if c in counter}

    def to_dict(self) -> dict[str, Any]:
        """JSON-serializable form of the report (the schema is stable)."""
        return {
            "ok": self.ok,
            "lake_dir": self.lake_dir,
            "input_dir": self.input_dir,
            "ledger": self.ledger,
            "started_at": self.started_at,
            "seconds": round(self.seconds, 2),
            "checks": dict(self.checks),
            "counts": dict(self.counts),
            "ignored": dict(self.ignored),
            "findings_by_category": self.by_category(),
            "datasets": {k: dict(v) for k, v in sorted(self.datasets.items())},
            "findings": [f.to_dict() for f in self.findings],
            "links": [dict(link) for link in self.links],
            "notes": list(self.notes),
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def to_text(self, *, max_per_category: int = 10) -> str:
        """Human-readable summary: totals, checks and findings by category."""
        c = self.counts
        status = "APROVADO" if self.ok else f"REPROVADO ({len(self.findings)} achados)"
        lines = [
            f"Auditoria do lake {self.lake_dir}: {status}",
            f"  {c.get('parquets', 0)} Parquets em {c.get('datasets', 0)} datasets, "
            f"{c.get('rows', 0):,} linhas, {c.get('members', 0)} membros "
            f"de arquivos agrupados; {self.seconds:.1f}s",
        ]
        if "expected_files" in c:
            lines.append(
                f"  cobertura: {c['expected_files']} arquivos vigentes, "
                f"{c.get('with_parquet', 0)} com Parquet, "
                f"{sum(self.ignored.values())} ignorados"
                + (
                    " ("
                    + ", ".join(f"{k}={v}" for k, v in sorted(self.ignored.items()))
                    + ")"
                    if self.ignored
                    else ""
                )
            )
        for name, state in self.checks.items():
            if not state.startswith("ok"):
                lines.append(f"  verificação {name}: {state}")
        for link in self.links:
            lines.append(f"  link {link['path']} -> {link['target']}: {link['action']}")
        for note in self.notes:
            lines.append(f"  aviso: {note}")
        by_category = self.by_category()
        if by_category:
            lines.append("Achados:")
        for category, n in by_category.items():
            lines.append(f"  {category}: {n}  ({CATEGORIES[category]})")
            shown = [f for f in self.findings if f.category == category]
            for f in shown[:max_per_category]:
                where = f"{f.source}/{f.dataset}" if f.dataset else (f.source or "")
                lines.append(f"    - [{where}] {f.path or ''}: {f.message}")
            if len(shown) > max_per_category:
                lines.append(f"    ... e mais {len(shown) - max_per_category}")
        return "\n".join(lines)


# --- ledger ------------------------------------------------------------------


def _norm_rel(path: str) -> str:
    text = path.strip().replace("\\", "/")
    while text.startswith("./"):
        text = text[2:]
    return PurePosixPath(text).as_posix()


def read_ledger(path: Path | str) -> dict[str, set[str]]:
    """Read a ledger export: relative path -> set of ``sha256`` digests.

    The format is described in the module docstring. Raises ``ValueError`` on
    a record without ``path`` or ``sha256``.
    """
    path = Path(path)
    out: dict[str, set[str]] = defaultdict(set)

    def add(record: Any, where: str) -> None:
        if not isinstance(record, Mapping):
            raise ValueError(f"{path}:{where}: record is not an object")
        rel, digest = record.get("path"), record.get("sha256")
        if not rel or not digest:
            raise ValueError(f"{path}:{where}: needs 'path' and 'sha256'")
        out[_norm_rel(str(rel))].add(str(digest).strip().lower())

    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8", newline="") as fh:
            for n, row in enumerate(csv.DictReader(fh), start=2):
                add(row, str(n))
    else:
        with path.open(encoding="utf-8") as fh:
            for n, line in enumerate(fh, start=1):
                if line.strip():
                    try:
                        add(json.loads(line), str(n))
                    except json.JSONDecodeError as exc:
                        raise ValueError(f"{path}:{n}: invalid JSON ({exc})") from exc
    return dict(out)


# --- scanning the lake ----------------------------------------------------------


@dataclass
class _LakeScan:
    parquets: list[tuple[tuple[str, ...], Path]] = field(default_factory=list)
    tmp: list[str] = field(default_factory=list)
    datasets: dict[tuple[str, str], Path] = field(default_factory=dict)
    links: list[dict[str, str]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _is_link(entry: os.DirEntry[str]) -> bool:
    return entry.is_symlink() or entry.is_junction()


def _has_files(directory: Path) -> bool:
    for _root, _dirs, names in os.walk(directory):
        if names:
            return True
    return False


def _scan_lake(lake_dir: Path, sources: Collection[str] | None) -> _LakeScan:
    """Walk the lake once: Parquets, leftovers and datasets, links counted once."""
    scan = _LakeScan()
    root_real = Path(os.path.realpath(lake_dir))
    visited: set[Path] = {root_real}
    followed: list[Path] = []  # real destinations of the links already followed
    seen_files: set[Path] = set()
    stack: list[tuple[Path, Path, tuple[str, ...]]] = [(lake_dir, root_real, ())]
    while stack:
        directory, real, rel = stack.pop()
        try:
            with os.scandir(directory) as it:
                entries = sorted(it, key=lambda e: e.name)
        except OSError as exc:
            scan.errors.append(f"{'/'.join(rel) or '.'}: cannot list ({exc})")
            continue
        for entry in entries:
            name, parts = entry.name, (*rel, entry.name)
            posix = "/".join(parts)
            if entry.is_dir():
                if name == TMP_DIRNAME:
                    if _has_files(Path(entry.path)):
                        scan.tmp.append(posix)
                    continue
                if name.startswith("."):  # .build, .lock...
                    continue
                if not rel and sources is not None and name not in sources:
                    continue
                if _is_link(entry):
                    target = Path(os.path.realpath(entry.path))
                    inside = (
                        target == root_real
                        or target.is_relative_to(root_real)
                        or root_real.is_relative_to(target)
                    )
                    if inside:
                        scan.links.append(
                            {
                                "path": posix,
                                "target": str(target),
                                "action": "ignorado (aponta para dentro do lake)",
                            }
                        )
                        continue
                    if target in visited or any(
                        target.is_relative_to(f) for f in followed
                    ):
                        scan.links.append(
                            {
                                "path": posix,
                                "target": str(target),
                                "action": "ignorado (destino já lido)",
                            }
                        )
                        continue
                    followed.append(target)
                    visited.add(target)
                    scan.links.append(
                        {"path": posix, "target": str(target), "action": "seguido"}
                    )
                    child_path, child_real = Path(entry.path), target
                else:
                    child_real = real / name
                    if child_real in visited:
                        continue
                    visited.add(child_real)
                    child_path = Path(entry.path)
                if len(parts) == 2:
                    scan.datasets[(parts[0], parts[1])] = child_path
                stack.append((child_path, child_real, parts))
            elif entry.is_file():
                if not rel and sources is not None:
                    continue
                if name.endswith(TMP_SUFFIX):
                    scan.tmp.append(posix)
                elif name.endswith(".parquet"):
                    if entry.is_symlink():
                        target = Path(os.path.realpath(entry.path))
                        if target.is_relative_to(root_real) or target in seen_files:
                            continue
                        seen_files.add(target)
                    scan.parquets.append((parts, Path(entry.path)))
    return scan


@dataclass
class _FileInfo:
    parts: tuple[str, ...]
    size: int = 0
    num_rows: int = 0
    footer: dict[str, str] = field(default_factory=dict)
    error: str | None = None


def _inspect(item: tuple[tuple[str, ...], Path], integrity: bool) -> _FileInfo:
    """Open one Parquet: footer and (optionally) one column of its last row group."""
    parts, path = item
    info = _FileInfo(parts)
    try:
        info.size = path.stat().st_size
        with pq.ParquetFile(path) as pf:
            md = pf.metadata
            info.num_rows = md.num_rows
            info.footer = footer_from_metadata(md)
            if integrity:
                if pf.num_row_groups == 0:
                    if md.num_rows:
                        info.error = "sem row groups, mas o rodapé declara linhas"
                elif pf.schema_arrow.names:
                    pf.read_row_group(
                        pf.num_row_groups - 1,
                        columns=[pf.schema_arrow.names[0]],
                        use_threads=False,
                    )
    except (OSError, ValueError, pa.ArrowException) as exc:
        info.error = f"{type(exc).__name__}: {str(exc)[:150]}"
    return info


# --- raw input ---------------------------------------------------------------------


def _list_files(root: Path, sources: Collection[str] | None) -> list[str]:
    out = []
    for dirpath, dirs, names in os.walk(root):
        dirs[:] = sorted(d for d in dirs if not d.startswith("."))
        here = Path(dirpath).relative_to(root)
        if sources is not None and here == Path("."):
            dirs[:] = [d for d in dirs if d in sources]
            continue  # loose files at the root belong to no source
        out.extend((here / n).as_posix() for n in sorted(names))
    return out


def _current(paths: Iterable[str]) -> list[str]:
    """Current version of each raw file (no sidecars, no leftovers)."""
    return [
        p
        for p in select_current(paths)
        if not p.endswith(MANIFEST_SIDECAR_SUFFIX) and not p.endswith(TMP_SUFFIX)
    ]


def _source_of(rel: str) -> str:
    head, sep, _ = rel.partition("/")
    return head if sep else ""


def _is_non_tabular(name: str) -> bool:
    return Path(name).suffix.lower() in NON_TABULAR_SUFFIXES or bool(
        NON_TABULAR_NAME.match(name)
    )


def _index(paths: Iterable[str]) -> tuple[dict, dict]:
    by_name: dict[tuple[str, str], list[str]] = defaultdict(list)
    by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
    for rel in paths:
        name = rel.rpartition("/")[2]
        by_name[(_source_of(rel), name)].append(rel)
        by_key[(_source_of(rel), file_key(name))].append(rel)
    return by_name, by_key


@dataclass(frozen=True)
class _Origin:
    """The raw origin a Parquet (or a member of one) declares in its footer."""

    source: str
    dataset: str
    name: str
    sha256: str
    key: str
    unit: str  # where it lives: the partition directory, or the grouped file
    lake_rel: str


def _group(findings: list[tuple[_Origin, str, str]]) -> list[AuditFinding]:
    """One finding per (category, dataset, origin, detail), counting the files."""
    grouped: dict[tuple[str, str, str, str, str], list[_Origin]] = {}
    messages: dict[tuple[str, str, str, str, str], str] = {}
    for origin, category, message in findings:
        k = (category, origin.source, origin.dataset, origin.name, message)
        grouped.setdefault(k, []).append(origin)
        messages[k] = message
    out = []
    for k, origins in grouped.items():
        message = messages[k]
        if len(origins) > 1:
            message += f" ({len(origins)} arquivos)"
        out.append(AuditFinding(k[0], message, origins[0].lake_rel, k[1], k[2]))
    return out


def _matches(rel: str, patterns: Collection[str]) -> bool:
    return any(rel == p or fnmatch.fnmatchcase(rel, p) for p in patterns)


# --- the audit ---------------------------------------------------------------


def lake_audit(
    lake_dir: Path | str,
    *,
    input_dir: Path | str | None = None,
    ledger: Path | str | None = None,
    sources: Collection[str] | None = None,
    readers: bool = True,
    integrity: bool = True,
    known_exceptions: Collection[str] = (),
    converters: Mapping[str, Any] | None = None,
    rehash: bool = False,
    workers: int | None = None,
    progress: Progress | None = None,
) -> AuditReport:
    """Audit ``lake_dir``; see the module docstring for the checks.

    Args:
        lake_dir: Root of the lake (``<source>/<dataset>/...``).
        input_dir: The raw input the lake was built from (a view with the
            current version of each file, or a raw tree: the current version is
            chosen with :func:`quantilica.core.lake.select_current`). Without
            it, provenance against the raw files and coverage are skipped.
        ledger: Path of a ledger export (see the module docstring) to check
            provenance against. Optional and independent of ``input_dir``.
        sources: Audit only these source ids (top-level directories).
        readers: Run :func:`~quantilica.analytics.lake.check_dataset` on every
            dataset. Costly on a large lake.
        integrity: Read the last row group of every Parquet. Off, only the
            footer is read.
        known_exceptions: Raw paths (relative to ``input_dir``; ``fnmatch``
            patterns allowed) that are known to have no Parquet, such as files
            corrupt at the source. They are not reported by the coverage check.
        converters: ``source_id -> converter`` whose ``inventory()`` decides what
            is ignored. ``None`` looks each source up in the ``quantilica.lake``
            entry points; pass ``{}`` to use the heuristic everywhere. A source
            whose converter has no ``inventory()`` uses the heuristic.
        rehash: Always hash the raw files instead of trusting the digest in
            their manifest sidecar.
        workers: Threads reading footers (default: twice the CPUs, up to 32).
        progress: Called as ``progress(stage, done, total)``.

    Returns:
        The :class:`AuditReport`; ``report.ok`` is the verdict.
    """
    started = time.monotonic()
    lake_path = Path(lake_dir)
    if not lake_path.is_dir():
        raise NotADirectoryError(f"lake directory not found: {lake_path}")
    input_path = Path(input_dir) if input_dir is not None else None
    if input_path is not None and not input_path.is_dir():
        raise NotADirectoryError(f"input directory not found: {input_path}")
    wanted = set(sources) if sources is not None else None
    report = AuditReport(
        lake_dir=str(lake_path),
        input_dir=str(input_path) if input_path else None,
        ledger=str(ledger) if ledger else None,
        started_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    ledger_shas = read_ledger(ledger) if ledger is not None else None
    if ledger_shas is not None and wanted is not None:
        ledger_shas = {p: s for p, s in ledger_shas.items() if _source_of(p) in wanted}

    def step(stage: str, done: int, total: int) -> None:
        if progress is not None:
            progress(stage, done, total)

    n_workers = workers or min(32, (os.cpu_count() or 4) * 2)

    # footers, integrity and structure
    scan = _scan_lake(lake_path, wanted)
    report.links = scan.links
    foot = _read_footprint(scan, integrity, n_workers, step)
    findings = foot.findings
    for message in scan.errors:
        findings.append(AuditFinding("parquet_corrompido", message))
    for rel in scan.tmp:
        findings.append(
            AuditFinding(
                "tmp_sobrando",
                "restos de escrita interrompida",
                rel,
                *_where(tuple(rel.split("/"))),
            )
        )
    findings += _check_depth(foot, scan)
    report.checks["rodapes"] = "ok"
    report.checks["integridade"] = "ok" if integrity else "desligada (só o rodapé)"
    report.checks["estrutura"] = "ok"

    # provenance and duplicates
    traced = [o for o in foot.origins if o.name and o.sha256]
    findings += [
        AuditFinding(
            "sem_proveniencia",
            "o rodapé não registra origin_file e origin_sha256",
            o.lake_rel,
            o.source,
            o.dataset,
        )
        for o in foot.origins
        if not (o.name and o.sha256)
    ]
    findings += _check_duplicates(traced)
    current = None
    gone: set[_Origin] = set()
    if input_path is not None:
        current = _current(_list_files(input_path, wanted))
        flagged, gone = _check_input(
            traced, current, input_path, rehash, n_workers, step
        )
        findings += _group(flagged)
    if ledger_shas is not None:
        findings += _group(_check_ledger(traced, ledger_shas, gone))
    sides = [
        n for n, v in (("bruto", current), ("ledger", ledger_shas)) if v is not None
    ]
    report.checks["proveniencia"] = (
        f"ok ({' e '.join(sides)})"
        if sides
        else "não executada (sem input_dir nem ledger)"
    )

    # coverage
    expected = current
    if expected is None and ledger_shas is not None:
        expected = _current(ledger_shas)
    if expected is None:
        report.checks["cobertura"] = "não executada (sem input_dir nem ledger)"
    else:
        findings += _coverage(
            report,
            expected,
            {(o.source, o.name) for o in traced},
            input_path if current is not None else None,
            known_exceptions,
            converters,
        )
        report.checks["cobertura"] = "ok"

    # readers
    if readers:
        findings += _check_readers(scan, step)
        report.checks["leitores"] = "ok"
    else:
        report.checks["leitores"] = "desligada"

    order = {c: i for i, c in enumerate(CATEGORIES)}
    report.findings = sorted(
        findings,
        key=lambda f: (
            order[f.category],
            f.source or "",
            f.dataset or "",
            f.path or "",
            f.message,
        ),
    )
    datasets = foot.datasets
    for f in report.findings:
        if f.dataset and f"{f.source}/{f.dataset}" in datasets:
            stats = datasets[f"{f.source}/{f.dataset}"]
            stats["findings"] = stats.get("findings", 0) + 1
    report.datasets = datasets
    report.counts.update(
        parquets=len(foot.infos),
        datasets=len(datasets),
        rows=foot.rows,
        members=foot.members,
        findings=len(report.findings),
    )
    report.seconds = time.monotonic() - started
    return report


@dataclass
class _Footprint:
    """What the footers say: per-dataset counts, origins and partition layouts."""

    infos: list[_FileInfo] = field(default_factory=list)
    datasets: dict[str, dict[str, int]] = field(default_factory=dict)
    origins: list[_Origin] = field(default_factory=list)
    depths: dict[tuple[str, str], Counter[tuple[str, ...]]] = field(
        default_factory=lambda: defaultdict(Counter)
    )
    examples: dict[tuple[str, str, tuple[str, ...]], str] = field(default_factory=dict)
    findings: list[AuditFinding] = field(default_factory=list)
    rows: int = 0
    members: int = 0


def _read_footprint(
    scan: _LakeScan, integrity: bool, n_workers: int, step: Progress
) -> _Footprint:
    """Read every footer (in threads) and digest them into a :class:`_Footprint`."""
    foot = _Footprint()
    foot.datasets = {
        f"{s}/{d}": {"parquets": 0, "rows": 0, "members": 0} for s, d in scan.datasets
    }
    total = len(scan.parquets)
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        for i, info in enumerate(
            pool.map(lambda item: _inspect(item, integrity), scan.parquets), start=1
        ):
            foot.infos.append(info)
            _digest(foot, info)
            if i % 100 == 0 or i == total:
                step("rodapés", i, total)
    return foot


def _digest(foot: _Footprint, info: _FileInfo) -> None:
    parts = info.parts
    rel = "/".join(parts)
    source, dataset = _where(parts)
    if len(parts) < 3:
        foot.findings.append(
            AuditFinding(
                "fora_do_layout", "Parquet fora de <fonte>/<dataset>/", rel, source
            )
        )
        return
    stats = foot.datasets.setdefault(
        f"{source}/{dataset}", {"parquets": 0, "rows": 0, "members": 0}
    )
    stats["parquets"] += 1
    layout = tuple(p.split("=")[0] for p in parts[2:-1])
    foot.depths[(source, dataset)][layout] += 1
    foot.examples.setdefault((source, dataset, layout), rel)
    if info.error:
        foot.findings.append(
            AuditFinding("parquet_corrompido", info.error, rel, source, dataset)
        )
        if not info.footer:
            return
    stats["rows"] += info.num_rows
    foot.rows += info.num_rows
    kv = info.footer
    if META_MEMBERS not in kv:
        name = kv.get("quantilica.origin_file", "")
        match = _PART_NAME.match(parts[-1])
        key = kv.get(META_FILE_KEY) or (match["key"] if match else parts[-1])
        foot.origins.append(
            _Origin(
                source,
                dataset,
                name,
                kv.get(META_ORIGIN_SHA, "").lower(),
                file_key(name) if name else key,
                "/".join(parts[:-1]),
                rel,
            )
        )
        return
    try:
        members = json.loads(kv[META_MEMBERS])
        member_rows = sum(int(m.get("rows", 0)) for m in members.values())
    except (ValueError, AttributeError, TypeError) as exc:
        foot.findings.append(
            AuditFinding(
                "parquet_corrompido",
                f"quantilica.members ilegível ({exc})",
                rel,
                source,
                dataset,
            )
        )
        return
    if member_rows != info.num_rows:
        foot.findings.append(
            AuditFinding(
                "membros_divergentes",
                f"membros somam {member_rows} linhas, o arquivo tem {info.num_rows}",
                rel,
                source,
                dataset,
            )
        )
    stats["members"] += len(members)
    foot.members += len(members)
    for key, m in members.items():
        name = m.get("origin_file") or ""
        foot.origins.append(
            _Origin(
                source,
                dataset,
                name,
                (m.get("origin_sha256") or "").lower(),
                file_key(name) if name else key,
                rel,
                rel,
            )
        )


def _check_depth(foot: _Footprint, scan: _LakeScan) -> list[AuditFinding]:
    """``particao_mista``: more than one partition layout in a dataset."""
    out = []
    for (source, dataset), layouts in sorted(foot.depths.items()):
        if len(layouts) < 2:
            continue
        expected = _declared_layout(scan.datasets.get((source, dataset)))
        desc = "; ".join(
            f"{_show_layout(layout)}: {n}" for layout, n in layouts.most_common()
        )
        # The odd one out: whatever the descriptor does not declare, else the
        # rarest layout (the shallower one on a tie).
        odd = [layout for layout in layouts if layout != expected]
        if expected is None or len(odd) == len(layouts):
            odd = sorted(layouts, key=lambda lay: (layouts[lay], len(lay)))[:1]
        out.append(
            AuditFinding(
                "particao_mista",
                f"profundidades diferentes ({desc})",
                foot.examples[(source, dataset, odd[0])],
                source,
                dataset,
            )
        )
    return out


def _check_duplicates(traced: list[_Origin]) -> list[AuditFinding]:
    """``versao_duplicada``: one raw file in more than one place of a dataset."""
    by_key: dict[tuple[str, str, str], list[_Origin]] = defaultdict(list)
    for origin in traced:
        by_key[(origin.source, origin.dataset, origin.key)].append(origin)
    out = []
    for (source, dataset, key), group in sorted(by_key.items()):
        units = {o.unit for o in group}
        if len(units) > 1 or len({(o.name, o.sha256) for o in group}) > 1:
            where = sorted({o.lake_rel for o in group})
            names = sorted({o.name for o in group})
            out.append(
                AuditFinding(
                    "versao_duplicada",
                    f"{key!r} está em {len(units)} lugares "
                    f"({', '.join(names)}): {', '.join(where[:4])}",
                    where[0],
                    source,
                    dataset,
                )
            )
    return out


def _check_input(
    traced: list[_Origin],
    current: list[str],
    input_path: Path,
    rehash: bool,
    n_workers: int,
    step: Progress,
) -> tuple[list[tuple[_Origin, str, str]], set[_Origin]]:
    """Origins against the raw input; also the origins with no current file."""
    by_name, by_file_key = _index(current)
    need = sorted({rel for o in traced for rel in by_name.get((o.source, o.name), ())})

    def digest_of(rel: str) -> tuple[str, str]:
        if not rehash:
            manifest = read_manifest_sidecar(input_path / rel)
            if manifest is not None and manifest.sha256:
                return rel, manifest.sha256.lower()
        return rel, sha256_file(input_path / rel).lower()

    digests: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        for i, (rel, digest) in enumerate(pool.map(digest_of, need), start=1):
            digests[rel] = digest
            if i % 100 == 0 or i == len(need):
                step("sha256", i, len(need))
    flagged: list[tuple[_Origin, str, str]] = []
    gone: set[_Origin] = set()
    for o in traced:
        files = by_name.get((o.source, o.name))
        if files:
            if o.sha256 not in {digests[f] for f in files}:
                flagged.append(
                    (
                        o,
                        "sha256_divergente",
                        f"{o.name}: o rodapé tem {o.sha256[:12]}, o bruto "
                        f"{digests[files[0]][:12]}",
                    )
                )
            continue
        gone.add(o)
        newest = by_file_key.get((o.source, file_key(o.name)))
        if newest:
            message = f"{o.name} não é a versão vigente ({newest[0]})"
            flagged.append((o, "origem_superada", message))
        else:
            flagged.append((o, "origem_sem_bruto", f"{o.name} não existe no bruto"))
    return flagged, gone


def _check_ledger(
    traced: list[_Origin], ledger: dict[str, set[str]], gone: set[_Origin]
) -> list[tuple[_Origin, str, str]]:
    """Origins against the ledger (skipping those already flagged as gone)."""
    by_name, _ = _index(ledger)
    flagged: list[tuple[_Origin, str, str]] = []
    for o in traced:
        if o in gone:
            continue
        files = by_name.get((o.source, o.name))
        if not files:
            flagged.append((o, "origem_fora_do_ledger", f"{o.name} não está no ledger"))
        elif o.sha256 not in {s for f in files for s in ledger[f]}:
            flagged.append(
                (
                    o,
                    "sha256_divergente",
                    f"{o.name}: o rodapé tem {o.sha256[:12]}, o ledger tem outro",
                )
            )
    return flagged


def _check_readers(scan: _LakeScan, step: Progress) -> list[AuditFinding]:
    """``leitores``: ``check_dataset`` on each dataset."""
    out = []
    todo = sorted(scan.datasets.items())
    for i, ((source, dataset), path) in enumerate(todo, start=1):
        step("leitores", i, len(todo))
        try:
            errors = check_dataset(path)
        except Exception as exc:  # noqa: BLE001 - a reader crash is a finding
            errors = [f"check_dataset failed: {type(exc).__name__}: {exc}"]
        out += [
            AuditFinding("leitores", e, f"{source}/{dataset}", source, dataset)
            for e in errors
        ]
    return out


def _show_layout(layout: tuple[str, ...]) -> str:
    return "/".join(f"{k}=" for k in layout) or "(sem partição)"


def _declared_layout(dataset_dir: Path | None) -> tuple[str, ...] | None:
    """Partition keys the dataset's descriptor declares, if it loads."""
    if dataset_dir is None:
        return None
    try:
        return tuple(DatasetDescriptor.load(dataset_dir).partition_keys)
    except (OSError, ValueError, TypeError):
        return None


def _where(parts: tuple[str, ...]) -> tuple[str | None, str | None]:
    return (
        parts[0] if len(parts) > 1 else None,
        parts[1] if len(parts) > 2 else None,
    )


def _coverage(
    report: AuditReport,
    expected: list[str],
    covered_names: set[tuple[str, str]],
    input_path: Path | None,
    known_exceptions: Collection[str],
    converters: Mapping[str, Any] | None,
) -> list[AuditFinding]:
    """``sem_parquet`` for current raw files with no Parquet that are not ignored."""
    exceptions = [_norm_rel(p) for p in known_exceptions]
    planned: dict[str, str | None] = {}  # rel -> skip reason (None: expects Parquet)
    if input_path is not None:
        for source in sorted({_source_of(r) for r in expected} - {""}):
            converter = (
                converters.get(source)
                if converters is not None
                else _discover(source, report.notes)
            )
            if converter is None:
                continue
            try:
                inventory = converter_inventory(converter, input_path / source)
            except Exception as exc:  # noqa: BLE001 - fall back to the heuristic
                report.notes.append(
                    f"inventory() de {source} falhou ({type(exc).__name__}: {exc}); "
                    "usando a heurística"
                )
                continue
            if inventory is None:
                continue
            for item in inventory:
                path = Path(item.path)
                if not path.is_absolute():
                    path = input_path / source / path
                try:
                    rel = path.relative_to(input_path).as_posix()
                except ValueError:
                    continue
                planned[rel] = item.skip_reason

    ignored: Counter[str] = Counter()
    missing: list[AuditFinding] = []
    covered = 0
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for rel in expected:
        name = rel.rpartition("/")[2]
        source = _source_of(rel)
        if _matches(rel, exceptions):
            ignored["excecao_conhecida"] += 1
        elif rel in planned:
            reason = planned[rel]
            if reason is not None:
                ignored[reason] += 1
            elif (source, name) in covered_names:
                covered += 1
            else:
                missing.append(_missing(rel, "o conversor espera um Parquet"))
        elif _is_non_tabular(name):
            ignored["nao_tabular"] += 1
        else:
            groups[(rel.rpartition("/")[0], file_key(name))].append(rel)
    for files in (groups[k] for k in sorted(groups)):
        have = [
            f for f in files if (_source_of(f), f.rpartition("/")[2]) in covered_names
        ]
        if have:
            covered += len(have)
            if len(files) > len(have):
                ignored["gemeo"] += len(files) - len(have)
        else:
            for rel in files:
                twins = f" (gêmeos: {len(files)})" if len(files) > 1 else ""
                missing.append(_missing(rel, f"sem Parquet{twins}"))
    report.ignored = dict(ignored)
    report.counts["expected_files"] = len(expected)
    report.counts["with_parquet"] = covered
    return missing


def _missing(rel: str, message: str) -> AuditFinding:
    return AuditFinding("sem_parquet", message, rel, _source_of(rel) or None)


def _discover(source: str, notes: list[str]) -> Any | None:
    try:
        return load_lake_converter(source)
    except Exception as exc:  # noqa: BLE001 - a broken plugin must not stop the audit
        notes.append(
            f"conversor de {source} não carregou ({type(exc).__name__}: {exc})"
        )
        return None
