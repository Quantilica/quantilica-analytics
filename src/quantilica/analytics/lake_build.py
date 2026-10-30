"""Resumable, process-isolated builder of the Hive lake.

:func:`lake_build` converts a tree of raw files (``<input_dir>/<source>/...``)
into the lake, one task per ``(source, dataset)``:

* **Tasks come from the converter.** The datasets of a source are every id in
  ``converter.datasets()``, never the name of a raw folder (the ids do not
  always match the folders). Each task calls
  ``converter.convert(input_dir/<source>, lake_dir, datasets=[id], force=...)``.
* **One process per task.** A task runs in its own ``spawn`` process, so a
  ``MemoryError``, a crash or ``os._exit`` in one task never takes the others
  down. On interruption (Ctrl+C, SIGTERM, SIGHUP, Ctrl+Break) the parent ends
  every child *and its descendants* before re-raising.
* **Resumable state.** ``<lake_dir>/.build/<run-id>.jsonl`` holds one line per
  finished task. A later run skips what ended ``ok`` in any earlier run;
  ``retry_errors`` redoes the failed ones, ``force`` redoes everything. There
  are no global phase markers.
* **Resources and safety nets.** DuckDB memory, threads and the scratch
  directory are parameters (they become the ``QUANTILICA_DUCKDB_MEMORY``,
  ``QUANTILICA_DUCKDB_THREADS`` and ``QUANTILICA_LAKE_SCRATCH`` variables in
  each child), free disk space is checked up front, and the machine is kept
  from sleeping meanwhile (:func:`keep_awake`).

Import it as ``quantilica.analytics.lake_build`` (the package ``__init__`` does
not re-export the function, so the name does not shadow the module).
"""

from __future__ import annotations

import contextlib
import importlib
import json
import logging
import multiprocessing
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from multiprocessing.connection import wait as wait_ready
from pathlib import Path
from typing import Any

from quantilica.core.lake import load_lake_converter

from quantilica.analytics.lake import LAKE_SCRATCH_ENV

logger = logging.getLogger("quantilica.analytics.lake_build")

BUILD_DIRNAME = ".build"
DUCKDB_MEMORY_ENV = "QUANTILICA_DUCKDB_MEMORY"
DUCKDB_THREADS_ENV = "QUANTILICA_DUCKDB_THREADS"
STATUS_OK = "ok"
STATUS_ERROR = "erro"
# Estimativas conservadoras da pré-checagem de disco, como fração do bruto
# pendente: o Parquet do lake fica em ~35% do bruto (380 GB de 1,1 TB); a
# extração de ZIP/7z e o temporário do DuckDB são transitórios, por arquivo.
DEFAULT_DISK_FACTOR = 0.6
DEFAULT_SCRATCH_FACTOR = 0.25
# Tempo para os filhos saírem após SIGTERM antes do SIGKILL na árvore inteira.
TERMINATE_GRACE_SECONDS = 5.0
_MESSAGE_LIMIT = 2000


class LakeBuildError(RuntimeError):
    """The build cannot start (bad arguments, unknown source, no space)."""


class InsufficientDiskError(LakeBuildError):
    """Free space on a volume is below the estimate for the pending raw data."""


@dataclass
class TaskRecord:
    """One line of the ``.build/*.jsonl`` state: a finished task.

    ``status`` is ``"ok"`` or ``"erro"``; ``errors`` counts the converter's
    error messages (``message`` carries the first ones, or the traceback of an
    exception, or how the process died). Times are ISO 8601 in UTC.
    """

    run_id: str
    source: str
    dataset: str
    started_at: str
    finished_at: str
    seconds: float
    status: str
    converted: int = 0
    skipped: int = 0
    errors: int = 0
    message: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == STATUS_OK


@dataclass
class SourceSummary:
    """Totals of one source in a run (files counted by the converters)."""

    source: str
    converted: int = 0
    skipped: int = 0
    errors: int = 0
    tasks_ok: int = 0
    tasks_failed: int = 0
    tasks_already_done: int = 0
    failed_datasets: list[str] = field(default_factory=list)


@dataclass
class BuildSummary:
    """What :func:`lake_build` did.

    * ``sources``: per-source totals (only sources that had tasks);
    * ``failed_tasks``: the tasks that failed in this run;
    * ``previous_failures``: ``(source, dataset)`` that failed in an earlier run
      and were left alone because ``retry_errors`` was off;
    * ``ignored_sources``: folders skipped because they have no converter;
    * ``failed_sources``: sources whose dataset list could not be read.

    :attr:`has_errors` is what a command should turn into its exit code.
    """

    run_id: str
    state_file: Path
    sources: dict[str, SourceSummary] = field(default_factory=dict)
    failed_tasks: list[TaskRecord] = field(default_factory=list)
    previous_failures: list[tuple[str, str]] = field(default_factory=list)
    ignored_sources: dict[str, str] = field(default_factory=dict)
    failed_sources: dict[str, str] = field(default_factory=dict)
    seconds: float = 0.0

    @property
    def has_errors(self) -> bool:
        return bool(self.failed_tasks or self.previous_failures or self.failed_sources)

    @property
    def converted(self) -> int:
        return sum(s.converted for s in self.sources.values())

    @property
    def skipped(self) -> int:
        return sum(s.skipped for s in self.sources.values())

    @property
    def errors(self) -> int:
        return sum(s.errors for s in self.sources.values())

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state_file"] = str(self.state_file)
        data["has_errors"] = self.has_errors
        return data

    def format(self) -> str:
        """Plain-text report, one line per source."""
        lines = [f"lake build {self.run_id} ({self.seconds:.0f}s)"]
        for s in self.sources.values():
            lines.append(
                f"  {s.source}: converted={s.converted} skipped={s.skipped} "
                f"errors={s.errors} | tasks ok={s.tasks_ok} failed={s.tasks_failed} "
                f"already_done={s.tasks_already_done}"
            )
            lines.extend(f"    failed: {s.source}/{d}" for d in s.failed_datasets)
        lines.extend(
            f"  failed earlier (not retried): {src}/{ds}"
            for src, ds in self.previous_failures
        )
        lines.extend(
            f"  source failed: {s}: {r}" for s, r in self.failed_sources.items()
        )
        lines.extend(f"  ignored: {s}: {r}" for s, r in self.ignored_sources.items())
        lines.append("  result: " + ("ERRORS" if self.has_errors else "ok"))
        return "\n".join(lines)


@dataclass(frozen=True)
class _Task:
    source: str
    dataset: str
    input_dir: Path
    size: int
    converter: Any  # None: entry point; str "pkg.mod:attr"; or a picklable object


# --- keep awake -----------------------------------------------------------


@contextlib.contextmanager
def keep_awake(reason: str = "building the Quantilica lake") -> Iterator[None]:
    """Keep the machine from suspending while the block runs.

    Windows: ``SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)``,
    restored on exit. macOS: ``caffeinate -i -w <pid>``. Linux:
    ``systemd-inhibit`` when it is in ``PATH``. The helper process is ended on
    exit and also stops by itself if this process dies. Without any of them it
    only logs a warning, and the block runs anyway.
    """
    release = _acquire_wake_lock(reason)
    try:
        yield
    finally:
        release()


def _acquire_wake_lock(reason: str):
    def nothing() -> None:
        pass

    try:
        if sys.platform.startswith("win"):
            import ctypes

            es_continuous, es_system_required = 0x80000000, 0x00000001
            kernel32 = ctypes.windll.kernel32
            kernel32.SetThreadExecutionState(es_continuous | es_system_required)
            return lambda: kernel32.SetThreadExecutionState(es_continuous)
        if sys.platform == "darwin":
            cmd = ["caffeinate", "-i", "-w", str(os.getpid())]
        elif shutil.which("systemd-inhibit"):
            # O auxiliar morre sozinho quando este processo some (kill -0).
            cmd = [
                "systemd-inhibit",
                "--what=idle:sleep",
                "--who=quantilica",
                f"--why={reason}",
                "--mode=block",
                "sh",
                "-c",
                'while kill -0 "$0" 2>/dev/null; do sleep 5; done',
                str(os.getpid()),
            ]
        else:
            logger.warning("no way to block suspension on this system; continuing")
            return nothing
        helper = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(0.2)  # falha imediata (sem sessão logind, binário ausente)
        if helper.poll() is not None:
            logger.warning("%s exited at once; the system may suspend", cmd[0])
            return nothing

        def stop() -> None:
            helper.terminate()
            try:
                helper.wait(5)
            except subprocess.TimeoutExpired:
                helper.kill()

        return stop
    except Exception as exc:  # noqa: BLE001 - nunca impede o build
        logger.warning("could not block suspension (%s); continuing", exc)
        return nothing


# --- planning -------------------------------------------------------------


def _dir_size(path: Path) -> int:
    total = 0
    stack = [str(path)]
    while stack:
        try:
            with os.scandir(stack.pop()) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        elif entry.is_file():
                            total += entry.stat().st_size
                    except OSError:
                        pass
        except OSError:
            pass
    return total


def _resolve_converter(source: str, ref: Any) -> Any | None:
    """The converter for ``source``: ``ref`` (object or ``"pkg.mod:attr"``) or
    the ``quantilica.lake`` entry point."""
    if ref is None:
        return load_lake_converter(source)
    if isinstance(ref, str):
        module_name, _, attr = ref.partition(":")
        return getattr(importlib.import_module(module_name), attr)
    return ref


def _existing_ancestor(path: Path) -> Path:
    probe = path.resolve()
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    return probe


def _check_disk(
    lake_dir: Path,
    scratch: Path | None,
    pending_bytes: int,
    disk_factor: float,
    scratch_factor: float,
) -> None:
    """Raise :class:`InsufficientDiskError` if the volumes cannot hold the run.

    The lake needs ``pending_bytes * disk_factor``. Scratch work needs
    ``pending_bytes * scratch_factor``, on the scratch volume, or on the lake's
    when there is no scratch directory or it shares the volume.
    """
    lake_probe = _existing_ancestor(lake_dir)
    need: dict[Path, tuple[int, list[str]]] = {
        lake_probe: (int(pending_bytes * disk_factor), ["lake"])
    }
    scratch_need = int(pending_bytes * scratch_factor)
    scratch_probe = _existing_ancestor(scratch) if scratch is not None else lake_probe
    if scratch_probe == lake_probe or (
        os.stat(scratch_probe).st_dev == os.stat(lake_probe).st_dev
    ):
        total, names = need[lake_probe]
        need[lake_probe] = (total + scratch_need, names + ["scratch"])
    else:
        need[scratch_probe] = (scratch_need, ["scratch"])
    for probe, (required, names) in need.items():
        free = shutil.disk_usage(probe).free
        if free < required:
            raise InsufficientDiskError(
                f"not enough free space for {' + '.join(names)} on {probe}: "
                f"{free / 1e9:.1f} GB free, about {required / 1e9:.1f} GB needed "
                f"for {pending_bytes / 1e9:.1f} GB of pending raw data "
                f"(disk_factor={disk_factor}, scratch_factor={scratch_factor}); "
                "free space or pass check_disk=False to ignore"
            )


def _load_state(build_dir: Path) -> dict[tuple[str, str], str]:
    """Best status of each ``(source, dataset)`` over every earlier run."""
    state: dict[tuple[str, str], str] = {}
    for path in sorted(build_dir.glob("*.jsonl")):
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                    key = (rec["source"], rec["dataset"])
                    status = rec["status"]
                except (ValueError, KeyError, TypeError):
                    continue  # linha truncada por queda de energia
                if status == STATUS_OK or state.get(key) != STATUS_OK:
                    state[key] = status
    return state


def _new_run_id(build_dir: Path) -> str:
    base = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id, n = base, 1
    while (build_dir / f"{run_id}.jsonl").exists():
        n += 1
        run_id = f"{base}-{n}"
    return run_id


def _append_record(path: Path, record: TaskRecord) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


# --- child process --------------------------------------------------------


def _task_main(conn: Any, spec: dict[str, Any]) -> None:
    """Entry point of a task process: convert one dataset, report on ``conn``."""
    if hasattr(os, "setsid"):
        # Grupo próprio: o pai encerra o filho e os netos com um killpg, e o
        # Ctrl+C do terminal não chega aqui (quem decide é o pai).
        with contextlib.suppress(OSError):
            os.setsid()
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    for env, value in (
        (DUCKDB_MEMORY_ENV, spec["memory"]),
        (DUCKDB_THREADS_ENV, spec["threads"]),
        (LAKE_SCRATCH_ENV, spec["scratch"]),
    ):
        if value is not None:
            os.environ[env] = str(value)
    payload: dict[str, Any]
    try:
        converter = _resolve_converter(spec["source"], spec["converter"])
        if converter is None:
            raise LakeBuildError(f"no converter for source {spec['source']!r}")
        outcome = converter.convert(
            Path(spec["input_dir"]),
            Path(spec["lake_dir"]),
            datasets=[spec["dataset"]],
            force=spec["force"],
        )
        payload = {
            "converted": int(outcome.converted),
            "skipped": int(outcome.skipped),
            "errors": [str(e) for e in outcome.errors],
        }
    except Exception:  # noqa: BLE001 - inclui MemoryError; o pai registra e segue
        payload = {
            "converted": 0,
            "skipped": 0,
            "errors": [],
            "exception": traceback.format_exc()[-_MESSAGE_LIMIT:],
        }
    conn.send(payload)
    conn.close()


@dataclass
class _Running:
    task: _Task
    process: Any
    conn: Any
    started_at: str
    t0: float
    group_killed: bool = False


def _describe_exit(exitcode: int | None) -> str:
    if exitcode is None:
        return "process vanished without a result"
    if exitcode < 0:
        try:
            name = signal.Signals(-exitcode).name
        except ValueError:
            name = f"signal {-exitcode}"
        hint = " (possibly out of memory)" if -exitcode == signal.SIGKILL else ""
        return f"process killed by {name}{hint}"
    return f"process exited with code {exitcode} without a result"


def _join_messages(errors: list[str]) -> str | None:
    if not errors:
        return None
    text = "; ".join(errors[:5])
    if len(errors) > 5:
        text += f" (+{len(errors) - 5} more)"
    return text[:_MESSAGE_LIMIT]


class _Pool:
    """Runs task processes, at most ``workers`` at a time, and ends them all.

    Not a ``concurrent.futures`` pool on purpose: stopping one of those leaves
    its children alive (the cause of the lock-holding orphans), and a worker
    that dies breaks the whole pool.
    """

    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.ctx = multiprocessing.get_context("spawn")
        self.running: list[_Running] = []
        self._job = _WindowsJob.create() if sys.platform.startswith("win") else None

    def start(self, task: _Task, spec: dict[str, Any]) -> None:
        parent_conn, child_conn = self.ctx.Pipe(duplex=False)
        process = self.ctx.Process(
            target=_task_main,
            args=(child_conn, spec),
            name=f"lake-build:{task.source}/{task.dataset}",
        )
        started_at = datetime.now(UTC).isoformat(timespec="seconds")
        process.start()
        child_conn.close()  # sem a ponta do filho aqui, a morte dele vira EOF
        if self._job is not None:
            self._job.assign(process.pid)
        self.running.append(
            _Running(task, process, parent_conn, started_at, time.monotonic())
        )

    def collect(self, timeout: float = 1.0) -> list[tuple[_Running, dict[str, Any]]]:
        """Wait for finished tasks; return ``(running, payload)`` for each."""
        waitables = [r.conn for r in self.running] + [
            r.process.sentinel for r in self.running
        ]
        wait_ready(waitables, timeout)
        done = []
        for r in list(self.running):
            payload = self._payload(r)
            if payload is None:
                continue
            self.running.remove(r)
            self._reap(r)
            done.append((r, payload))
        return done

    def _payload(self, r: _Running) -> dict[str, Any] | None:
        try:
            if r.conn.poll():
                return r.conn.recv()
        except (EOFError, OSError):
            pass  # o filho morreu sem enviar o resultado
        else:
            if not wait_ready([r.process.sentinel], 0):  # is_alive() colheria
                return None
        # Morreu sem resultado (crash, OOM kill): limpa os netos, depois colhe.
        _kill_tree(r.process, hard=True)
        r.group_killed = True
        r.process.join(5)
        return {
            "converted": 0,
            "skipped": 0,
            "errors": [],
            "exception": _describe_exit(r.process.exitcode),
        }

    def _reap(self, r: _Running) -> None:
        # Espera sem colher o filho: o pid só é liberado no join, depois do kill
        # do grupo (senão o killpg poderia atingir um pid reaproveitado).
        if not r.group_killed:
            wait_ready([r.process.sentinel], TERMINATE_GRACE_SECONDS)
            # Filho que não sai depois de entregar o resultado, ou netos que ficaram.
            _kill_tree(r.process, hard=True)
        r.process.join(TERMINATE_GRACE_SECONDS)
        r.conn.close()
        r.process.close()

    def terminate_all(self) -> None:
        """End every running child and all of its descendants."""
        running, self.running = self.running, []
        for r in running:
            _kill_tree(r.process, hard=False)
        deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
        for r in running:
            wait_ready([r.process.sentinel], max(0.0, deadline - time.monotonic()))
        for r in running:
            _kill_tree(r.process, hard=True)
            r.process.join(TERMINATE_GRACE_SECONDS)
            with contextlib.suppress(Exception):
                r.conn.close()
            with contextlib.suppress(Exception):
                r.process.close()
        if self._job is not None:
            self._job.close()
            self._job = None


def _running(process: Any) -> bool:
    """Whether ``process`` has not exited (without reaping it, unlike is_alive)."""
    return not wait_ready([process.sentinel], 0)


def _kill_tree(process: Any, *, hard: bool) -> None:
    """Terminate ``process`` and its descendants (SIGTERM, or SIGKILL if ``hard``).

    POSIX: the child leads its own process group (``setsid``), so ``killpg``
    reaches the grandchildren even when the child itself has exited (call it
    before reaping the child, so its pid is not reused). Windows:
    ``taskkill /T /F`` walks the process tree (the job object, when available,
    is the safety net for the detached ones).
    """
    pid = process.pid
    if pid is None:
        return
    if sys.platform.startswith("win"):
        if hard or _running(process):
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                check=False,
            )
        return
    sig = signal.SIGKILL if hard else signal.SIGTERM
    try:
        os.killpg(pid, sig)
    except (ProcessLookupError, PermissionError):
        # Ainda sem grupo próprio (corrida com o setsid) ou já sem membros.
        if _running(process):
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, sig)


class _WindowsJob:
    """Job object that kills its processes when the handle closes (parent death
    included). Best effort: ``create`` returns ``None`` if anything fails."""

    def __init__(self, kernel32: Any, handle: Any) -> None:
        self._k32 = kernel32
        self._handle = handle

    @classmethod
    def create(cls) -> _WindowsJob | None:
        try:
            import ctypes
            from ctypes import wintypes

            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            k32.CreateJobObjectW.restype = wintypes.HANDLE
            k32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
            k32.SetInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                wintypes.LPVOID,
                wintypes.DWORD,
            ]
            k32.OpenProcess.restype = wintypes.HANDLE
            k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            k32.CloseHandle.argtypes = [wintypes.HANDLE]

            class BasicLimits(ctypes.Structure):
                _fields_ = [
                    ("PerProcessUserTimeLimit", ctypes.c_int64),
                    ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", wintypes.DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", wintypes.DWORD),
                    ("SchedulingClass", wintypes.DWORD),
                ]

            class IoCounters(ctypes.Structure):
                _fields_ = [(n, ctypes.c_uint64) for n in "abcdef"]

            class ExtendedLimits(ctypes.Structure):
                _fields_ = [
                    ("BasicLimitInformation", BasicLimits),
                    ("IoInfo", IoCounters),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t),
                ]

            handle = k32.CreateJobObjectW(None, None)
            if not handle:
                return None
            info = ExtendedLimits()
            info.BasicLimitInformation.LimitFlags = 0x2000  # KILL_ON_JOB_CLOSE
            ok = k32.SetInformationJobObject(
                handle, 9, ctypes.byref(info), ctypes.sizeof(info)
            )
            if not ok:
                k32.CloseHandle(handle)
                return None
            return cls(k32, handle)
        except Exception as exc:  # noqa: BLE001
            logger.debug("job object unavailable: %s", exc)
            return None

    def assign(self, pid: int) -> None:
        # PROCESS_SET_QUOTA | PROCESS_TERMINATE
        proc = self._k32.OpenProcess(0x0100 | 0x0001, False, pid)
        if proc:
            self._k32.AssignProcessToJobObject(self._handle, proc)
            self._k32.CloseHandle(proc)

    def close(self) -> None:
        self._k32.CloseHandle(self._handle)


@contextlib.contextmanager
def _interrupts_as_keyboard_interrupt() -> Iterator[None]:
    """Turn SIGTERM/SIGHUP/Ctrl+Break into ``KeyboardInterrupt`` in the main
    thread, so one ``except`` path ends the children for every kind of stop."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def handler(signum: int, frame: Any) -> None:
        raise KeyboardInterrupt(f"interrupted by signal {signum}")

    previous: dict[int, Any] = {}
    for name in ("SIGTERM", "SIGHUP", "SIGBREAK"):
        signum = getattr(signal, name, None)
        if signum is not None:
            previous[signum] = signal.signal(signum, handler)
    try:
        yield
    finally:
        for signum, old in previous.items():
            signal.signal(signum, old)


# --- public API -----------------------------------------------------------


def lake_build(
    input_dir: str | os.PathLike[str],
    lake_dir: str | os.PathLike[str],
    *,
    sources: list[str] | None = None,
    workers: int | None = None,
    memory: str | None = None,
    threads: int | None = None,
    scratch: str | os.PathLike[str] | None = None,
    force: bool = False,
    retry_errors: bool = False,
    check_disk: bool = True,
    disk_factor: float = DEFAULT_DISK_FACTOR,
    scratch_factor: float = DEFAULT_SCRATCH_FACTOR,
    prevent_sleep: bool = True,
    converters: Mapping[str, Any] | None = None,
    run_id: str | None = None,
) -> BuildSummary:
    """Convert the raw tree under ``input_dir`` into ``lake_dir``.

    Args:
        input_dir: Raw tree with one folder per source (``<input_dir>/<source>``);
            usually a "current versions" view.
        lake_dir: Root of the lake. The state goes to ``<lake_dir>/.build/``.
        sources: Sources to build. ``None`` takes every sub-folder of
            ``input_dir`` that has a converter (the others are reported in
            ``ignored_sources``). A name without folder or converter raises
            :class:`LakeBuildError`.
        workers: Parallel task processes (default: half the CPUs, at most 4).
        memory: DuckDB ``memory_limit`` of each task, such as ``"12GB"``.
        threads: DuckDB threads of each task.
        scratch: Directory for archive extraction and DuckDB's temporary files
            (put it on a fast disk).
        force: Redo every task and reconvert even what the lake has current.
        retry_errors: Also redo tasks that failed in an earlier run. Without it
            they are left alone and reported in ``previous_failures``. Tasks
            that never finished (interrupted) always run.
        check_disk: Compare free space with the estimate before converting;
            ``False`` skips the check.
        disk_factor: Lake size as a fraction of the pending raw data.
        scratch_factor: Transient scratch use as a fraction of the pending raw data.
        prevent_sleep: Hold off system suspension meanwhile (:func:`keep_awake`).
        converters: ``{source: converter}`` overriding the ``quantilica.lake``
            entry points. A value is an object (must be picklable, since the
            task processes are spawned) or a ``"package.module:attribute"``
            string imported in the task process.
        run_id: Name of this run's state file; defaults to a UTC timestamp.

    Returns:
        BuildSummary: per-source totals; check ``has_errors`` for the exit code.

    Raises:
        LakeBuildError: bad arguments or unknown source.
        InsufficientDiskError: not enough free space (see ``check_disk``).
        KeyboardInterrupt: after ending every child process, when interrupted
            by Ctrl+C, SIGTERM, SIGHUP or Ctrl+Break. What finished stays in
            the state file, so running again resumes.
    """
    started = time.monotonic()
    input_path, lake_path = Path(input_dir), Path(lake_dir)
    scratch_path = Path(scratch) if scratch is not None else None
    if not input_path.is_dir():
        raise LakeBuildError(f"input directory not found: {input_path}")
    if workers is not None and workers < 1:
        raise LakeBuildError("workers must be at least 1")
    workers = workers or max(1, min(4, (os.cpu_count() or 2) // 2))
    converters = dict(converters or {})

    build_dir = lake_path / BUILD_DIRNAME
    run_id = run_id or _new_run_id(build_dir)
    summary = BuildSummary(run_id=run_id, state_file=build_dir / f"{run_id}.jsonl")

    tasks = _plan(input_path, sources, converters, summary)
    state = _load_state(build_dir) if build_dir.is_dir() else {}
    pending: list[_Task] = []
    for task in tasks:
        status = state.get((task.source, task.dataset))
        sums = summary.sources.setdefault(task.source, SourceSummary(task.source))
        if force or status is None:
            pending.append(task)
        elif status == STATUS_OK:
            sums.tasks_already_done += 1
        elif retry_errors:
            pending.append(task)
        else:
            summary.previous_failures.append((task.source, task.dataset))

    if not pending:
        logger.info("lake build: nothing to do (%d tasks already done)", len(tasks))
        summary.seconds = time.monotonic() - started
        return summary

    # Maiores fontes primeiro; dentro da fonte vale a ordem do conversor.
    pending.sort(key=lambda t: -t.size)
    pending_bytes = sum({t.source: t.size for t in pending}.values())
    logger.info(
        "lake build %s: %d tasks (%d already done), %.1f GB raw, %d processes",
        summary.run_id,
        len(pending),
        len(tasks) - len(pending),
        pending_bytes / 1e9,
        workers,
    )
    if check_disk:
        _check_disk(lake_path, scratch_path, pending_bytes, disk_factor, scratch_factor)
    build_dir.mkdir(parents=True, exist_ok=True)

    wake = keep_awake() if prevent_sleep else contextlib.nullcontext()
    with wake, _interrupts_as_keyboard_interrupt():
        _run(pending, summary, workers, lake_path, force, memory, threads, scratch_path)
    summary.seconds = time.monotonic() - started
    logger.info("lake build %s finished:\n%s", summary.run_id, summary.format())
    return summary


def _plan(
    input_dir: Path,
    sources: list[str] | None,
    converters: dict[str, Any],
    summary: BuildSummary,
) -> list[_Task]:
    """One task per ``(source, dataset)``, from ``converter.datasets()``."""
    if sources is None:
        names = sorted(
            p.name
            for p in input_dir.iterdir()
            if p.is_dir() and not p.name.startswith((".", "_"))
        )
        explicit = False
    else:
        names, explicit = list(dict.fromkeys(sources)), True
    tasks: list[_Task] = []
    for name in names:
        folder = input_dir / name
        ref = converters.get(name)
        try:
            converter = _resolve_converter(name, ref)
        except Exception as exc:  # noqa: BLE001
            summary.failed_sources[name] = f"converter failed to load: {exc}"
            continue
        if explicit and not folder.is_dir():
            raise LakeBuildError(f"source folder not found: {folder}")
        if converter is None:
            if explicit:
                raise LakeBuildError(f"no converter for source {name!r}")
            summary.ignored_sources[name] = "no converter"
            continue
        try:
            datasets = list(dict.fromkeys(converter.datasets()))
        except Exception as exc:  # noqa: BLE001
            summary.failed_sources[name] = f"datasets() failed: {exc}"
            continue
        if not datasets:
            summary.ignored_sources[name] = "converter lists no datasets"
            continue
        size = _dir_size(folder)
        tasks.extend(_Task(name, ds, folder, size, ref) for ds in datasets)
    return tasks


def _run(
    pending: list[_Task],
    summary: BuildSummary,
    workers: int,
    lake_dir: Path,
    force: bool,
    memory: str | None,
    threads: int | None,
    scratch: Path | None,
) -> None:
    queue = list(pending)
    total = len(queue)
    finished = 0
    pool = _Pool(workers)
    try:
        while queue or pool.running:
            while queue and len(pool.running) < workers:
                task = queue.pop(0)
                spec = {
                    "source": task.source,
                    "dataset": task.dataset,
                    "input_dir": str(task.input_dir),
                    "lake_dir": str(lake_dir),
                    "converter": task.converter,
                    "force": force,
                    "memory": memory,
                    "threads": threads,
                    "scratch": str(scratch) if scratch is not None else None,
                }
                logger.info("lake build: start %s/%s", task.source, task.dataset)
                pool.start(task, spec)
            for running, payload in pool.collect():
                finished += 1
                record = _record(summary.run_id, running, payload)
                _append_record(summary.state_file, record)
                _account(summary, record)
                logger.log(
                    logging.INFO if record.ok else logging.ERROR,
                    "lake build: %s/%s %s converted=%d skipped=%d errors=%d "
                    "%.0fs | %d/%d tasks%s",
                    record.source,
                    record.dataset,
                    record.status,
                    record.converted,
                    record.skipped,
                    record.errors,
                    record.seconds,
                    finished,
                    total,
                    f" | {record.message.splitlines()[-1][:200]}"
                    if record.message
                    else "",
                )
    except BaseException:
        logger.warning("lake build interrupted; ending %d task(s)", len(pool.running))
        pool.terminate_all()
        raise
    finally:
        pool.terminate_all()


def _record(run_id: str, running: _Running, payload: dict[str, Any]) -> TaskRecord:
    errors: list[str] = payload["errors"]
    exception: str | None = payload.get("exception")
    ok = not errors and exception is None
    message = exception if exception is not None else _join_messages(errors)
    return TaskRecord(
        run_id=run_id,
        source=running.task.source,
        dataset=running.task.dataset,
        started_at=running.started_at,
        finished_at=datetime.now(UTC).isoformat(timespec="seconds"),
        seconds=round(time.monotonic() - running.t0, 1),
        status=STATUS_OK if ok else STATUS_ERROR,
        converted=payload["converted"],
        skipped=payload["skipped"],
        errors=len(errors) + (0 if exception is None else 1),
        message=message,
    )


def _account(summary: BuildSummary, record: TaskRecord) -> None:
    sums = summary.sources.setdefault(record.source, SourceSummary(record.source))
    sums.converted += record.converted
    sums.skipped += record.skipped
    sums.errors += record.errors
    if record.ok:
        sums.tasks_ok += 1
    else:
        sums.tasks_failed += 1
        sums.failed_datasets.append(record.dataset)
        summary.failed_tasks.append(record)
