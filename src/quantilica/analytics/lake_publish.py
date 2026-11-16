"""Publication markers of a lake kept in a synced folder.

A lake that lives in a folder synced by a cloud client is written by one machine
(the one that runs ``lake build``) and read by another one, through a read-only
mount that never writes there. The sync client uploads files in any order, so a
reader can see a new Parquet without its neighbours, or the marker already gone
before the Parquets arrived. The reader needs a criterion of its own for
"published". Three small JSON files carry it:

* ``<lake>/<source>/_em-atualizacao.json`` (the *marker*): written by
  ``lake build`` before it writes anything of the source, removed by
  ``lake audit --clear-marker`` once the audit of the source passes. While it
  exists, the source is being updated.
* ``<lake>/<source>/_build.json`` (the *build manifest*): written, atomically, at
  the end of a build that succeeded on the source. It lists every Parquet of the
  source with its size in bytes, so a reader can tell whether everything that
  the build wrote has arrived.
* a *state file*, outside the lake, written by the read-only audit: per source,
  the ``build_id`` audited, the result (:data:`RESULTS`) and when. Readers that
  receive the state only use the sources it calls ``publicado`` whose
  ``build_id`` is that of the current ``_build.json``.

The file names, the result names and the state layout are a contract with
``quantilica-cli``, which reads them without importing this package.

State layout (``versao`` 1)::

    {"versao": 1,
     "fontes": {"<source>": {"build_id": "...", "resultado": "publicado",
                             "auditado_em": "2026-11-16T10:00:00+00:00"}}}
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantilica.analytics.lake import TMP_SUFFIX

MARKER_NAME = "_em-atualizacao.json"
BUILD_MANIFEST_NAME = "_build.json"
STATE_VERSION = 1

RESULT_PUBLISHED = "publicado"
RESULT_UPDATING = "em_atualizacao"
RESULT_INCOMPLETE = "incompleto"
RESULT_FAILED = "falhou"
RESULTS = (RESULT_PUBLISHED, RESULT_UPDATING, RESULT_INCOMPLETE, RESULT_FAILED)


# A sync client or an antivirus may hold a file for a moment (WinError 5/32 on
# Windows): replacing or removing it is tried again, waiting 0.2 s, 0.4 s...
RETRY_ATTEMPTS = 5
RETRY_FIRST_WAIT = 0.2
RETRY_MAX_WAIT = 2.0


class LakePublishError(ValueError):
    """A marker, build manifest or state file that cannot be read."""


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def analytics_version() -> str:
    """Installed version of ``quantilica-analytics`` (``"desconhecida"`` if none)."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("quantilica-analytics")
    except PackageNotFoundError:  # pragma: no cover - running from a bare checkout
        return "desconhecida"


def _retrying(operation: Callable[[], None]) -> None:
    """Run ``operation``, trying again when it fails with ``PermissionError``."""
    wait = RETRY_FIRST_WAIT
    for attempt in range(RETRY_ATTEMPTS):
        try:
            operation()
            return
        except PermissionError:
            if attempt == RETRY_ATTEMPTS - 1:
                raise
            time.sleep(wait)
            wait = min(wait * 2, RETRY_MAX_WAIT)


def write_json_atomic(path: Path, data: Mapping[str, Any]) -> None:
    """Write ``data`` as JSON to ``path``: a temporary file, then ``os.replace``.

    The temporary file sits next to ``path`` (same volume), is named
    ``.<name>.<pid>.tmp`` and is removed if the write fails. ``os.replace``
    is tried again (up to :data:`RETRY_ATTEMPTS` times) on ``PermissionError``.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}{TMP_SUFFIX}")
    try:
        with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        _retrying(lambda: os.replace(tmp, path))
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


def _read_json(path: Path) -> dict[str, Any] | None:
    """The JSON object in ``path``; ``None`` if the file does not exist."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError) as exc:
        raise LakePublishError(f"{path}: cannot read ({exc})") from exc
    try:
        data = json.loads(text)
    except ValueError as exc:
        raise LakePublishError(f"{path}: invalid JSON ({exc})") from exc
    if not isinstance(data, dict):
        raise LakePublishError(f"{path}: expected a JSON object")
    return data


# --- marker -------------------------------------------------------------------------


def marker_path(source_dir: Path | str) -> Path:
    return Path(source_dir) / MARKER_NAME


def has_marker(source_dir: Path | str) -> bool:
    return marker_path(source_dir).exists()


def write_marker(source_dir: Path | str, build_id: str) -> Path:
    """Mark the source as being updated by ``build_id`` (before writing anything).

    A marker of the same ``build_id`` (a resumed run) is left as it is, with its
    original ``iniciado_em``; any other marker is replaced.
    """
    path = marker_path(source_dir)
    try:
        current = _read_json(path)
    except LakePublishError:
        current = None  # unreadable leftover: replace it
    if current is not None and current.get("build_id") == build_id:
        return path
    write_json_atomic(
        path,
        {
            "build_id": build_id,
            "host": socket.gethostname(),
            "iniciado_em": now_iso(),
        },
    )
    return path


def read_marker(source_dir: Path | str) -> dict[str, Any] | None:
    """The marker's content, ``None`` if there is none.

    A marker that exists but cannot be parsed (the sync client may have shown the
    file before its content) still means "being updated": it comes back as an
    empty dict.
    """
    path = marker_path(source_dir)
    try:
        return _read_json(path)
    except LakePublishError:
        return {} if path.exists() else None


def clear_marker(source_dir: Path | str) -> bool:
    """Remove the marker (trying again on ``PermissionError``); whether there was
    one."""
    path = marker_path(source_dir)
    if not path.exists():
        return False
    try:
        _retrying(path.unlink)
    except FileNotFoundError:
        return False
    return True


# --- build manifest ------------------------------------------------------------------


def build_manifest_path(source_dir: Path | str) -> Path:
    return Path(source_dir) / BUILD_MANIFEST_NAME


def write_build_manifest(
    source_dir: Path | str,
    build_id: str,
    parquets: Mapping[str, int],
    *,
    rows: Mapping[str, int] | None = None,
) -> Path:
    """Write ``_build.json`` of a source whose build succeeded.

    ``parquets`` maps the path of every Parquet of the source, relative to the
    source folder and with ``/``, to its size in bytes. ``rows`` (rows per
    dataset) is recorded only when the caller has it without reopening files.
    """
    path = build_manifest_path(source_dir)
    data: dict[str, Any] = {
        "build_id": build_id,
        "concluido_em": now_iso(),
        "versao_analytics": analytics_version(),
        "parquets": dict(sorted(parquets.items())),
    }
    if rows is not None:
        data["linhas"] = dict(sorted(rows.items()))
    write_json_atomic(path, data)
    return path


def read_build_manifest(source_dir: Path | str) -> dict[str, Any] | None:
    """The build manifest; ``None`` if there is none (a lake from before it).

    Raises:
        LakePublishError: the file exists but is not a manifest.
    """
    path = build_manifest_path(source_dir)
    data = _read_json(path)
    if data is None:
        return None
    sizes = data.get("parquets")
    if not isinstance(sizes, dict) or not all(
        isinstance(k, str) and isinstance(v, int) and not isinstance(v, bool)
        for k, v in sizes.items()
    ):
        raise LakePublishError(f"{path}: 'parquets' must map paths to sizes")
    return data


# --- state file ---------------------------------------------------------------------


def read_state(path: Path | str) -> dict[str, dict[str, Any]]:
    """The per-source entries of a state file; ``{}`` if it does not exist yet.

    Raises:
        LakePublishError: the file exists but is not a state file.
    """
    path = Path(path)
    data = _read_json(path)
    if data is None:
        return {}
    sources = data.get("fontes")
    if data.get("versao") != STATE_VERSION or not isinstance(sources, dict):
        raise LakePublishError(
            f"{path}: not a lake state file (expected versao={STATE_VERSION} "
            "and a 'fontes' object)"
        )
    return {k: dict(v) for k, v in sources.items() if isinstance(v, dict)}


def update_state(
    path: Path | str, entries: Mapping[str, Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Merge ``entries`` (source -> entry) into the state file, atomically.

    The sources not in ``entries`` keep what the file had. Returns the whole
    state written.
    """
    path = Path(path)
    state = read_state(path)
    state.update({k: dict(v) for k, v in entries.items()})
    write_json_atomic(
        path, {"versao": STATE_VERSION, "fontes": dict(sorted(state.items()))}
    )
    return state


def state_entry(
    build_id: str | None, result: str, audited_at: str | None = None
) -> dict[str, Any]:
    """One source's entry of the state file."""
    if result not in RESULTS:
        raise ValueError(f"unknown result {result!r}: expected one of {RESULTS}")
    return {
        "build_id": build_id,
        "resultado": result,
        "auditado_em": audited_at or now_iso(),
    }
