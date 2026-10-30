"""``lake_build``: tasks per converter dataset, isolation, resume, interruption."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from collections import namedtuple
from pathlib import Path

import pytest
from lake_build_fakes import FakeConverter, count_calls

from quantilica.analytics import lake_build as lb
from quantilica.analytics.lake_build import (
    InsufficientDiskError,
    LakeBuildError,
    lake_build,
)

WINDOWS = sys.platform.startswith("win")
HERE = Path(__file__).parent


def _raw(tmp_path: Path, *folders: str) -> Path:
    root = tmp_path / "input"
    for folder in folders:
        (root / folder).mkdir(parents=True, exist_ok=True)
        (root / folder / "file.csv").write_text("a,b\n1,2\n")
    return root


def _records(lake: Path) -> list[dict]:
    rows = []
    for path in sorted((lake / ".build").glob("*.jsonl")):
        rows += [json.loads(line) for line in path.read_text().splitlines()]
    return rows


def _build(tmp_path: Path, converters: dict, **kwargs):
    kwargs.setdefault("workers", 2)
    kwargs.setdefault("prevent_sleep", False)
    return lake_build(
        tmp_path / "input", tmp_path / "lake", converters=converters, **kwargs
    )


def test_tasks_follow_converter_dataset_ids_not_folder_names(tmp_path):
    # Regressão: o id do dataset difere do nome da pasta da fonte.
    control = tmp_path / "control"
    _raw(tmp_path, "alpha", "docs")
    conv = FakeConverter("alpha", ["aero-helideck", "resgates", "alpha"], control)

    summary = _build(tmp_path, {"alpha": conv})

    assert not summary.has_errors
    assert summary.sources["alpha"].converted == 3
    for ds in ("aero-helideck", "resgates", "alpha"):
        assert (tmp_path / "lake" / "alpha" / ds / "part.txt").exists()
        assert count_calls(control, "alpha", ds) == 1
    # Pasta sem conversor: ignorada, não é erro.
    assert summary.ignored_sources == {"docs": "no converter"}
    assert "docs" not in summary.sources


def test_failures_are_isolated_recorded_and_retried(tmp_path):
    control = tmp_path / "control"
    _raw(tmp_path, "src")
    behaviors = {
        "raises": "raise",
        "crash": "exit",
        "memory": "memory",
        "partial": "errors",
    }
    conv = FakeConverter(
        "src",
        ["good-1", "raises", "crash", "memory", "partial", "good-2"],
        control,
        behaviors,
    )

    summary = _build(tmp_path, {"src": conv})

    assert summary.has_errors
    sums = summary.sources["src"]
    assert sums.tasks_ok == 2 and sums.tasks_failed == 4
    assert sorted(sums.failed_datasets) == ["crash", "memory", "partial", "raises"]
    by_ds = {r["dataset"]: r for r in _records(tmp_path / "lake")}
    assert {d: r["status"] for d, r in by_ds.items()} == {
        "good-1": "ok",
        "good-2": "ok",
        "raises": "erro",
        "crash": "erro",
        "memory": "erro",
        "partial": "erro",
    }
    assert "ValueError: boom in raises" in by_ds["raises"]["message"]
    assert "MemoryError" in by_ds["memory"]["message"]
    assert "code 1" in by_ds["crash"]["message"]
    assert by_ds["partial"]["errors"] == 2 and by_ds["partial"]["converted"] == 1
    assert "bad file a.csv" in by_ds["partial"]["message"]
    assert set(by_ds["good-1"]) >= {
        "run_id", "source", "dataset", "started_at", "finished_at", "seconds",
        "status", "converted", "skipped", "errors", "message",
    }  # fmt: skip
    assert by_ds["good-1"]["converted"] == 1 and by_ds["good-1"]["errors"] == 0

    # Segunda execução, sem retry_errors: não refaz nada, mas continua falhando.
    again = _build(tmp_path, {"src": conv})
    assert again.has_errors
    assert sorted(ds for _, ds in again.previous_failures) == [
        "crash", "memory", "partial", "raises",
    ]  # fmt: skip
    assert count_calls(control, "src", "good-1") == 1
    assert count_calls(control, "src", "raises") == 1

    # retry_errors refaz só as que falharam (agora corrigidas).
    fixed = FakeConverter("src", conv.datasets(), control)
    retried = _build(tmp_path, {"src": fixed}, retry_errors=True)
    assert not retried.has_errors
    assert retried.sources["src"].tasks_ok == 4
    assert retried.sources["src"].tasks_already_done == 2
    assert count_calls(control, "src", "good-1") == 1
    assert count_calls(control, "src", "raises") == 2
    # Terceira execução: tudo concluído, nada a fazer.
    assert not _build(tmp_path, {"src": fixed}).has_errors
    assert count_calls(control, "src", "raises") == 2


def test_force_redoes_everything(tmp_path):
    control = tmp_path / "control"
    _raw(tmp_path, "src")
    conv = FakeConverter("src", ["a", "b"], control)
    _build(tmp_path, {"src": conv})
    _build(tmp_path, {"src": conv}, force=True)
    assert count_calls(control, "src", "a") == 2
    forced = [
        json.loads(p.read_text())["force"] for p in (control / "calls").glob("*.json")
    ]
    assert sorted(forced) == [False, False, True, True]


def test_sources_selection(tmp_path):
    control = tmp_path / "control"
    _raw(tmp_path, "one", "two", "bare")
    convs = {
        "one": FakeConverter("one", ["one"], control),
        "two": FakeConverter("two", ["two"], control),
    }
    summary = _build(tmp_path, convs, sources=["two"])
    assert list(summary.sources) == ["two"]
    assert count_calls(control, "one", "one") == 0
    with pytest.raises(LakeBuildError, match="no converter"):
        _build(tmp_path, convs, sources=["bare"])
    with pytest.raises(LakeBuildError, match="not found"):
        _build(tmp_path, convs, sources=["nope"])
    with pytest.raises(LakeBuildError, match="not found"):
        lake_build(tmp_path / "missing", tmp_path / "lake", converters=convs)


def test_converter_by_import_path_and_resources_reach_the_task(tmp_path, monkeypatch):
    control = tmp_path / "control"
    _raw(tmp_path, "src")
    conv = FakeConverter("src", ["d"], control, {"d": "env"})
    scratch = tmp_path / "scratch"

    _build(tmp_path, {"src": conv}, memory="3GB", threads=2, scratch=scratch)

    seen = json.loads((control / "env-d.json").read_text())
    assert seen == {
        "QUANTILICA_DUCKDB_MEMORY": "3GB",
        "QUANTILICA_DUCKDB_THREADS": "2",
        "QUANTILICA_LAKE_SCRATCH": str(scratch),
    }
    # O ambiente do pai fica intacto.
    assert os.environ.get("QUANTILICA_DUCKDB_MEMORY") != "3GB"

    # Referência "pacote.módulo:atributo", resolvida no processo da tarefa.
    monkeypatch.setenv("LAKE_BUILD_FAKE_CONTROL", str(control))
    summary = _build(tmp_path, {"src": "lake_build_fakes:IMPORTABLE"}, force=True)
    assert not summary.has_errors
    assert count_calls(control, "src", "d") == 2


# --- pré-checagem de disco ---------------------------------------------------

_Usage = namedtuple("_Usage", "total used free")


def _fake_disk(monkeypatch, free: int) -> None:
    monkeypatch.setattr(
        lb.shutil, "disk_usage", lambda path: _Usage(10**13, 10**13 - free, free)
    )


def test_disk_precheck_blocks_before_converting(tmp_path, monkeypatch):
    control = tmp_path / "control"
    raw = _raw(tmp_path, "src")
    (raw / "src" / "big.bin").write_bytes(b"x" * 10_000)
    conv = FakeConverter("src", ["a"], control)
    _fake_disk(monkeypatch, free=1_000)

    with pytest.raises(InsufficientDiskError, match="not enough free space"):
        _build(tmp_path, {"src": conv}, disk_factor=1.0, scratch_factor=0.0)
    assert count_calls(control, "src", "a") == 0
    assert not (tmp_path / "lake" / ".build").exists()

    # A margem do fator: 10 KB * 0,05 = ~500 B cabem em 1000 B.
    summary = _build(tmp_path, {"src": conv}, disk_factor=0.05, scratch_factor=0.0)
    assert not summary.has_errors


def test_disk_precheck_can_be_ignored_and_counts_scratch(tmp_path, monkeypatch):
    control = tmp_path / "control"
    raw = _raw(tmp_path, "src")
    (raw / "src" / "big.bin").write_bytes(b"x" * 10_000)
    conv = FakeConverter("src", ["a"], control)
    _fake_disk(monkeypatch, free=1_000)

    # Lake e scratch no mesmo volume: as duas estimativas se somam (500 + 500 > 900).
    _fake_disk(monkeypatch, free=900)
    with pytest.raises(InsufficientDiskError, match="lake \\+ scratch"):
        _build(
            tmp_path,
            {"src": conv},
            disk_factor=0.05,
            scratch_factor=0.05,
            scratch=tmp_path / "scratch",
        )
    summary = _build(
        tmp_path,
        {"src": conv},
        disk_factor=0.05,
        scratch_factor=0.05,
        check_disk=False,
    )
    assert not summary.has_errors


def test_disk_precheck_skipped_when_nothing_is_pending(tmp_path, monkeypatch):
    control = tmp_path / "control"
    _raw(tmp_path, "src")
    conv = FakeConverter("src", ["a"], control)
    _build(tmp_path, {"src": conv})
    _fake_disk(monkeypatch, free=0)
    assert not _build(tmp_path, {"src": conv}).has_errors


# --- interrupção -------------------------------------------------------------


def _pid_alive(pid: int) -> bool:
    if WINDOWS:
        out = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:  # zumbi sem init que o colha conta como morto
        stat = Path(f"/proc/{pid}/stat").read_text()
        return stat.rsplit(")", 1)[1].split()[0] != "Z"
    except OSError:
        return True


def _wait_for(condition, timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.1)
    return False


_INTERRUPTS = (
    [("ctrl-break", None)]
    if WINDOWS
    else [("sigint", signal.SIGINT), ("sigterm", signal.SIGTERM)]
)


@pytest.mark.parametrize(("label", "sig"), _INTERRUPTS, ids=[i[0] for i in _INTERRUPTS])
def test_interrupt_ends_whole_process_tree_and_resume_skips_done(tmp_path, label, sig):
    control = tmp_path / "control"
    lake = tmp_path / "lake"
    _raw(tmp_path, "slow")
    kwargs = (
        {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        if WINDOWS
        else {"start_new_session": True}
    )
    # Arquivo, não pipe: um neto órfão seguraria o pipe e travaria o teste.
    log = (tmp_path / "driver.log").open("w")
    driver = subprocess.Popen(
        [
            sys.executable,
            str(HERE / "lake_build_driver.py"),
            str(tmp_path / "input"),
            str(lake),
            str(control),
        ],
        cwd=tmp_path,
        stderr=log,
        **kwargs,
    )
    pids = control / "pids"
    try:
        # "a" terminou (linha no JSONL) e "b" está pendurada com um neto.
        assert _wait_for(
            lambda: (
                (pids / "b.grandchild").exists()
                and (pids / "b.task").exists()
                and any(r["dataset"] == "a" for r in _records(lake))
            )
        ), "the build did not reach the hanging task"
        task_pid = int((pids / "b.task").read_text())
        grandchild_pid = int((pids / "b.grandchild").read_text())
        assert _pid_alive(task_pid) and _pid_alive(grandchild_pid)

        driver.send_signal(signal.CTRL_BREAK_EVENT if WINDOWS else sig)
        driver.wait(timeout=60)
    finally:
        if driver.poll() is None:
            driver.kill()
        driver.wait()
        log.close()

    assert driver.returncode != 0
    assert _wait_for(
        lambda: not _pid_alive(task_pid) and not _pid_alive(grandchild_pid), 15
    ), "orphan process left behind"
    assert not _pid_alive(driver.pid)

    # Só "a" está registrada; "b" ficou sem linha (não terminou).
    assert [(r["dataset"], r["status"]) for r in _records(lake)] == [("a", "ok")]

    # Retomada: "a" não é refeita, "b" (agora sem travar) é convertida.
    conv = FakeConverter("slow", ["a", "b"], control)
    summary = lake_build(
        tmp_path / "input",
        lake,
        workers=2,
        converters={"slow": conv},
        check_disk=False,
        prevent_sleep=False,
    )
    assert not summary.has_errors
    assert summary.sources["slow"].tasks_already_done == 1
    assert summary.sources["slow"].tasks_ok == 1
    assert count_calls(control, "slow", "a") == 1
    assert count_calls(control, "slow", "b") == 2
    assert len(list((lake / ".build").glob("*.jsonl"))) == 2


def test_truncated_state_line_is_ignored(tmp_path):
    control = tmp_path / "control"
    _raw(tmp_path, "src")
    conv = FakeConverter("src", ["a", "b"], control)
    _build(tmp_path, {"src": conv})
    state = next((tmp_path / "lake" / ".build").glob("*.jsonl"))
    state.write_text(state.read_text() + '{"source": "src", "data')  # queda de energia
    summary = _build(tmp_path, {"src": conv})
    assert summary.sources["src"].tasks_already_done == 2
    assert count_calls(control, "src", "a") == 1


# --- keep_awake --------------------------------------------------------------


class _FakePopen:
    instances: list[_FakePopen] = []

    def __init__(self, cmd, **kwargs):
        self.cmd = cmd
        self.terminated = False
        self._exit = None
        _FakePopen.instances.append(self)

    def poll(self):
        return self._exit

    def terminate(self):
        self.terminated = True
        self._exit = 0

    def wait(self, timeout=None):
        return 0

    def kill(self):
        self._exit = -9


@pytest.fixture
def fake_popen(monkeypatch):
    _FakePopen.instances = []
    monkeypatch.setattr(subprocess, "Popen", _FakePopen)
    monkeypatch.setattr(lb.time, "sleep", lambda s: None)
    return _FakePopen.instances


def test_keep_awake_linux_uses_systemd_inhibit(monkeypatch, fake_popen):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(lb.shutil, "which", lambda name: "/usr/bin/" + name)
    with lb.keep_awake():
        assert len(fake_popen) == 1
        assert fake_popen[0].cmd[0] == "systemd-inhibit"
        assert str(os.getpid()) in fake_popen[0].cmd
        assert not fake_popen[0].terminated
    assert fake_popen[0].terminated


def test_keep_awake_macos_uses_caffeinate(monkeypatch, fake_popen):
    monkeypatch.setattr(sys, "platform", "darwin")
    with lb.keep_awake():
        assert fake_popen[0].cmd == ["caffeinate", "-i", "-w", str(os.getpid())]
    assert fake_popen[0].terminated


def test_keep_awake_without_tool_only_warns(monkeypatch, fake_popen, caplog):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(lb.shutil, "which", lambda name: None)
    with caplog.at_level("WARNING", logger="quantilica.analytics.lake_build"):
        with lb.keep_awake():
            pass
    assert not fake_popen
    assert "no way to block suspension" in caplog.text


def test_keep_awake_helper_failing_at_once_only_warns(monkeypatch, fake_popen, caplog):
    monkeypatch.setattr(sys, "platform", "darwin")

    class Dead(_FakePopen):
        def __init__(self, cmd, **kwargs):
            super().__init__(cmd, **kwargs)
            self._exit = 1

    monkeypatch.setattr(subprocess, "Popen", Dead)
    with caplog.at_level("WARNING", logger="quantilica.analytics.lake_build"):
        with lb.keep_awake():
            pass
    assert "exited at once" in caplog.text


def test_keep_awake_windows_sets_and_restores_execution_state(monkeypatch):
    calls: list[int] = []

    class Kernel32:
        @staticmethod
        def SetThreadExecutionState(flags):
            calls.append(flags)

    class Windll:
        kernel32 = Kernel32()

    import ctypes

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(ctypes, "windll", Windll(), raising=False)
    with lb.keep_awake():
        assert calls == [0x80000000 | 0x00000001]
    assert calls == [0x80000000 | 0x00000001, 0x80000000]
