"""Fake lake converter for the ``lake_build`` tests.

Lives in an importable module because the task processes are spawned: they
receive the converter by pickle (class path + state), not by monkeypatch.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Outcome:
    converted: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


class FakeConverter:
    """Converter whose per-dataset behavior is picked by ``behaviors``.

    * ``ok`` (default): writes a marker file in the lake and reports 1 converted;
    * ``errors``: reports an error message, like a converter that skips a bad file;
    * ``raise`` / ``memory``: raises ``ValueError`` / ``MemoryError``;
    * ``exit``: ``os._exit(1)``, as a crash would;
    * ``env``: records the resource variables seen by the task;
    * ``hang``: starts a grandchild process and sleeps (to be interrupted).

    Every call leaves ``control_dir/calls/<source>__<dataset>__<pid>.json``.
    """

    def __init__(
        self,
        source_id: str,
        datasets: list[str],
        control_dir: Path,
        behaviors: dict[str, str] | None = None,
    ) -> None:
        self.source_id = source_id
        self._datasets = datasets
        self.control_dir = Path(control_dir)
        self.behaviors = behaviors or {}

    def datasets(self) -> list[str]:
        return list(self._datasets)

    def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
        (dataset,) = datasets
        calls = self.control_dir / "calls"
        calls.mkdir(parents=True, exist_ok=True)
        (calls / f"{self.source_id}__{dataset}__{os.getpid()}.json").write_text(
            json.dumps({"input_dir": str(input_dir), "force": force})
        )
        if Path(input_dir).name != self.source_id:
            return Outcome(errors=[f"wrong input_dir {input_dir}"])
        behavior = self.behaviors.get(dataset, "ok")
        if behavior == "raise":
            raise ValueError(f"boom in {dataset}")
        if behavior == "memory":
            raise MemoryError
        if behavior == "exit":
            os._exit(1)
        if behavior == "errors":
            return Outcome(converted=1, errors=["bad file a.csv", "bad file b.csv"])
        if behavior == "env":
            seen = {
                k: os.environ.get(k)
                for k in (
                    "QUANTILICA_DUCKDB_MEMORY",
                    "QUANTILICA_DUCKDB_THREADS",
                    "QUANTILICA_LAKE_SCRATCH",
                )
            }
            (self.control_dir / f"env-{dataset}.json").write_text(json.dumps(seen))
        if behavior == "hang":
            pids = self.control_dir / "pids"
            pids.mkdir(exist_ok=True)
            grandchild = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(600)"]
            )
            (pids / f"{dataset}.grandchild").write_text(str(grandchild.pid))
            (pids / f"{dataset}.task").write_text(str(os.getpid()))
            time.sleep(600)
        marker = Path(lake_dir) / self.source_id / dataset / "part.txt"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("x")
        return Outcome(converted=1)


def count_calls(control_dir: Path, source: str, dataset: str) -> int:
    return len(list((Path(control_dir) / "calls").glob(f"{source}__{dataset}__*.json")))


class EnvControlConverter(FakeConverter):
    """Importable by path (``"lake_build_fakes:IMPORTABLE"``); the control
    directory comes from the environment, which the task process inherits."""

    def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
        self.control_dir = Path(os.environ["LAKE_BUILD_FAKE_CONTROL"])
        return super().convert(input_dir, lake_dir, datasets=datasets, force=force)


IMPORTABLE = EnvControlConverter("src", ["d"], Path("."))


def _ano_2024(_path):
    return {"ano": 2024}


class ReadSpyConverter:
    """Real spec converter (``LakeWriter`` underneath) that logs every raw file read
    to hash it: the log is ``control_dir/hashed.log``, one path per line.

    The spy sits on ``quantilica.core.manifests.sha256_file``, the only reader of
    ``trusted_sha256``, inside the task process (patching the parent is useless
    in a spawned process).
    """

    source_id = "demo"

    def __init__(self, control_dir: Path) -> None:
        self.control_dir = Path(control_dir)

    def datasets(self) -> list[str]:
        return ["vendas"]

    def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
        import quantilica.core.manifests as manifests

        from quantilica.analytics.lake import DatasetSpec, convert_with_specs

        log = self.control_dir / "hashed.log"
        real = manifests.sha256_file

        def spy(path, *args, **kwargs):
            with log.open("a", encoding="utf-8") as stream:
                stream.write(f"{path}\n")
            return real(path, *args, **kwargs)

        manifests.sha256_file = spy
        specs = {
            "vendas": DatasetSpec(
                dataset_id="vendas",
                partition_keys=("ano",),
                partitions_from=_ano_2024,
                reader="duckdb",
                separator=";",
            )
        }
        return convert_with_specs(
            self.source_id,
            specs,
            Path(input_dir),
            Path(lake_dir),
            datasets=datasets,
            force=force,
        )
