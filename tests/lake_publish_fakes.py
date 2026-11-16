"""Converter that writes real Parquets, for the publication-marker tests.

Importable module because the task processes are spawned and receive the
converter by pickle.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from lake_build_fakes import FakeConverter


class ParquetConverter(FakeConverter):
    """Writes ``<lake>/<source>/<dataset>/ano=2026/p.parquet``.

    Behaviors are those of :class:`FakeConverter` (``raise``...). Besides the
    Parquet it leaves ``<control>/marker-<dataset>`` saying whether the update
    marker existed when the conversion ran.
    """

    def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
        (dataset,) = datasets
        marker = Path(lake_dir) / self.source_id / "_em-atualizacao.json"
        self.control_dir.mkdir(parents=True, exist_ok=True)
        (self.control_dir / f"marker-{dataset}").write_text(str(marker.exists()))
        outcome = super().convert(input_dir, lake_dir, datasets=datasets, force=force)
        if self.behaviors.get(dataset, "ok") != "ok":
            return outcome
        target = Path(lake_dir) / self.source_id / dataset / "ano=2026"
        target.mkdir(parents=True, exist_ok=True)
        # Size depends on the dataset and on the run, so a changed file shows.
        rows = 10 * (len(dataset) + int(os.environ.get("LAKE_FAKE_ROWS", "0")))
        pq.write_table(pa.table({"v": list(range(rows))}), target / "p.parquet")
        return outcome
