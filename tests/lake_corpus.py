"""Synthetic raw archive and lake, with planted defects, for the lake tests.

``build_corpus(tmp_path)`` creates, under ``tmp_path``, a raw tree of four
fake sources, its current-version view and the lake converted from that view.
Everything is real: manifests come from ``quantilica.core.manifests``, the
view from ``materialize_view`` and the Parquets from ``LakeWriter``.

Sources (``<raw>/<source>/<folder>/<file>``)::

    alfa   vendas   vendas_2025@20260101.csv   superseded by @20261021
                    vendas_2025@20261021.csv   -> ano=2025
                    vendas_2026@20261021.csv   -> ano=2026
                    vendas_2026@20261021.zip   format twin of the csv (no Parquet)
           manual@20261021.pdf                 not tabular (no Parquet)
    beta   precos   precos_202501@20261021.csv -> ano=2025/mes=01
                    precos_202502@20260901.csv superseded
                    precos_202502@20261021.csv -> ano=2025/mes=02
                    leia-me@20261021.txt        not tabular (no Parquet)
    gama   grupos   g_202001-{sp,ac}@20260301.csv, g_202002-{sp,ac}@20260301.csv
                    (+ g_202001-sp@20260101.csv, superseded) grouped files,
                    one ``g_<AAAAMM>.parquet`` per ano=/mes= with a member per uf
    delta  pasta    cadastro_a@20261021.csv -> dataset "cad-a" (no partitions)
                    cadastro_b@20261021.csv -> dataset "cad-b" (ids != folder)

Every raw file has a manifest sidecar. The lake holds only the current
versions, and a clean corpus passes ``lake_audit(corpus.lake,
input_dir=corpus.view)`` with ``ok=True``.

API
---
``build_corpus(tmp_path) -> Corpus``
    ``Corpus.raw`` (every version), ``Corpus.view`` (current versions, copied),
    ``Corpus.lake``; ``view_file(name)`` and ``raw_file(name)`` (path by file
    name), ``parquet_for(name)`` (the lake Parquet whose origin is that file),
    ``dataset_dir(source, dataset)``, ``lake_parquets()``, ``write(name,
    dataset, partitions)`` (convert a raw file into the lake by hand).

Planted defects (each mutates the corpus and returns the path it touched):

``plant_truncated_parquet``   cut a Parquet in half (footer gone)
``plant_duplicate_version``   convert the superseded version into another partition
``plant_duplicate_member``    same, for a member of a grouped file
``plant_stale_origin``        the lake holds the superseded version of a file
``plant_mixed_depth``         lift a Parquet one partition level up
``plant_missing_parquet``     delete the Parquet of a current file
``plant_tmp_leftover``        a ``.tmp`` staging directory with a file in it
``plant_sha_mismatch``        the view's digest no longer matches the lake's
``plant_directory_link``      a directory symlink into the lake (or to outside it)

Extra raw files (``add_raw``, which writes the file and its sidecar into the raw
tree and, unless ``view=False``, into the view) and ``convert`` (a raw file into
the lake, whatever its location) build the cases the base corpus does not hold:
the same file name in two folders with different content, a version that only
differs from the current one in its stamp, a source with no lake directory, a
helper folder (``_documentacao``).

Also ``FakeConverter``: a converter with an ``inventory()`` built from
``{file name: dataset id or skip reason}``.
"""

from __future__ import annotations

import json
import shutil
import zipfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from quantilica.core.lake import (
    SKIP_REASONS,
    DatasetDescriptor,
    Planned,
    materialize_view,
)
from quantilica.core.manifests import (
    DownloadManifest,
    manifest_sidecar_path,
    write_manifest_sidecar,
)

from quantilica.analytics.lake import LakeWriter

CSV_CONTENT = "valor\n1\n2\n3\n"

# raw name -> (source, folder)
_RAW: dict[str, tuple[str, str]] = {
    "vendas_2025@20260101.csv": ("alfa", "vendas"),
    "vendas_2025@20261021.csv": ("alfa", "vendas"),
    "vendas_2026@20261021.csv": ("alfa", "vendas"),
    "vendas_2026@20261021.zip": ("alfa", "vendas"),
    "manual@20261021.pdf": ("alfa", ""),
    "precos_202501@20261021.csv": ("beta", "precos"),
    "precos_202502@20260901.csv": ("beta", "precos"),
    "precos_202502@20261021.csv": ("beta", "precos"),
    "leia-me@20261021.txt": ("beta", ""),
    "g_202001-sp@20260101.csv": ("gama", "grupos"),
    "g_202001-sp@20260301.csv": ("gama", "grupos"),
    "g_202001-ac@20260301.csv": ("gama", "grupos"),
    "g_202002-sp@20260301.csv": ("gama", "grupos"),
    "g_202002-ac@20260301.csv": ("gama", "grupos"),
    "cadastro_a@20261021.csv": ("delta", "pasta"),
    "cadastro_b@20261021.csv": ("delta", "pasta"),
}

# current file -> (source, dataset, partitions, group key, member columns)
_CONVERT: dict[str, tuple[str, str, dict[str, str], str | None, dict[str, str]]] = {
    "vendas_2025@20261021.csv": ("alfa", "vendas", {"ano": "2025"}, None, {}),
    "vendas_2026@20261021.csv": ("alfa", "vendas", {"ano": "2026"}, None, {}),
    "precos_202501@20261021.csv": (
        "beta",
        "precos",
        {"ano": "2025", "mes": "01"},
        None,
        {},
    ),
    "precos_202502@20261021.csv": (
        "beta",
        "precos",
        {"ano": "2025", "mes": "02"},
        None,
        {},
    ),
    "g_202001-sp@20260301.csv": (
        "gama",
        "grupos",
        {"ano": "2020", "mes": "01"},
        "g_202001",
        {"uf": "sp"},
    ),
    "g_202001-ac@20260301.csv": (
        "gama",
        "grupos",
        {"ano": "2020", "mes": "01"},
        "g_202001",
        {"uf": "ac"},
    ),
    "g_202002-sp@20260301.csv": (
        "gama",
        "grupos",
        {"ano": "2020", "mes": "02"},
        "g_202002",
        {"uf": "sp"},
    ),
    "g_202002-ac@20260301.csv": (
        "gama",
        "grupos",
        {"ano": "2020", "mes": "02"},
        "g_202002",
        {"uf": "ac"},
    ),
    "cadastro_a@20261021.csv": ("delta", "cad-a", {}, None, {}),
    "cadastro_b@20261021.csv": ("delta", "cad-b", {}, None, {}),
}


@dataclass
class Corpus:
    """A raw archive, its current-version view and the lake built from it."""

    root: Path
    raw: Path
    view: Path
    lake: Path

    def raw_file(self, name: str) -> Path:
        return self.raw / _location(name)

    def view_file(self, name: str) -> Path:
        return self.view / _location(name)

    def dataset_dir(self, source: str, dataset: str) -> Path:
        return self.lake / source / dataset

    def lake_parquets(self) -> list[Path]:
        return sorted(self.lake.rglob("*.parquet"))

    def parquet_for(self, name: str) -> Path:
        """The lake Parquet whose footer names ``name`` as its origin."""
        for path in self.lake_parquets():
            meta = _footer(path)
            origins = {meta.get("quantilica.origin_file")}
            origins |= {m.get("origin_file") for m in _members(meta).values()}
            if name in origins:
                return path
        raise KeyError(f"no Parquet for {name}")

    def write(
        self,
        name: str,
        dataset: str,
        partitions: dict[str, str],
        *,
        from_view: bool = False,
        group_key: str | None = None,
        member_columns: dict[str, str] | None = None,
        source: str | None = None,
    ) -> Path:
        """Convert the raw file ``name`` into ``dataset``; returns the Parquet."""
        src_dir, _ = _RAW[name]
        source = source or src_dir
        src = self.view_file(name) if from_view else self.raw_file(name)
        writer = LakeWriter(
            self.dataset_dir(source, dataset),
            DatasetDescriptor(
                source_id=source,
                dataset_id=dataset,
                alias=dataset,
                partition_keys=list(partitions),
                partition_types={k: "varchar" for k in partitions},
            ),
        )
        frame = pl.DataFrame({"valor": [1, 2, 3]})
        if group_key:
            result = writer.write_member(
                frame,
                src,
                group_key=group_key,
                partitions=partitions,
                member_columns=member_columns,
            )
        else:
            result = writer.write_polars(frame, src, partitions=partitions)
        return result.output


def _location(name: str) -> Path:
    source, folder = _RAW[name]
    return Path(source) / folder / name


def _footer(path: Path) -> dict[str, str]:
    try:
        raw = pq.ParquetFile(path).metadata.metadata or {}
    except (OSError, pa.ArrowException):  # a Parquet planted as corrupt
        return {}
    return {k.decode(): v.decode() for k, v in raw.items()}


def _members(meta: dict[str, str]) -> dict[str, Any]:
    return json.loads(meta.get("quantilica.members", "{}"))


def _write_raw(path: Path, name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if name.endswith(".zip"):
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("data.csv", CSV_CONTENT)
    else:
        # The name goes in the content, so no two files share a digest.
        path.write_text(CSV_CONTENT + f"# {name}\n", encoding="utf-8")
    source, folder = _RAW[name]
    manifest = DownloadManifest.from_file(
        source_id=source,
        dataset_id=folder or source,
        url=f"https://example.invalid/{name}",
        file_path=path,
    )
    write_manifest_sidecar(path, manifest)


def build_corpus(tmp_path: Path) -> Corpus:
    """Create the raw tree, the view and the lake (see the module docstring)."""
    corpus = Corpus(
        root=tmp_path,
        raw=tmp_path / "raw",
        view=tmp_path / "view",
        lake=tmp_path / "lake",
    )
    for name in _RAW:
        _write_raw(corpus.raw_file(name), name)
    materialize_view(corpus.raw, corpus.view, mode="copy")
    for name, (
        source,
        dataset,
        partitions,
        group_key,
        member_columns,
    ) in _CONVERT.items():
        corpus.write(
            name,
            dataset,
            partitions,
            from_view=True,
            group_key=group_key,
            member_columns=member_columns,
            source=source,
        )
    return corpus


# --- planted defects ------------------------------------------------------------


def plant_truncated_parquet(
    corpus: Corpus, name: str = "vendas_2026@20261021.csv"
) -> Path:
    """Cut the Parquet of ``name`` in half: the footer is lost."""
    path = corpus.parquet_for(name)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])
    return path


def plant_duplicate_version(corpus: Corpus) -> Path:
    """Convert the superseded ``vendas_2025@20260101.csv`` into ``ano=2024``.

    The lake then holds two versions of the same raw file in two partitions.
    """
    return corpus.write(
        "vendas_2025@20260101.csv", "vendas", {"ano": "2024"}, source="alfa"
    )


def plant_duplicate_member(corpus: Corpus) -> Path:
    """Convert the superseded ``g_202001-sp@20260101.csv`` into another group file."""
    return corpus.write(
        "g_202001-sp@20260101.csv",
        "grupos",
        {"ano": "2019", "mes": "12"},
        group_key="g_201912",
        member_columns={"uf": "sp"},
    )


def plant_stale_origin(corpus: Corpus) -> Path:
    """Overwrite the Parquet of ``precos_202502`` with the superseded version."""
    return corpus.write(
        "precos_202502@20260901.csv", "precos", {"ano": "2025", "mes": "02"}
    )


def plant_mixed_depth(corpus: Corpus) -> Path:
    """Lift ``precos_202502.parquet`` from ``ano=/mes=`` to ``ano=``."""
    path = corpus.parquet_for("precos_202502@20261021.csv")
    target = path.parent.parent / path.name
    shutil.move(path, target)
    return target


def plant_missing_parquet(
    corpus: Corpus, name: str = "precos_202501@20261021.csv"
) -> Path:
    """Delete the Parquet of ``name`` (a current file with no Parquet)."""
    path = corpus.parquet_for(name)
    path.unlink()
    return path


def plant_tmp_leftover(corpus: Corpus) -> Path:
    """A staging directory left behind with a half-written file in it."""
    leftover = corpus.dataset_dir("beta", "precos") / ".tmp" / "tmpabc" / "x.parquet"
    leftover.parent.mkdir(parents=True)
    leftover.write_bytes(b"PAR1")
    return leftover


def plant_sha_mismatch(
    corpus: Corpus,
    name: str = "vendas_2026@20261021.csv",
    *,
    how: str = "manifest",
) -> Path:
    """Make the view's digest of ``name`` differ from the one in the lake.

    ``how="manifest"`` rewrites the sidecar with another digest; ``"file"``
    deletes the sidecar and replaces the file's content (the audit hashes it).
    """
    path = corpus.view_file(name)
    if how == "manifest":
        sidecar = manifest_sidecar_path(path)
        manifest = DownloadManifest.read_json(sidecar)
        write_manifest_sidecar(path, replace(manifest, sha256="0" * 64))
    elif how == "file":
        manifest_sidecar_path(path).unlink()
        path.unlink()  # a new inode: a hardlinked raw file is left alone
        path.write_text(CSV_CONTENT + "# changed\n", encoding="utf-8")
    else:
        raise ValueError(how)
    return path


def plant_directory_link(corpus: Corpus, *, outside: bool = False) -> Path:
    """Add a directory symlink to the lake.

    Inside (default): ``lake/alfa-espelho -> lake/alfa``, a second way to reach
    Parquets the lake already holds. Outside: ``lake/externo -> <root>/externo``,
    a copy of ``lake/delta`` kept in another directory, which is the only way to
    reach those Parquets. Skips the test where symlinks cannot be created.
    """
    if outside:
        target = corpus.root / "externo"
        shutil.copytree(corpus.lake / "delta", target)
        shutil.rmtree(corpus.lake / "delta")
        link = corpus.lake / "delta"
    else:
        target = corpus.lake / "alfa"
        link = corpus.lake / "alfa-espelho"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("cannot create directory symlinks here")
    return link


def add_raw(
    corpus: Corpus,
    source: str,
    folder: str,
    name: str,
    content: str | None = None,
    *,
    view: bool = True,
) -> Path:
    """Write a raw file (and its manifest) into the raw tree and, by default, the view.

    The content defaults to something unique to ``source/folder/name``. Returns
    the path in the view (or in the raw tree with ``view=False``).
    """
    text = (
        content if content is not None else f"{CSV_CONTENT}# {source}/{folder}/{name}\n"
    )
    paths = []
    for root in (corpus.raw, corpus.view) if view else (corpus.raw,):
        path = root / source / folder / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        manifest = DownloadManifest.from_file(
            source_id=source,
            dataset_id=folder or source,
            url=f"https://example.invalid/{source}/{folder}/{name}",
            file_path=path,
        )
        write_manifest_sidecar(path, manifest)
        paths.append(path)
    return paths[-1]


def convert(
    corpus: Corpus,
    raw_file: Path,
    source: str,
    dataset: str,
    partitions: dict[str, str] | None = None,
) -> Path:
    """Convert any raw file into ``lake/<source>/<dataset>``; returns the Parquet."""
    partitions = partitions or {}
    writer = LakeWriter(
        corpus.dataset_dir(source, dataset),
        DatasetDescriptor(
            source_id=source,
            dataset_id=dataset,
            alias=dataset,
            partition_keys=list(partitions),
            partition_types={k: "varchar" for k in partitions},
        ),
    )
    result = writer.write_polars(
        pl.DataFrame({"valor": [1, 2, 3]}), raw_file, partitions=partitions
    )
    return result.output


class FakeConverter:
    """A converter with ``inventory()``: ``plan`` maps file name -> dataset or reason.

    A value in ``SKIP_REASONS`` ("gemeo", "nao_tabular", "superado") marks the
    file as ignored; any other value is the id of the dataset it feeds.
    """

    def __init__(self, source_id: str, plan: dict[str, str]) -> None:
        self.source_id = source_id
        self.plan = plan

    def datasets(self) -> list[str]:
        return sorted({v for v in self.plan.values() if v not in SKIP_REASONS})

    def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
        raise NotImplementedError

    def inventory(self, input_dir: Path) -> list[Planned]:
        out = []
        for path in sorted(Path(input_dir).rglob("*")):
            value = self.plan.get(path.name)
            if value is None:
                continue
            if value in SKIP_REASONS:
                out.append(Planned(path, skip_reason=value))
            else:
                out.append(Planned(path, dataset=value))
        return out
