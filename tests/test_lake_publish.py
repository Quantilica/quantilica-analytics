"""Publication markers: update marker, build manifest, read-only audit, state."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from lake_corpus import Corpus, build_corpus, plant_truncated_parquet
from lake_publish_fakes import ParquetConverter

from quantilica.analytics import lake_audit
from quantilica.analytics import lake_build as lb
from quantilica.analytics.lake_audit import list_source_parquets
from quantilica.analytics.lake_build import lake_build
from quantilica.analytics.lake_publish import (
    BUILD_MANIFEST_NAME,
    MARKER_NAME,
    LakePublishError,
    read_build_manifest,
    read_state,
    write_build_manifest,
    write_marker,
)

# --- build ---------------------------------------------------------------------------


def _raw(tmp_path: Path, *folders: str) -> None:
    for folder in folders:
        (tmp_path / "input" / folder).mkdir(parents=True, exist_ok=True)
        (tmp_path / "input" / folder / "file.csv").write_text("a,b\n1,2\n")


def _build(tmp_path: Path, converters: dict, **kwargs):
    kwargs.setdefault("workers", 2)
    kwargs.setdefault("prevent_sleep", False)
    return lake_build(
        tmp_path / "input", tmp_path / "lake", converters=converters, **kwargs
    )


def _actual_sizes(source_dir: Path) -> dict[str, int]:
    return {
        p.relative_to(source_dir).as_posix(): p.stat().st_size
        for p in source_dir.rglob("*.parquet")
    }


def test_build_writes_the_marker_first_and_the_manifest_last(tmp_path):
    _raw(tmp_path, "src")
    control = tmp_path / "control"
    conv = ParquetConverter("src", ["aa", "bbbb"], control)

    summary = _build(tmp_path, {"src": conv})

    assert not summary.has_errors
    # The marker already existed when each dataset began to be written.
    assert (control / "marker-aa").read_text() == "True"
    assert (control / "marker-bbbb").read_text() == "True"
    source = tmp_path / "lake" / "src"
    marker = json.loads((source / MARKER_NAME).read_text())
    assert marker["build_id"] == summary.run_id
    assert marker["host"] and marker["iniciado_em"]
    manifest = json.loads((source / BUILD_MANIFEST_NAME).read_text())
    assert manifest["build_id"] == summary.run_id
    assert manifest["concluido_em"] and manifest["versao_analytics"]
    assert manifest["parquets"] == _actual_sizes(source)
    assert set(manifest["parquets"]) == {
        "aa/ano=2026/p.parquet",
        "bbbb/ano=2026/p.parquet",
    }
    # No temporary file of the atomic write is left behind.
    assert not [p for p in source.rglob("*.tmp")]


def test_failed_source_keeps_the_marker_and_writes_no_manifest(tmp_path):
    _raw(tmp_path, "src", "ok")
    control = tmp_path / "control"
    bad = ParquetConverter("src", ["good", "boom"], control, {"boom": "raise"})
    good = ParquetConverter("ok", ["fine"], control)

    summary = _build(tmp_path, {"src": bad, "ok": good})

    assert summary.has_errors
    lake = tmp_path / "lake"
    assert (lake / "src" / MARKER_NAME).exists()
    assert not (lake / "src" / BUILD_MANIFEST_NAME).exists()
    # The other source finished and has its manifest.
    assert (lake / "ok" / BUILD_MANIFEST_NAME).exists()
    assert (lake / "ok" / MARKER_NAME).exists()  # only the audit removes it


def test_a_failed_rebuild_leaves_the_old_manifest_and_a_new_marker(tmp_path):
    _raw(tmp_path, "src")
    control = tmp_path / "control"
    first = _build(tmp_path, {"src": ParquetConverter("src", ["d"], control)})
    broken = ParquetConverter("src", ["d"], control, {"d": "raise"})

    second = _build(tmp_path, {"src": broken}, force=True)

    assert second.has_errors and second.run_id != first.run_id
    source = tmp_path / "lake" / "src"
    assert read_build_manifest(source)["build_id"] == first.run_id
    assert json.loads((source / MARKER_NAME).read_text())["build_id"] == second.run_id


def test_manifest_lists_every_parquet_after_a_partial_build(tmp_path):
    _raw(tmp_path, "src")
    control = tmp_path / "control"
    conv = ParquetConverter("src", ["aa", "bbbb"], control)
    _build(tmp_path, {"src": conv})
    source = tmp_path / "lake" / "src"
    before = dict(read_build_manifest(source)["parquets"])

    os.environ["LAKE_FAKE_ROWS"] = "7"  # the rewritten dataset changes size
    try:
        summary = _build(tmp_path, {"src": conv}, force=True, datasets=["aa"])
    finally:
        del os.environ["LAKE_FAKE_ROWS"]

    manifest = read_build_manifest(source)
    assert manifest["build_id"] == summary.run_id
    assert set(manifest["parquets"]) == set(before)  # bbbb too, not rewritten
    assert manifest["parquets"] == _actual_sizes(source)
    assert (
        manifest["parquets"]["aa/ano=2026/p.parquet"] != before["aa/ano=2026/p.parquet"]
    )
    assert (
        manifest["parquets"]["bbbb/ano=2026/p.parquet"]
        == before["bbbb/ano=2026/p.parquet"]
    )


def test_interrupted_build_keeps_the_marker_and_writes_no_manifest(
    tmp_path, monkeypatch
):
    _raw(tmp_path, "src")
    conv = ParquetConverter("src", ["d"], tmp_path / "control")

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(lb, "_run", interrupted)
    with pytest.raises(KeyboardInterrupt):
        _build(tmp_path, {"src": conv})

    source = tmp_path / "lake" / "src"
    assert (source / MARKER_NAME).exists()
    assert not (source / BUILD_MANIFEST_NAME).exists()


def test_marker_and_manifest_are_not_datasets_nor_findings(tmp_path):
    _raw(tmp_path, "src")
    _build(tmp_path, {"src": ParquetConverter("src", ["d"], tmp_path / "control")})
    # The fake writes no descriptor: give the dataset one so the audit can run.
    from quantilica.core.lake import DatasetDescriptor

    (tmp_path / "lake" / "src" / "d" / "_dataset.json").write_text(
        DatasetDescriptor(
            source_id="src", dataset_id="d", alias="d", partition_keys=["ano"]
        ).to_json()
    )

    report = lake_audit(tmp_path / "lake", readers=False)

    assert report.datasets.keys() == {"src/d"}
    assert not [f for f in report.findings if f.category == "tmp_sobrando"]
    assert not [f for f in report.findings if "_build" in (f.path or "")]
    assert not [f for f in report.findings if "_em-atualizacao" in (f.path or "")]


# --- the audit ---------------------------------------------------------------------


@pytest.fixture
def lake(tmp_path: Path) -> Corpus:
    corpus = build_corpus(tmp_path)
    for source in sorted(p.name for p in corpus.lake.iterdir() if p.is_dir()):
        write_build_manifest(
            corpus.lake / source, "B1", list_source_parquets(corpus.lake, source)
        )
    return corpus


def _tree(root: Path) -> dict[str, tuple[int, int]]:
    out = {}
    for dirpath, dirs, names in os.walk(root):
        for name in dirs + names:
            p = Path(dirpath) / name
            st = p.stat()
            out[p.relative_to(root).as_posix()] = (
                st.st_size if p.is_file() else -1,
                st.st_mtime_ns,
            )
    return out


def _audit(corpus: Corpus, tmp_path: Path, **kwargs):
    kwargs.setdefault("input_dir", corpus.view)
    kwargs.setdefault("converters", {})
    kwargs.setdefault("read_only", True)
    kwargs.setdefault("state", tmp_path / "estado" / "lake.json")
    return lake_audit(corpus.lake, **kwargs)


def _result(report, source: str) -> str:
    return report.publicacao[source]["resultado"]


def test_read_only_audit_writes_nothing_in_the_lake(lake, tmp_path, monkeypatch):
    # The corpus is complete, with a source being updated and one scratch-prone
    # reader pass; the process even sits inside the lake.
    write_marker(lake.lake / "gama", "B2")
    before = _tree(lake.lake)
    monkeypatch.chdir(lake.lake)

    report = _audit(lake, tmp_path)

    assert report.ok, report.to_text()
    assert _tree(lake.lake) == before  # same names, sizes and mtimes
    assert report.markers_cleared == []
    assert (tmp_path / "estado" / "lake.json").exists()  # the state, outside


def test_clean_lake_is_published_and_state_records_it(lake, tmp_path):
    report = _audit(lake, tmp_path)

    assert report.ok, report.to_text()
    assert report.checks["manifesto"] == "ok"
    assert {_result(report, s) for s in report.publicacao} == {"publicado"}
    state = read_state(tmp_path / "estado" / "lake.json")
    assert state.keys() == report.publicacao.keys()
    for entry in state.values():
        assert entry["build_id"] == "B1"
        assert entry["resultado"] == "publicado"
        assert entry["auditado_em"]


def test_missing_file_is_incomplete(lake, tmp_path):
    victim = lake.parquet_for("vendas_2026@20261021.csv")
    source = victim.relative_to(lake.lake).parts[0]
    victim.unlink()

    # Without the raw input, so the missing Parquet is not also a coverage gap.
    report = _audit(lake, tmp_path, input_dir=None)

    assert not report.ok
    assert _result(report, source) == "incompleto"
    found = [f for f in report.findings if f.category == "manifesto_divergente"]
    assert [f.path for f in found] == [victim.relative_to(lake.lake).as_posix()]
    assert "ausente" in found[0].message
    assert read_state(tmp_path / "estado" / "lake.json")[source]["resultado"] == (
        "incompleto"
    )


def test_different_size_is_incomplete(lake, tmp_path):
    victim = lake.parquet_for("vendas_2026@20261021.csv")
    source = victim.relative_to(lake.lake).parts[0]
    # A Parquet that arrived only in part would not even open: keep it valid by
    # changing the manifest instead of the file.
    manifest = read_build_manifest(lake.lake / source)
    rel = victim.relative_to(lake.lake / source).as_posix()
    manifest["parquets"][rel] += 1
    (lake.lake / source / BUILD_MANIFEST_NAME).write_text(json.dumps(manifest))

    report = _audit(lake, tmp_path)

    assert _result(report, source) == "incompleto"
    (finding,) = [f for f in report.findings if f.category == "manifesto_divergente"]
    assert "tamanho" in finding.message


def test_extra_parquet_is_incomplete(lake, tmp_path):
    victim = lake.parquet_for("vendas_2026@20261021.csv")
    source = victim.relative_to(lake.lake).parts[0]
    extra = victim.with_name("outro.parquet")
    table = pq.read_table(victim)
    meta = dict(pq.ParquetFile(victim).metadata.metadata or {})
    meta[b"quantilica.origin_file"] = b"outra-origem.csv"  # not a duplicate
    pq.write_table(table.replace_schema_metadata(meta), extra)

    report = _audit(lake, tmp_path, input_dir=None, readers=False)

    assert _result(report, source) == "incompleto"
    assert [
        f.path for f in report.findings if f.category == "parquet_fora_do_manifesto"
    ] == [extra.relative_to(lake.lake).as_posix()]


def test_other_findings_make_the_source_failed(lake, tmp_path):
    victim = plant_truncated_parquet(lake)
    source = victim.relative_to(lake.lake).parts[0]

    report = _audit(lake, tmp_path)

    # The truncated file also differs in size from the manifest.
    assert _result(report, source) == "falhou"
    assert all(
        _result(report, s) == "publicado" for s in report.publicacao if s != source
    )


def test_marker_means_updating_not_failure(lake, tmp_path):
    victim = lake.parquet_for("vendas_2026@20261021.csv")
    source = victim.relative_to(lake.lake).parts[0]
    write_marker(lake.lake / source, "B2")
    victim.write_bytes(b"half a Parquet, the sync is still running")

    report = _audit(lake, tmp_path)

    assert report.ok, report.to_text()
    assert _result(report, source) == "em_atualizacao"
    assert report.publicacao[source]["build_id"] == "B2"
    assert read_state(tmp_path / "estado" / "lake.json")[source]["resultado"] == (
        "em_atualizacao"
    )
    assert any(source in n and "em atualização" in n for n in report.notes)
    assert all(
        _result(report, s) == "publicado" for s in report.publicacao if s != source
    )


def test_read_only_without_manifest_is_incomplete(lake, tmp_path):
    source = "gama"
    (lake.lake / source / BUILD_MANIFEST_NAME).unlink()

    report = _audit(lake, tmp_path)

    assert _result(report, source) == "incompleto"
    assert not report.ok
    # Not read-only (a lake from before the manifest): nothing to compare.
    again = lake_audit(lake.lake, input_dir=lake.view, converters={})
    assert again.ok and again.publicacao[source]["resultado"] == "publicado"
    assert again.publicacao[source]["build_id"] is None


def test_clear_marker_removes_it_only_when_the_source_is_published(lake, tmp_path):
    write_marker(lake.lake / "gama", "B1")  # the build of its manifest
    write_marker(lake.lake / "alfa", "B9")  # a build that did not finish
    state = tmp_path / "estado.json"

    report = lake_audit(
        lake.lake,
        input_dir=lake.view,
        converters={},
        clear_marker=True,
        state=state,
    )

    assert report.ok, report.to_text()
    assert report.markers_cleared == ["gama"]
    assert not (lake.lake / "gama" / MARKER_NAME).exists()
    assert (lake.lake / "alfa" / MARKER_NAME).exists()
    assert _result(report, "gama") == "publicado"
    assert _result(report, "alfa") == "em_atualizacao"
    assert any("alfa" in n and "B9" in n for n in report.notes)
    # Without the flag the audit writes nothing in the lake.
    write_marker(lake.lake / "gama", "B1")
    before = _tree(lake.lake)
    lake_audit(lake.lake, input_dir=lake.view, converters={})
    assert _tree(lake.lake) == before


def test_clear_marker_keeps_the_marker_of_a_failed_source(lake, tmp_path):
    victim = lake.parquet_for("vendas_2026@20261021.csv")
    source = victim.relative_to(lake.lake).parts[0]
    write_marker(lake.lake / source, "B1")
    victim.unlink()

    report = lake_audit(lake.lake, converters={}, clear_marker=True)

    assert _result(report, source) == "incompleto"
    assert (lake.lake / source / MARKER_NAME).exists()
    assert report.markers_cleared == []


def test_read_only_and_clear_marker_are_incompatible(lake):
    with pytest.raises(ValueError, match="read_only"):
        lake_audit(lake.lake, read_only=True, clear_marker=True)


def test_state_file_inside_the_lake_is_refused(lake):
    with pytest.raises(ValueError, match="outside the lake"):
        lake_audit(lake.lake, state=lake.lake / "_estado.json")
    assert not (lake.lake / "_estado.json").exists()


def test_state_merges_with_the_sources_audited_before(lake, tmp_path):
    state = tmp_path / "estado.json"
    lake_audit(lake.lake, sources=["alfa"], state=state, converters={})
    first = read_state(state)["alfa"]

    lake_audit(lake.lake, sources=["gama"], state=state, converters={})

    merged = read_state(state)
    assert set(merged) == {"alfa", "gama"}
    assert merged["alfa"] == first


def test_source_with_only_a_marker_is_updating(lake, tmp_path):
    # A build began on a new source: nothing but the marker exists yet.
    (lake.lake / "nova").mkdir()
    write_marker(lake.lake / "nova", "B3")

    report = _audit(lake, tmp_path)

    assert report.ok, report.to_text()
    assert _result(report, "nova") == "em_atualizacao"


def test_unreadable_state_file_is_an_error(tmp_path):
    bad = tmp_path / "estado.json"
    bad.write_text("[1, 2]")
    with pytest.raises(LakePublishError):
        read_state(bad)
    bad.write_text("not json")
    with pytest.raises(LakePublishError):
        read_state(bad)
