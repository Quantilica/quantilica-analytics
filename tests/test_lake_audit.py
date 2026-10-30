"""lake_audit: a clean corpus passes, and each planted defect is found."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from lake_corpus import (
    FakeConverter,
    build_corpus,
    plant_directory_link,
    plant_duplicate_member,
    plant_duplicate_version,
    plant_missing_parquet,
    plant_mixed_depth,
    plant_sha_mismatch,
    plant_stale_origin,
    plant_tmp_leftover,
    plant_truncated_parquet,
)

from quantilica.analytics import AuditReport, lake_audit
from quantilica.analytics.lake_audit import read_ledger

NO_CONVERTERS: dict = {}


@pytest.fixture
def corpus(tmp_path: Path):
    return build_corpus(tmp_path)


def audit(corpus, **kwargs) -> AuditReport:
    kwargs.setdefault("input_dir", corpus.view)
    kwargs.setdefault("converters", NO_CONVERTERS)
    return lake_audit(corpus.lake, **kwargs)


def categories(report: AuditReport) -> dict[str, int]:
    return report.by_category()


def paths(report: AuditReport, category: str) -> list[str]:
    return [f.path for f in report.findings if f.category == category]


# --- clean lake --------------------------------------------------------------------


def test_clean_lake_is_ok(corpus) -> None:
    report = audit(corpus)
    assert report.ok, report.to_text()
    assert report.counts["parquets"] == 8
    assert report.counts["datasets"] == 5
    assert report.counts["members"] == 4  # the four uf files of gama
    assert report.counts["expected_files"] == 13
    assert report.ignored == {"gemeo": 1, "nao_tabular": 2}
    assert report.datasets["gama/grupos"]["members"] == 4
    assert report.checks["leitores"] == "ok"


def test_without_input_skips_provenance_and_coverage(corpus) -> None:
    report = lake_audit(corpus.lake)
    assert report.ok
    assert "não executada" in report.checks["proveniencia"]
    assert "não executada" in report.checks["cobertura"]


def test_missing_dirs_raise(tmp_path: Path) -> None:
    with pytest.raises(NotADirectoryError):
        lake_audit(tmp_path / "nada")
    (tmp_path / "lake").mkdir()
    with pytest.raises(NotADirectoryError):
        lake_audit(tmp_path / "lake", input_dir=tmp_path / "nada")


def test_sources_filter(corpus) -> None:
    plant_truncated_parquet(corpus)  # in alfa
    assert audit(corpus, sources=["beta", "gama"], readers=False).ok
    assert not audit(corpus, sources=["alfa"], readers=False).ok


# --- footers and integrity ----------------------------------------------------------


def test_truncated_parquet(corpus) -> None:
    path = plant_truncated_parquet(corpus)
    report = audit(corpus, readers=False)
    assert not report.ok
    assert paths(report, "parquet_corrompido") == ["alfa/vendas/ano=2026/" + path.name]
    # Its origin is lost with the footer, so the file (and its twin) is uncovered.
    assert paths(report, "sem_parquet") == [
        "alfa/vendas/vendas_2026@20261021.csv",
        "alfa/vendas/vendas_2026@20261021.zip",
    ]


def test_unreadable_row_group_is_corrupt(corpus) -> None:
    path = corpus.parquet_for("precos_202501@20261021.csv")
    data = bytearray(path.read_bytes())
    for i in range(8, 40):  # smash the start of the first (only) row group
        data[i] ^= 0xFF
    path.write_bytes(bytes(data))
    assert "parquet_corrompido" in categories(audit(corpus, readers=False))
    # With integrity off only the footer is read, and the footer is fine.
    assert audit(corpus, integrity=False, readers=False).ok


def test_empty_parquet_is_legitimate(tmp_path: Path) -> None:
    import polars as pl
    from quantilica.core.lake import DatasetDescriptor

    from quantilica.analytics import LakeWriter

    raw = tmp_path / "raw" / "s" / "d" / "vazio@20261021.csv"
    raw.parent.mkdir(parents=True)
    raw.write_text("valor\n")
    writer = LakeWriter(
        tmp_path / "lake" / "s" / "d",
        DatasetDescriptor(source_id="s", dataset_id="d", alias="d"),
    )
    writer.write_polars(pl.DataFrame({"valor": []}, schema={"valor": pl.Int64}), raw)
    report = lake_audit(tmp_path / "lake", input_dir=tmp_path / "raw", converters={})
    assert report.ok, report.to_text()
    assert report.counts["rows"] == 0


def test_members_must_add_up(corpus) -> None:
    import pyarrow.parquet as pq

    path = corpus.parquet_for("g_202001-sp@20260301.csv")
    table = pq.read_table(path)
    meta = dict(table.schema.metadata)
    members = json.loads(meta[b"quantilica.members"])
    members["g_202001-sp"]["rows"] = 99
    meta[b"quantilica.members"] = json.dumps(members).encode()
    pq.write_table(table.replace_schema_metadata(meta), path)
    report = audit(corpus, readers=False)
    assert paths(report, "membros_divergentes") == [
        "gama/grupos/ano=2020/mes=01/g_202001.parquet"
    ]


# --- provenance ---------------------------------------------------------------------


def test_sha_mismatch_against_manifest(corpus) -> None:
    plant_sha_mismatch(corpus)
    report = audit(corpus, readers=False)
    assert categories(report) == {"sha256_divergente": 1}
    assert paths(report, "sha256_divergente") == [
        "alfa/vendas/ano=2026/vendas_2026.parquet"
    ]


def test_sha_mismatch_against_file_hash(corpus) -> None:
    plant_sha_mismatch(corpus, how="file")
    assert categories(audit(corpus, readers=False)) == {"sha256_divergente": 1}


def test_manifest_digest_is_trusted_unless_rehash(corpus) -> None:
    # Change the file but keep its sidecar: only a rehash notices.
    corpus.view_file("vendas_2026@20261021.csv").write_text("outra coisa\n")
    assert audit(corpus, readers=False).ok
    report = audit(corpus, readers=False, rehash=True)
    assert categories(report) == {"sha256_divergente": 1}


def test_origin_without_raw_file(corpus) -> None:
    corpus.view_file("precos_202501@20261021.csv").unlink()
    report = audit(corpus, readers=False)
    assert paths(report, "origem_sem_bruto") == [
        "beta/precos/ano=2025/mes=01/precos_202501.parquet"
    ]


def test_origin_superseded(corpus) -> None:
    plant_stale_origin(corpus)
    report = audit(corpus, readers=False)
    assert paths(report, "origem_superada") == [
        "beta/precos/ano=2025/mes=02/precos_202502.parquet"
    ]
    # The current version has no Parquet of its own either.
    assert paths(report, "sem_parquet") == ["beta/precos/precos_202502@20261021.csv"]


def test_origin_without_provenance(tmp_path: Path) -> None:
    import polars as pl
    from quantilica.core.lake import DatasetDescriptor

    from quantilica.analytics import LakeWriter

    writer = LakeWriter(
        tmp_path / "lake" / "s" / "d",
        DatasetDescriptor(source_id="s", dataset_id="d", alias="d"),
    )
    writer.write_polars(pl.DataFrame({"a": [1]}), None, file_key_name="derivado")
    report = lake_audit(tmp_path / "lake", converters={})
    assert categories(report) == {"sem_proveniencia": 1}


# --- ledger -------------------------------------------------------------------------


def write_ledger(corpus, path: Path, *, fmt: str = "jsonl", tamper=()) -> Path:
    from quantilica.core.files import sha256_file

    rows = []
    for f in sorted(corpus.view.rglob("*")):
        if f.is_file() and not f.name.endswith(".manifest.json"):
            sha = "f" * 64 if f.name in tamper else sha256_file(f)
            rows.append((f.relative_to(corpus.view).as_posix(), sha))
    if fmt == "jsonl":
        path.write_text(
            "\n".join(json.dumps({"path": p, "sha256": s}) for p, s in rows) + "\n"
        )
    else:
        path.write_text("path,sha256\n" + "\n".join(f"{p},{s}" for p, s in rows) + "\n")
    return path


@pytest.mark.parametrize("fmt", ["jsonl", "csv"])
def test_ledger_ok_and_mismatch(corpus, tmp_path: Path, fmt: str) -> None:
    ledger = write_ledger(corpus, tmp_path / f"ledger.{fmt}", fmt=fmt)
    assert audit(corpus, ledger=ledger, readers=False).ok
    bad = write_ledger(
        corpus, tmp_path / f"bad.{fmt}", fmt=fmt, tamper={"vendas_2026@20261021.csv"}
    )
    report = audit(corpus, ledger=bad, readers=False)
    assert categories(report) == {"sha256_divergente": 1}


def test_ledger_missing_origin(corpus, tmp_path: Path) -> None:
    ledger = write_ledger(corpus, tmp_path / "ledger.jsonl")
    lines = [
        line for line in ledger.read_text().splitlines() if "precos_202501" not in line
    ]
    ledger.write_text("\n".join(lines) + "\n")
    report = audit(corpus, ledger=ledger, readers=False)
    assert categories(report) == {"origem_fora_do_ledger": 1}


def test_ledger_alone_covers_provenance_and_coverage(corpus, tmp_path: Path) -> None:
    ledger = write_ledger(corpus, tmp_path / "ledger.jsonl")
    report = lake_audit(corpus.lake, ledger=ledger, readers=False, converters={})
    assert report.ok, report.to_text()
    assert report.counts["expected_files"] == 13
    plant_missing_parquet(corpus)
    report = lake_audit(corpus.lake, ledger=ledger, readers=False, converters={})
    assert paths(report, "sem_parquet") == ["beta/precos/precos_202501@20261021.csv"]


def test_ledger_rejects_bad_records(tmp_path: Path) -> None:
    bad = tmp_path / "l.jsonl"
    bad.write_text('{"path": "a/b"}\n')
    with pytest.raises(ValueError, match="sha256"):
        read_ledger(bad)
    bad.write_text("not json\n")
    with pytest.raises(ValueError, match="invalid JSON"):
        read_ledger(bad)


# --- duplicates ---------------------------------------------------------------------


def test_duplicate_version(corpus) -> None:
    plant_duplicate_version(corpus)
    report = audit(corpus, readers=False)
    # The old version is a duplicate, and also not the current one.
    assert categories(report) == {"origem_superada": 1, "versao_duplicada": 1}
    (finding,) = [f for f in report.findings if f.category == "versao_duplicada"]
    assert finding.dataset == "vendas"
    assert "vendas_2025" in finding.message


def test_duplicate_member(corpus) -> None:
    plant_duplicate_member(corpus)
    assert "versao_duplicada" in categories(audit(corpus, readers=False))


# --- coverage -----------------------------------------------------------------------


def test_file_without_parquet(corpus) -> None:
    plant_missing_parquet(corpus)
    report = audit(corpus, readers=False)
    assert categories(report) == {"sem_parquet": 1}
    assert paths(report, "sem_parquet") == ["beta/precos/precos_202501@20261021.csv"]


def test_twin_and_non_tabular_are_not_accused(corpus) -> None:
    # The corpus has a .zip twin without Parquet and two non-tabular files.
    report = audit(corpus, readers=False)
    assert report.ok
    assert report.ignored["gemeo"] == 1
    assert report.ignored["nao_tabular"] == 2


def test_twin_is_accused_when_neither_has_parquet(corpus) -> None:
    corpus.parquet_for("vendas_2026@20261021.csv").unlink()
    report = audit(corpus, readers=False)
    assert paths(report, "sem_parquet") == [
        "alfa/vendas/vendas_2026@20261021.csv",
        "alfa/vendas/vendas_2026@20261021.zip",
    ]


def test_known_exception_is_not_accused(corpus) -> None:
    plant_missing_parquet(corpus)
    for exceptions in (
        ["beta/precos/precos_202501@20261021.csv"],
        ["beta/precos/precos_202501*"],
    ):
        report = audit(corpus, readers=False, known_exceptions=exceptions)
        assert report.ok
        assert report.ignored["excecao_conhecida"] == 1


def test_superseded_versions_in_raw_are_not_expected(corpus) -> None:
    # Pointing at the full raw tree: only the current versions count.
    report = audit(corpus, input_dir=corpus.raw, readers=False)
    assert report.ok, report.to_text()
    assert report.counts["expected_files"] == 13


def test_inventory_prevails_over_heuristic(corpus) -> None:
    # The heuristic ignores the pdf and would accuse the missing csv; the
    # converter's inventory says the opposite on both counts.
    corpus.parquet_for("precos_202501@20261021.csv").unlink()
    converter = FakeConverter(
        "beta",
        {
            "precos_202501@20261021.csv": "nao_tabular",
            "precos_202502@20261021.csv": "precos",
            "leia-me@20261021.txt": "precos",
        },
    )
    report = audit(corpus, readers=False, converters={"beta": converter})
    assert paths(report, "sem_parquet") == ["beta/leia-me@20261021.txt"]
    # alfa's pdf (heuristic) and beta's csv (inventory); the txt is now expected
    assert report.ignored["nao_tabular"] == 2


def test_inventory_skip_reasons_are_counted(corpus) -> None:
    converter = FakeConverter(
        "alfa",
        {
            "vendas_2026@20261021.zip": "gemeo",
            "manual@20261021.pdf": "nao_tabular",
            "vendas_2025@20261021.csv": "vendas",
            "vendas_2026@20261021.csv": "vendas",
        },
    )
    report = audit(corpus, readers=False, converters={"alfa": converter})
    assert report.ok
    assert report.ignored["gemeo"] == 1


def test_converter_without_inventory_uses_heuristic(corpus) -> None:
    class Plain:
        source_id = "beta"

    report = audit(corpus, readers=False, converters={"beta": Plain()})
    assert report.ok


def test_failing_inventory_falls_back_with_a_note(corpus) -> None:
    class Broken:
        source_id = "beta"

        def inventory(self, input_dir):
            raise RuntimeError("boom")

    report = audit(corpus, readers=False, converters={"beta": Broken()})
    assert report.ok
    assert any("boom" in n for n in report.notes)


# --- structure ----------------------------------------------------------------------


def test_mixed_partition_depth(corpus) -> None:
    moved = plant_mixed_depth(corpus)
    report = audit(corpus, readers=False)
    assert paths(report, "particao_mista") == ["beta/precos/ano=2025/" + moved.name]
    (finding,) = [f for f in report.findings if f.category == "particao_mista"]
    assert (finding.source, finding.dataset) == ("beta", "precos")
    assert "ano=/mes=: 1" in finding.message


def test_tmp_leftovers(corpus) -> None:
    plant_tmp_leftover(corpus)
    (corpus.lake / "alfa" / "vendas" / "ano=2025" / "x.parquet.tmp").write_bytes(b"")
    report = audit(corpus, readers=False)
    assert paths(report, "tmp_sobrando") == [
        "alfa/vendas/ano=2025/x.parquet.tmp",
        "beta/precos/.tmp",
    ]
    # The leftover Parquet inside .tmp is not counted as data.
    assert report.counts["parquets"] == 8


def test_empty_tmp_dir_is_not_a_leftover(corpus) -> None:
    (corpus.lake / "beta" / "precos" / ".tmp").mkdir()
    assert audit(corpus, readers=False).ok


def test_parquet_outside_layout(corpus) -> None:
    (corpus.lake / "alfa" / "solto.parquet").write_bytes(b"PAR1")
    report = audit(corpus, readers=False)
    assert paths(report, "fora_do_layout") == ["alfa/solto.parquet"]


# --- directory links ----------------------------------------------------------------


def test_link_into_the_lake_is_counted_once(corpus) -> None:
    link = plant_directory_link(corpus)
    report = audit(corpus)
    assert report.ok, report.to_text()
    assert report.counts["parquets"] == 8
    assert report.counts["datasets"] == 5
    assert [(x["path"], x["action"]) for x in report.links] == [
        (link.name, "ignorado (aponta para dentro do lake)")
    ]


def test_link_outside_is_followed_once(corpus) -> None:
    link = plant_directory_link(corpus, outside=True)
    # A second link to the same destination, and one to a part of it.
    (corpus.lake / "delta2").symlink_to(link.resolve(), target_is_directory=True)
    (corpus.lake / "delta3").symlink_to(
        link.resolve() / "cad-a", target_is_directory=True
    )
    report = audit(corpus)
    assert report.counts["parquets"] == 8
    actions = {x["path"]: x["action"] for x in report.links}
    assert actions["delta"] == "seguido"
    assert actions["delta2"].startswith("ignorado")
    assert actions["delta3"].startswith("ignorado")
    assert report.ok, report.to_text()


def test_link_cycle_is_not_followed(corpus) -> None:
    (corpus.lake / "alfa" / "volta").symlink_to(corpus.lake, target_is_directory=True)
    report = audit(corpus, readers=False)
    assert report.counts["parquets"] == 8


# --- readers ------------------------------------------------------------------------


def test_readers_report_check_dataset_errors(corpus) -> None:
    (corpus.dataset_dir("alfa", "vendas") / "_dataset.json").unlink()
    report = audit(corpus)
    assert [f.dataset for f in report.findings if f.category == "leitores"] == [
        "vendas"
    ]
    assert audit(corpus, readers=False).ok


# --- report -------------------------------------------------------------------------


def test_json_and_text(corpus) -> None:
    plant_missing_parquet(corpus)
    plant_sha_mismatch(corpus)
    report = audit(corpus, readers=False)
    data = json.loads(report.to_json())
    assert data == report.to_dict()
    assert data["ok"] is False
    assert data["findings_by_category"] == {"sha256_divergente": 1, "sem_parquet": 1}
    assert set(data) == {
        "ok",
        "lake_dir",
        "input_dir",
        "ledger",
        "started_at",
        "seconds",
        "checks",
        "counts",
        "ignored",
        "findings_by_category",
        "datasets",
        "findings",
        "links",
        "notes",
    }
    assert set(data["findings"][0]) == {
        "category",
        "source",
        "dataset",
        "path",
        "message",
    }
    text = report.to_text()
    assert "REPROVADO (2 achados)" in text
    assert "sha256_divergente: 1" in text
    assert "beta/precos/precos_202501@20261021.csv" in text
    assert "leitores" in text  # the skipped check is mentioned


def test_clean_json_and_text(tmp_path: Path) -> None:
    corpus = build_corpus(tmp_path)
    report = audit(corpus)
    data = json.loads(report.to_json())
    assert data["ok"] is True and data["findings"] == []
    assert "APROVADO" in report.to_text()


def test_text_truncates_long_lists(corpus) -> None:
    for i in range(15):
        (corpus.lake / "alfa" / "vendas" / f"r{i}.parquet.tmp").write_bytes(b"")
    text = audit(corpus, readers=False).to_text(max_per_category=3)
    assert "... e mais 12" in text


def test_progress_callback(corpus) -> None:
    seen = []
    audit(corpus, progress=lambda stage, done, total: seen.append(stage))
    assert {"rodapés", "sha256", "leitores"} <= set(seen)
