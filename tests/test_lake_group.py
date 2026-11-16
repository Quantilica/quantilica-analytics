"""``LakeWriter.write_group``: a group of members written once, in streaming."""

from __future__ import annotations

import errno
import json
import os
import zipfile
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from lake_corpus import add_raw, build_corpus
from quantilica.core.lake import DatasetDescriptor, composite_file_key

from quantilica.analytics import lake_audit
from quantilica.analytics.lake import (
    META_MEMBER_COLUMNS,
    META_MEMBERS,
    ORIGIN_COLUMN,
    DuckSource,
    GroupMember,
    LakeError,
    LakeWriter,
    check_dataset,
    scan_dataset,
)

PARTS = {"ano": "2024"}


def _raw(tmp_path: Path, name: str, content: str | None = None) -> Path:
    path = tmp_path / "raw" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content if content is not None else name)
    return path


def _writer(lake: Path, keys: list[str] | None = None, **kwargs) -> LakeWriter:
    keys = ["ano"] if keys is None else keys
    return LakeWriter(
        lake / "src" / "ds",
        DatasetDescriptor(
            source_id="src",
            dataset_id="ds",
            alias="ds",
            partition_keys=keys,
            partition_types={k: "varchar" for k in keys},
        ),
        **kwargs,
    )


def _dataset(lake: Path) -> Path:
    return lake / "src" / "ds"


def _footer(path: Path) -> dict[str, str]:
    raw = pq.ParquetFile(path).metadata.metadata
    return {k.decode(): v.decode() for k, v in raw.items()}


def _entries(path: Path) -> dict:
    return json.loads(_footer(path)[META_MEMBERS])


def _members(tmp_path: Path, n: int, *, rows: int = 3, calls: list | None = None):
    """``n`` members ``m00..``: a DataFrame each (or a function that records)."""
    out = []
    for i in range(n):
        name = f"m{i:02d}@20260101.csv"
        src = _raw(tmp_path, name)
        frame = pl.DataFrame({"valor": [i * 100 + r for r in range(rows)]})

        def make(frame=frame, name=name):
            if calls is not None:
                calls.append(name)
            return frame

        out.append(
            GroupMember(
                src, make, columns={"uf": f"u{i % 3}", "parte": f"{(i % 12) + 1:02d}"}
            )
        )
    return out


def test_write_group_escreve_o_grupo_numa_passada(tmp_path: Path, monkeypatch) -> None:
    writer = _writer(tmp_path / "lake")
    publishes = []
    original = LakeWriter._publish

    def spy(self, *args, **kwargs):
        publishes.append(args[1])
        return original(self, *args, **kwargs)

    monkeypatch.setattr(LakeWriter, "_publish", spy)
    res = writer.write_group("ds_2024", _members(tmp_path, 10), partitions=PARTS)

    assert res.status == "converted"
    assert res.rows == 30
    assert len(res.written) == 10 and res.kept == [] and res.removed == []
    assert publishes == ["ds_2024"]  # uma publicação, não dez
    files = list(_dataset(tmp_path / "lake").rglob("*.parquet"))
    assert [f.name for f in files] == ["ds_2024.parquet"]
    df = scan_dataset(_dataset(tmp_path / "lake")).collect()
    assert df.height == 30
    assert set(df[ORIGIN_COLUMN]) == {f"m{i:02d}" for i in range(10)}
    assert df.schema["parte"] == pl.String
    assert df.schema["uf"] == pl.String
    footer = _footer(files[0])
    assert json.loads(footer[META_MEMBER_COLUMNS]) == ["uf", "parte"]
    entries = _entries(files[0])
    assert entries["m03"]["parte"] == "04" and entries["m03"]["uf"] == "u0"
    assert entries["m03"]["rows"] == 3 and entries["m03"]["origin_sha256"]
    assert check_dataset(_dataset(tmp_path / "lake")) == []


def test_grupo_em_dia_e_pulado_sem_avaliar_os_dados(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    members = _members(tmp_path, 4)
    writer.write_group("ds_2024", members, partitions=PARTS)
    calls: list[str] = []
    again = _members(tmp_path, 4, calls=calls)
    assert writer.is_current_group("ds_2024", again, partitions=PARTS)
    res = writer.write_group("ds_2024", again, partitions=PARTS)
    assert res.status == "skipped" and res.rows == 0
    assert [p.name for p in res.outputs] == ["ds_2024.parquet"]
    assert calls == []  # nada foi decodificado


def test_so_o_membro_mudado_e_avaliado_e_o_resto_e_preservado(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    writer.write_group("ds_2024", _members(tmp_path, 4), partitions=PARTS)
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    before = _entries(path)
    calls: list[str] = []
    members = _members(tmp_path, 4, rows=3, calls=calls)
    (tmp_path / "raw" / "m01@20260101.csv").write_text("novo conteudo")
    members[1] = GroupMember(
        members[1].src,
        lambda: pl.DataFrame({"valor": [7, 8]}),
        columns=members[1].columns,
    )
    assert not writer.is_current_group("ds_2024", members, partitions=PARTS)
    res = writer.write_group("ds_2024", members, partitions=PARTS)
    assert res.written == ["m01"] and sorted(res.kept) == ["m00", "m02", "m03"]
    assert calls == []  # os membros em dia nem foram chamados
    after = _entries(path)
    assert after["m01"]["rows"] == 2
    assert after["m01"]["origin_sha256"] != before["m01"]["origin_sha256"]
    for k in ("m00", "m02", "m03"):
        assert after[k] == before[k]  # inclusive converted_at
    df = scan_dataset(_dataset(tmp_path / "lake")).collect()
    assert df.height == 3 * 3 + 2
    assert df.filter(pl.col(ORIGIN_COLUMN) == "m01")["valor"].to_list() == [7, 8]
    assert df.filter(pl.col(ORIGIN_COLUMN) == "m02")["valor"].to_list() == [
        200,
        201,
        202,
    ]
    assert check_dataset(_dataset(tmp_path / "lake")) == []


def test_membro_orfao_sai_do_arquivo_e_do_rodape(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    members = _members(tmp_path, 4)
    writer.write_group("ds_2024", members, partitions=PARTS)
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"

    # prune=False: lista parcial só acrescenta/troca.
    res = writer.write_group("ds_2024", members[:2], partitions=PARTS, prune=False)
    assert res.status == "skipped"

    assert not writer.is_current_group("ds_2024", members[:3], partitions=PARTS)
    res = writer.write_group("ds_2024", members[:3], partitions=PARTS)
    assert res.status == "converted" and res.removed == ["m03"]
    assert res.written == []  # nada foi reconvertido, só o órfão saiu
    assert sorted(_entries(path)) == ["m00", "m01", "m02"]
    df = pl.read_parquet(path)
    assert "m03" not in set(df[ORIGIN_COLUMN]) and df.height == 9
    assert writer.is_current_group("ds_2024", members[:3], partitions=PARTS)


def test_chave_composta_de_membro_de_zip(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    zip_path = tmp_path / "raw" / "cnpj_202401@20260101.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("Parte 1.csv", "a")
        zf.writestr("Parte 2.csv", "b")
    members = [
        GroupMember(
            zip_path,
            pl.DataFrame({"valor": [i]}),
            key=composite_file_key(zip_path, f"Parte {i}.csv"),
            columns={"parte": str(i)},
        )
        for i in (1, 2)
    ]
    writer.write_group("ds_2024", members, partitions=PARTS)
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    entries = _entries(path)
    assert sorted(entries) == [
        "cnpj_202401__parte_1",
        "cnpj_202401__parte_2",
    ]
    assert entries["cnpj_202401__parte_1"]["origin_file"] == zip_path.name
    # is_current_member / write_member também aceitam a chave composta.
    assert writer.is_current_member(
        zip_path,
        group_key="ds_2024",
        partitions=PARTS,
        member_key="cnpj_202401__parte_2",
    )
    assert not writer.is_current_member(zip_path, group_key="ds_2024", partitions=PARTS)


def test_versao_do_contrato_e_do_conversor_no_skip(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    members = _members(tmp_path, 2)
    # Escrito sem versões (como um rodapé antigo): sem versão no chamador, em dia.
    writer.write_group("ds_2024", members, partitions=PARTS)
    assert writer.is_current_group("ds_2024", members, partitions=PARTS)
    # Chamador que passa versão reconverte o que não tem versão gravada.
    assert not writer.is_current_group(
        "ds_2024", members, partitions=PARTS, contract_version=1
    )
    res = writer.write_group(
        "ds_2024",
        members,
        partitions=PARTS,
        contract_version=1,
        converter_version="2.0",
    )
    assert sorted(res.written) == ["m00", "m01"]
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    assert _entries(path)["m00"]["contract_version"] == 1
    assert _entries(path)["m00"]["converter_version"] == "2.0"
    kw = {"partitions": PARTS, "contract_version": 1, "converter_version": "2.0"}
    assert writer.is_current_group("ds_2024", members, **kw)
    assert not writer.is_current_group(
        "ds_2024", members, partitions=PARTS, contract_version=2
    )
    assert not writer.is_current_group(
        "ds_2024",
        members,
        partitions=PARTS,
        contract_version=1,
        converter_version="2.1",
    )
    # Sem versão no chamador: não compara.
    assert writer.is_current_group("ds_2024", members, partitions=PARTS)
    # Mesma regra em is_current_member / write_member.
    src = members[0].src
    assert writer.is_current_member(
        src, group_key="ds_2024", partitions=PARTS, contract_version=1
    )
    assert not writer.is_current_member(
        src, group_key="ds_2024", partitions=PARTS, contract_version=3
    )
    res = writer.write_member(
        pl.DataFrame({"valor": [1]}),
        src,
        group_key="ds_2024",
        partitions=PARTS,
        member_columns={"uf": "u0", "parte": "01"},
        contract_version=3,
    )
    assert res.status == "converted"
    assert _entries(path)["m00"]["contract_version"] == 3
    assert writer.is_current_member(
        src, group_key="ds_2024", partitions=PARTS, contract_version=3
    )


def test_coluna_de_membro_nova_reescreve_o_grupo(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    plain = [
        GroupMember(m.src, m.data, columns={"uf": m.columns["uf"]})
        for m in _members(tmp_path, 2)
    ]
    writer.write_group("ds_2024", plain, partitions=PARTS)
    with_month = _members(tmp_path, 2)
    assert not writer.is_current_group("ds_2024", with_month, partitions=PARTS)
    res = writer.write_group("ds_2024", with_month, partitions=PARTS)
    assert sorted(res.written) == ["m00", "m01"]
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    assert json.loads(_footer(path)[META_MEMBER_COLUMNS]) == ["uf", "parte"]
    # Valor da coluna de um membro mudou (mês corrigido): reconverte só ele.
    fixed = _members(tmp_path, 2)
    fixed[0] = GroupMember(
        fixed[0].src, fixed[0].data, columns={"uf": "u0", "parte": "09"}
    )
    res = writer.write_group("ds_2024", fixed, partitions=PARTS)
    assert res.written == ["m00"] and res.kept == ["m01"]
    assert pl.read_parquet(path).filter(pl.col("parte") == "09").height == 3


def test_fontes_lazy_path_e_duckdb_sem_collect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    csv = tmp_path / "raw" / "c.csv"
    csv.parent.mkdir(parents=True)
    csv.write_text("valor,outro\n1,a\n2,b\n3,c\n")
    ready = tmp_path / "ready.parquet"
    pl.DataFrame({"valor": [10, 11], "uf": ["x", "x"]}).write_parquet(ready)
    lazy_src = _raw(tmp_path, "lazy.csv")
    big = tmp_path / "raw" / "big.parquet"
    pl.DataFrame({"valor": range(1000)}).write_parquet(big)

    members = [
        GroupMember(
            lazy_src,
            pl.scan_parquet(big).filter(pl.col("valor") < 500),
            columns={"uf": "lz"},
        ),
        GroupMember(
            csv,
            DuckSource(f"SELECT * FROM read_csv('{csv}', all_varchar=true)"),
            columns={"uf": "dk"},
        ),
        GroupMember(
            _raw(tmp_path, "pq.csv"),
            ready,  # traz uma coluna "uf" que o membro substitui
            columns={"uf": "pq"},
        ),
    ]

    def boom(*args, **kwargs):
        raise AssertionError("o grupo não pode ser carregado na memória")

    monkeypatch.setattr(pl.LazyFrame, "collect", boom)
    writer = _writer(tmp_path / "lake")
    res = writer.write_group("ds_2024", members, partitions=PARTS)
    monkeypatch.undo()

    assert res.rows == 500 + 3 + 2
    df = pl.read_parquet(res.output)
    assert df.filter(pl.col("uf") == "pq")["valor"].cast(pl.Int64).to_list() == [10, 11]
    assert df.filter(pl.col("uf") == "dk")["outro"].to_list() == ["a", "b", "c"]
    assert df.filter(pl.col("uf") == "lz").height == 500
    assert set(df[ORIGIN_COLUMN]) == {"lazy", "c", "pq"}
    assert check_dataset(_dataset(tmp_path / "lake")) == []


def test_fonte_sem_coluna_de_dados_e_recusada(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    only_partition = GroupMember(
        _raw(tmp_path, "a.csv"), pl.DataFrame({"ano": ["2024"]}), columns={}
    )
    with pytest.raises(LakeError, match="no payload columns"):
        writer.write_group("ds_2024", [only_partition], partitions=PARTS)
    assert not list(_dataset(tmp_path / "lake").rglob("*.parquet"))


def test_entradas_invalidas(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    a, b = _members(tmp_path, 2)
    with pytest.raises(ValueError, match="at least one"):
        writer.write_group("g", [], partitions=PARTS)
    with pytest.raises(ValueError, match="duplicate member key"):
        writer.write_group("g", [a, a], partitions=PARTS)
    other = GroupMember(b.src, b.data, columns={"uf": "x"})
    with pytest.raises(ValueError, match="same member columns"):
        writer.write_group("g", [a, other], partitions=PARTS)
    bad = GroupMember(a.src, a.data, columns={"uf": 1})  # type: ignore[dict-item]
    with pytest.raises(TypeError):
        writer.write_group("g", [bad], partitions=PARTS)
    clash = GroupMember(a.src, a.data, columns={"ano": "2024"})
    with pytest.raises(ValueError, match="clash"):
        writer.write_group("g", [clash], partitions=PARTS)
    weird = GroupMember(a.src, 42)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="DataFrame, LazyFrame, Path"):
        writer.write_group("g", [weird], partitions=PARTS)


def test_recusa_arquivo_que_nao_e_do_grupo(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    src = _raw(tmp_path, "solo@20260101.csv")
    writer.write_polars(
        pl.DataFrame({"valor": [1]}), src, partitions=PARTS, file_key_name="ds_2024"
    )
    with pytest.raises(LakeError, match="not the grouped file"):
        writer.write_group("ds_2024", _members(tmp_path, 1), partitions=PARTS)


def test_force_reescreve_tudo(tmp_path: Path) -> None:
    members = _members(tmp_path, 3)
    _writer(tmp_path / "lake").write_group("ds_2024", members, partitions=PARTS)
    forced = _writer(tmp_path / "lake", force=True)
    assert not forced.is_current_group("ds_2024", members, partitions=PARTS)
    res = forced.write_group("ds_2024", members, partitions=PARTS)
    assert res.status == "converted" and len(res.written) == 3 and res.kept == []


def test_sort_by_ordena_dentro_do_arquivo(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    members = [
        GroupMember(
            _raw(tmp_path, f"s{i}.csv"),
            pl.DataFrame({"valor": [5 - i, 9 - i]}),
            columns={"uf": f"u{i}"},
        )
        for i in range(3)
    ]
    res = writer.write_group("ds_2024", members, partitions=PARTS, sort_by=["valor"])
    values = pl.read_parquet(res.output)["valor"].to_list()
    assert values == sorted(values)
    assert check_dataset(_dataset(tmp_path / "lake")) == []


def test_max_part_bytes_nao_divide_o_grupo(tmp_path: Path, caplog) -> None:
    members = [
        GroupMember(
            _raw(tmp_path, f"p{i:02d}.csv"),
            pl.DataFrame({"valor": range(i * 2000, i * 2000 + 2000)}),
            columns={"uf": f"u{i % 2}"},
        )
        for i in range(6)
    ]
    lake = tmp_path / "lake"
    writer = _writer(lake, max_part_bytes=1000)
    with caplog.at_level("WARNING", logger="quantilica.analytics.lake"):
        res = writer.write_group("ds_2024", members, partitions=PARTS)
    assert [p.name for p in res.outputs] == ["ds_2024.parquet"]
    assert "max_part_bytes" in caplog.text
    assert res.rows == 12000
    assert check_dataset(_dataset(lake)) == []


def test_partes_numeradas_antigas_sao_substituidas(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    members = _members(tmp_path, 3)
    writer = _writer(lake)
    res = writer.write_group("ds_2024", members, partitions=PARTS)
    folder = res.output.parent
    # Um conjunto -001/-002 do mesmo grupo, como o de uma versão antiga.
    old = pq.read_table(res.output)
    pq.write_table(old, folder / "ds_2024-001.parquet")
    pq.write_table(old, folder / "ds_2024-002.parquet")
    res.output.unlink()
    assert not writer.is_current_group("ds_2024", members, partitions=PARTS)
    res = writer.write_group("ds_2024", members, partitions=PARTS)
    assert len(res.written) == 3 and res.kept == []
    assert [p.name for p in folder.glob("*.parquet")] == ["ds_2024.parquet"]
    assert check_dataset(_dataset(lake)) == []


def test_temporarios_no_scratch_e_publicacao_entre_volumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scratch = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    lake = tmp_path / "lake"
    writer = _writer(lake)

    staged_in: list[Path] = []
    real_replace = os.replace

    def fake_replace(src, dst, *args, **kwargs):
        # Simula volumes distintos: renomear a partir do scratch falha.
        if Path(src).is_relative_to(scratch):
            staged_in.append(Path(src))
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, "replace", fake_replace)
    res = writer.write_group("ds_2024", _members(tmp_path, 3), partitions=PARTS)

    assert staged_in  # o Parquet pronto estava no scratch
    assert pl.read_parquet(res.output).height == 9
    assert list(scratch.iterdir()) == []  # o scratch fica vazio
    leftovers = [p for p in _dataset(lake).rglob("*") if p.is_file()]
    assert [
        p.name for p in leftovers if p.suffix == ".parquet" or p.suffix == ".tmp"
    ] == ["ds_2024.parquet"]
    assert not (_dataset(lake) / ".tmp").exists() or not any(
        (_dataset(lake) / ".tmp").iterdir()
    )
    assert check_dataset(_dataset(lake)) == []

    # A mesma regra vale para write_member e write_polars.
    src = _raw(tmp_path, "solto@20260101.csv")
    res = writer.write_polars(
        pl.DataFrame({"valor": [1]}), src, partitions={"ano": "2025"}
    )
    assert res.output.is_file() and list(scratch.iterdir()) == []


def test_sem_scratch_o_temporario_fica_no_dataset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("QUANTILICA_LAKE_SCRATCH", raising=False)
    writer = _writer(tmp_path / "lake")
    seen: list[Path] = []
    original = LakeWriter._staging_dir

    def spy(self):
        path = original(self)
        seen.append(path)
        return path

    monkeypatch.setattr(LakeWriter, "_staging_dir", spy)
    writer.write_group("ds_2024", _members(tmp_path, 2), partitions=PARTS)
    assert seen[0].parent == _dataset(tmp_path / "lake") / ".tmp"
    assert not (_dataset(tmp_path / "lake") / ".tmp").exists()  # limpo ao fim


def test_scratch_nao_e_removido_ao_limpar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scratch = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    _writer(tmp_path / "lake").write_group(
        "ds_2024", _members(tmp_path, 1), partitions=PARTS
    )
    assert scratch.is_dir()  # o diretório do chamador permanece


def test_grupo_novo_passa_na_auditoria(tmp_path: Path) -> None:
    corpus = build_corpus(tmp_path)
    zip_src = add_raw(corpus, "eps", "grupo", "z_2024@20261021.csv")
    members = []
    for i, name in enumerate(("e_202401@20261021.csv", "e_202402@20261021.csv")):
        src = add_raw(corpus, "eps", "grupo", name)
        members.append(
            GroupMember(
                src,
                pl.DataFrame({"valor": [1, 2, 3 + i]}),
                columns={"parte": str(i + 1)},
            )
        )
    members.append(
        GroupMember(zip_src, pl.DataFrame({"valor": [9]}), columns={"parte": "3"})
    )
    writer = LakeWriter(
        corpus.dataset_dir("eps", "grupo"),
        DatasetDescriptor(
            source_id="eps",
            dataset_id="grupo",
            alias="grupo",
            partition_keys=["ano"],
            partition_types={"ano": "varchar"},
        ),
    )
    writer.write_group("grupo_2024", members, partitions={"ano": "2024"})
    report = lake_audit(corpus.lake, input_dir=corpus.view, converters={})
    assert report.ok, report.to_text()
    assert report.datasets["eps/grupo"]["members"] == 3
    assert check_dataset(corpus.dataset_dir("eps", "grupo")) == []

    # Depois de remover um membro (órfão), ainda audita limpo.
    writer.write_group("grupo_2024", members[:2], partitions={"ano": "2024"})
    report = lake_audit(corpus.lake, input_dir=corpus.view, converters={})
    assert not [f for f in report.findings if f.category == "membros_divergentes"]
    assert report.datasets["eps/grupo"]["members"] == 2


def test_auditoria_acusa_membros_divergentes_de_grupo_adulterado(
    tmp_path: Path,
) -> None:
    corpus = build_corpus(tmp_path)
    src = add_raw(corpus, "eps", "grupo", "e_202401@20261021.csv")
    writer = LakeWriter(
        corpus.dataset_dir("eps", "grupo"),
        DatasetDescriptor(
            source_id="eps",
            dataset_id="grupo",
            alias="grupo",
            partition_keys=["ano"],
            partition_types={"ano": "varchar"},
        ),
    )
    res = writer.write_group(
        "grupo_2024",
        [GroupMember(src, pl.DataFrame({"valor": [1, 2]}))],
        partitions={"ano": "2024"},
    )
    table = pq.read_table(res.output)
    meta = dict(table.schema.metadata)
    entries = json.loads(meta[META_MEMBERS.encode()])
    entries["e_202401"]["rows"] = 99
    meta[META_MEMBERS.encode()] = json.dumps(entries).encode()
    pq.write_table(table.replace_schema_metadata(meta), res.output)
    report = lake_audit(
        corpus.lake, input_dir=corpus.view, converters={}, readers=False
    )
    assert [f.category for f in report.findings] == ["membros_divergentes"]
    assert isinstance(table, pa.Table)
