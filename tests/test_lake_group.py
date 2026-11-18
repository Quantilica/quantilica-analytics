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


def test_fontes_lazy_path_e_duckdb_em_streaming(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    csv = tmp_path / "raw" / "c.csv"
    csv.parent.mkdir(parents=True)
    csv.write_text("valor\n1\n2\n3\n")
    ready = tmp_path / "ready.parquet"
    pl.DataFrame({"valor": [10, 11]}).write_parquet(ready)
    big = tmp_path / "raw" / "big.parquet"
    pl.DataFrame({"valor": range(1000)}).write_parquet(big)

    members = [
        GroupMember(
            _raw(tmp_path, "lazy.csv"),
            pl.scan_parquet(big).filter(pl.col("valor") < 500),
            columns={"uf": "lz"},
        ),
        GroupMember(
            csv,
            DuckSource(f"SELECT CAST(valor AS BIGINT) AS valor FROM read_csv('{csv}')"),
            columns={"uf": "dk"},
        ),
        GroupMember(_raw(tmp_path, "pq.csv"), ready, columns={"uf": "pq"}),
    ]

    # O que o teste prova: o membro lazy sai por sink_parquet (streaming), nenhum
    # LazyFrame é coletado e nenhum DataFrame é materializado e escrito.
    # O polars >= 1.41 executa o próprio sink com LazyFrame.collect(engine=...);
    # isso é streaming e fica permitido só dentro do sink_parquet.
    sunk: list[Path] = []
    dentro_do_sink = False
    original_sink = pl.LazyFrame.sink_parquet
    original_collect = pl.LazyFrame.collect

    def spy_sink(self, path, *args, **kwargs):
        nonlocal dentro_do_sink
        sunk.append(Path(path))
        dentro_do_sink = True
        try:
            return original_sink(self, path, *args, **kwargs)
        finally:
            dentro_do_sink = False

    def boom(*args, **kwargs):
        raise AssertionError("o grupo não pode ser carregado na memória")

    def collect_guard(self, *args, **kwargs):
        if not dentro_do_sink:
            boom()
        return original_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", spy_sink)
    monkeypatch.setattr(pl.LazyFrame, "collect", collect_guard)
    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)
    writer = _writer(tmp_path / "lake")
    res = writer.write_group("ds_2024", members, partitions=PARTS)
    monkeypatch.undo()

    assert len(sunk) == 1 and "member-000000" in sunk[0].name
    assert res.rows == 500 + 3 + 2
    df = pl.read_parquet(res.output)
    assert df.filter(pl.col("uf") == "pq")["valor"].to_list() == [10, 11]
    assert df.filter(pl.col("uf") == "lz").height == 500
    assert set(df[ORIGIN_COLUMN]) == {"lazy", "c", "pq"}
    assert check_dataset(_dataset(tmp_path / "lake")) == []


def test_coluna_de_dados_com_nome_de_coluna_de_membro_e_recusada(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path / "lake")
    ready = tmp_path / "ready.parquet"
    pl.DataFrame({"valor": [1], "uf": ["x"]}).write_parquet(ready)
    for data in (
        ready,
        pl.DataFrame({"valor": [1], "uf": ["x"]}),
        pl.DataFrame({"valor": [1], ORIGIN_COLUMN: ["x"]}).lazy(),
    ):
        member = GroupMember(_raw(tmp_path, "a.csv"), data, columns={"uf": "sp"})
        with pytest.raises(LakeError, match="name of a member column"):
            writer.write_group("g", [member], partitions=PARTS)


def test_nome_de_coluna_de_membro_igual_a_campo_do_rodape(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    for name in ("rows", "origin_sha256", "converter_version", "origin_file"):
        member = GroupMember(
            _raw(tmp_path, "a.csv"), pl.DataFrame({"v": [1]}), columns={name: "x"}
        )
        with pytest.raises(ValueError, match="fields of a quantilica.members"):
            writer.write_group("g", [member], partitions=PARTS)


def test_zip_com_varios_membros_e_lido_uma_vez(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quantilica.analytics.lake as lake_mod

    zip_path = _raw(tmp_path, "z@20260101.zip", "conteudo")
    reads: list[Path] = []
    real = lake_mod.sha256_file

    def spy(path):
        reads.append(Path(path))
        return real(path)

    monkeypatch.setattr(lake_mod, "sha256_file", spy)
    members = [
        GroupMember(
            zip_path,
            pl.DataFrame({"v": [i]}),
            key=composite_file_key(zip_path, f"m{i}.csv"),
        )
        for i in range(5)
    ]
    writer = _writer(tmp_path / "lake")
    writer.write_group("g", members, partitions=PARTS)
    assert reads == [zip_path]
    reads.clear()
    assert writer.is_current_group("g", members, partitions=PARTS)
    assert reads == [zip_path]


# --- prune=False, estado inutilizável, esquema, migração ------------------------


def test_prune_false_preserva_os_membros_fora_da_lista(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    members = _members(tmp_path, 4, rows=10)  # 40 linhas
    writer.write_group("g", members, partitions=PARTS)
    path = _dataset(tmp_path / "lake") / "ano=2024" / "g.parquet"

    # Membro novo, lista parcial.
    novo = GroupMember(
        _raw(tmp_path, "novo.csv"),
        pl.DataFrame({"valor": [999]}),
        columns={"uf": "nn", "parte": "99"},
    )
    res = writer.write_group("g", [novo], partitions=PARTS, prune=False)
    assert res.status == "converted" and res.written == ["novo"]
    assert sorted(res.kept) == ["m00", "m01", "m02", "m03"] and res.removed == []
    assert sorted(_entries(path)) == ["m00", "m01", "m02", "m03", "novo"]
    assert pl.read_parquet(path).height == 41

    # Membro alterado, lista parcial.
    (tmp_path / "raw" / "m02@20260101.csv").write_text("mudou")
    changed = GroupMember(
        tmp_path / "raw" / "m02@20260101.csv",
        pl.DataFrame({"valor": [1, 2]}),
        columns=members[2].columns,
    )
    res = writer.write_group("g", [changed], partitions=PARTS, prune=False)
    assert res.written == ["m02"] and len(res.kept) == 4 and res.removed == []
    df = pl.read_parquet(path)
    assert df.height == 41 - 10 + 2
    assert set(df[ORIGIN_COLUMN]) == {"m00", "m01", "m02", "m03", "novo"}
    # Lista parcial em dia: pula.
    assert writer.write_group("g", [novo], partitions=PARTS, prune=False).status == (
        "skipped"
    )
    # prune=True com a lista parcial remove o resto e diz o que saiu.
    res = writer.write_group("g", [novo], partitions=PARTS)
    assert sorted(res.removed) == ["m00", "m01", "m02", "m03"]
    assert pl.read_parquet(path).height == 1


def _break_footer(path: Path, **changes: str) -> None:
    table = pq.read_table(path)
    meta = dict(table.schema.metadata)
    for key, value in changes.items():
        meta[key.encode()] = value.encode()
    pq.write_table(table.replace_schema_metadata(meta), path)


def test_estado_inutilizavel(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    members = _members(tmp_path, 3)
    writer.write_group("g", members, partitions=PARTS)
    path = _dataset(tmp_path / "lake") / "ano=2024" / "g.parquet"
    entries = _entries(path)
    entries["fantasma"] = {**entries["m00"], "rows": 0}
    entries["m00"]["rows"] = 99  # a soma não fecha com as linhas do arquivo
    _break_footer(path, **{META_MEMBERS: json.dumps(entries)})

    assert not writer.is_current_group("g", members, partitions=PARTS)
    with pytest.raises(LakeError, match="not in a usable state"):
        writer.write_group("g", members[:1], partitions=PARTS, prune=False)
    assert "fantasma" in _entries(path)  # nada foi tocado

    res = writer.write_group("g", members[:2], partitions=PARTS)
    assert sorted(res.written) == ["m00", "m01"] and res.kept == []
    assert sorted(res.removed) == ["fantasma", "m02"]
    assert sorted(_entries(path)) == ["m00", "m01"]
    assert check_dataset(_dataset(tmp_path / "lake")) == []


def test_esquema_divergente_e_recusado(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    a = GroupMember(
        _raw(tmp_path, "a.csv"), pl.DataFrame({"v": [1], "x": ["a"]}), columns={}
    )
    for other, match in (
        (pl.DataFrame({"v": ["1"], "x": ["a"]}), "column 'v' is VARCHAR"),
        (pl.DataFrame({"v": [1.5], "x": ["a"]}), "column 'v' is DOUBLE"),
        (pl.DataFrame({"v": [True], "x": ["a"]}), "column 'v' is BOOLEAN"),
        (pl.DataFrame({"v": [1]}), "no column 'x'"),
        (pl.DataFrame({"v": [1], "x": ["a"], "y": [1]}), "has column 'y'"),
    ):
        b = GroupMember(_raw(tmp_path, "b.csv"), other, columns={})
        with pytest.raises(LakeError, match=match) as err:
            writer.write_group("g", [a, b], partitions=PARTS)
        assert "member 'b'" in str(err.value)
    assert not list(_dataset(tmp_path / "lake").rglob("*.parquet"))

    # Contra o arquivo existente também.
    writer.write_group("g", [a], partitions=PARTS)
    b = GroupMember(_raw(tmp_path, "b.csv"), pl.DataFrame({"v": ["1"], "x": ["a"]}))
    with pytest.raises(LakeError, match="existing file"):
        writer.write_group("g", [a, b], partitions=PARTS)


def test_schema_forca_o_tipo_e_preenche_coluna_ausente(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    a = GroupMember(
        _raw(tmp_path, "a.csv"), pl.DataFrame({"v": [1], "x": ["a"]}), columns={}
    )
    b = GroupMember(_raw(tmp_path, "b.csv"), pl.DataFrame({"v": ["2"]}), columns={})
    schema = {"v": "BIGINT", "x": pl.String}
    res = writer.write_group("g", [a, b], partitions=PARTS, schema=schema)
    df = pl.read_parquet(res.output).sort("v")
    assert df.schema["v"] == pl.Int64
    assert df["v"].to_list() == [1, 2] and df["x"].to_list() == ["a", None]
    # Em dia de novo; e um membro novo é conformado ao esquema do arquivo.
    assert writer.is_current_group("g", [a, b], partitions=PARTS)
    c = GroupMember(_raw(tmp_path, "c.csv"), pl.DataFrame({"v": ["3"], "x": ["z"]}))
    res = writer.write_group("g", [a, b, c], partitions=PARTS, schema=schema)
    assert res.written == ["c"]
    assert pl.read_parquet(res.output).sort("v")["v"].to_list() == [1, 2, 3]
    d = GroupMember(_raw(tmp_path, "d.csv"), pl.DataFrame({"v": ["4"], "x": ["w"]}))
    with pytest.raises(LakeError, match="unknown type"):
        writer.write_group(
            "g", [d], partitions=PARTS, schema={"v": "NOPE"}, prune=False
        )


def test_replaces_remove_os_parquets_por_bruto_depois_do_grupo(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    writer = _writer(lake)
    old = []
    for i in range(3):
        src = _raw(tmp_path, f"m{i:02d}@20260101.csv")
        res = writer.write_polars(
            pl.DataFrame({"valor": [i, i]}), src, partitions=PARTS
        )
        old.append(res.output)
    other = _writer(lake)  # um Parquet de outro dataset na mesma pasta: intocável
    foreign = old[0].parent / "alheio.parquet"
    table = pq.read_table(old[0])
    meta = {
        **pq.ParquetFile(old[0]).metadata.metadata,
        b"quantilica.dataset_id": b"outro",
    }
    pq.write_table(table.replace_schema_metadata(meta), foreign)
    members = [
        GroupMember(
            tmp_path / "raw" / f"m{i:02d}@20260101.csv", pl.DataFrame({"valor": [i, i]})
        )
        for i in range(3)
    ]
    res = other.write_group(
        "g", members, partitions=PARTS, replaces=["m00", "m01", "alheio", "ausente"]
    )
    assert sorted(p.name for p in res.replaced) == ["m00.parquet", "m01.parquet"]
    names = sorted(p.name for p in old[0].parent.glob("*.parquet"))
    assert names == [
        "alheio.parquet",
        "g.parquet",
        "m02.parquet",
    ]  # alheio: outro dataset
    # replace_partition: tudo que não é o grupo e é deste dataset.
    res = other.write_group("g", members, partitions=PARTS, replace_partition=True)
    assert res.status == "skipped"
    assert [p.name for p in res.replaced] == ["m02.parquet"]
    assert sorted(p.name for p in old[0].parent.glob("*.parquet")) == [
        "alheio.parquet",
        "g.parquet",
    ]
    assert scan_df_rows(lake) == 6


def scan_df_rows(lake: Path) -> int:
    return pl.read_parquet(_dataset(lake) / "ano=2024" / "g.parquet").height


def test_replaces_so_depois_de_publicar(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    writer = _writer(lake)
    src = _raw(tmp_path, "m00@20260101.csv")
    writer.write_polars(pl.DataFrame({"valor": [1]}), src, partitions=PARTS)

    def boom():
        raise RuntimeError("falhou ao decodificar")

    with pytest.raises(RuntimeError):
        writer.write_group(
            "g", [GroupMember(src, boom)], partitions=PARTS, replaces=["m00"]
        )
    assert (_dataset(lake) / "ano=2024" / "m00.parquet").is_file()  # nada removido


def test_publicacao_decide_pelo_dispositivo_e_limpa_o_tmp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quantilica.analytics.lake as lake_mod

    scratch = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    monkeypatch.setattr(lake_mod, "_same_volume", lambda a, b: False)
    calls = []
    real = os.replace

    def spy(src, dst, *a, **k):
        calls.append(Path(src))
        return real(src, dst, *a, **k)

    monkeypatch.setattr(os, "replace", spy)
    lake = tmp_path / "lake"
    res = _writer(lake).write_group(
        "g",
        [GroupMember(_raw(tmp_path, "a.csv"), pl.DataFrame({"v": [1]}))],
        partitions=PARTS,
    )
    assert res.output.is_file()
    assert calls and all(
        not c.is_relative_to(scratch) for c in calls
    )  # sem rename direto
    assert not (_dataset(lake) / ".tmp").exists()  # o .tmp da cópia foi removido

    # rename que falha com errno qualquer também cai na cópia
    monkeypatch.setattr(lake_mod, "_same_volume", lambda a, b: True)

    def flaky(src, dst, *a, **k):
        if Path(src).is_relative_to(scratch):
            raise OSError(errno.EINVAL, "disco virtual")
        return real(src, dst, *a, **k)

    monkeypatch.setattr(os, "replace", flaky)
    res = _writer(lake).write_group(
        "h",
        [GroupMember(_raw(tmp_path, "b.csv"), pl.DataFrame({"v": [2]}))],
        partitions=PARTS,
    )
    assert res.output.is_file()
    assert not (_dataset(lake) / ".tmp").exists()


def test_partes_do_write_polars_vindas_do_scratch_sao_copiadas_antes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quantilica.analytics.lake as lake_mod

    scratch = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    monkeypatch.setattr(
        lake_mod, "_same_volume", lambda a, b: not Path(a).is_relative_to(scratch)
    )
    events: list[str] = []
    real_replace, real_copy = os.replace, lake_mod._copy_to

    def spy_replace(src, dst, *a, **k):
        events.append("rename")
        return real_replace(src, dst, *a, **k)

    def spy_copy(tmp, hop_dir):
        events.append("copy")
        return real_copy(tmp, hop_dir)

    monkeypatch.setattr(os, "replace", spy_replace)
    monkeypatch.setattr(lake_mod, "_copy_to", spy_copy)
    writer = _writer(tmp_path / "lake", max_part_bytes=2000)
    src = _raw(tmp_path, "grande@20260101.csv")
    frame = pl.DataFrame({"valor": [f"linha {i} " * 20 for i in range(400)]})
    res = writer.write_polars(frame, src, partitions=PARTS)
    n = len(res.outputs)
    assert n >= 2
    # Todas as cópias para o destino antes do primeiro rename: janela curta.
    assert events[:n] == ["copy"] * n and events[n] == "rename"
    assert list(scratch.iterdir()) == []


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


def test_rodape_do_grupo_e_aberto_uma_vez_por_checagem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quantilica.analytics.lake as lake_mod

    writer = _writer(tmp_path / "lake")
    members = _members(tmp_path, 3)
    writer.write_group("g", members, partitions=PARTS)
    opened: list[Path] = []
    real = pq.ParquetFile

    def spy(path, *args, **kwargs):
        opened.append(Path(path))
        return real(path, *args, **kwargs)

    monkeypatch.setattr(lake_mod.pq, "ParquetFile", spy)
    assert writer.is_current_group("g", members, partitions=PARTS)
    assert len(opened) == 1
    opened.clear()
    assert writer.write_group("g", members, partitions=PARTS).status == "skipped"
    assert len(opened) == 1


# --- remoção segura (replaces), esquema com perda, scratch ---------------------


def _per_raw(writer: LakeWriter, tmp_path: Path, name: str, rows: int, parts=PARTS):
    src = _raw(tmp_path, name)
    res = writer.write_polars(
        pl.DataFrame({"valor": range(rows)}), src, partitions=parts
    )
    return src, res.output


def test_replace_partition_nao_apaga_outro_grupo_da_mesma_particao(
    tmp_path: Path, caplog
) -> None:
    lake = tmp_path / "lake"
    writer = _writer(lake)
    a_src, a_old = _per_raw(writer, tmp_path, "a@20260101.csv", 2)
    b_members = [
        GroupMember(_raw(tmp_path, f"b{i}.csv"), pl.DataFrame({"valor": [i]}))
        for i in range(2)
    ]
    writer.write_group("outro", b_members, partitions=PARTS)
    other_group = a_old.parent / "outro.parquet"
    mine = [GroupMember(a_src, pl.DataFrame({"valor": [0, 1]}))]
    with caplog.at_level("WARNING", logger="quantilica.analytics.lake"):
        res = writer.write_group("meu", mine, partitions=PARTS, replace_partition=True)
    assert [p.name for p in res.replaced] == ["a.parquet"]
    assert [p.name for p in res.not_replaced] == ["outro.parquet"]
    assert other_group.is_file() and not a_old.exists()
    assert "grouped file" in caplog.text


def test_replace_partition_em_dataset_sem_particao(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    writer = _writer(lake, keys=[])
    src, old = _per_raw(writer, tmp_path, "a@20260101.csv", 2, parts={})
    other = [GroupMember(_raw(tmp_path, "z.csv"), pl.DataFrame({"valor": [1]}))]
    writer.write_group("grupo_z", other, partitions={})
    res = writer.write_group(
        "grupo_a",
        [GroupMember(src, pl.DataFrame({"valor": [0, 1]}))],
        partitions={},
        replace_partition=True,
    )
    names = sorted(p.name for p in _dataset(lake).glob("*.parquet"))
    assert names == ["grupo_a.parquet", "grupo_z.parquet"]
    assert [p.name for p in res.not_replaced] == ["grupo_z.parquet"]
    assert not old.exists()


def test_replaces_com_chave_fora_do_grupo_fica_com_aviso(
    tmp_path: Path, caplog
) -> None:
    writer = _writer(tmp_path / "lake")
    a_src, a_old = _per_raw(writer, tmp_path, "a@20260101.csv", 2)
    _c_src, c_old = _per_raw(writer, tmp_path, "c@20260101.csv", 3)
    with caplog.at_level("WARNING", logger="quantilica.analytics.lake"):
        res = writer.write_group(
            "g",
            [GroupMember(a_src, pl.DataFrame({"valor": [0, 1]}))],
            partitions=PARTS,
            replaces=["a", "c"],
        )
    assert [p.name for p in res.replaced] == ["a.parquet"]
    assert [p.name for p in res.not_replaced] == ["c.parquet"]
    assert c_old.is_file() and not a_old.exists()
    assert "not a member of the group" in caplog.text


def test_replaces_membro_de_zip_com_chave_composta_pelo_sha_do_zip(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path / "lake")
    zip_src, zip_old = _per_raw(writer, tmp_path, "pacote@20260101.zip", 4)
    members = [
        GroupMember(
            zip_src,
            pl.DataFrame({"valor": [i]}),
            key=composite_file_key(zip_src, f"p{i}.csv"),
        )
        for i in range(2)
    ]
    res = writer.write_group("g", members, partitions=PARTS, replaces=["pacote"])
    assert [p.name for p in res.replaced] == ["pacote.parquet"]
    assert not zip_old.exists()


def test_grupo_pulado_nao_apaga_arquivo_cujo_sha_nao_e_membro(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path / "lake")
    a_src, _ = _per_raw(writer, tmp_path, "a@20260101.csv", 2)
    members = [GroupMember(a_src, pl.DataFrame({"valor": [0, 1]}))]
    writer.write_group("g", members, partitions=PARTS, replaces=["a"])
    # Depois, outro caminho grava um Parquet legítimo na mesma partição.
    _x, later = _per_raw(writer, tmp_path, "tardio@20260201.csv", 5)
    res = writer.write_group(
        "g", members, partitions=PARTS, replace_partition=True, replaces=["tardio"]
    )
    assert res.status == "skipped" and res.replaced == []
    assert [p.name for p in res.not_replaced] == ["tardio.parquet"]
    assert later.is_file()


def _two_types(tmp_path: Path, a_frame: pl.DataFrame, b_frame: pl.DataFrame, schema):
    writer = _writer(tmp_path / "lake")
    a = GroupMember(_raw(tmp_path, "a.csv"), a_frame)
    b = GroupMember(_raw(tmp_path, "b.csv"), b_frame)
    return writer, [a, b], schema


@pytest.mark.parametrize(
    ("frame", "schema", "target"),
    [
        (pl.DataFrame({"v": [1.5]}), {"v": "BIGINT"}, "BIGINT"),  # arredonda
        (pl.DataFrame({"v": ["007"]}), {"v": "INTEGER"}, "INTEGER"),  # perde zeros
        (pl.DataFrame({"v": ["abc"]}), {"v": "BIGINT"}, "BIGINT"),  # não cabe
        (
            pl.DataFrame({"v": [1.2345]}).cast({"v": pl.Decimal(10, 4)}),
            {"v": "DECIMAL(10,2)"},
            "DECIMAL(10,2)",
        ),
        (
            pl.DataFrame({"v": [1]}).select(
                pl.lit(__import__("datetime").datetime(2024, 1, 1, 13)).alias("v")
            ),
            {"v": "DATE"},
            "DATE",
        ),
    ],
)
def test_schema_com_perda_e_recusado(tmp_path: Path, frame, schema, target) -> None:
    ok = pl.DataFrame({"v": [1]}).cast({"v": frame.schema["v"]}) if False else None
    writer = _writer(tmp_path / "lake")
    member = GroupMember(_raw(tmp_path, "a.csv"), frame)
    with pytest.raises(LakeError, match="loses information") as err:
        writer.write_group("g", [member], partitions=PARTS, schema=schema)
    assert "'v'" in str(err.value) and target in str(err.value)
    assert ok is None
    assert not list(_dataset(tmp_path / "lake").rglob("*.parquet"))


def test_schema_que_alarga_passa_e_linhas_ja_gravadas_sao_verificadas(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path / "lake")
    a = GroupMember(
        _raw(tmp_path, "a.csv"),
        pl.DataFrame({"v": [1, 2]}).cast({"v": pl.Int32}),
    )
    writer.write_group("g", [a], partitions=PARTS)
    b = GroupMember(_raw(tmp_path, "b.csv"), pl.DataFrame({"v": [3]}))
    res = writer.write_group("g", [a, b], partitions=PARTS, schema={"v": "BIGINT"})
    assert pl.read_parquet(res.output).schema["v"] == pl.Int64
    c = GroupMember(_raw(tmp_path, "c2.csv"), pl.DataFrame({"v": ["4"]}))
    res = writer.write_group("g", [a, b, c], partitions=PARTS, schema={"v": "VARCHAR"})
    assert sorted(pl.read_parquet(res.output)["v"].to_list()) == ["1", "2", "3", "4"]
    # O esquema só vale quando o grupo é reescrito: em dia, pula.
    again = writer.write_group("g", [a, b, c], partitions=PARTS, schema={"v": "DOUBLE"})
    assert again.status == "skipped"
    # Estreitar as linhas já gravadas ("007" não volta de BIGINT) é recusado.
    x = GroupMember(_raw(tmp_path, "x.csv"), pl.DataFrame({"v": ["007"]}))
    writer.write_group("h", [x], partitions=PARTS)
    y = GroupMember(_raw(tmp_path, "y.csv"), pl.DataFrame({"v": [8]}))
    with pytest.raises(LakeError, match="the existing file.*loses information"):
        writer.write_group("h", [x, y], partitions=PARTS, schema={"v": "BIGINT"})


def test_schema_com_nome_reservado_e_recusado(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    member = GroupMember(
        _raw(tmp_path, "a.csv"), pl.DataFrame({"v": [1]}), columns={"uf": "sp"}
    )
    for name in ("ano", "uf", ORIGIN_COLUMN):
        with pytest.raises(ValueError, match="schema cannot declare"):
            writer.write_group(
                "g", [member], partitions=PARTS, schema={name: "VARCHAR"}
            )


def test_timestamp_ns_e_coluna_nula_sao_normalizados(tmp_path: Path) -> None:
    import datetime as dt

    writer = _writer(tmp_path / "lake")
    ts = dt.datetime(2024, 1, 2, 3, 4, 5)
    a = GroupMember(
        _raw(tmp_path, "a.csv"),
        pl.DataFrame({"t": [ts], "v": [1]}).cast({"t": pl.Datetime("us")}),
    )
    b = GroupMember(
        _raw(tmp_path, "b.csv"),
        pl.DataFrame({"t": [ts], "v": [None]}).cast(
            {"t": pl.Datetime("ns"), "v": pl.Null}
        ),
    )
    res = writer.write_group("g", [a, b], partitions=PARTS)
    df = pl.read_parquet(res.output)
    assert df.schema["t"] == pl.Datetime("us") and df.schema["v"] == pl.Int64
    assert df["v"].to_list().count(None) == 1
    # Nanossegundos que não cabem em TIMESTAMP: a normalização é recusada.
    c = GroupMember(
        _raw(tmp_path, "c.csv"),
        pl.DataFrame({"t": [123456789]})
        .cast({"t": pl.Datetime("ns")})
        .with_columns(v=pl.lit(1)),
    )
    with pytest.raises(LakeError, match="loses information"):
        writer.write_group("g2", [a, c], partitions=PARTS)
    # Outras divergências continuam sendo erro.
    d = GroupMember(_raw(tmp_path, "d.csv"), pl.DataFrame({"t": ["x"], "v": [1]}))
    with pytest.raises(LakeError, match="column 't'"):
        writer.write_group("g3", [a, d], partitions=PARTS)


def test_configure_scratch_uma_vez_por_processo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import atexit

    import quantilica.analytics.lake as lake_mod

    scratch = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    monkeypatch.delenv("POLARS_TEMP_DIR", raising=False)
    monkeypatch.setattr(lake_mod, "_scratch_configured", False)
    registered = []
    monkeypatch.setattr(atexit, "register", lambda *a, **k: registered.append(a))
    _writer(tmp_path / "lake")
    spill = Path(os.environ["POLARS_TEMP_DIR"])
    assert spill == scratch / f"polars-{os.getpid()}" and spill.is_dir()
    assert registered and registered[0][0] is lake_mod.shutil.rmtree
    # Segunda criação: nada novo.
    monkeypatch.setenv("POLARS_TEMP_DIR", "/outro")
    _writer(tmp_path / "lake2")
    assert os.environ["POLARS_TEMP_DIR"] == "/outro" and len(registered) == 1
    monkeypatch.delenv("POLARS_TEMP_DIR")  # o monkeypatch restaura o que havia


def test_configure_scratch_respeita_variavel_ja_definida(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quantilica.analytics.lake as lake_mod

    scratch = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    monkeypatch.setenv("POLARS_TEMP_DIR", str(tmp_path / "meu"))
    monkeypatch.setattr(lake_mod, "_scratch_configured", False)
    _writer(tmp_path / "lake")
    assert os.environ["POLARS_TEMP_DIR"] == str(tmp_path / "meu")
    assert not list(scratch.glob("polars-*"))


def test_limpeza_do_scratch_so_de_processo_morto(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import socket
    import subprocess
    import sys

    import quantilica.analytics.lake as lake_mod

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    host = socket.gethostname()

    def make(name: str, owner: dict | None, days: float) -> Path:
        d = scratch / name
        d.mkdir()
        if owner is not None:
            (d / "owner.json").write_text(json.dumps(owner))
        old = d.stat().st_mtime - days * 24 * 3600
        os.utime(d, (old, old))
        return d

    gone = [
        make("lake-stage-dead", {"pid": dead.pid, "hostname": host}, 2),
        make("lake-work-dead", {"pid": dead.pid, "hostname": host}, 2),
        make("lake-stage-sem-dono", None, 8),
    ]
    stays = [
        make("lake-stage-vivo", {"pid": os.getpid(), "hostname": host}, 30),
        make("lake-stage-recente", {"pid": dead.pid, "hostname": host}, 0.1),
        make("lake-stage-outro-host", {"pid": dead.pid, "hostname": "outro"}, 30),
        make("lake-stage-sem-dono-novo", None, 3),
        make("tmp-de-outro-programa", {"pid": dead.pid, "hostname": host}, 30),
    ]
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    monkeypatch.setattr(lake_mod, "_scratch_configured", False)
    monkeypatch.setenv("POLARS_TEMP_DIR", "x")  # só a limpeza interessa aqui
    _writer(tmp_path / "lake")
    assert not any(d.exists() for d in gone)
    assert all(d.exists() for d in stays)


def test_o_staging_e_o_diretorio_de_trabalho_levam_dono(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scratch = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    writer = _writer(tmp_path / "lake")
    staging = writer._staging_dir()
    owner = json.loads((staging / "owner.json").read_text())
    assert owner["pid"] == os.getpid() and staging.name.startswith("lake-stage-")
    import quantilica.analytics.lake as lake_mod

    work = lake_mod._scratch_dir(staging)
    assert work.name.startswith("lake-work-") and (work / "owner.json").is_file()


def test_rename_com_erro_que_nao_e_de_volume_e_relancado(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quantilica.analytics.lake as lake_mod

    src = tmp_path / "a.tmp"
    src.write_bytes(b"x")
    final = tmp_path / "out" / "a.parquet"
    final.parent.mkdir()

    def boom(*args, **kwargs):
        raise OSError(errno.ENOSPC, "sem espaço")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError) as err:
        lake_mod._durable_replace(src, final)
    assert err.value.errno == errno.ENOSPC and src.exists() and not final.exists()

    def denied(*args, **kwargs):
        raise PermissionError(errno.EACCES, "arquivo aberto")

    monkeypatch.setattr(os, "replace", denied)
    with pytest.raises(PermissionError):
        lake_mod._durable_replace(src, final)


def test_publicacao_de_varias_partes_limpa_as_copias_se_uma_falhar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quantilica.analytics.lake as lake_mod

    scratch = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(scratch))
    monkeypatch.setattr(
        lake_mod, "_same_volume", lambda a, b: not Path(a).is_relative_to(scratch)
    )
    real = lake_mod._copy_to
    calls = []

    def flaky(tmp, hop_dir):
        calls.append(tmp)
        if len(calls) == 3:
            raise OSError(errno.ENOSPC, "disco cheio")
        return real(tmp, hop_dir)

    monkeypatch.setattr(lake_mod, "_copy_to", flaky)
    writer = _writer(tmp_path / "lake", max_part_bytes=2000)
    src = _raw(tmp_path, "grande@20260101.csv")
    frame = pl.DataFrame({"valor": [f"linha {i} " * 20 for i in range(400)]})
    with pytest.raises(OSError, match="disco cheio"):
        writer.write_polars(frame, src, partitions=PARTS)
    assert len(calls) == 3
    dataset = _dataset(tmp_path / "lake")
    assert not list(dataset.rglob("*.parquet"))
    assert not (dataset / ".tmp").exists()  # as duas cópias já feitas foram apagadas


def test_pid_alive_no_windows_nao_usa_os_kill(monkeypatch) -> None:
    """No Windows, ``os.kill(pid, 0)`` manda Ctrl+C ao grupo e derruba o build."""
    from quantilica.analytics import lake as lake_mod

    def _proibido(*_a, **_k):
        raise AssertionError("os.kill não pode ser chamado no Windows")

    monkeypatch.setattr(lake_mod.os, "name", "nt")
    monkeypatch.setattr(lake_mod.os, "kill", _proibido)
    monkeypatch.setattr(lake_mod, "_pid_alive_windows", lambda pid: pid == 4242)
    assert lake_mod._pid_alive(4242) is True
    assert lake_mod._pid_alive(4243) is False
    assert lake_mod._pid_alive(0) is False


def test_pid_alive_posix(tmp_path) -> None:
    import subprocess
    import sys

    from quantilica.analytics import lake as lake_mod

    if lake_mod.os.name == "nt":
        pytest.skip("caminho POSIX")
    assert lake_mod._pid_alive(lake_mod.os.getpid()) is True
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    assert lake_mod._pid_alive(proc.pid) is False


def test_limpeza_do_scratch_nao_consulta_pid_de_pasta_recente(
    tmp_path, monkeypatch
) -> None:
    """Pasta ``polars-<pid>`` recente fica sem consultar o pid (workers vivos)."""
    from quantilica.analytics import lake as lake_mod

    def _proibido(_pid):
        raise AssertionError("pid consultado para pasta recente")

    monkeypatch.setattr(lake_mod, "_pid_alive", _proibido)
    viva = tmp_path / "polars-12345"
    viva.mkdir()
    lake_mod._remove_stale_scratch(tmp_path)
    assert viva.is_dir()


# --- member_stats: nulos exatos por membro no rodapé -------------------------


def _stat_members(tmp_path: Path, calls: list | None = None) -> list[GroupMember]:
    """a: tem ``x`` (2 nulos em 4) e ``y``; b: só ``y`` (sem ``x``); c: 0 linhas."""
    frames = {
        "a": pl.DataFrame(
            {"x": [1, None, 3, None], "y": ["p", "q", None, "s"]},
            schema={"x": pl.Int64, "y": pl.String},
        ),
        "b": pl.DataFrame({"y": ["p", None]}, schema={"y": pl.String}),
        "c": pl.DataFrame({"x": [], "y": []}, schema={"x": pl.Int64, "y": pl.String}),
    }
    out = []
    for i, (name, frame) in enumerate(frames.items()):
        src = _raw(tmp_path, f"{name}@20260101.csv")

        def make(frame=frame, name=name):
            if calls is not None:
                calls.append(name)
            return frame

        out.append(GroupMember(src, make, columns={"mes": f"{i + 1:02d}"}))
    return out


_FILL = {"x": "BIGINT"}


def test_member_stats_conta_nulos_exatos_so_das_colunas_do_membro(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path / "lake")
    res = writer.write_group(
        "ds_2024",
        _stat_members(tmp_path),
        partitions=PARTS,
        schema=_FILL,
        member_stats=True,
    )
    assert res.status == "converted"
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    entries = _entries(path)
    assert entries["a"]["null_counts"] == {"x": 2, "y": 1}
    # coluna ausente do membro não aparece (≠ presente e toda nula)
    assert entries["b"]["null_counts"] == {"y": 1}
    # 0 linhas: toda coluna que o membro trazia, com 0
    assert entries["c"]["null_counts"] == {"x": 0, "y": 0}
    for entry in entries.values():
        assert "mes" not in entry["null_counts"]
        assert ORIGIN_COLUMN not in entry["null_counts"]
    df = scan_dataset(_dataset(tmp_path / "lake")).collect()
    # a união tem 4 nulos em x: 2 do membro a e as 2 linhas do b, preenchidas
    assert df["x"].null_count() == 4


def test_member_stats_membro_mantido_preserva_a_entrada(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    writer.write_group(
        "ds_2024",
        _stat_members(tmp_path),
        partitions=PARTS,
        schema=_FILL,
        member_stats=True,
    )
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    before = _entries(path)
    calls: list[str] = []
    members = _stat_members(tmp_path, calls)
    assert writer.is_current_group(
        "ds_2024", members, partitions=PARTS, member_stats=True
    )
    # um membro muda: só ele é reavaliado, os outros levam a entrada intacta
    _raw(tmp_path, "b@20260101.csv", "outro conteudo")
    res = writer.write_group(
        "ds_2024", members, partitions=PARTS, schema=_FILL, member_stats=True
    )
    after = _entries(path)
    assert calls == ["b"] and res.kept == ["a", "c"]
    assert after["a"] == before["a"]
    assert after["c"] == before["c"]
    assert after["b"]["null_counts"] == {"y": 1}


def test_member_stats_entrada_sem_estatistica_forca_a_reescrita(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path / "lake")
    writer.write_group(
        "ds_2024", _stat_members(tmp_path), partitions=PARTS, schema=_FILL
    )
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    assert all("null_counts" not in e for e in _entries(path).values())
    members = _stat_members(tmp_path)
    # sem a opção o grupo está em dia; com ela, não
    assert writer.is_current_group("ds_2024", members, partitions=PARTS)
    assert not writer.is_current_group(
        "ds_2024", members, partitions=PARTS, member_stats=True
    )
    calls: list[str] = []
    res = writer.write_group(
        "ds_2024",
        _stat_members(tmp_path, calls),
        partitions=PARTS,
        schema=_FILL,
        member_stats=True,
    )
    assert res.status == "converted" and sorted(calls) == ["a", "b", "c"]
    assert all("null_counts" in e for e in _entries(path).values())
    assert writer.is_current_group(
        "ds_2024", members, partitions=PARTS, member_stats=True
    )


def test_member_stats_desligado_deixa_o_rodape_como_era(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    writer.write_group(
        "ds_2024", _stat_members(tmp_path), partitions=PARTS, schema=_FILL
    )
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    entries = _entries(path)
    assert set(entries["a"]) == {
        "origin_file",
        "origin_sha256",
        "rows",
        "converted_at",
        "mes",
    }
    # ligado, o único acréscimo ao rodapé é null_counts
    other = _writer(tmp_path / "lake2")
    other.write_group(
        "ds_2024",
        _stat_members(tmp_path),
        partitions=PARTS,
        schema=_FILL,
        member_stats=True,
    )
    path2 = _dataset(tmp_path / "lake2") / "ano=2024" / "ds_2024.parquet"
    for key, entry in _entries(path2).items():
        entry = dict(entry)
        entry.pop("null_counts")
        assert set(entry) == set(entries[key])


def test_member_stats_nome_reservado_na_coluna_de_membro(tmp_path: Path) -> None:
    writer = _writer(tmp_path / "lake")
    src = _raw(tmp_path, "z.csv")
    member = GroupMember(src, pl.DataFrame({"v": [1]}), columns={"null_counts": "1"})
    with pytest.raises(ValueError, match="null_counts"):
        writer.write_group("ds_2024", [member], partitions=PARTS)


def test_member_stats_coluna_exclusiva_de_membro_removido_sai_do_arquivo(
    tmp_path: Path,
) -> None:
    def make(name: str, frame: pl.DataFrame, mes: str, calls: list | None = None):
        src = _raw(tmp_path, f"{name}.csv")

        def data(frame=frame):
            if calls is not None:
                calls.append(name)
            return frame

        return GroupMember(src, data, columns={"mes": mes})

    a = pl.DataFrame({"v": [1, None, 3]})
    b = pl.DataFrame({"v": [4], "so_no_b": ["x"]})
    writer = _writer(tmp_path / "lake")
    writer.write_group(
        "ds_2024",
        [make("a", a, "01"), make("b", b, "02")],
        partitions=PARTS,
        schema={"so_no_b": "VARCHAR"},
        member_stats=True,
    )
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    assert "so_no_b" in pq.ParquetFile(path).schema_arrow.names
    before = (
        scan_dataset(_dataset(tmp_path / "lake"))
        .collect()
        .filter(pl.col(ORIGIN_COLUMN) == "a")
    )
    kept_entry = _entries(path)["a"]

    calls: list[str] = []
    res = writer.write_group(
        "ds_2024",
        [make("a", a, "01", calls)],
        partitions=PARTS,
        member_stats=True,
    )
    assert res.status == "converted" and res.removed == ["b"] and res.kept == ["a"]
    assert calls == []  # o membro mantido não foi decodificado
    assert "so_no_b" not in pq.ParquetFile(path).schema_arrow.names
    after = scan_dataset(_dataset(tmp_path / "lake")).collect()
    assert after.drop(ORIGIN_COLUMN).equals(before.drop(ORIGIN_COLUMN, "so_no_b"))
    assert _entries(path) == {"a": kept_entry}
    assert check_dataset(_dataset(tmp_path / "lake")) == []


def test_member_stats_desligado_mantem_a_coluna_do_membro_removido(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path / "lake")
    src_a, src_b = _raw(tmp_path, "a.csv"), _raw(tmp_path, "b.csv")
    a = GroupMember(src_a, pl.DataFrame({"v": [1]}), columns={"mes": "01"})
    b = GroupMember(
        src_b, pl.DataFrame({"v": [2], "extra": ["x"]}), columns={"mes": "02"}
    )
    writer.write_group("ds_2024", [a, b], partitions=PARTS, schema={"extra": "VARCHAR"})
    writer.write_group("ds_2024", [a], partitions=PARTS)
    path = _dataset(tmp_path / "lake") / "ano=2024" / "ds_2024.parquet"
    assert "extra" in pq.ParquetFile(path).schema_arrow.names
