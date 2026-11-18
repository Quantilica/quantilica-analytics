"""``LakeWriter.origin_recorded``: is a raw file in the lake, whatever its partition."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from quantilica.core.lake import DatasetDescriptor

from quantilica.analytics import DataContract, Field, LakeWriter
from quantilica.analytics.lake import GroupMember, output_id


def _writer(tmp_path: Path, *, force: bool = False) -> LakeWriter:
    return LakeWriter(
        tmp_path / "lake" / "antt" / "acidentes",
        DatasetDescriptor(
            source_id="antt",
            dataset_id="acidentes",
            alias="acidentes",
            partition_keys=["ano"],
            partition_types={"ano": "int"},
        ),
        force=force,
    )


def _raw(tmp_path: Path, name: str = "acidentes.csv", content: str = "a") -> Path:
    path = tmp_path / "raw" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _write_years(writer: LakeWriter, raw: Path, years=(2020, 2021, 2022), **kw) -> None:
    for year in years:
        writer.write_polars(
            pl.DataFrame({"v": [year]}), raw, partitions={"ano": year}, **kw
        )


def test_registrado_e_igual(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    assert not writer.origin_recorded(raw)  # empty dataset
    writer.write_polars(pl.DataFrame({"v": [1]}), raw, partitions={"ano": 2020})
    assert writer.origin_recorded(raw)
    assert _writer(tmp_path).origin_recorded(raw)  # a fresh writer sees the lake


def test_dataset_vazio(tmp_path: Path) -> None:
    assert not _writer(tmp_path).origin_recorded(_raw(tmp_path))


def test_sha_diferente(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_years(writer, raw, years=(2020,))
    raw.write_text("outro conteudo")
    assert not _writer(tmp_path).origin_recorded(raw)


def test_outro_arquivo_de_origem(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_years(writer, raw, years=(2020,))
    assert not writer.origin_recorded(_raw(tmp_path, "outro.csv", "a"))
    assert writer.origin_recorded(
        _raw(tmp_path, "copia.csv", "a"), member="acidentes.csv"
    )


def test_varios_anos_todos_iguais(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_years(writer, raw)
    assert writer.origin_recorded(raw)


def test_um_ano_com_sha_antigo(tmp_path: Path) -> None:
    raw = _raw(tmp_path, content="v1")
    writer = _writer(tmp_path)
    _write_years(writer, raw)
    raw.write_text("v2")
    # The new raw file is converted for two of the three years only.
    _write_years(writer, raw, years=(2020, 2021))
    assert not _writer(tmp_path).origin_recorded(raw)
    _write_years(writer, raw, years=(2022,))
    assert _writer(tmp_path).origin_recorded(raw)


def test_bruto_novo_que_acrescenta_um_ano(tmp_path: Path) -> None:
    raw = _raw(tmp_path, content="v1")
    writer = _writer(tmp_path)
    _write_years(writer, raw, years=(2020, 2021))
    raw.write_text("v2 com 2022")
    assert not writer.origin_recorded(raw)


def test_force(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    _write_years(_writer(tmp_path), raw, years=(2020,))
    assert _writer(tmp_path).origin_recorded(raw)
    assert not _writer(tmp_path, force=True).origin_recorded(raw)


def test_contrato_como_origin_matches(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    contract = DataContract(
        dataset_id="acidentes", version=2, fields=[Field("v", pl.Int64)]
    )
    _write_years(_writer(tmp_path), raw, years=(2020,), contract=contract)
    writer = _writer(tmp_path)
    assert writer.origin_recorded(raw, contract_version=2)
    assert not writer.origin_recorded(raw, contract_version=3)
    assert not writer.origin_recorded(raw)  # written with a contract, asked without


def test_versao_de_layout_diferente(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    _write_years(_writer(tmp_path), raw, years=(2020,))
    writer = _writer(tmp_path)
    writer.descriptor.layout_version += 1
    assert not writer.origin_recorded(raw)


def test_partes_incompletas(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    writer.max_part_bytes = 200
    writer.write_polars(
        pl.DataFrame({"v": list(range(5000))}), raw, partitions={"ano": 2020}
    )
    parts = sorted((writer.dataset_dir).rglob("*-00*.parquet"))
    assert len(parts) > 1
    assert _writer(tmp_path).origin_recorded(raw)
    parts[0].unlink()  # a gap: -001 missing
    assert not _writer(tmp_path).origin_recorded(raw)


def test_arquivo_agrupado(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "sih-rd_2020-sp.dbc", "sp")
    other = _raw(tmp_path, "sih-rd_2020-ac.dbc", "ac")
    writer = _writer(tmp_path)
    writer.write_group(
        "g_2020",
        [
            GroupMember(raw, pl.DataFrame({"v": [1]}), columns={"uf": "sp"}),
            GroupMember(other, pl.DataFrame({"v": [2]}), columns={"uf": "ac"}),
        ],
        partitions={"ano": 2020},
        converter_version="1",
    )
    fresh = _writer(tmp_path)
    assert fresh.origin_recorded(raw)
    assert fresh.origin_recorded(raw, converter_version="1")
    assert not fresh.origin_recorded(raw, converter_version="2")
    other.write_text("changed")
    assert not fresh.origin_recorded(other)
    assert fresh.origin_recorded(raw)


def test_indice_lido_uma_vez_e_refeito_apos_escrita(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _raw(tmp_path)
    _write_years(_writer(tmp_path), raw)
    writer = _writer(tmp_path)
    calls: list[object] = []
    real = pq.read_metadata

    def counting(path, *args, **kwargs):
        calls.append(path)
        return real(path, *args, **kwargs)

    monkeypatch.setattr(pq, "read_metadata", counting)
    for _ in range(5):
        assert writer.origin_recorded(raw)
    assert not writer.origin_recorded(_raw(tmp_path, "x.csv"))
    assert len(calls) == 3  # one footer per Parquet, once

    writer.write_polars(pl.DataFrame({"v": [9]}), raw, partitions={"ano": 2023})
    calls.clear()
    assert writer.origin_recorded(raw)
    assert writer.origin_recorded(raw)
    assert len(calls) == 4  # rebuilt once after the write


# --- origin_outputs / origin_complete ------------------------------------------------


def _decl(*years: int, key: str = "acidentes") -> list[str]:
    return [output_id({"ano": y}, key) for y in years]


def _write_declared(
    writer: LakeWriter, raw: Path, written, declared, key: str = "acidentes", **kw
) -> None:
    for year in written:
        writer.write_polars(
            pl.DataFrame({"v": [year]}),
            raw,
            partitions={"ano": year},
            file_key_name=key,
            origin_outputs=declared,
            **kw,
        )


def test_output_id() -> None:
    assert output_id({"ano": 2021, "mes": "01"}, "repetro__a") == (
        "ano=2021/mes=01/repetro__a"
    )
    assert output_id({}, "repetro__a") == "repetro__a"
    assert output_id(None, "k") == "k"
    assert output_id({"uf": "SP"}, "k") == output_id({"uf": "SP"}, "k")


def test_output_id_igual_ao_caminho_do_writer(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_declared(writer, raw, [2020], _decl(2020, key="k"), key="k")
    (path,) = writer.dataset_dir.rglob("*.parquet")
    rel = path.relative_to(writer.dataset_dir).with_suffix("").as_posix()
    assert rel == output_id({"ano": 2020}, "k")


def test_conversao_completa(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    declared = _decl(2020, 2021, 2022)
    assert not writer.origin_complete(raw)
    _write_declared(writer, raw, (2020, 2021, 2022), declared)
    assert writer.origin_complete(raw)
    assert _writer(tmp_path).origin_complete(raw)
    assert writer.origin_outputs(raw) == sorted(declared)
    assert _writer(tmp_path).origin_outputs(raw) == sorted(declared)


def test_conversao_parcial(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_declared(writer, raw, (2020, 2021), _decl(2020, 2021, 2022))
    fresh = _writer(tmp_path)
    assert fresh.origin_recorded(raw)  # the old test: blind to the missing year
    assert not fresh.origin_complete(raw)
    _write_declared(fresh, raw, (2022,), _decl(2020, 2021, 2022))
    assert fresh.origin_complete(raw)


def test_lake_antigo_sem_declaracao(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_years(writer, raw)
    assert writer.origin_recorded(raw)
    assert not writer.origin_complete(raw)
    assert writer.origin_outputs(raw) is None


def test_declaracao_em_parte_dos_parquets(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_years(writer, raw, years=(2020,))  # without declaration
    _write_declared(writer, raw, (2021,), _decl(2020, 2021))
    assert not writer.origin_complete(raw)
    assert writer.origin_outputs(raw) is None


def test_sha_mudou(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_declared(writer, raw, (2020, 2021), _decl(2020, 2021))
    assert writer.origin_complete(raw)
    raw.write_text("outro conteudo")
    assert not _writer(tmp_path).origin_complete(raw)


def test_declaracao_divergente(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_declared(writer, raw, (2020,), _decl(2020, 2021))
    _write_declared(writer, raw, (2021,), _decl(2020, 2021, 2022))
    assert not writer.origin_complete(raw)
    assert writer.origin_outputs(raw) is None


def test_declaracao_em_ordem_diferente_e_duplicada_e_igual(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_declared(writer, raw, (2020,), _decl(2021, 2020))
    _write_declared(writer, raw, (2021,), _decl(2020, 2021, 2020))
    assert writer.origin_complete(raw)


def test_force_origin_complete(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    _write_declared(_writer(tmp_path), raw, (2020,), _decl(2020))
    assert _writer(tmp_path).origin_complete(raw)
    assert not _writer(tmp_path, force=True).origin_complete(raw)


def test_member_e_nome(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_declared(writer, raw, (2020,), _decl(2020))
    copy = _raw(tmp_path, "copia.csv", "a")
    assert not writer.origin_complete(copy)
    assert writer.origin_complete(copy, member="acidentes.csv")
    assert writer.origin_outputs(copy, member="acidentes.csv") == _decl(2020)


def test_contrato_e_versoes(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    contract = DataContract(
        dataset_id="acidentes", version=2, fields=[Field("v", pl.Int64)]
    )
    _write_declared(_writer(tmp_path), raw, (2020,), _decl(2020), contract=contract)
    writer = _writer(tmp_path)
    assert writer.origin_complete(raw, contract_version=2)
    assert not writer.origin_complete(raw, contract_version=3)
    assert not writer.origin_complete(raw)


def test_converter_tag(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    _write_declared(
        _writer(tmp_path), raw, (2020,), _decl(2020), converter="anp-fetcher 2.12.0"
    )
    writer = _writer(tmp_path)
    assert writer.origin_complete(raw)
    assert writer.origin_complete(raw, converter="anp-fetcher 2.12.0")
    assert not writer.origin_complete(raw, converter="anp-fetcher 2.13.0")
    assert not writer.origin_complete(raw, converter="anp-fetcher")


def test_saida_declarada_some(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_declared(writer, raw, (2020, 2021), _decl(2020, 2021))
    assert writer.origin_complete(raw)
    (writer.dataset_dir / "ano=2021" / "acidentes.parquet").unlink()
    assert not _writer(tmp_path).origin_complete(raw)


def test_outra_chave_na_mesma_particao_nao_conta(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    declared = [output_id({"ano": 2020}, "a"), output_id({"ano": 2020}, "b")]
    _write_declared(writer, raw, (2020,), declared, key="a")
    assert not writer.origin_complete(raw)
    _write_declared(writer, raw, (2020,), declared, key="b")
    assert writer.origin_complete(raw)


def test_partes_incompletas_com_declaracao(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    writer.max_part_bytes = 200
    writer.write_polars(
        pl.DataFrame({"v": list(range(5000))}),
        raw,
        partitions={"ano": 2020},
        origin_outputs=[output_id({"ano": 2020}, "acidentes")],
    )
    parts = sorted(writer.dataset_dir.rglob("*-00*.parquet"))
    assert len(parts) > 1
    assert _writer(tmp_path).origin_complete(raw)
    parts[0].unlink()
    assert not _writer(tmp_path).origin_complete(raw)


def test_duckdb_declara(tmp_path: Path) -> None:
    csv = _raw(tmp_path, "a.csv", "x;y\n1;2\n")
    writer = LakeWriter(
        tmp_path / "lake" / "s" / "d",
        DatasetDescriptor(source_id="s", dataset_id="d", alias="d"),
    )
    declared = ["a", "b"]
    writer.write_duckdb(csv, file_key_name="a", origin_outputs=declared)
    assert not writer.origin_complete(csv)
    writer.write_duckdb(csv, file_key_name="b", origin_outputs=declared)
    assert writer.origin_complete(csv)
    assert writer.origin_outputs(csv) == ["a", "b"]


def test_rodape_de_arquivo_unico(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_declared(writer, raw, (2021, 2020), _decl(2021, 2020))
    (first, *_) = sorted(writer.dataset_dir.rglob("*.parquet"))
    footer = pq.read_metadata(first).metadata
    assert json.loads(footer[b"quantilica.origin_outputs"]) == sorted(_decl(2020, 2021))


def test_origin_outputs_invalido(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    frame = pl.DataFrame({"v": [1]})
    with pytest.raises(TypeError):
        writer.write_polars(frame, raw, partitions={"ano": 1}, origin_outputs="abc")
    with pytest.raises(ValueError, match="does not list"):
        writer.write_polars(
            frame, raw, partitions={"ano": 1}, origin_outputs=["ano=2/acidentes"]
        )
    with pytest.raises(ValueError):
        writer.write_polars(
            frame,
            raw,
            partitions={"ano": 1},
            origin_outputs=["ano=1/acidentes", ""],
        )
    assert not list(writer.dataset_dir.rglob("*.parquet"))


def test_sem_origin_outputs_nada_muda(tmp_path: Path) -> None:
    """A write that does not pass origin_outputs records nothing new."""
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_years(writer, raw, years=(2020,))
    (path,) = writer.dataset_dir.rglob("*.parquet")
    assert b"quantilica.origin_outputs" not in pq.read_metadata(path).metadata
    # Not declaring still skips a file that was written with a declaration.
    _write_declared(writer, raw, (2021,), _decl(2021))
    result = writer.write_polars(
        pl.DataFrame({"v": [2021]}), raw, partitions={"ano": 2021}
    )
    assert result.status == "skipped"
    assert (
        writer.write_polars(
            pl.DataFrame({"v": [2021]}),
            raw,
            partitions={"ano": 2021},
            origin_outputs=_decl(2021),
        ).status
        == "skipped"
    )


def test_declarar_depois_regrava_arquivo_sem_declaracao(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    writer = _writer(tmp_path)
    _write_years(writer, raw, years=(2020, 2021))
    declared = _decl(2020, 2021)
    assert not writer.origin_complete(raw)
    for year in (2020, 2021):
        assert not writer.is_current(
            raw, partitions={"ano": year}, origin_outputs=declared
        )
        assert writer.is_current(raw, partitions={"ano": year})
    results = [
        writer.write_polars(
            pl.DataFrame({"v": [y]}),
            raw,
            partitions={"ano": y},
            origin_outputs=declared,
        )
        for y in (2020, 2021)
    ]
    assert [r.status for r in results] == ["converted", "converted"]
    assert writer.origin_complete(raw)
    # And now it is current for the declaring caller too.
    assert writer.is_current(raw, partitions={"ano": 2020}, origin_outputs=declared)
    assert not writer.is_current(
        raw, partitions={"ano": 2020}, origin_outputs=_decl(2020, 2021, 2022)
    )


# grouped files


def _group_writer(tmp_path: Path) -> LakeWriter:
    return LakeWriter(
        tmp_path / "lake" / "ipea" / "series",
        DatasetDescriptor(
            source_id="ipea",
            dataset_id="series",
            alias="series",
            partition_keys=["ano"],
            partition_types={"ano": "int"},
        ),
    )


def _gid(year: int) -> str:
    return output_id({"ano": year}, "series")


def test_membro_agrupado_em_dois_grupos(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "serie_x.csv", "v1")
    writer = _group_writer(tmp_path)
    declared = [_gid(2020), _gid(2021)]

    def group(year: int, *, decl=declared) -> None:
        writer.write_group(
            "series",
            [
                GroupMember(
                    raw,
                    pl.DataFrame({"v": [year]}),
                    columns={"serie": "x"},
                    origin_outputs=decl,
                )
            ],
            partitions={"ano": year},
        )

    group(2020)
    assert _group_writer(tmp_path).origin_recorded(raw)
    assert not _group_writer(tmp_path).origin_complete(raw)  # 2021 missing
    group(2021)
    fresh = _group_writer(tmp_path)
    assert fresh.origin_complete(raw)
    assert fresh.origin_outputs(raw) == declared

    # The series is revised and gets a new year; the write of the new group fails.
    raw.write_text("v2")
    declared3 = [_gid(2020), _gid(2021), _gid(2022)]
    group(2020, decl=declared3)
    group(2021, decl=declared3)
    assert not _group_writer(tmp_path).origin_complete(raw)
    group(2022, decl=declared3)
    assert _group_writer(tmp_path).origin_complete(raw)


def test_membro_agrupado_entrada_guarda_outputs(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "serie_x.csv", "v1")
    writer = _group_writer(tmp_path)
    writer.write_group(
        "series",
        [GroupMember(raw, pl.DataFrame({"v": [1]}), origin_outputs=[_gid(2020)])],
        partitions={"ano": 2020},
    )
    (path,) = writer.dataset_dir.rglob("*.parquet")
    members = json.loads(pq.read_metadata(path).metadata[b"quantilica.members"])
    assert members["serie_x"]["outputs"] == [_gid(2020)]


def test_membro_agrupado_sem_declaracao_e_regravado_ao_declarar(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "serie_x.csv", "v1")
    writer = _group_writer(tmp_path)

    def run(**kw):
        return writer.write_group(
            "series",
            [GroupMember(raw, pl.DataFrame({"v": [1]}), **kw)],
            partitions={"ano": 2020},
        )

    assert run().status == "converted"
    assert not writer.origin_complete(raw)
    assert run().status == "skipped"
    declared = [_gid(2020)]
    assert not writer.is_current_member(
        raw,
        group_key="series",
        partitions={"ano": 2020},
        origin_outputs=declared,
    )
    assert run(origin_outputs=declared).status == "converted"
    assert writer.origin_complete(raw)
    assert run(origin_outputs=declared).status == "skipped"
    assert writer.is_current_member(
        raw, group_key="series", partitions={"ano": 2020}, origin_outputs=declared
    )


def test_write_group_exige_o_proprio_grupo_na_declaracao(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "serie_x.csv", "v1")
    writer = _group_writer(tmp_path)
    with pytest.raises(ValueError, match="does not list"):
        writer.write_group(
            "series",
            [GroupMember(raw, pl.DataFrame({"v": [1]}), origin_outputs=[_gid(2021)])],
            partitions={"ano": 2020},
        )


def test_write_member_declara(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "serie_x.csv", "v1")
    writer = _group_writer(tmp_path)
    declared = [_gid(2020), _gid(2021)]
    writer.write_member(
        pl.DataFrame({"v": [1]}),
        raw,
        group_key="series",
        partitions={"ano": 2020},
        origin_outputs=declared,
    )
    assert not writer.origin_complete(raw)
    writer.write_member(
        pl.DataFrame({"v": [2]}),
        raw,
        group_key="series",
        partitions={"ano": 2021},
        origin_outputs=declared,
    )
    assert _group_writer(tmp_path).origin_complete(raw)


def test_grupo_com_membros_de_declaracoes_diferentes(tmp_path: Path) -> None:
    a = _raw(tmp_path, "a.csv", "a")
    b = _raw(tmp_path, "b.csv", "b")
    writer = _group_writer(tmp_path)
    writer.write_group(
        "series",
        [
            GroupMember(a, pl.DataFrame({"v": [1]}), origin_outputs=[_gid(2020)]),
            GroupMember(
                b, pl.DataFrame({"v": [2]}), origin_outputs=[_gid(2020), _gid(9)]
            ),
        ],
        partitions={"ano": 2020},
    )
    fresh = _group_writer(tmp_path)
    assert fresh.origin_complete(a)  # its declaration is only this group
    assert not fresh.origin_complete(b)


def test_entrada_que_nao_e_dict_e_ignorada(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "serie_x.csv", "v1")
    writer = _group_writer(tmp_path)
    writer.write_group(
        "series",
        [GroupMember(raw, pl.DataFrame({"v": [1]}), origin_outputs=[_gid(2020)])],
        partitions={"ano": 2020},
    )
    (path,) = writer.dataset_dir.rglob("*.parquet")
    table = pq.read_table(path)
    members = json.loads(table.schema.metadata[b"quantilica.members"])
    members["lixo"] = "texto"
    members["lixo2"] = None
    members["lixo3"] = ["x"]
    meta = dict(table.schema.metadata)
    meta[b"quantilica.members"] = json.dumps(members).encode()
    pq.write_table(table.replace_schema_metadata(meta), path)
    fresh = _group_writer(tmp_path)
    assert fresh.origin_complete(raw)  # no AttributeError
    # A footer whose members are not a mapping at all is ignored too.
    meta[b"quantilica.members"] = b'["a", "b"]'
    pq.write_table(table.replace_schema_metadata(meta), path)
    assert not _group_writer(tmp_path).origin_recorded(raw)
    meta[b"quantilica.members"] = b"not json"
    pq.write_table(table.replace_schema_metadata(meta), path)
    assert not _group_writer(tmp_path).origin_recorded(raw)


def test_coluna_de_membro_nao_pode_chamar_outputs(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "serie_x.csv", "v1")
    with pytest.raises(ValueError, match="clash"):
        _group_writer(tmp_path).write_group(
            "series",
            [GroupMember(raw, pl.DataFrame({"v": [1]}), columns={"outputs": "x"})],
            partitions={"ano": 2020},
        )


# sidecar and cache


def test_sidecar_confiavel_nao_le_o_bruto(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from quantilica.core import manifests

    raw = _raw(tmp_path, content="conteudo " * 1000)
    digest = hashlib.sha256(raw.read_bytes()).hexdigest()
    sidecar = raw.with_name(raw.name + ".manifest.json")
    sidecar.write_text(json.dumps({"sha256": digest, "size_bytes": raw.stat().st_size}))
    now = time.time()
    os.utime(raw, (now - 100, now - 100))
    os.utime(sidecar, (now, now))
    writer = _writer(tmp_path)
    _write_declared(writer, raw, (2020, 2021), _decl(2020, 2021))

    reads: list[object] = []
    real_open = Path.open

    def spy_open(self, *args, **kwargs):
        if self == raw:
            reads.append(self)
        return real_open(self, *args, **kwargs)

    def boom(path, *args, **kwargs):
        reads.append(path)
        raise AssertionError("o bruto foi lido")

    monkeypatch.setattr(Path, "open", spy_open)
    monkeypatch.setattr(manifests, "sha256_file", boom)
    fresh = _writer(tmp_path)
    assert fresh.origin_complete(raw)
    assert fresh.origin_outputs(raw) == sorted(_decl(2020, 2021))
    assert not reads


def test_cache_entre_instancias(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    reader = _writer(tmp_path)
    other = _writer(tmp_path)
    declared = _decl(2020, 2021)
    assert not reader.origin_complete(raw)  # builds an (empty) index
    _write_declared(other, raw, (2020,), declared)
    assert reader.origin_recorded(raw)  # sees what another instance wrote
    assert not reader.origin_complete(raw)
    _write_declared(other, raw, (2021,), declared)
    assert reader.origin_complete(raw)
    # ...and what it removes or rewrites.
    (other.dataset_dir / "ano=2021" / "acidentes.parquet").unlink()
    assert not reader.origin_complete(raw)
    raw.write_text("novo")
    _write_declared(other, raw, (2020, 2021), declared)
    assert reader.origin_complete(raw)
    assert reader.origin_outputs(raw) == sorted(declared)


def test_cache_nao_le_rodape_sem_mudanca(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _raw(tmp_path)
    _write_declared(_writer(tmp_path), raw, (2020, 2021), _decl(2020, 2021))
    writer = _writer(tmp_path)
    calls: list[object] = []
    real = pq.read_metadata

    def counting(path, *args, **kwargs):
        calls.append(path)
        return real(path, *args, **kwargs)

    monkeypatch.setattr(pq, "read_metadata", counting)
    for _ in range(5):
        assert writer.origin_complete(raw)
        assert writer.origin_outputs(raw)
    assert len(calls) == 2
    # Another instance adds a file: only that footer is read again.
    _write_declared(_writer(tmp_path), raw, (2022,), _decl(2020, 2021, 2022))
    calls.clear()
    assert not writer.origin_complete(raw)
    assert len(calls) == 1


def test_colisao_de_basename_nunca_da_pulo_falso(tmp_path: Path) -> None:
    """Two raw files named alike in different folders are one origin by name:
    the answer is False for both once their digests differ, never True."""
    a = _raw(tmp_path, "x/dados.zip", "aaa")
    b = _raw(tmp_path, "y/dados.zip", "bbb")
    writer = _writer(tmp_path)
    _write_declared(writer, a, (2020,), _decl(2020))
    assert writer.origin_complete(a)
    assert not writer.origin_complete(b)  # other content, same name
    _write_declared(writer, b, (2021,), _decl(2021))
    assert not writer.origin_complete(a)
    assert not writer.origin_complete(b)
    # The way out is a distinguishing member name.
    assert not writer.origin_complete(a, member="x/dados.zip")
