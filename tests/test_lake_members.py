"""Grouped files: one Parquet per partition with the rows of several raw files."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from quantilica.core.lake import DatasetDescriptor

from quantilica.analytics.lake import (
    META_MEMBERS,
    ORIGIN_COLUMN,
    LakeError,
    LakeWriter,
    check_dataset,
    compact_members,
    scan_dataset,
)


def _raw(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / "raw" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _writer(lake: Path, keys: list[str]) -> LakeWriter:
    return LakeWriter(
        lake / "datasus" / "sih-rd",
        DatasetDescriptor(
            source_id="datasus",
            dataset_id="sih-rd",
            alias="sih-rd",
            partition_keys=keys,
            partition_types={k: "varchar" for k in keys},
        ),
    )


def _members(path: Path) -> dict:
    raw = pq.ParquetFile(path).metadata.metadata
    return json.loads(raw[META_MEMBERS.encode()])


def test_write_member_junta_as_ufs_num_arquivo(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    writer = _writer(lake, ["ano", "mes"])
    parts = {"ano": "2020", "mes": "01"}
    sp = _raw(tmp_path, "sih-rd_202001-sp@20200301.dbc", "sp")
    ac = _raw(tmp_path, "sih-rd_202001-ac@20200301.dbc", "ac")
    for src, uf, rows in ((sp, "sp", 3), (ac, "ac", 2)):
        res = writer.write_member(
            pl.DataFrame({"valor": list(range(rows))}),
            src,
            group_key="sih-rd_202001",
            partitions=parts,
            member_columns={"uf": uf},
        )
        assert res.status == "converted"

    dataset = lake / "datasus" / "sih-rd"
    files = list(dataset.rglob("*.parquet"))
    assert [f.relative_to(dataset).as_posix() for f in files] == [
        "ano=2020/mes=01/sih-rd_202001.parquet"
    ]
    df = scan_dataset(dataset).collect()
    assert df["uf"].to_list() == ["ac"] * 2 + ["sp"] * 3  # ordenado por uf
    assert set(df[ORIGIN_COLUMN]) == {
        "sih-rd_202001-sp",
        "sih-rd_202001-ac",
    }  # sem a data
    members = _members(files[0])
    assert members["sih-rd_202001-sp"]["rows"] == 3
    assert members["sih-rd_202001-ac"]["uf"] == "ac"
    assert check_dataset(dataset) == []

    # Mesmo sha256: pula sem ler.
    assert writer.is_current_member(sp, group_key="sih-rd_202001", partitions=parts)
    again = writer.write_member(
        pl.DataFrame({"valor": [9]}),
        sp,
        group_key="sih-rd_202001",
        partitions=parts,
        member_columns={"uf": "sp"},
    )
    assert again.status == "skipped"

    # Nova versão de SP troca só as linhas de SP.
    sp2 = _raw(tmp_path, "sih-rd_202001-sp@20200401.dbc", "sp v2")
    assert not writer.is_current_member(
        sp2, group_key="sih-rd_202001", partitions=parts
    )
    writer.write_member(
        pl.DataFrame({"valor": [7]}),
        sp2,
        group_key="sih-rd_202001",
        partitions=parts,
        member_columns={"uf": "sp"},
    )
    df = scan_dataset(dataset).collect()
    assert df.filter(pl.col("uf") == "sp")["valor"].to_list() == [7]
    assert df.filter(pl.col("uf") == "ac").height == 2
    members = _members(files[0])
    assert members["sih-rd_202001-sp"]["origin_file"] == sp2.name


def test_write_member_recusa_arquivo_que_nao_e_do_grupo(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    plain = _writer(lake, ["ano"])
    src = _raw(tmp_path, "sih-rd_2020@20200301.dbc", "x")
    plain.write_polars(
        pl.DataFrame({"valor": [1]}),
        src,
        partitions={"ano": "2020"},
        file_key_name="sih-rd_2020",
    )
    other = _raw(tmp_path, "sih-rd_2020-sp@20200301.dbc", "y")
    with pytest.raises(LakeError, match="not the grouped file"):
        plain.write_member(
            pl.DataFrame({"valor": [2]}),
            other,
            group_key="sih-rd_2020",
            partitions={"ano": "2020"},
            member_columns={"uf": "sp"},
        )


def test_compact_members_migra_o_layout_por_uf(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    writer = _writer(lake, ["uf", "ano", "mes"])
    expected = 0
    for uf, mes, rows in (("sp", "01", 3), ("ac", "01", 2), ("sp", "02", 4)):
        src = _raw(tmp_path, f"sih-rd_2020{mes}-{uf}@20200301.dbc", uf + mes)
        writer.write_polars(
            pl.DataFrame({"valor": list(range(rows))}),
            src,
            partitions={"uf": uf, "ano": "2020", "mes": mes},
            file_key_name=src.name,  # como as chaves antigas, com a data
        )
        expected += rows
    dataset = lake / "datasus" / "sih-rd"
    before = scan_dataset(dataset).collect()
    # Versão anterior do AC que ficou no lake ao lado da atual: é descartada.
    old = _raw(tmp_path, "sih-rd_202001-ac@20200201.dbc", "ac antigo")
    writer.write_polars(
        pl.DataFrame({"valor": [100, 101, 102, 103]}),
        old,
        partitions={"uf": "ac", "ano": "2020", "mes": "01"},
        file_key_name=old.name,
    )

    counts = compact_members(dataset, workers=2)
    assert counts == {"groups": 2, "files": 4}
    assert sorted(
        f.relative_to(dataset).as_posix() for f in dataset.rglob("*.parquet")
    ) == [
        "ano=2020/mes=01/sih-rd_202001.parquet",
        "ano=2020/mes=02/sih-rd_202002.parquet",
    ]
    assert not list(dataset.glob("uf=*"))
    assert DatasetDescriptor.load(dataset).partition_keys == ["ano", "mes"]
    assert check_dataset(dataset) == []

    after = scan_dataset(dataset).collect()
    assert after.height == expected
    jan = after.filter(pl.col("mes") == "01")
    assert jan["uf"].to_list() == sorted(jan["uf"].to_list())  # ordenado sem ORDER BY
    cols = ["uf", "ano", "mes", "valor"]
    assert after.select(cols).sort(cols).equals(before.select(cols).sort(cols))
    members = _members(dataset / "ano=2020" / "mes=01" / "sih-rd_202001.parquet")
    assert members["sih-rd_202001-ac"]["rows"] == 2
    assert members["sih-rd_202001-sp"]["origin_sha256"]

    # Depois da migração, o mesmo bruto é reconhecido como membro atual.
    grouped = _writer(lake, ["ano", "mes"])
    src = tmp_path / "raw" / "sih-rd_202001-sp@20200301.dbc"
    assert grouped.is_current_member(
        src, group_key="sih-rd_202001", partitions={"ano": "2020", "mes": "01"}
    )
    assert compact_members(dataset) == {"groups": 0, "files": 0}


def test_esquema_que_muda_entre_periodos(tmp_path: Path) -> None:
    # Coluna nova a partir de 2021 e tipo que muda: Polars lê como o DuckDB.
    lake = tmp_path / "lake"
    writer = _writer(lake, ["ano"])
    old = _raw(tmp_path, "sih-rd_2020-sp@20210101.dbc", "a")
    new = _raw(tmp_path, "sih-rd_2021-sp@20220101.dbc", "b")
    writer.write_member(
        pl.DataFrame({"valor": [1, 2], "codigo": [10, 11]}),
        old,
        group_key="sih-rd_2020",
        partitions={"ano": "2020"},
        member_columns={"uf": "sp"},
    )
    writer.write_member(
        pl.DataFrame({"valor": [3], "codigo": ["A1"], "nat_jur": ["2062"]}),
        new,
        group_key="sih-rd_2021",
        partitions={"ano": "2021"},
        member_columns={"uf": "sp"},
    )
    dataset = lake / "datasus" / "sih-rd"
    assert check_dataset(dataset) == []
    df = scan_dataset(dataset).collect().sort("valor")
    assert df["valor"].to_list() == [1, 2, 3]
    assert df["nat_jur"].to_list() == [None, None, "2062"]
    assert df["codigo"].to_list() == ["10", "11", "A1"]
    assert df["ano"].to_list() == ["2020", "2020", "2021"]
