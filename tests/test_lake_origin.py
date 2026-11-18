"""``LakeWriter.origin_recorded``: is a raw file in the lake, whatever its partition."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from quantilica.core.lake import DatasetDescriptor

from quantilica.analytics import DataContract, Field, LakeWriter
from quantilica.analytics.lake import GroupMember


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
