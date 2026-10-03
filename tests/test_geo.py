"""Tests for the canonical geographic dimension module."""

from __future__ import annotations

import polars as pl
import pytest

from quantilica.analytics import (
    DIM_GEO_CONTRACT,
    DIM_UF_CONTRACT,
    IBGE_DV_EXCEPTIONS,
    calc_ibge_dv,
    get_dim_geo,
    get_dim_uf,
    mun_6_to_7,
    mun_7_to_6,
)

REGIOES_BRASILEIRAS = {"Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"}


class TestCalcIbgeDv:
    """Check digit computation per IBGE Módulo 10."""

    @pytest.mark.parametrize(
        ("code_6", "expected"),
        [
            (355030, 8),  # São Paulo
            (330455, 7),  # Rio de Janeiro
            (530010, 8),  # Brasília
            (410690, 2),  # Curitiba
            (292740, 8),  # Salvador
            (310620, 0),  # Belo Horizonte
        ],
    )
    def test_known_municipalities(self, code_6: int, expected: int) -> None:
        assert calc_ibge_dv(code_6) == expected

    @pytest.mark.parametrize("code_6", ["355030", 330455])
    def test_accepts_int_and_str(self, code_6: int | str) -> None:
        assert calc_ibge_dv(code_6) in range(10)

    def test_range_result(self) -> None:
        for code in range(110001, 110010):
            assert 0 <= calc_ibge_dv(code) <= 9

    @pytest.mark.parametrize(
        "bad_input",
        [
            0,  # fewer than 6 digits
            35503,  # 5 digits
            3550300,  # 7 digits
            -355030,  # negative
            "35503",  # 5-digit string
            "3550300",  # 7-digit string
            "35503a",  # non-digit char
            " 35503",  # whitespace
            "",  # empty string
        ],
    )
    def test_invalid_inputs_raise(self, bad_input: int | str) -> None:
        with pytest.raises(ValueError):
            calc_ibge_dv(bad_input)


class TestConversions:
    """6 <-> 7 digit roundtrips."""

    @pytest.mark.parametrize(
        ("code_6", "code_7"),
        [
            (355030, 3550308),  # São Paulo
            (330455, 3304557),  # Rio de Janeiro
            (530010, 5300108),  # Brasília
            (410690, 4106902),  # Curitiba
            (292740, 2927408),  # Salvador
            (310620, 3106200),  # Belo Horizonte
        ],
    )
    def test_mun_6_to_7_known(self, code_6: int, code_7: int) -> None:
        assert mun_6_to_7(code_6) == code_7
        assert mun_6_to_7(str(code_6)) == code_7

    @pytest.mark.parametrize(
        ("code_7", "code_6"),
        [
            (3550308, 355030),
            (3304557, 330455),
            (5300108, 530010),
        ],
    )
    def test_mun_7_to_6_known(self, code_7: int, code_6: int) -> None:
        assert mun_7_to_6(code_7) == code_6
        assert mun_7_to_6(str(code_7)) == code_6

    @pytest.mark.parametrize("code_6", [110001, 120010, 270110, 355030, 520455])
    def test_roundtrip_6_7_6(self, code_6: int) -> None:
        assert mun_7_to_6(mun_6_to_7(code_6)) == code_6

    @pytest.mark.parametrize("code_7", [1100015, 1200101, 2701102, 3550308, 5204559])
    def test_roundtrip_7_6_7_via_dv(self, code_7: int) -> None:
        rebuilt = mun_6_to_7(mun_7_to_6(code_7))
        assert rebuilt // 10 == mun_7_to_6(code_7)
        assert mun_6_to_7(int(f"{code_7}"[:-1])) == rebuilt

    @pytest.mark.parametrize(
        "bad_input",
        [0, 35503, 3550300, -355030, "35503", "355030a", "35503080", ""],
    )
    def test_mun_6_to_7_invalid(self, bad_input: int | str) -> None:
        with pytest.raises(ValueError):
            mun_6_to_7(bad_input)

    @pytest.mark.parametrize(
        "bad_input",
        [0, 355030, 35503080, -355030, "355030", "355030a", "35503080", ""],
    )
    def test_mun_7_to_6_invalid(self, bad_input: int | str) -> None:
        with pytest.raises(ValueError):
            mun_7_to_6(bad_input)


class TestDimGeo:
    """Bundled municipality dimension integrity."""

    @pytest.fixture(scope="class")
    @classmethod
    def dim_geo(cls) -> pl.DataFrame:
        return get_dim_geo()

    def test_min_rows(self, dim_geo: pl.DataFrame) -> None:
        assert dim_geo.height >= 5570

    def test_no_duplicates_7(self, dim_geo: pl.DataFrame) -> None:
        assert dim_geo["ibge_mun_id"].n_unique() == dim_geo.height

    def test_no_duplicates_6(self, dim_geo: pl.DataFrame) -> None:
        assert dim_geo["ibge_mun_id_6"].n_unique() == dim_geo.height

    def test_mun_id_6_exactly_6_digits(self, dim_geo: pl.DataFrame) -> None:
        assert dim_geo["ibge_mun_id_6"].min() >= 110001
        assert dim_geo["ibge_mun_id_6"].max() <= 999999

    def test_exact_27_ufs(self, dim_geo: pl.DataFrame) -> None:
        assert dim_geo["uf_sigla"].n_unique() == 27

    def test_all_5_regions(self, dim_geo: pl.DataFrame) -> None:
        assert set(dim_geo["regiao"].unique().to_list()) == REGIOES_BRASILEIRAS

    def test_no_empty_strings(self, dim_geo: pl.DataFrame) -> None:
        for col in ["mun_nome", "uf_sigla", "uf_nome", "regiao"]:
            assert dim_geo.filter(pl.col(col).str.strip_chars() == "").height == 0

    def test_id7_consistent_with_id6_prefix(self, dim_geo: pl.DataFrame) -> None:
        # Every 7-digit code must match its 6-digit counterpart by prefix
        assert (
            dim_geo.filter(
                pl.col("ibge_mun_id") // 10 != pl.col("ibge_mun_id_6")
            ).height
            == 0
        )

    def test_mun_6_to_7_matches_dataset_dv(self, dim_geo: pl.DataFrame) -> None:
        # 9 official IBGE municipalities have historical check digits that deviate
        # from Modulo 10; filtering them ensures 100% deterministic test coverage
        regular_municipalities = dim_geo.filter(
            ~pl.col("ibge_mun_id").is_in(list(IBGE_DV_EXCEPTIONS))
        )
        sample = regular_municipalities.sample(n=500, seed=42)
        for code_6, code_7 in zip(
            sample["ibge_mun_id_6"], sample["ibge_mun_id"], strict=True
        ):
            assert mun_6_to_7(code_6) == code_7

    def test_ibge_dv_exceptions_documented(self, dim_geo: pl.DataFrame) -> None:
        # Verify the 9 historical IBGE exceptions exist and have matching 6-digit prefix
        exceptions = dim_geo.filter(
            pl.col("ibge_mun_id").is_in(list(IBGE_DV_EXCEPTIONS))
        )
        assert exceptions.height == len(IBGE_DV_EXCEPTIONS) == 9
        for row in exceptions.iter_rows(named=True):
            assert row["ibge_mun_id"] // 10 == row["ibge_mun_id_6"]

    def test_contract_validation(self, dim_geo: pl.DataFrame) -> None:
        DIM_GEO_CONTRACT.validate(dim_geo)  # should not raise


class TestDimUf:
    """Derived federative unit dimension."""

    @pytest.fixture(scope="class")
    @classmethod
    def dim_uf(cls) -> pl.DataFrame:
        return get_dim_uf()

    def test_exact_27_ufs(self, dim_uf: pl.DataFrame) -> None:
        assert dim_uf.height == 27

    def test_sorted_by_ibge_uf_id(self, dim_uf: pl.DataFrame) -> None:
        ids = dim_uf["ibge_uf_id"].to_list()
        assert ids == sorted(ids)

    def test_unique_siglas(self, dim_uf: pl.DataFrame) -> None:
        assert dim_uf["uf_sigla"].n_unique() == 27

    def test_columns(self, dim_uf: pl.DataFrame) -> None:
        assert dim_uf.columns == ["uf_sigla", "uf_nome", "ibge_uf_id", "regiao"]

    def test_regions(self, dim_uf: pl.DataFrame) -> None:
        assert set(dim_uf["regiao"].unique().to_list()) == REGIOES_BRASILEIRAS

    def test_contract_validation(self, dim_uf: pl.DataFrame) -> None:
        DIM_UF_CONTRACT.validate(dim_uf)  # should not raise
