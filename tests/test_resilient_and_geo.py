"""Tests for resilient CSV reader, EraSchemaResolver, and GeoJSON Lake helpers."""

import json
from pathlib import Path

import polars as pl

from quantilica.analytics import (
    BBOX_COLUMNS,
    GEOMETRY_WKB_COLUMN,
    EraSchemaResolver,
    extract_wkb_and_bbox,
    geometry_to_wkb,
    read_csv_resilient,
)


def test_read_csv_resilient_encoding_and_null_bytes(tmp_path: Path):
    # CSV encoded in latin-1 with null byte and leading zero code
    raw_content = (
        "codigo;municipio;valor\x00\n0123;São Paulo;100,50\n0456;Brasília;200,00\n"
    ).encode("latin-1")
    csv_file = tmp_path / "test.csv"
    csv_file.write_bytes(raw_content)

    df = read_csv_resilient(csv_file, separator=";", code_columns=["codigo"])
    assert df.shape == (2, 3)
    assert df.columns == ["codigo", "municipio", "valor"]
    assert df["codigo"].to_list() == ["0123", "0456"]
    assert df["municipio"].to_list() == ["São Paulo", "Brasília"]


def test_era_schema_resolver():
    canonical = {
        "ano": pl.Int64,
        "mes": pl.Int64,
        "cd_mun": pl.String,
        "valor": pl.Float64,
        "status": pl.String,
    }
    era_mappings = {
        "2010_2019": {
            "AN": "ano",
            "MES_REF": "mes",
            "COD_MUN": "cd_mun",
            "VL": "valor",
        },
        "2020_present": {
            "ano_coleta": "ano",
            "mes_coleta": "mes",
            "id_municipio": "cd_mun",
            "valor_total": "valor",
        },
    }
    defaults = {
        "status": "ATIVO",
    }

    resolver = EraSchemaResolver(
        canonical_schema=canonical,
        era_mappings=era_mappings,
        default_values=defaults,
    )

    # Era 2010_2019 data
    df_old = pl.DataFrame(
        {
            "AN": ["2015", "2016"],
            "MES_REF": [1, 2],
            "COD_MUN": ["3550308", "3304557"],
            "VL": ["10.5", "20.0"],
        }
    )
    resolved_old = resolver.resolve(df_old, era="2010_2019")
    assert resolved_old.columns == ["ano", "mes", "cd_mun", "valor", "status"]
    assert resolved_old["ano"].to_list() == [2015, 2016]
    assert resolved_old["status"].to_list() == ["ATIVO", "ATIVO"]

    # Era 2020_present data
    df_new = pl.DataFrame(
        {
            "ano_coleta": [2021],
            "mes_coleta": [6],
            "id_municipio": ["3550308"],
            "valor_total": [55.2],
            "status": ["INATIVO"],
        }
    )
    resolved_new = resolver.resolve(df_new, era="2020_present")
    assert resolved_new["valor"].to_list() == [55.2]
    assert resolved_new["status"].to_list() == ["INATIVO"]


def test_geometry_to_wkb_and_bbox():
    pt = {"type": "Point", "coordinates": [-46.6333, -23.5505]}
    wkb = geometry_to_wkb(pt)
    assert isinstance(wkb, bytes)
    assert len(wkb) == 21  # 1 byte byte-order + 4 bytes type + 8 bytes X + 8 bytes Y

    poly = {
        "type": "Polygon",
        "coordinates": [
            [
                [-46.0, -23.0],
                [-46.0, -24.0],
                [-47.0, -24.0],
                [-47.0, -23.0],
                [-46.0, -23.0],
            ]
        ],
    }
    wkb_poly = geometry_to_wkb(poly)
    assert isinstance(wkb_poly, bytes)

    # Invalid / empty
    assert geometry_to_wkb(None) is None
    assert geometry_to_wkb({"type": "Unknown"}) is None


def test_extract_wkb_and_bbox_from_geojson(tmp_path: Path):
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": 1, "nome": "Ponto A"},
                "geometry": {"type": "Point", "coordinates": [-43.2, -22.9]},
            },
            {
                "type": "Feature",
                "properties": {"id": 2, "nome": "Linha B"},
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[-43.0, -22.0], [-44.0, -23.0]],
                },
            },
        ],
    }
    json_path = tmp_path / "geo.geojson"
    json_path.write_text(json.dumps(geojson_data), encoding="utf-8")

    df = extract_wkb_and_bbox(json_path)
    # id, nome, geom_wkb, bbox_minx, bbox_miny, bbox_maxx, bbox_maxy
    assert df.shape == (2, 7)
    assert GEOMETRY_WKB_COLUMN in df.columns
    for b in BBOX_COLUMNS:
        assert b in df.columns

    row0 = df.filter(pl.col("id") == 1).to_dicts()[0]
    assert row0["bbox_minx"] == -43.2
    assert row0["bbox_maxx"] == -43.2
    assert row0["bbox_miny"] == -22.9
    assert row0["bbox_maxy"] == -22.9
