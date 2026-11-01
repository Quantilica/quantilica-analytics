"""Unit tests for quantilica.analytics.fixed_width."""

from pathlib import Path

import duckdb

from quantilica.analytics.fixed_width import (
    FixedField,
    fixed_width_sql,
    layout_from_datasus,
    layout_from_sas_input,
)


def test_layout_from_datasus():
    sample = """
    Coluna,Tamanho,Inicio,Fim,Tipo
    CO_PROCEDIMENTO,10,1,10,CHAR
    NO_PROCEDIMENTO,250,11,260,CHAR
    TP_COMPLEXIDADE,1,261,261,CHAR
    VL_SH,12,262,273,NUM
    QT_TEMPO_PERMANENCIA,4,274,277,INT
    """
    fields = layout_from_datasus(sample)
    assert len(fields) == 5
    assert fields[0] == FixedField(
        name="co_procedimento", start=1, width=10, kind="str", scale=0
    )
    assert fields[1] == FixedField(
        name="no_procedimento", start=11, width=250, kind="str", scale=0
    )
    assert fields[3] == FixedField(
        name="vl_sh", start=262, width=12, kind="decimal", scale=2
    )
    assert fields[4] == FixedField(
        name="qt_tempo_permanencia", start=274, width=4, kind="int", scale=0
    )


def test_layout_from_sas_input():
    sample = """
    INPUT
      @1 V0001 $2.
      @3 V0002 4.
      @7 V0101 6.2
    ;
    """
    fields = layout_from_sas_input(sample)
    assert len(fields) == 3
    assert fields[0] == FixedField(name="v0001", start=1, width=2, kind="str", scale=0)
    assert fields[1] == FixedField(name="v0002", start=3, width=4, kind="int", scale=0)
    assert fields[2] == FixedField(
        name="v0101", start=7, width=6, kind="decimal", scale=2
    )


def test_fixed_width_sql_execution(tmp_path: Path):
    txt_file = tmp_path / "data.txt"
    # Format: id(4), desc(10), value in cents(6)
    # Line 1: '0001Alpha     001050' -> id=1, desc='Alpha', val=10.50
    content = "0001Alpha     001050\n0002Beta      000099\n"
    txt_file.write_text(content, encoding="latin-1")

    layout = [
        FixedField(name="id", start=1, width=4, kind="int"),
        FixedField(name="desc", start=5, width=10, kind="str"),
        FixedField(name="valor", start=15, width=6, kind="decimal", scale=2),
    ]

    sql = fixed_width_sql(str(txt_file), layout, encoding="latin-1")
    conn = duckdb.connect()
    rows = conn.execute(sql).fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0] == (1, "Alpha", 10.50)
    assert rows[1] == (2, "Beta", 0.99)
