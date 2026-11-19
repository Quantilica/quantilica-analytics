"""QUANTILICA_DUCKDB_MEMORY/THREADS are validated before reaching DuckDB."""

from __future__ import annotations

from pathlib import Path

import pytest

from quantilica.analytics.lake import LakeError, _connect, duckdb_env_settings


def _setting(conn, name: str) -> str:
    return str(conn.execute(f"SELECT current_setting('{name}')").fetchone()[0])


def test_no_variables_keeps_duckdb_defaults(tmp_path: Path, monkeypatch):
    import duckdb

    monkeypatch.delenv("QUANTILICA_DUCKDB_MEMORY", raising=False)
    monkeypatch.delenv("QUANTILICA_DUCKDB_THREADS", raising=False)
    default = duckdb.connect()
    conn = _connect(tmp_path)
    assert _setting(conn, "memory_limit") == _setting(default, "memory_limit")
    assert _setting(conn, "threads") == _setting(default, "threads")


def test_blank_variables_are_the_same_as_missing(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("QUANTILICA_DUCKDB_MEMORY", "  ")
    monkeypatch.setenv("QUANTILICA_DUCKDB_THREADS", "")
    assert duckdb_env_settings() == (None, None)
    _connect(tmp_path)  # does not raise


def test_memory_and_threads_reach_the_connection(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("QUANTILICA_DUCKDB_MEMORY", "2GB")
    monkeypatch.setenv("QUANTILICA_DUCKDB_THREADS", "3")
    conn = _connect(tmp_path)
    # DuckDB normalizes the unit (e.g. '1.8 GiB')
    assert _setting(conn, "memory_limit").startswith(("1.8", "2.0", "1.9"))
    assert _setting(conn, "threads") == "3"


@pytest.mark.parametrize("value", ["64GB", "512MiB", "1.5 GB", "80%", "2tb"])
def test_valid_memory(value: str):
    assert duckdb_env_settings({"QUANTILICA_DUCKDB_MEMORY": value}) == (value, None)


@pytest.mark.parametrize(
    "value", ["64", "muito", "GB", "-4GB", "64 giga", "1;DROP", "1GB'; --", "٣GB"]
)
def test_invalid_memory_raises_lakeerror(value: str):
    with pytest.raises(LakeError, match="QUANTILICA_DUCKDB_MEMORY"):
        duckdb_env_settings({"QUANTILICA_DUCKDB_MEMORY": value})


@pytest.mark.parametrize("value", ["0", "-2", "abc", "2.5", "4 8", "²", "٣"])
def test_invalid_threads_raises_lakeerror(value: str):
    with pytest.raises(LakeError, match="QUANTILICA_DUCKDB_THREADS"):
        duckdb_env_settings({"QUANTILICA_DUCKDB_THREADS": value})


def test_threads_parsed():
    assert duckdb_env_settings({"QUANTILICA_DUCKDB_THREADS": " 4 "}) == (None, 4)


def test_connect_propagates_validation_error(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("QUANTILICA_DUCKDB_THREADS", "zero")
    with pytest.raises(LakeError, match="QUANTILICA_DUCKDB_THREADS"):
        _connect(tmp_path)
    monkeypatch.delenv("QUANTILICA_DUCKDB_THREADS")
    monkeypatch.setenv("QUANTILICA_DUCKDB_MEMORY", "x'; DROP")
    with pytest.raises(LakeError, match="QUANTILICA_DUCKDB_MEMORY"):
        _connect(tmp_path)
