"""Shared test setup."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _no_global_scratch_setup(monkeypatch: pytest.MonkeyPatch):
    """Keep ``configure_scratch`` (once per process) from leaking into other tests.

    Tests of it reset the flag themselves.
    """
    import quantilica.analytics.lake as lake_mod

    monkeypatch.setattr(lake_mod, "_scratch_configured", True)
