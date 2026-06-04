"""Analytical data processing and conversion layer for Quantilica."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("quantilica-analytics")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["__version__"]
